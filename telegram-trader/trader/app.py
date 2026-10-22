import asyncio
import fcntl
import hmac
import logging
import os
import time
from pathlib import Path

import aiohttp
from aiohttp import web
from telethon import TelegramClient, events, utils

from .channel import ChannelReader, describe_message
from .engine import Engine, Review
from .gate import Gate, GateError
from .models import Decisions, Plan
from .signals import SignalInbox
from .store import Store

log = logging.getLogger("trader")


def missing_config():
    required = (
        "TG_API_ID",
        "TG_API_HASH",
        "TG_CHANNEL_ID",
        "GATE_API_KEY",
        "GATE_API_SECRET",
        "TRADER_TOKEN",
    )
    missing = [key for key in required if not os.getenv(key)]
    if not Path(os.getenv("TG_SESSION_PATH", "/state/telegram.session")).is_file():
        missing.append("TG_SESSION_PATH (authorized user session)")
    return missing


@web.middleware
async def auth(request, handler):
    if request.path == "/health":
        return await handler(request)
    expected = os.getenv("TRADER_TOKEN", "")
    supplied = request.headers.get("Authorization", "").removeprefix("Bearer ")
    if not expected or not hmac.compare_digest(expected, supplied):
        raise web.HTTPUnauthorized()
    try:
        return await handler(request)
    except (Review, ValueError, GateError) as exc:
        raise web.HTTPConflict(text=str(exc)) from exc


def make_app(engine, channel=None):
    app = web.Application(middlewares=[auth], client_max_size=32 * 1024)
    inbox = SignalInbox(engine)

    async def health(request):
        return web.json_response(
            {
                "alive": True,
                "enabled": engine.enabled,
                "ready": engine.enabled and engine.listener_ready and not engine.paused(),
                "missing": missing_config(),
            }
        )

    async def status(request):
        return web.json_response(
            {
                "enabled": engine.enabled,
                "paused": engine.paused(),
                "pause_reason": engine.store.get("pause_reason", "activation required"),
                "listener_ready": engine.listener_ready,
                "missing": missing_config(),
                "planner_seen": engine.store.get("planner_seen"),
                "interpreter_seen": engine.store.get("interpreter_seen"),
                "pending_messages": engine.store.db.execute(
                    "SELECT COUNT(*) FROM events WHERE status IN ('queued','analyzing')"
                ).fetchone()[0],
                "unknown_writes": len(engine.store.unknowns()),
                "trades": [
                    {
                        k: t.get(k)
                        for k in (
                            "id",
                            "symbol",
                            "side",
                            "status",
                            "filled",
                            "position_size",
                            "entry_price",
                            "margin_budget",
                            "exits",
                            "plan_needed",
                            "last_error",
                        )
                    }
                    for t in engine.active()
                ],
            }
        )

    async def pause(request):
        engine.pause("operator requested pause")
        return web.json_response({"paused": True})

    async def jobs(request):
        engine.store.set("planner_seen", time.time())
        return web.json_response(await engine.claim_plan() if engine.enabled else None)

    async def plan(request):
        body = await request.json()
        await engine.apply_plan(
            body["trade_id"], body["version"], body["lease"], Plan.model_validate(body["plan"])
        )
        return web.json_response({"accepted": True})

    async def plan_failed(request):
        body = await request.json()
        trade = engine.store.trade(body.get("trade_id"))
        if trade and trade.get("plan_token") == body.get("lease"):
            error = str(body.get("error", "planner failed"))[:100]
            if trade.get("plan_error") != error:
                engine.store.notice(
                    f"Exit planning failed for {trade['symbol']}: {error}. "
                    "No invented stop or automatic position close; review protection status."
                )
                trade["plan_error"] = error
                engine.store.save_trade(trade)
        return web.json_response({"recorded": True})

    async def plan_propose(request):
        body = await request.json()
        await engine.propose_plan(
            body["trade_id"], body["version"], body["lease"], Plan.model_validate(body["plan"])
        )
        return web.json_response({"accepted": True, "executed": False})

    async def plan_proposal(request):
        return web.json_response(
            engine.plan_proposal(
                request.match_info["trade_id"],
                int(request.query["version"]),
                request.query["lease"],
            )
        )

    async def signal_jobs(request):
        engine.store.set("interpreter_seen", time.time())
        return web.json_response(await inbox.claim())

    async def signal_fresh(request):
        return web.json_response({"fresh": inbox.fresh(request.match_info["lease"])})

    async def signal_result(request):
        body = await request.json()
        await inbox.apply(body["lease"], Decisions.model_validate(body["result"]))
        return web.json_response({"accepted": True})

    async def signal_propose(request):
        body = await request.json()
        await inbox.propose(body["lease"], Decisions.model_validate(body["result"]))
        return web.json_response({"accepted": True, "executed": False})

    async def signal_proposal(request):
        return web.json_response(inbox.proposal(request.match_info["lease"]))

    async def signal_failed(request):
        body = await request.json()
        inbox.failed(body["lease"], str(body.get("error", "model unavailable"))[:100])
        return web.json_response({"recorded": True})

    async def history(request):
        if channel is None:
            raise web.HTTPServiceUnavailable(text="channel reader unavailable")
        return web.json_response(
            await channel.history(
                before_message_id=int(request.query.get("before_message_id", 0)),
                limit=int(request.query.get("limit", 30)),
                query=request.query.get("query", ""),
            )
        )

    async def message(request):
        if channel is None:
            raise web.HTTPServiceUnavailable(text="channel reader unavailable")
        return web.json_response(await channel.read(int(request.match_info["message_id"])))

    async def image(request):
        if channel is None:
            raise web.HTTPServiceUnavailable(text="channel reader unavailable")
        data, mime = await channel.image(int(request.match_info["message_id"]))
        return web.Response(body=data, content_type=mime, headers={"Cache-Control": "no-store"})

    app.router.add_get("/health", health)
    app.router.add_get("/status", status)
    app.router.add_post("/pause", pause)
    app.router.add_post("/jobs/claim", jobs)
    app.router.add_post("/plans", plan)
    app.router.add_post("/plans/proposals", plan_propose)
    app.router.add_get("/plans/{trade_id}/proposal", plan_proposal)
    app.router.add_post("/jobs/fail", plan_failed)
    app.router.add_post("/signals/claim", signal_jobs)
    app.router.add_get("/signals/{lease}/fresh", signal_fresh)
    app.router.add_post("/signals/decisions", signal_result)
    app.router.add_post("/signals/proposals", signal_propose)
    app.router.add_get("/signals/{lease}/proposal", signal_proposal)
    app.router.add_post("/signals/fail", signal_failed)
    app.router.add_get("/channel/history", history)
    app.router.add_get("/channel/messages/{message_id}", message)
    app.router.add_get("/channel/messages/{message_id}/image", image)
    # No free-form entry/activation endpoint. Decisions require a current durable
    # inbox lease; model tools expose evidence and staging, never execution.
    return app


async def listen(engine, channel):
    while True:
        if [key for key in missing_config() if key not in {"GATE_API_KEY", "GATE_API_SECRET"}]:
            await asyncio.sleep(10)
            continue
        channel_id = int(os.environ["TG_CHANNEL_ID"])
        session = os.getenv("TG_SESSION_PATH", "/state/telegram.session")
        client = TelegramClient(
            session,
            int(os.environ["TG_API_ID"]),
            os.environ["TG_API_HASH"],
            catch_up=False,
            flood_sleep_threshold=0,
        )
        try:
            await client.connect()
            if not await client.is_user_authorized():
                engine.pause("Telegram user session is not authorized")
                await asyncio.sleep(30)
                continue
            # The numeric allowlist must agree with the configured public username.
            peer = await client.get_entity(os.getenv("TG_CHANNEL", "") or channel_id)
            if utils.get_peer_id(peer) != channel_id:
                raise Review("Telegram channel ID does not match configured peer")
            channel.client, channel.peer = client, peer
            live_since = time.time()

            async def incoming(event, live_since=live_since, channel_id=channel_id):
                message = event.message
                source_time = message.date.timestamp()
                edited = isinstance(event, events.MessageEdited.Event)
                if edited and message.edit_date:
                    source_time = message.edit_date.timestamp()
                fresh = live_since <= source_time and time.time() - source_time <= 60
                engine.store.event(
                    channel_id,
                    message.id,
                    message.message or "",
                    message.reply_to_msg_id,
                    source_time,
                    fresh and engine.listener_ready,
                    edited,
                    media=describe_message(message)["media"],
                )

            client.add_event_handler(incoming, events.NewMessage(chats=peer))
            client.add_event_handler(incoming, events.MessageEdited(chats=peer))
            engine.listener_ready = True
            await client.run_until_disconnected()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 -- listener boundary must fail closed
            engine.pause("Telegram listener disconnected/not authorized")
            log.warning("Telegram listener: %s", type(exc).__name__)
        finally:
            engine.listener_ready = False
            channel.client, channel.peer = None, None
            await client.disconnect()
        await asyncio.sleep(5)


async def reconcile(engine):
    while True:
        if os.getenv("GATE_API_KEY") and os.getenv("GATE_API_SECRET"):
            try:
                await engine.poll()
            except Exception as exc:  # noqa: BLE001 -- pause new risk on any reconciliation failure
                engine.pause("exchange reconciliation disconnected")
                log.warning("Reconciliation: %s", type(exc).__name__)
        await asyncio.sleep(2)


async def notifications(engine, session):
    while True:
        token, chat = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_ADMIN_USER_ID")
        if token and chat:
            rows = engine.store.db.execute(
                "SELECT * FROM notices WHERE sent=0 ORDER BY id LIMIT 10"
            ).fetchall()
            for row in rows:
                try:
                    async with session.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat, "text": "[Telegram Trader] " + row["text"]},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as response:
                        result = await response.json()
                        if response.status != 200 or not result.get("ok"):
                            break
                    with engine.store.db:
                        engine.store.db.execute(
                            "UPDATE notices SET sent=1 WHERE id=?", (row["id"],)
                        )
                except Exception:  # noqa: BLE001 -- notification-only retry, no exchange side effect
                    break  # notification retry is allowed; trade-write retry is not
        await asyncio.sleep(2)


async def main():
    os.umask(0o077)
    state = Path(os.getenv("TRADER_STATE", "/state"))
    state.mkdir(parents=True, exist_ok=True)
    lock = (state / "process.lock").open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    store = Store(state / "trader.sqlite3")
    store.recover_analysis()
    async with aiohttp.ClientSession() as session:
        gate = Gate(
            session,
            os.getenv("GATE_API_KEY", ""),
            os.getenv("GATE_API_SECRET", ""),
            testnet=os.getenv("GATE_ENVIRONMENT", "testnet") == "testnet",
        )
        if os.getenv("GATE_ENVIRONMENT", "testnet") not in {"testnet", "live"}:
            raise ValueError("GATE_ENVIRONMENT must be testnet or live")
        engine = Engine(store, gate, enabled=os.getenv("TRADER_ENABLED", "false") == "true")
        if store.get("paused") is None:
            engine.pause("initial activation required")
        if engine.active() and os.getenv("GATE_API_KEY"):
            await engine.poll()
        channel = ChannelReader()
        runner = web.AppRunner(make_app(engine, channel))
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", 8080).start()
        log.info(
            "Trader online; enabled=%s; missing=%s", engine.enabled, ", ".join(missing_config())
        )
        try:
            await asyncio.gather(
                listen(engine, channel),
                reconcile(engine),
                notifications(engine, session),
            )
        finally:
            await runner.cleanup()
            store.close()
            lock.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main())
