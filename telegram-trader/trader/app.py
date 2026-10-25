import asyncio
import fcntl
import hmac
import json
import logging
import math
import os
import time
from pathlib import Path

import aiohttp
from aiohttp import web

from .channel import READ_TIMEOUT_SECONDS, ChannelReader, describe_message
from .credentials import load_credentials
from .engine import Engine, Review
from .gate import Gate, GateError
from .models import Decisions, Plan
from .signals import SignalInbox
from .store import Store

log = logging.getLogger("trader")


def missing_config(credentials=None):
    required = (
        "TG_API_ID",
        "TG_API_HASH",
        "TG_CHANNEL_ID",
        "TRADER_TOKEN",
    )
    missing = [key for key in required if not os.getenv(key)]
    if not Path(os.getenv("TG_SESSION_PATH", "/state/telegram.session")).is_file():
        missing.append("TG_SESSION_PATH (authorized user session)")
    if credentials is not None and not credentials.ready:
        missing.append(credentials.error)
    return missing


def business_readiness(engine, credentials=None):
    """Observe local progress without taking the execution lock or probing upstreams."""
    now = time.monotonic()
    ages = {
        "interpreter": engine.worker_age("interpreter"),
        "planner": engine.worker_age("planner"),
        "channel_scan": None if engine.last_channel_scan is None else now - engine.last_channel_scan,
        "reconcile": None if engine.last_reconcile is None else now - engine.last_reconcile,
    }
    ages = {
        name: max(0, value) if value is not None and math.isfinite(value) else None
        for name, value in ages.items()
    }
    limits = {"interpreter": 60, "planner": 60, "channel_scan": 60, "reconcile": 180}
    reasons = []
    if engine.enabled:
        if missing_config(credentials) or not engine.gate.ready:
            reasons.append("configuration_missing")
        if not engine.listener_ready:
            reasons.append("listener_disconnected")
        for name, age in ages.items():
            if age is None or age > limits[name]:
                reasons.append(name + "_stale")
        if engine.store.unknowns():
            reasons.append("exchange_write_unconfirmed")
        if any(trade.get("last_error") for trade in engine.active()):
            reasons.append("position_requires_review")
    return {
        "ready": not reasons,
        "state": "disabled" if not engine.enabled else ("degraded" if reasons else "ready"),
        "enabled": engine.enabled,
        "paused": engine.paused(),
        "reasons": reasons,
        "ages_seconds": ages,
        "limits_seconds": limits,
    }


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


def make_app(engine, channel=None, credentials=None):
    app = web.Application(middlewares=[auth], client_max_size=32 * 1024)
    inbox = SignalInbox(engine)

    async def health(request):
        return web.json_response(
            {
                "alive": True,
                "enabled": engine.enabled,
                "ready": (
                    engine.enabled
                    and engine.listener_ready
                    and not engine.paused()
                    and not missing_config(credentials)
                ),
                "missing": missing_config(credentials),
            }
        )

    async def ready(request):
        result = business_readiness(engine, credentials)
        return web.json_response(result, status=200 if result["ready"] else 503)

    async def status(request):
        last_review = engine.store.db.execute(
            "SELECT message AS message_id,received AS received_at,detail AS reason "
            "FROM events WHERE status='review' ORDER BY received DESC,rowid DESC LIMIT 1"
        ).fetchone()
        return web.json_response(
            {
                "enabled": engine.enabled,
                "paused": engine.paused(),
                "new_entries_enabled": (
                    engine.enabled
                    and not engine.paused()
                    and engine.listener_ready
                    and not missing_config(credentials)
                ),
                "entries_started_after": engine.store.get("entries_started_after"),
                "pause_scope": "停止提交新开仓；已有仓位继续管理；已提交的订单不自动撤销，仍可能成交。",
                "pause_reason": engine.store.get("pause_reason", "activation required"),
                "listener_ready": engine.listener_ready,
                "missing": missing_config(credentials),
                "credentials": credentials.status() if credentials else None,
                "readiness": business_readiness(engine, credentials),
                "planner_seen": engine.worker_seen("planner"),
                "interpreter_seen": engine.worker_seen("interpreter"),
                "pending_messages": engine.store.db.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE status IN ('queued','deferred','analyzing','processing')"
                ).fetchone()[0],
                "unknown_writes": len(engine.store.unknowns()),
                "last_review": dict(last_review) if last_review else None,
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
                            "entry_terminal",
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
        return await status(request)

    async def start(request):
        if missing_config(credentials):
            raise Review("启动配置未就绪，未开启跟单；请查询状态中的 missing。")
        try:
            await engine.resume()
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise Review("交易所连接检查未完成，未确认开启；请查询状态后重试。") from exc
        return await status(request)

    async def jobs(request):
        engine.record_heartbeat("planner")
        return web.json_response(await engine.claim_plan() if engine.enabled else None)

    async def worker_heartbeat(request):
        worker = request.match_info["worker"]
        if worker not in {"interpreter", "planner"}:
            raise web.HTTPBadRequest(text="unknown worker")
        engine.record_heartbeat(worker)
        return web.json_response({"recorded": True})

    async def plan(request):
        body = await request.json()
        await engine.apply_plan(
            body["trade_id"], body["version"], body["lease"], Plan.model_validate(body["plan"])
        )
        return web.json_response({"accepted": True})

    async def plan_failed(request):
        body = await request.json()
        released = await engine.fail_plan(
            body.get("trade_id"), body.get("version"), body.get("lease"),
            str(body.get("error", "planner failed"))[:100],
        )
        return web.json_response({"recorded": True, "released": released})

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
        engine.record_heartbeat("interpreter")
        return web.json_response(await inbox.claim())

    async def signal_fresh(request):
        return web.json_response({"fresh": inbox.fresh(request.match_info["lease"])})

    async def signal_result(request):
        body = await request.json()
        job = engine.store.analysis(body["lease"])
        if channel is not None and job and job["status"] == "analyzing":
            channel.connected()
            rows = [
                engine.store.db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
                for event_id in json.loads(job["event_ids"])
            ]
            ids = list(dict.fromkeys(row["message"] for row in rows))
            eligible = {
                row["message"]: json.loads(row["payload"]).get("entry_eligible", False)
                for row in rows
            }
            # Polling new message IDs cannot see an edited cancellation. Verify
            # the claimed source versions immediately before applying a proposal.
            try:
                async with asyncio.timeout(READ_TIMEOUT_SECONDS), channel.ingress_lock:
                    messages = await channel.client.get_messages(channel.peer, ids=ids)
                    for message_id, message in zip(ids, messages, strict=True):
                        if message is None or not getattr(message, "date", None):
                            for row in rows:
                                if row["message"] == message_id:
                                    engine.store.finish(
                                        row["id"], "superseded", "source message deleted"
                                    )
                        else:
                            receive_message(
                                engine, job["channel"], message, time.time(), via="verification",
                                entry_eligible=eligible[message_id],
                            )
            except TimeoutError as exc:
                raise web.HTTPServiceUnavailable(
                    text="channel source verification temporarily unavailable"
                ) from exc
        # Only the read-only source verification is cancellable by our deadline.
        # Once execution starts, preserve the durable exchange-write protocol.
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
        try:
            return web.json_response(
                await channel.history(
                    before_message_id=int(request.query.get("before_message_id", 0)),
                    limit=int(request.query.get("limit", 30)),
                    query=request.query.get("query", ""),
                )
            )
        except TimeoutError as exc:
            raise web.HTTPServiceUnavailable(text="channel history temporarily unavailable") from exc

    async def message(request):
        if channel is None:
            raise web.HTTPServiceUnavailable(text="channel reader unavailable")
        try:
            return web.json_response(await channel.read(int(request.match_info["message_id"])))
        except TimeoutError as exc:
            raise web.HTTPServiceUnavailable(text="channel message temporarily unavailable") from exc

    async def image(request):
        if channel is None:
            raise web.HTTPServiceUnavailable(text="channel reader unavailable")
        try:
            data, mime = await channel.image(int(request.match_info["message_id"]))
        except TimeoutError as exc:
            raise web.HTTPServiceUnavailable(text="channel image temporarily unavailable") from exc
        return web.Response(body=data, content_type=mime, headers={"Cache-Control": "no-store"})

    app.router.add_get("/health", health)
    app.router.add_get("/ready", ready)
    app.router.add_get("/status", status)
    app.router.add_post("/pause", pause)
    app.router.add_post("/start", start)
    app.router.add_post("/jobs/claim", jobs)
    app.router.add_post("/workers/{worker}/heartbeat", worker_heartbeat)
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
    # No free-form order endpoint. Decisions require a current durable
    # inbox lease; model tools expose evidence and staging, never execution.
    return app


async def listen(engine, channel):
    from telethon import TelegramClient, events, utils
    from telethon.errors import FloodWaitError

    while True:
        if missing_config():
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
            ingress_lock = channel.ingress_lock

            async def incoming(
                event, live_since=live_since, channel_id=channel_id,
                client=client, peer=peer, ingress_lock=ingress_lock,
            ):
                await receive_live_message(
                    channel, client, peer, engine, channel_id, event.message, live_since, ingress_lock,
                    edited=isinstance(event, events.MessageEdited.Event),
                )

            client.add_event_handler(incoming, events.NewMessage(chats=peer))
            client.add_event_handler(incoming, events.MessageEdited(chats=peer))
            cursor_key = f"telegram_cursor:{channel_id}"
            cursor = engine.store.get(cursor_key)
            if cursor is None:
                cursor = engine.store.db.execute(
                    "SELECT MAX(message) FROM events WHERE channel=?", (channel_id,)
                ).fetchone()[0]
                if cursor is None:
                    async with asyncio.timeout(READ_TIMEOUT_SECONDS):
                        head = await client.get_messages(peer, limit=1)
                    cursor = head[0].id if head else 0
                engine.store.set(cursor_key, cursor)
            await refresh_known_messages(
                client, peer, engine, channel_id, live_since, ingress_lock
            )
            cursor = await collect_new_messages(
                client, peer, cursor, engine, channel_id, live_since, ingress_lock
            )
            engine.listener_ready = True
            log.info("Channel listener ready; push enabled; new-message reconciliation every 3s")
            tail = asyncio.create_task(
                follow_new_messages(
                    client, peer, cursor, engine, channel_id, live_since, ingress_lock
                )
            )
            connection = asyncio.create_task(client.run_until_disconnected())
            try:
                done, _ = await asyncio.wait({tail, connection}, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
            finally:
                tail.cancel()
                connection.cancel()
                await asyncio.gather(tail, connection, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 -- listener boundary must fail closed
            engine.pause("Telegram listener disconnected/not authorized")
            log.warning("Telegram listener: %s", type(exc).__name__)
            if isinstance(exc, FloodWaitError):
                await asyncio.sleep(exc.seconds)
        finally:
            engine.listener_ready = False
            channel.client, channel.peer = None, None
            await client.disconnect()
        await asyncio.sleep(5)


def receive_message(
    engine, channel_id, message, live_since, *, edited=False, via="push", entry_eligible=None,
):
    source_time = message.date.timestamp()
    edited_at = message.edit_date.timestamp() if message.edit_date else None
    edited = edited or edited_at is not None
    key = engine.store.event(
        channel_id, message.id, message.message or "", message.reply_to_msg_id,
        source_time, True,
        edited, media=describe_message(message)["media"],
        edited_at=edited_at,
        # Source verification inherits the claimed delivery's eligibility; it
        # must not treat verification time as a new listener activation.
        entry_eligible=live_since <= source_time if entry_eligible is None else entry_eligible,
    )
    if key:
        log.info("Channel message %s received; delivery_seconds=%.3f; edited=%s; via=%s",
                 message.id, time.time() - (edited_at or source_time), edited, via)


async def receive_current_message(
    client, peer, engine, channel_id, message, live_since, ingress_lock=None,
    *, edited=False, via="push",
):
    # Telegram edit_date has only second precision. Re-read edits in a short
    # serialized section so a late same-second push cannot overwrite newer text.
    async with asyncio.timeout(READ_TIMEOUT_SECONDS), ingress_lock or asyncio.Lock():
        message_id = message.id
        if edited or message.edit_date:
            message = await client.get_messages(peer, ids=message.id)
        if message is not None and getattr(message, "date", None):
            receive_message(engine, channel_id, message, live_since, edited=edited, via=via)
        else:
            with engine.store.db:
                engine.store.db.execute(
                    "UPDATE events SET status='superseded',detail='source message deleted' "
                    "WHERE channel=? AND message=? "
                    "AND status IN ('queued','deferred','analyzing','processing')",
                    (channel_id, message_id),
                )


async def receive_live_message(
    channel, client, peer, engine, channel_id, message, live_since, ingress_lock, *, edited=False,
):
    """Retry read-only push ingestion; older edits are not covered by the new-ID cursor."""
    from telethon.errors import FloodWaitError, RPCError

    while channel.client is client:
        try:
            await receive_current_message(
                client, peer, engine, channel_id, message, live_since, ingress_lock, edited=edited
            )
            return
        except FloodWaitError as exc:
            log.warning("Channel push read rate-limited; retry in %s seconds", exc.seconds)
            await asyncio.sleep(exc.seconds)
        except (RPCError, OSError, TimeoutError) as exc:
            log.warning("Channel push read failed: %s; retrying read-only ingestion",
                        type(exc).__name__)
            await asyncio.sleep(3)


async def refresh_known_messages(client, peer, engine, channel_id, live_since, ingress_lock=None):
    """Recover edits/deletions relevant to unfinished work and existing positions."""
    active = [t["message"] for t in engine.active() if t["channel"] == channel_id]
    ids = [
        r[0] for r in engine.store.db.execute(
            "SELECT DISTINCT message FROM events WHERE channel=? AND "
            "(status IN ('queued','deferred','analyzing','processing') OR message>=?) "
            "ORDER BY message", (channel_id, min(active) if active else 9223372036854775807),
        ).fetchall()
    ]
    for offset in range(0, len(ids), 100):
        batch = ids[offset:offset + 100]
        async with asyncio.timeout(READ_TIMEOUT_SECONDS):
            messages = await client.get_messages(peer, ids=batch)
        for message_id, message in zip(batch, messages, strict=True):
            if message is None or not getattr(message, "date", None):
                with engine.store.db:
                    engine.store.db.execute(
                        "UPDATE events SET status='superseded',detail='source message deleted' "
                        "WHERE channel=? AND message=? "
                        "AND status IN ('queued','deferred','analyzing','processing')",
                        (channel_id, message_id),
                    )
                continue
            await receive_current_message(
                client, peer, engine, channel_id, message, live_since, ingress_lock, via="recovery"
            )


async def collect_new_messages(client, peer, cursor, engine, channel_id, live_since, ingress_lock=None):
    cursor_key = f"telegram_cursor:{channel_id}"
    # A previous scan may have persisted a prefix before timing out. Resume that
    # prefix, rather than repeatedly spending the whole deadline on the same IDs.
    cursor = max(cursor, engine.store.get(cursor_key, cursor))
    async with asyncio.timeout(READ_TIMEOUT_SECONDS):
        async for message in client.iter_messages(
            peer, min_id=cursor, reverse=True, limit=None, wait_time=3
        ):
            if getattr(message, "date", None) and getattr(message, "message", None) is not None:
                await receive_current_message(
                    client, peer, engine, channel_id, message, live_since, ingress_lock, via="history"
                )
            cursor = max(cursor, message.id)
            # Only an ordered history scan advances the durable cursor. A later push
            # must not skip an earlier message that push delivery has not delivered yet.
            engine.store.set(cursor_key, cursor)
    # Even an empty scan proves that the durable cursor was checked against the
    # channel tail. Partial progress/timeouts must not count as a complete scan.
    engine.last_channel_scan = time.monotonic()
    return cursor


async def follow_new_messages(client, peer, cursor, engine, channel_id, live_since, ingress_lock=None):
    """Reconcile new posts if push delivery is delayed; both paths share durable deduplication.

    The durable cursor resumes after reconnect; recovered posts cannot open entries.
    Respect Telegram's flood wait rather than retrying it.
    """
    from telethon.errors import FloodWaitError, RPCError

    while True:
        await asyncio.sleep(3)
        try:
            cursor = await collect_new_messages(
                client, peer, cursor, engine, channel_id, live_since, ingress_lock
            )
        except FloodWaitError as exc:
            log.warning("Channel new-message check rate-limited; retry in %s seconds", exc.seconds)
            await asyncio.sleep(exc.seconds)
        except (RPCError, OSError, TimeoutError) as exc:
            log.warning("Channel new-message check failed: %s; push listener remains active",
                        type(exc).__name__)
            await asyncio.sleep(3)


async def reconcile(engine):
    while True:
        if engine.gate.ready:
            try:
                await engine.poll()
                engine.last_reconcile = time.monotonic()
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
        credentials = await asyncio.to_thread(load_credentials)
        gate = Gate(
            session,
            credentials.key,
            credentials.secret,
            testnet=credentials.environment == "testnet",
        )
        engine = Engine(store, gate, enabled=os.getenv("TRADER_ENABLED", "false") == "true")
        if store.get("paused") is None:
            engine.pause("initial activation required")
        if engine.active() and gate.ready:
            await engine.poll()
        channel = ChannelReader()
        runner = web.AppRunner(make_app(engine, channel, credentials))
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", 8080).start()
        log.info(
            "Trader online; enabled=%s; missing=%s",
            engine.enabled,
            ", ".join(missing_config(credentials)),
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
