"""Opt-in actual-provider evaluation. Uses memory-only fixtures; never applies decisions."""

import asyncio
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

import aiohttp
from aiohttp import web
from pydantic import ValidationError

from trader.engine import Engine, Review
from trader.models import Decisions, Plan
from trader.signals import SignalInbox
from trader.store import Store


class Evidence:
    def __init__(self, messages, image):
        self.messages, self.image_bytes, self.calls = messages, image, []

    async def read(self, message_id):
        self.calls.append(["message", message_id])
        return self.messages[message_id]

    async def history(self, before_message_id=0, limit=30, query=""):
        self.calls.append(["history", before_message_id, query])
        rows = [
            m
            for i, m in sorted(self.messages.items(), reverse=True)
            if (not before_message_id or i < before_message_id)
            and (not query or query in m["text"])
        ]
        return {"messages": rows[:limit], "next_before_message_id": None, "context_only": True}

    async def image(self, message_id):
        self.calls.append(["image", message_id])
        return self.image_bytes, "image/png"

    def app(self, engine):
        inbox = SignalInbox(engine)

        @web.middleware
        async def validation(request, handler):
            try:
                return await handler(request)
            except (ValueError, Review) as exc:
                raise web.HTTPConflict(text=str(exc)) from exc

        async def read(request):
            return web.json_response(await self.read(int(request.match_info["message_id"])))

        async def history(request):
            return web.json_response(
                await self.history(
                    int(request.query.get("before_message_id", 0)),
                    int(request.query.get("limit", 30)),
                    request.query.get("query", ""),
                )
            )

        async def image(request):
            data, mime = await self.image(int(request.match_info["message_id"]))
            return web.Response(body=data, content_type=mime)

        async def state(request):
            return web.json_response({"enabled": engine.enabled, "trades": engine.active()})

        async def propose(request):
            body = await request.json()
            await inbox.propose(body["lease"], Decisions.model_validate(body["result"]))
            self.calls.append(["submit_signal_decisions"])
            return web.json_response({"accepted": True, "executed": False})

        async def proposal(request):
            return web.json_response(inbox.proposal(request.match_info["lease"]))

        async def plan_propose(request):
            body = await request.json()
            await engine.propose_plan(
                body["trade_id"], body["version"], body["lease"], Plan.model_validate(body["plan"])
            )
            self.calls.append(["submit_exit_plan"])
            return web.json_response({"accepted": True, "executed": False})

        async def plan_proposal(request):
            return web.json_response(
                engine.plan_proposal(
                    request.match_info["trade_id"],
                    int(request.query["version"]),
                    request.query["lease"],
                )
            )

        app = web.Application(middlewares=[validation])
        app.router.add_get("/channel/history", history)
        app.router.add_get("/channel/messages/{message_id}", read)
        app.router.add_get("/channel/messages/{message_id}/image", image)
        app.router.add_get("/status", state)
        app.router.add_post("/signals/proposals", propose)
        app.router.add_get("/signals/{lease}/proposal", proposal)
        app.router.add_post("/plans/proposals", plan_propose)
        app.router.add_get("/plans/{trade_id}/proposal", plan_proposal)
        return app


def msg(message_id, text, reply=None):
    return {
        "message_id": message_id,
        "text": text,
        "reply_to_message_id": reply,
        "source_time": time.time(),
        "media": None,
        "context_only": True,
    }


def owned(symbol, filled):
    return {
        "id": "fixture-owned-" + symbol,
        "channel": -100123,
        "message": 100,
        "symbol": symbol + "_USDT",
        "side": "long",
        "status": "working",
        "version": 1,
        "filled": filled,
        "position_size": filled,
        "entry_terminal": bool(filled),
        "entry_id": "fixture-order-100",
        "entry_price": "10",
        "lots": 14,
        "exits": {},
        "plan_needed": bool(filled),
        "margin_budget": "20",
    }


async def case(module, name, texts, expected, trade=None, image=None):
    store = Store(":memory:")
    source = {
        100: msg(
            100,
            f"#{trade['symbol'].removesuffix('_USDT')} 当前价格入场一笔多单"
            if trade
            else "#INJ 在10挂一笔多单",
        )
    }
    evidence = Evidence(source, image)
    engine = Engine(store, None, enabled=False)
    engine.listener_ready = True
    if trade:
        store.save_trade(trade)
    for i, text in enumerate(texts, start=101):
        reply = 100 if trade and name != "unowned_exit_is_noop" else None
        source[i] = msg(i, text, reply)
        published_at = time.time() - 65 if name == "delayed_explicit_market_entry" else time.time()
        source[i]["source_time"] = published_at
        store.event(
            -100123,
            i,
            text,
            reply,
            published_at,
            True,
            media={"id": "fixture-image", "mime": "image/png"} if image else None,
        )
    job = await SignalInbox(engine).claim()
    runner = web.AppRunner(evidence.app(engine))
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    os.environ["TELEGRAM_TRADER_URL"] = "http://127.0.0.1:" + str(
        site._server.sockets[0].getsockname()[1]
    )
    started = time.monotonic()
    try:
        async with aiohttp.ClientSession() as session:
            result = await asyncio.wait_for(
                module.interpret_current(job, int(os.environ["ADMIN_USER_ID"]), session), 150
            )
        decisions = Decisions.model_validate(result)
        actual = [d.action for d in decisions.decisions]
        report = {
            "case": name,
            "seconds": round(time.monotonic() - started, 2),
            "expected": expected,
            "actual": actual,
            "tools": evidence.calls,
            "decisions": result,
            "passed": actual == expected,
        }
        if trade and expected == ["close"]:
            report["passed"] &= decisions.decisions[0].target_trade_id == trade["id"]
            report["passed"] &= any(call[0] in {"message", "history"} for call in evidence.calls)
        print(json.dumps(report, ensure_ascii=False), flush=True)
        return report["passed"]
    except ValidationError as exc:
        print(
            json.dumps(
                {
                    "case": name,
                    "passed": False,
                    "result": result,
                    "errors": exc.errors(include_url=False),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return False
    except Exception as exc:  # noqa: BLE001 -- collect independent evaluation failures
        print(json.dumps({"case": name, "passed": False, "error": type(exc).__name__}), flush=True)
        return False
    finally:
        assert store.db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0
        await runner.cleanup()
        store.close()


async def main():
    root = Path(__file__).resolve().parents[1]
    path = root / "condor-assets/telegram_agent/routines/signal_interpreter.py"
    spec = importlib.util.spec_from_file_location("candidate_signal_interpreter", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    os.environ["TRADER_TOKEN"] = "isolated-model-eval-not-a-production-token"
    cases = [
        (
            "delayed_explicit_market_entry",
            ["#FIL\n當前價格入場一筆多單📈", "我的開倉價：0.9958"],
            ["open", "ignore"], None, None,
        ),
        ("explicit_limit_entry", ["#INJ 在8.63限价开一笔空单"], ["open"], None, None),
        (
            "open_then_cancel_before_execution",
            ["#INJ 当前价格入场一笔空单", "算了，这笔撤掉，不做了"],
            ["ignore", "ignore"],
            None,
            None,
        ),
        ("contextual_close", ["平掉，离场"], ["close"], owned("JTO", 14), None),
        ("cancel_unfilled_limit", ["这笔挂单撤掉吧"], ["cancel"], owned("INJ", 0), None),
        ("unowned_exit_is_noop", ["JTO 平掉"], ["ignore"], owned("BTC", 14), None),
        ("conflicting_reply_needs_review", ["JTO 平掉"], ["review"], owned("BTC", 14), None),
        ("negation_is_not_close", ["先别平，继续拿着"], ["ignore"], owned("JTO", 14), None),
    ]
    if len(sys.argv) > 1:
        cases.append(
            ("image_only_conditional_idea", [""], ["ignore"], None, Path(sys.argv[1]).read_bytes())
        )
    selected = os.getenv("EVAL_CASE")
    from condor.runtime.llm_client import build_llm_client

    model = os.getenv("EVAL_MODEL", "custom:devin/gemini-4-argon")

    def evaluation_client(agent_key, **kwargs):
        assert agent_key.startswith("custom:devin/gemini-4-argon")
        return build_llm_client(model, **kwargs)

    with patch("condor.runtime.llm_client.build_llm_client", side_effect=evaluation_client):
        results = [
            await case(module, *args) for args in cases if not selected or args[0] == selected
        ]
    print(
        json.dumps(
            {"model": model, "passed": sum(results), "total": len(results), "exchange_writes": 0}
        ),
        flush=True,
    )
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
