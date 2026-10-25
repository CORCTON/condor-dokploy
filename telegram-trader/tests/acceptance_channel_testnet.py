"""Opt-in Gemini -> production decision engine -> Gate TestNet lifecycle acceptance.

Synthetic channel messages, real model/tools and real TestNet orders. Credentials
arrive on stdin, never in model prompts/environment. No Telegram listener or live
endpoint. Only this run's owned orders can be cleaned up.
"""

import asyncio
import importlib.util
import json
import os
import sys
import tempfile
import time
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from unittest.mock import patch

import aiohttp
from aiohttp import web
from eval_channel_model import Evidence, msg

from trader.engine import Engine
from trader.gate import Gate
from trader.models import Decisions, Plan
from trader.signals import SignalInbox
from trader.store import Store


def routine(name):
    path = Path(__file__).parents[1] / f"condor-assets/telegram_agent/routines/{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestnetGate(Gate):
    def __init__(self, session, credentials):
        super().__init__(session, credentials["key"], credentials["secret"], testnet=True)
        self.writes = []

    async def request(self, method, path, **kwargs):
        assert self.base == "https://api-testnet.gateapi.io", "Live access forbidden"
        result = await super().request(method, path, **kwargs)
        if method != "GET":
            self.writes.append(
                {
                    "method": method,
                    "path": path,
                    "id": result.get("id") if isinstance(result, dict) else None,
                }
            )
        return result


async def cycle(gate, session, name, interpreter, planner):
    directory = Path(tempfile.mkdtemp(prefix="gemini-channel-testnet-"))
    store = Store(directory / "trader.sqlite3")
    engine = Engine(store, gate, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    inbox = SignalInbox(engine)
    evidence = Evidence({}, None)
    app = evidence.app(engine)

    async def fresh(request):
        return web.json_response({"fresh": inbox.fresh(request.match_info["lease"])})

    app.router.add_get("/signals/{lease}/fresh", fresh)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    os.environ["TELEGRAM_TRADER_URL"] = "http://127.0.0.1:" + str(
        site._server.sockets[0].getsockname()[1]
    )
    channel = -(time.time_ns() // 1000)
    symbol = "BTC_USDT"
    report = {"case": name, "state": str(directory), "events": [], "result": "RUNNING"}
    baseline_writes = len(gate.writes)
    print(json.dumps(report), flush=True)

    async def send(text, number, expected, reply=None):
        evidence.messages[number] = msg(number, text, reply)
        event_id = store.event(channel, number, text, reply, time.time(), True)
        if event_id is None:
            assert not store.queued()
            return
        job = await inbox.claim()
        started = time.monotonic()
        async with interpreter.interpret_current(job, int(os.environ["ADMIN_USER_ID"]), session) as raw:
            assert raw is not None, "Analysis became stale; no test decision submitted"
            decisions = Decisions.model_validate(raw)
            actual = [d.action for d in decisions.decisions]
            item = {
                "message_id": number,
                "expected": expected,
                "actual": actual,
                "proposal_ready_seconds": round(time.monotonic() - started, 6),
                "model_seconds": round(time.monotonic() - started, 2),
                "timing_note": "model_seconds is legacy time to proposal, not pure inference",
                "decisions": raw,
            }
            report["events"].append(item)
            print(json.dumps(item, ensure_ascii=False), flush=True)
            acceptable = {expected} if isinstance(expected, str) else set(expected)
            assert len(actual) == 1 and actual[0] in acceptable, "Model intent did not match test case"
            await inbox.apply(job["lease"], decisions)
            item["execution_applied_seconds"] = round(time.monotonic() - started, 6)
            item["through_execution_seconds"] = round(time.monotonic() - started, 2)
        row = store.db.execute(
            "SELECT status,detail FROM events WHERE id=?", (event_id,)
        ).fetchone()
        assert row["status"] in {"done", "ignore"}, dict(row)
        before = len(gate.writes)
        await inbox.apply(job["lease"], decisions)
        assert len(gate.writes) == before, "Duplicate model response wrote again"

    async def settle(predicate, seconds=20):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            await engine.poll()
            if predicate():
                return
            await asyncio.sleep(1)
        raise AssertionError("Exchange reconciliation deadline exceeded")

    try:
        assert Decimal(str((await gate.get(f"/positions/{symbol}"))["size"])) == 0
        assert not await gate.get("/orders", contract=symbol, status="open")
        assert not await gate.get("/price_orders", contract=symbol, status="open")
        contract = await gate.get(f"/contracts/{symbol}")
        tick = Decimal(contract["order_price_round"])
        if name == "cancel_unfilled_limit":
            reference = Decimal((await gate.market(symbol))[0]["highest_bid"])
            price = (reference * Decimal("0.95") / tick).to_integral_value(
                rounding=ROUND_DOWN
            ) * tick
            await send(f"#BTC 在{price}挂一笔限价多单", 1, "open")
            await settle(lambda: bool(store.trades() and store.trades()[0].get("entry_id")))
            assert store.trades()[0]["filled"] == 0, "Limit unexpectedly filled"
            await send("这笔挂单撤掉，不做了", 2, "cancel", reply=1)
            await settle(lambda: not engine.active())
        else:
            direction = "多" if name == "long_market" else "空"
            entry = f"#BTC 当前价格入场一笔{direction}单"
            await send(entry, 1, "open")
            await settle(lambda: bool(store.trades() and store.trades()[0]["filled"]))
            trade = store.trades()[0]
            position = await gate.get(f"/positions/{symbol}")
            report["position"] = {
                k: position.get(k) for k in ("size", "lever", "mode", "pos_margin_mode")
            }
            assert position["pos_margin_mode"] == "isolated" and Decimal(position["lever"]) == 10
            assert Decimal(trade["margin_budget"]) == Decimal(trade["equity"]) * Decimal("0.2")
            before = len(gate.writes)
            await send(entry, 1, "open")
            await send("先别平，继续拿着", 2, "ignore", reply=1)
            assert len(gate.writes) == before
            job = await engine.claim_plan()
            assert job is not None
            started = time.monotonic()
            writes_before_plan = len(gate.writes)
            async with planner.generate_plan(job, int(os.environ["ADMIN_USER_ID"])) as raw:
                report["plan_proposal_ready_seconds"] = round(time.monotonic() - started, 6)
                assert ["submit_exit_plan"] in evidence.calls
                assert len(gate.writes) == writes_before_plan, "Proposal tool must not place orders"
                await engine.apply_plan(
                    job["trade_id"], job["version"], job["lease"], Plan.model_validate(raw)
                )
                report["plan_applied_seconds"] = round(time.monotonic() - started, 6)
            report["planner_and_exit_seconds"] = round(time.monotonic() - started, 2)
            trade = store.trade(trade["id"])
            stop = await gate.get("/price_orders/" + trade["exits"]["sl"]["id"])
            assert stop["status"] == "open" and stop["initial"]["is_reduce_only"]
            for target in ("tp1", "tp2"):
                order = await engine.order(trade["exits"][target]["id"])
                assert order.get("is_reduce_only", order.get("reduce_only"))
            delta = tick if name == "long_market" else -tick
            sl, tp2 = Decimal(raw["sl"]) - delta, Decimal(raw["tp2"]) + delta
            await send(f"BTC SL放在{sl}，TP2改为{tp2}", 3, "targets", reply=1)
            trade = store.trade(trade["id"])
            stop = await gate.get("/price_orders/" + trade["exits"]["sl"]["id"])
            assert Decimal(stop["trigger"]["price"]) == sl
            assert Decimal((await engine.order(trade["exits"]["tp2"]["id"]))["price"]) == tp2
            await send("BTC 当前价格做TP1", 4, "tp1", reply=1)
            await settle(lambda: store.trade(trade["id"])["exits"]["tp1"].get("taken_now"))
            position = await gate.get(f"/positions/{symbol}")
            assert 0 < abs(Decimal(str(position["size"]))) < trade["filled"]
            before = len(gate.writes)
            await send("BTC 当前价格做TP1", 5, ("tp1", "ignore"), reply=1)
            await send("JTO 平掉", 6, "ignore")
            assert len(gate.writes) == before
            await send("平掉，离场", 7, "close", reply=1)
            await settle(lambda: not engine.active())
        assert not store.unknowns()
        report["result"] = "PASS"
    except Exception as exc:  # noqa: BLE001 -- ensure TestNet cleanup on any failure
        report.update(result="FAIL", error_type=type(exc).__name__, error=str(exc))
        errors, pending = [], [exc]
        while pending:
            error = pending.pop()
            if isinstance(error, BaseExceptionGroup):
                pending.extend(error.exceptions)
            else:
                errors.append({"type": type(error).__name__, "detail": str(error)})
        report["causes"] = errors
    finally:
        for trade in engine.active():
            if trade.get("entry_id"):
                try:
                    await engine.close_trade(trade)
                    await settle(lambda: not engine.active())
                except Exception as exc:  # noqa: BLE001 -- report retained owned test exposure
                    report["cleanup_error"] = type(exc).__name__
        report["remaining_position"] = str((await gate.get(f"/positions/{symbol}"))["size"])
        report["remaining_orders"] = len(await gate.get("/orders", contract=symbol, status="open"))
        report["remaining_stops"] = len(
            await gate.get("/price_orders", contract=symbol, status="open")
        )
        report["unknown_writes"] = len(store.unknowns())
        report["exchange_writes"] = gate.writes[baseline_writes:]
        report["evidence_tools"] = evidence.calls
        (directory / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps(report, ensure_ascii=False), flush=True)
        await runner.cleanup()
        store.close()
    assert (
        report["remaining_position"] == "0"
        and not report["remaining_orders"]
        and not report["remaining_stops"]
    )
    assert report["result"] == "PASS", "TestNet acceptance failed; never enable live"


async def main():
    from copy import deepcopy

    from condor.agents.agent import AgentStore
    from condor.runtime.llm_client import build_llm_client

    credentials = json.load(sys.stdin)
    os.umask(0o077)
    os.environ["TRADER_TOKEN"] = "isolated-testnet-fixture-token"
    interpreter, planner = routine("signal_interpreter"), routine("exit_planner")
    model = os.getenv("ACCEPTANCE_MODEL", "custom:devin/gemini-4-argon")
    get_agent = AgentStore.get

    def candidate_agent(store, slug):
        agent = deepcopy(get_agent(store, slug))
        if slug == "telegram_agent" and agent is not None:
            instructions = Path(planner.__file__).parents[1] / "AGENT.md"
            agent.instructions = instructions.read_text().split("---", 2)[2].strip()
        return agent

    def evaluation_client(agent_key, **kwargs):
        assert agent_key.startswith("custom:devin/gemini-4-argon")
        return build_llm_client(model, **kwargs)

    print(json.dumps({"acceptance_model": model}), flush=True)
    with (
        patch("condor.runtime.llm_client.build_llm_client", side_effect=evaluation_client),
        patch.object(AgentStore, "get", candidate_agent),
    ):
        async with aiohttp.ClientSession() as session:
            gate = TestnetGate(session, credentials)
            for name in ("long_market", "short_market", "cancel_unfilled_limit"):
                await cycle(gate, session, name, interpreter, planner)
    print(
        "3 real-Gemini/TestNet cycles PASS; no production state or live endpoint used", flush=True
    )


if __name__ == "__main__":
    asyncio.run(main())
