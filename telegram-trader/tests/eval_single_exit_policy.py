"""Opt-in Gemini tool-calling evaluation with a memory-only exchange/database.

Run only in an isolated evaluation container containing the candidate sources and
an LLM-only JSON file: {"agent_key": "custom:...", "base_url": "...", "api_key": "..."}.
The key "model" is also accepted instead of "agent_key".
Example (from telegram-trader): PYTHONPATH=. python tests/eval_single_exit_policy.py --provider-config /run/provider.json
         --output /evidence/results.jsonl --repeat 2
Offline re-audit: replace --provider-config with --replay-jsonl original.jsonl.
This replays saved tool proposals, not model reasoning, against current assertions.

All examples and both candle series are constructed fixtures, not historical
signals, a market backtest, or evidence of exchange execution reliability.
"""

import argparse
import asyncio
import hashlib
import importlib.util
import json
import os
import time
from contextlib import asynccontextmanager, contextmanager
from copy import deepcopy
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

import aiohttp
from aiohttp import web, web_app
from comprehensive_fixtures import FakeExchange
from eval_channel_model import Evidence, msg

from trader.engine import EXIT_POLICY, Engine
from trader.gate import Gate
from trader.models import Decisions, Plan, Signal
from trader.signals import SignalInbox
from trader.store import Store

ROOT = Path(__file__).resolve().parents[1]
CHANNEL = -100123
SYMBOL = "INJ_USDT"
TOKEN = "isolated-single-exit-memory-only"

# Expectations are private harness data: only text/history/owned state reaches Gemini.
CASES = [
    {"name": "market_long_and_4h_plan", "text": "#INJ 当前价格入场一笔多单", "action": "open",
         "side": "long", "planner": True},
    {"name": "market_short_and_4h_plan", "text": "#INJ 当前价格入场一笔空单", "action": "open",
         "side": "short", "planner": True},
    {"name": "tp1_now_full_close", "text": "INJ 当前价格可以TP1", "action": "close", "owned": "long"},
    {"name": "tp2_now_without_plan", "text": "INJ 现在做TP2，止盈离场", "action": "close",
         "owned": "short", "seed_plan": False},
    {"name": "sl_exit_full_close", "text": "INJ SL离场，现在止损平掉", "action": "close", "owned": "short"},
    {"name": "profit_exit_full_close", "text": "INJ 可以止盈了，全部离场", "action": "close", "owned": "long"},
    {"name": "tp1_price_amends_only_tp", "text": "INJ TP1目标设置在11.50，不是现在平仓",
         "action": "targets", "owned": "long", "levels": {"tp": "11.50"}},
    {"name": "tp2_price_amends_only_tp", "text": "INJ TP2目标设置在8.50，等价格到位",
         "action": "targets", "owned": "short", "levels": {"tp": "8.50"}},
    {"name": "breakeven_uses_own_entry", "text": "INJ 带成本损，我自己的开仓价是10.50",
         "action": "sl", "owned": "long", "price": "10"},
    {"name": "numeric_sl_amends_only_stop", "text": "INJ SL设置9.50，继续持有",
         "action": "sl", "owned": "long", "price": "9.50"},
    {"name": "completed_tp_report_noop", "text": "INJ 昨天TP1已经完成，盈利不错，记录一下",
         "action": "ignore", "owned": "long"},
    {"name": "conditional_exit_noop", "text": "INJ 如果跌破9再平仓，现在继续拿着",
         "action": "ignore", "owned": "long"},
    {"name": "unowned_exit_noop", "text": "INJ 当前价格全部平掉", "action": "ignore"},
    {"name": "duplicate_opening_revision_noop", "text": "#INJ 当前价格入场一笔多单（重复确认原单）",
         "action": "ignore", "owned": "long", "message_id": 100},
    {"name": "negated_exit_noop", "text": "INJ 先不要平仓，继续持有", "action": "ignore", "owned": "long"},
    {"name": "conflicting_tp_prices_review", "text": "INJ TP1设11，TP2设12", "action": "ignore", "owned": "long"},
    {"name": "unsupported_addon_review", "text": "INJ 现在再补一仓多单", "action": "ignore", "owned": "long"},
    {"name": "opening_cancelled_in_same_batch", "texts": ["#INJ 当前价格入场一笔多单", "刚才那笔撤掉，不做了"],
         "actions": ["ignore", "ignore"]},
    {"name": "uncertain_right_side_setup_ignore", "text": "#INJ 这里可以考虑右侧空头布局，看看走势",
         "action": "ignore"},
    {"name": "entry_question_ignore", "text": "#INJ 当前价格入场空单？你们觉得呢", "action": "ignore"},
    {"name": "entry_missing_side_ignore", "text": "#INJ 当前价格入场一笔", "action": "ignore"},
    {"name": "missing_symbol_ignore", "text": "当前价格可以开一笔空单", "action": "ignore"},
    {"name": "hold_first_optional_exit_ignore", "text": "INJ 继续拿，也可以止盈", "action": "ignore", "owned": "long"},
]


class CandleExchange(FakeExchange):
    """Different closed-bar 5m/4h fixtures, intentionally separating SL and TP structure."""

    def __init__(self):
        super().__init__(equity="70")
        self.add_contract(SYMBOL, price="10", tick="0.01")
        self.add_contract("SAFE_USDT", price="10", tick="0.01")
        self.series = {}
        wave = [0, 1, 2, 3, 4, 3, 2, 1, 0, -1, -2, -3, -4, -3, -2, -1, 0, 1, 1, 0]
        for interval, seconds, amplitude, wick in (
            ("5m", 300, D("0.2"), D("0.1")), ("4h", 14400, D("1.5"), D("0.5")),
        ):
            end = int(time.time()) // seconds * seconds
            rows, previous = [], D(10)
            for i in range(120):
                close = D(10) + amplitude * D(wave[i % len(wave)]) / 4
                rows.append({"t": end - (120 - i) * seconds, "o": str(previous), "c": str(close),
                             "h": str(max(previous, close) + wick),
                             "l": str(min(previous, close) - wick), "v": str(1000 + i % 9 * 100)})
                previous = close
            self.series[interval] = rows

    async def candles(self, symbol, interval="5m"):
        self._contract(symbol)
        self.reads.append(("/candlesticks", {"contract": symbol, "interval": interval}))
        return deepcopy(self.series[interval])


def forbidden_gate(*_args, **_kwargs):
    raise AssertionError("Real Gate construction is forbidden in this evaluator")


def assert_loopback(url):
    parsed = urlsplit(url)
    assert parsed.scheme == "http" and parsed.hostname == "127.0.0.1" and parsed.port


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def wait_for_live_priority():
    if os.environ.get("EVAL_PRIORITY_FILE"):
        from eval_two_month_channel import wait_for_priority

        await wait_for_priority()


async def seed_owned(engine, exchange, side, with_plan):
    text = f"#INJ 当前价格入场一笔{'多' if side == 'long' else '空'}单"
    event_id = engine.store.event(CHANNEL, 100, text, None, time.time(), True)
    row = engine.store.db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    await engine.process(row, Signal(action="open", symbol="INJ", side=side))
    trade = engine.store.trades()[0]
    assert trade["filled"] == 14 and trade["exit_policy"] == "single_tp_4h_v1"
    if with_plan:
        job = await engine.claim_plan()
        plan = Plan(sl="9" if side == "long" else "11", tp="12" if side == "long" else "8",
                    rationale="constructed initial protection, not a model result")
        args = job["trade_id"], job["version"], job["lease"], plan
        await engine.propose_plan(*args)
        await engine.apply_plan(*args)
    exchange.set_market(SYMBOL, price="11" if side == "long" else "9")
    return engine.store.trade(trade["id"])


def evaluation_app(evidence, engine):
    app = evidence.app(engine)
    inbox = SignalInbox(engine)

    async def apply_signal(request):
        body = await request.json()
        await inbox.apply(body["lease"], Decisions.model_validate(body["result"]))
        return web.json_response({"applied": True})

    async def apply_plan(request):
        body = await request.json()
        await engine.apply_plan(body["trade_id"], body["version"], body["lease"],
                                Plan.model_validate(body["plan"]))
        return web.json_response({"applied": True})

    app.router.add_post("/signals/decisions", apply_signal)
    app.router.add_post("/plans", apply_plan)
    return app


def check_decisions(spec, result, job, owned, messages):
    decisions = Decisions.model_validate(result)
    mapping = {decision.event_id: decision for decision in decisions.decisions}
    assert len(mapping) == len(decisions.decisions) == len(job["events"])
    actual = [mapping[event["event_id"]] for event in job["events"]]
    expected_actions = spec.get("actions", [spec.get("action")])
    for decision, expected_action in zip(actual, expected_actions, strict=True):
        assert decision.action == expected_action or (expected_action == "sl" and decision.action == "targets")
        assert decision.evidence_message_ids and set(decision.evidence_message_ids) <= set(messages)
        assert decision.reason.strip()
        if decision.action == "open":
            assert decision.symbol == "INJ" and decision.side == spec["side"] and decision.price is None
        elif decision.action not in {"ignore"}:
            assert owned and decision.target_trade_id == owned["id"]
            assert decision.symbol in {None, "INJ"}
        if expected_action == "sl":
            assert decision.side in {None, spec["owned"]}
            if decision.action == "targets":
                assert decision.price is None
                assert {key: D(value) for key, value in decision.levels.items()} == {"sl": D(spec["price"])}
            else:
                assert decision.price is not None and D(decision.price) == D(spec["price"])
                assert decision.levels == {}
        elif "price" in spec:
            assert decision.price is not None and D(decision.price) == D(spec["price"])
        if expected_action != "sl":
            assert {key: D(value) for key, value in decision.levels.items()} == {
                key: D(value) for key, value in spec.get("levels", {}).items()}
    return actual


def check_execution(spec, engine, exchange, before, original):
    actions = spec.get("actions", [spec.get("action")])
    assert exchange.positions["SAFE_USDT"] == before["positions"]["SAFE_USDT"]
    assert not engine.store.unknowns()
    if set(actions) <= {"ignore"}:
        assert exchange.writes == []
        assert exchange.orders == before["orders"] and exchange.stops == before["stops"]
        assert exchange.positions == before["positions"]
        return
    action = actions[0]
    trade = engine.store.trades()[0]
    if action == "open":
        assert len(engine.store.trades()) == 1 and trade["filled"] == 14
        assert trade["exit_policy"] == "single_tp_4h_v1"
        assert exchange.positions[SYMBOL]["size"] == (14 if spec["side"] == "long" else -14)
        assert len(exchange.orders) == 1 and not exchange.stops
        assert [(m, p) for m, p, *_ in exchange.writes] == [
            ("POST", f"/positions/{SYMBOL}/set_leverage"), ("POST", "/orders")]
    elif action == "close":
        assert exchange.positions[SYMBOL]["size"] == 0 and trade["status"] == "closed"
        assert not any(row["status"] == "open" for row in exchange.orders.values())
        assert not any(row["status"] == "open" for row in exchange.stops.values())
        added = set(exchange.orders) - set(before["orders"])
        assert len(added) == 1
        close = exchange.orders[added.pop()]
        assert close["is_reduce_only"] and abs(close["size"]) == 14 and D(close["price"]) == 0
    else:
        assert exchange.positions == before["positions"]
        assert set(exchange.orders) == set(before["orders"])
        assert set(exchange.stops) == set(before["stops"])
        assert trade["exits"]["tp"]["id"] == original["exits"]["tp"]["id"]
        assert trade["exits"]["sl"]["id"] == original["exits"]["sl"]["id"]
        if action == "targets":
            order_id = original["exits"]["tp"]["id"]
            expected = D(spec["levels"]["tp"])
            assert D(exchange.orders[order_id]["price"]) == expected
            assert abs(exchange.orders[order_id]["size"]) == 14
            assert exchange.stops == before["stops"]
            assert [(m, p) for m, p, *_ in exchange.writes] == [("PUT", f"/orders/{order_id}")]
        else:
            stop_id = original["exits"]["sl"]["id"]
            assert D(exchange.stops[stop_id]["trigger"]["price"]) == D(spec["price"])
            assert exchange.orders == before["orders"]
            assert [(m, p) for m, p, *_ in exchange.writes] == [
                ("PUT", f"/price_orders/amend/{stop_id}")]


def check_plan(job, planned, engine, exchange):
    assert set(planned) == {"sl", "tp", "rationale"}
    plan = Plan.model_validate(planned)
    plan.validate_side(job["side"], D(job["entry"]))
    assert all(value % D(job["tick"]) == 0 for value in (plan.sl, plan.tp))
    # Independent, predeclared structural envelopes for the constructed series.
    if job["side"] == "long":
        assert D("9.4") <= plan.sl < 10 and D("11.5") <= plan.tp <= D("12.5")
    else:
        assert 10 < plan.sl <= D("10.6") and D("7.5") <= plan.tp <= D("8.5")
    trade = engine.store.trade(job["trade_id"])
    assert set(trade["exits"]) == {"sl", "tp"} and not trade["plan_needed"]
    target = exchange.orders[trade["exits"]["tp"]["id"]]
    assert target["contract"] == job["symbol"]
    assert target["is_reduce_only"] and target["size"] == (-14 if job["side"] == "long" else 14)
    assert target["left"] == 14
    assert D(target["price"]) == plan.tp and target["status"] == "open"
    stop = exchange.stops[trade["exits"]["sl"]["id"]]
    assert stop["initial"]["is_close"] and stop["initial"]["is_reduce_only"]
    assert stop["initial"]["contract"] == job["symbol"]
    assert stop["trigger"]["rule"] == (2 if job["side"] == "long" else 1)
    assert stop["trigger"]["price_type"] == 1
    assert stop["status"] == "open" and D(stop["trigger"]["price"]) == plan.sl
    assert len(exchange.orders) == 2 and len(exchange.stops) == 1


async def run_case(spec, interpreter, planner, run_number=1):
    report = {"case": spec["name"], "run": run_number, "source_kind": "constructed",
                  "semantic_passed": False, "execution_passed": False, "planner_passed": None}
    started = time.monotonic()
    store, exchange = Store(":memory:"), CandleExchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    runner = None
    with patch.object(Gate, "__init__", forbidden_gate):
        try:
            assert EXIT_POLICY == "single_tp_4h_v1", "Not the candidate single-exit engine"
            owned = (await seed_owned(engine, exchange, spec["owned"], spec.get("seed_plan", True))
                     if spec.get("owned") else None)
            messages = {}
            if owned:
                messages[100] = msg(100, f"#INJ 当前价格入场一笔{'多' if spec['owned'] == 'long' else '空'}单")
            for index, text in enumerate(spec.get("texts", [spec.get("text")])):
                mid = spec.get("message_id", 101) + index
                reply = 100 if owned and mid != 100 else (mid - 1 if index else None)
                messages[mid] = msg(mid, text, reply)
                store.event(CHANNEL, mid, text, reply, messages[mid]["source_time"], True,
                            edited_at=time.time() if mid == 100 else None)
            evidence = Evidence(messages, None)
            runner = web.AppRunner(evaluation_app(evidence, engine))
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            url = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
            assert_loopback(url)
            exchange.writes.clear()
            before = deepcopy({"positions": exchange.positions, "orders": exchange.orders, "stops": exchange.stops})
            report["owned_before"] = owned
            with patch.dict(os.environ, TELEGRAM_TRADER_URL=url, TRADER_TOKEN=TOKEN):
                job = await SignalInbox(engine).claim()
                assert job, "Fixture did not produce a claim"
                async with aiohttp.ClientSession() as session:
                    model_started = time.monotonic()
                    async with interpreter.interpret_current(job, 1, session) as result:
                        report["signal_proposal_seconds"] = round(time.monotonic() - model_started, 6)
                        assert result is not None, "No accepted signal tool proposal"
                        report["result"] = result
                        try:
                            check_decisions(spec, result, job, owned, messages)
                            report["semantic_passed"] = True
                        except AssertionError:
                            report["semantic_error"] = "Decision differs from preregistered fixture expectation"
                        # Even a semantically wrong but valid proposal is applied to
                        # FakeExchange so unexpected writes are visible, not hidden.
                        mapping = {d.event_id: d for d in Decisions.model_validate(result).decisions}
                        decisions = [mapping[event["event_id"]] for event in job["events"]]
                        assert ["submit_signal_decisions"] in evidence.calls, "Proposal bypassed HTTP tool"
                        body = {"lease": job["lease"], "result": result}
                        await interpreter.api(session, "POST", "/signals/decisions", json=body)
                        report["signal_apply_seconds"] = round(time.monotonic() - model_started, 6)
                    for _ in range(3):
                        await engine.poll()
                    for event, decision in zip(job["events"], decisions, strict=True):
                        row = store.db.execute("SELECT status,detail FROM events WHERE id=?", (event["event_id"],)).fetchone()
                        expected = decision.action if decision.action in {"ignore"} else "done"
                        assert row["status"] == expected, "Engine rejected or failed the expected operation"
                    check_execution(spec, engine, exchange, before, owned)
                    writes = deepcopy(exchange.writes)
                    await interpreter.api(session, "POST", "/signals/decisions", json=body)
                    assert exchange.writes == writes, "Duplicate apply wrote another order"
                    report["execution_passed"] = True
                    if spec.get("planner"):
                        await wait_for_live_priority()
                        plan_started = time.monotonic()
                        plan_job = await engine.claim_plan()
                        assert plan_job and plan_job["candles_5m"] != plan_job["candles_4h"]
                        report["planner_fixture"] = {key: plan_job[key] for key in ("entry", "tick", "candles_5m", "candles_4h")}
                        async with planner.generate_plan(plan_job, 1) as planned:
                            report["plan_proposal_seconds"] = round(time.monotonic() - plan_started, 6)
                            report["plan"] = planned
                            assert ["submit_exit_plan"] in evidence.calls, "Planner bypassed HTTP tool"
                            await interpreter.api(session, "POST", "/plans", json={
                                **{key: plan_job[key] for key in ("trade_id", "version", "lease")}, "plan": planned})
                            report["plan_apply_seconds"] = round(time.monotonic() - plan_started, 6)
                        check_plan(plan_job, planned, engine, exchange)
                        report["planner_passed"] = True
            report.update(tools=evidence.calls, writes=exchange.writes,
                          positions_after=exchange.positions, owned_after=store.trades())
        except Exception as exc:  # noqa: BLE001 -- independent evaluation evidence, no retry/fallback trade
            report["error_type"] = type(exc).__name__
            if isinstance(exc, AssertionError) and str(exc):
                report["assertion"] = str(exc)
            report.update(writes=exchange.writes, positions_after=exchange.positions,
                          owned_after=store.trades())
            if "evidence" in locals():
                report["tools"] = evidence.calls
            fatal = tuple(module.runtime["WorkerRestartRequired"] for module in (interpreter, planner)
                          if hasattr(module, "runtime"))
            report["fatal_cleanup_failure"] = isinstance(exc, fatal)
        finally:
            if runner:
                await runner.cleanup()
            cache = getattr(web_app, "_cached_build_middleware", None)
            if cache is not None:
                cache.cache_clear()  # isolated, closed per-case apps only
            store.close()
    report["seconds"] = round(time.monotonic() - started, 6)
    report["passed"] = report["semantic_passed"] and report["execution_passed"] and (
        report["planner_passed"] is not False) and "error_type" not in report
    return report


class RecordedProposals:
    """Only replay saved proposals via loopback; never import/call a provider."""

    def __init__(self, record):
        self.record = deepcopy(record)
        self.event_id_mapping = {}

    @staticmethod
    async def api(session, method, path, **kwargs):
        url = os.environ["TELEGRAM_TRADER_URL"]
        assert_loopback(url)
        async with session.request(method, url + path, **kwargs) as response:
            response.raise_for_status()
            return await response.json()

    @asynccontextmanager
    async def interpret_current(self, job, _owner, session):
        assert self.record.get("result") is not None, "Original report has no saved signal proposal"
        result = deepcopy(self.record["result"])
        current = {event["message_id"]: event["event_id"] for event in job["events"]}
        seen = set()
        for decision in result["decisions"]:
            original_id = decision["event_id"]
            channel, message, revision = original_id.split(":")
            message = int(message)
            assert int(channel) == CHANNEL and message in current and message not in seen
            assert len(revision) == 16 and all(char in "0123456789abcdef" for char in revision)
            seen.add(message)
            # Fixture edit timestamps change revision hashes. Preserve message identity,
            # all actions/prices/targets/evidence; only bind the new claimed event ID.
            self.event_id_mapping[original_id] = current[message]
            decision["event_id"] = current[message]
        assert seen == set(current), "Saved proposal does not cover the same fixture messages"
        await self.api(session, "POST", "/signals/proposals", json={"lease": job["lease"], "result": result})
        yield result

    @asynccontextmanager
    async def generate_plan(self, job, _owner):
        assert self.record.get("plan") is not None, "Original report has no saved planner proposal"
        plan = deepcopy(self.record["plan"])
        async with aiohttp.ClientSession() as session:
            await self.api(session, "POST", "/plans/proposals", json={
                **{key: job[key] for key in ("trade_id", "version", "lease")}, "plan": plan})
        yield plan


async def replay_jsonl(source, output_path, only=None):
    """New evidence file; raw original remains untouched, including original failures."""
    assert source.resolve() != output_path.resolve(), "Replay cannot overwrite its source"
    raw = source.read_bytes()
    rows = [(line_number, line, json.loads(line))
            for line_number, line in enumerate(raw.splitlines(), 1) if line.strip()]
    cases = {case["name"]: case for case in CASES}
    selected = [(number, line, record) for number, line, record in rows
                if "case" in record and (not only or record["case"] in only)]
    assert selected and (not only or set(only) == {record["case"] for _, _, record in selected})
    assert all(record["case"] in cases and record.get("source_kind") == "constructed"
               for _, _, record in selected), "Replay accepts only these constructed cases"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    reports = []
    with output_path.open("x") as output, patch.dict(os.environ, EVAL_PRIORITY_FILE=""):
        def emit(value):
            output.write(json.dumps(value, ensure_ascii=False) + "\n")
            output.flush()

        emit({"kind": "manifest", "evaluation_kind": "recorded_tool_proposal_replay",
              "source_path": str(source.resolve()), "source_sha256": hashlib.sha256(raw).hexdigest(),
              "selected_reports": len(selected),
              "scope": "Saved proposals only; recreated constructed memory fixtures and loopback HTTP. No new model, reasoning, image, or exchange validation. Original reports preserved.",
              "source_hashes": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                for path in [*(ROOT / "trader").glob("*.py"), Path(__file__)]}})
        for number, line, record in selected:
            replay = RecordedProposals(record)
            report = await run_case(cases[record["case"]], replay, replay, record["run"])
            report.update(evaluation_kind="recorded_tool_proposal_replay", source_line=number,
                          source_record_sha256=hashlib.sha256(line).hexdigest(),
                          original_status={key: record.get(key) for key in (
                              "passed", "semantic_passed", "execution_passed", "planner_passed", "error_type")},
                          event_id_mapping=replay.event_id_mapping)
            emit(report)
            reports.append(report)
        emit({"kind": "summary", "total": len(reports), "passed": sum(row["passed"] for row in reports)})
    return 0 if all(row["passed"] for row in reports) else 1


def provider_config(path):
    config = json.loads(path.read_text())
    if "agent_key" in config and "model" not in config:
        config["model"] = config.pop("agent_key")
    assert set(config) == {"model", "base_url", "api_key"}, "Expected LLM-only config keys"
    assert all(isinstance(value, str) and value for value in config.values())
    assert config["model"].startswith("custom:") and "gemini" in config["model"].lower()
    parsed = urlsplit(config["base_url"])
    assert parsed.scheme in {"http", "https"} and parsed.hostname
    assert not parsed.username and not parsed.password and not parsed.query and not parsed.fragment
    for key, value in os.environ.items():
        assert not (value and key.startswith(("GATE_", "TELEGRAM_API_", "TELEGRAM_SESSION", "TG_")))
        assert not (value and key in {"TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "BOT_TOKEN", "DATABASE_URL"})
    assert os.environ.get("TRADER_TOKEN", TOKEN) == TOKEN, "Do not mount a production trader token"
    return config


@contextmanager
def candidate_provider(config):
    from condor.agents.agent import Agent, AgentStore, parse_frontmatter
    from condor.runtime import llm_client

    metadata, instructions = parse_frontmatter((ROOT / "condor-assets/telegram_agent/AGENT.md").read_text())
    agent = Agent(slug="telegram_agent", name=metadata["name"], instructions=instructions,
                  agent_key=config["model"], server_required=False)
    original = llm_client.build_llm_client

    def build(key, **kwargs):
        assert key == config["model"]
        assert_loopback(os.environ["TELEGRAM_TRADER_URL"])
        servers = kwargs["mcp_servers"]
        assert len(servers) == 1
        assert servers[0]["name"] in {"telegram_channel_context", "telegram_exit_plan"}
        assert Path(servers[0]["args"][0]).resolve() == ROOT / "condor-assets/telegram_agent/channel_tools.py"
        return original(key, **kwargs)

    with (
        patch.object(Gate, "__init__", forbidden_gate),
        patch.object(AgentStore, "get", lambda _store, slug: deepcopy(agent) if slug == agent.slug else None),
        patch("condor.memory.domain_context", return_value=[]),
        patch.object(llm_client, "resolve_custom_endpoint", return_value=(config["base_url"], config["api_key"])),
        patch.object(llm_client, "build_llm_client", side_effect=build),
    ):
        yield


async def main(args):
    config = provider_config(args.provider_config)
    selected = [case for case in CASES if not args.only or case["name"] in args.only]
    assert selected and (not args.only or set(args.only) == {case["name"] for case in selected})
    assert args.repeat > 0
    routines = ROOT / "condor-assets/telegram_agent/routines"
    interpreter = load("single_exit_interpreter", routines / "signal_interpreter.py")
    planner = load("single_exit_planner", routines / "exit_planner.py")
    files = [path for path in (ROOT / "condor-assets/telegram_agent").rglob("*")
             if path.is_file() and path.suffix in {".py", ".md"}]
    files += [*(ROOT / "trader").glob("*.py"), Path(__file__)]
    metadata = {"kind": "manifest", "model": config["model"], "base_url": config["base_url"],
                    "cases": [case["name"] for case in selected], "repeat": args.repeat,
                    "scope": "Constructed text/candles; real model and HTTP proposal tools; memory-only execution. Not a backtest or live-exchange test.",
                    "source_hashes": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    reports = []
    with args.output.open("x") as output, candidate_provider(config):
        def emit(value):
            output.write(json.dumps(value, ensure_ascii=False) + "\n")
            output.flush()

        emit(metadata)
        for run in range(1, args.repeat + 1):
            for case in selected:
                await wait_for_live_priority()
                report = await run_case(case, interpreter, planner, run)
                report.update(model=config["model"], provider=config["base_url"])
                reports.append(report)
                emit(report)
                print(json.dumps({key: report.get(key) for key in ("case", "run", "passed", "seconds", "error_type")}), flush=True)
                if report.get("fatal_cleanup_failure"):
                    emit({"kind": "aborted", "reason": "Model cleanup failed; no further cases started"})
                    os._exit(70)  # dedicated evaluator process; never overlap an abandoned model task
        emit({"kind": "summary", "total": len(reports), "passed": sum(row["passed"] for row in reports)})
    return 0 if all(row["passed"] for row in reports) else 1


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--provider-config", type=Path)
    mode.add_argument("--replay-jsonl", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--only", nargs="*")
    args = parser.parse_args()
    if args.replay_jsonl:
        if args.repeat != 1:
            parser.error("Offline replay preserves original run numbers; do not specify --repeat")
        raise SystemExit(asyncio.run(replay_jsonl(args.replay_jsonl, args.output, args.only)))
    raise SystemExit(asyncio.run(main(args)))
