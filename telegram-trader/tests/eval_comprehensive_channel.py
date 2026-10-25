"""Opt-in real-model evaluation against the real HTTP app and memory-only exchange.

Channel text is a frozen current-revision snapshot, NOT a point-in-time backtest.
Real sources and constructed cases are reported separately. All market/position
data is simulated. Production access is restricted to GET /channel/... evidence.
"""

import asyncio
import hashlib
import importlib.util
import json
import os
import sys
import time
import traceback
import urllib.request
from copy import deepcopy
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

import aiohttp
from aiohttp import web
from comprehensive_cases import CASES
from comprehensive_channel import SnapshotChannel
from comprehensive_fixtures import FakeExchange

from trader.app import make_app
from trader.engine import Engine
from trader.models import Decisions, Plan, Signal
from trader.store import Store

CHANNEL = -1001793451069
PRODUCTION_URL = os.environ.get("TELEGRAM_TRADER_URL", "").rstrip("/")
PRODUCTION_TOKEN = os.environ.get("TRADER_TOKEN", "")


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source_get(path):
    assert path.startswith("/channel/"), "Evidence reader cannot mutate production"
    request = urllib.request.Request(PRODUCTION_URL + path, method="GET", headers={
        "Authorization": "Bearer " + PRODUCTION_TOKEN,
    })
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read(), response.headers.get_content_type()


def restart_errors(interpreter, planner):
    return tuple(routine.runtime["WorkerRestartRequired"]
                 for routine in (interpreter, planner) if hasattr(routine, "runtime"))


def message(mid, text, reply=None, **fields):
    return dict(message_id=mid, text=text, reply_to_message_id=reply,
                source_time=time.time(), edited_at=None, media=None, context_only=True, **fields)


async def seed(engine, exchange, fixture):
    symbol = fixture["symbol"] + "_USDT"
    exchange.add_contract(symbol, price=fixture.get("market", fixture["entry"]),
                          tick=fixture.get("tick", "0.01"))
    store = engine.store
    eid = store.event(CHANNEL, fixture["source"], "isolated owned entry", None, time.time(), True)
    await engine.process(store.db.execute("SELECT * FROM events WHERE id=?", (eid,)).fetchone(),
                         Signal(action="open", symbol=fixture["symbol"], side=fixture["side"],
                                price=fixture.get("limit")))
    trade = next(t for t in store.trades() if t["symbol"] == symbol)
    if fixture.get("limit") and fixture.get("fill"):
        exchange.fill(trade["entry_id"], fixture["fill"], price=fixture["limit"])
        await engine.poll()
    trade = store.trade(trade["id"])
    if fixture["plan"] and trade["filled"]:
        job = await engine.claim_plan()
        assert job and job["trade_id"] == trade["id"]
        entry, tick = D(trade["entry_price"]), D(trade["tick"])
        direction = 1 if trade["side"] == "long" else -1
        def level(factor):
            return (entry * D(str(factor)) // tick) * tick
        plan = Plan(sl=level(1 - direction * .1), tp1=level(1 + direction * .1),
                    tp2=level(1 + direction * .2), tp1_fraction="0.5", rationale="fixed test setup")
        args = job["trade_id"], job["version"], job["lease"], plan
        await engine.propose_plan(*args)
        await engine.apply_plan(*args)
    trade = store.trade(trade["id"])
    if fixture.get("tp1_done"):
        order = exchange.orders[trade["exits"]["tp1"]["id"]]
        exchange.fill(order["id"], price=order["price"])
        await engine.poll()
    if fixture.get("market_after"):
        exchange.set_market(symbol, price=fixture["market_after"])
    return store.trade(trade["id"])


def assert_decision(actual, expected, aliases, messages):
    allowed = expected["action"]
    assert actual.action in (allowed if isinstance(allowed, list) else [allowed]), actual.model_dump()
    for name in ("symbol", "side"):
        if name in expected:
            assert getattr(actual, name) == expected[name], actual.model_dump()
        if actual.action == "open" and name + "_if_open" in expected:
            assert getattr(actual, name) == expected[name + "_if_open"]
    if "price" in expected:
        assert (actual.price is None if expected["price"] is None
                else actual.price is not None and D(actual.price) == D(expected["price"]))
    target = expected.get("target")
    if actual.action not in {"ignore", "review"}:
        target = target or expected.get("target_if_action")
    if target:
        assert actual.target_trade_id == aliases[target]["id"]
    expected_levels = expected.get("levels", {})
    if "sl" in expected:
        if actual.action == "sl":
            assert D(actual.price) == D(expected["sl"])
        else:
            expected_levels = {"sl": expected["sl"]}
    assert {k: D(v) for k, v in actual.levels.items()} == {k: D(v) for k, v in expected_levels.items()}
    assert actual.evidence_message_ids and all(i in messages for i in actual.evidence_message_ids)


def assert_effects(spec, decisions, before, after, aliases, engine, exchange, event_rows,
                   before_orders=None, before_stops=None):
    """Assert independent expected outcomes, not expectations derived from model output."""
    affected = set()
    for expected, actual, row in zip(spec["expected"], decisions, event_rows, strict=True):
        if spec.get("execution_review"):
            assert row["status"] == (actual.action if actual.action in {"ignore", "review"} else "review")
            assert row["detail"], "Guard must explain why execution was blocked"
            if actual.action not in {"ignore", "review"}:
                guard_reason = {
                    "derived_old_entry_age_guard": "entry signal older than 60 seconds",
                    "derived_TP1_no_plan": "TP allocation not established yet",
                    "derived_opposite_entry_without_close": "same-symbol trade already active",
                }[spec["name"]]
                assert guard_reason in row["detail"], row
        elif actual.action in {"ignore", "review"}:
            assert row["status"] == actual.action
        else:
            assert row["status"] == "done", row
        if expected["action"] in ("ignore", "review") or (
            isinstance(expected["action"], list) and set(expected["action"]) <= {"ignore", "review"}
        ) or spec.get("execution_review"):
            continue
        if actual.action in {"ignore", "review"}:
            continue
        if actual.action == "open":
            symbol = expected["symbol"] + "_USDT"
            affected.add(symbol)
            trades = [t for t in engine.store.trades() if t["symbol"] == symbol]
            assert len(trades) == 1 and row["status"] == "done", (trades, row)
            t = trades[0]
            contract = exchange.contracts[symbol]
            price = D(expected["price"] or exchange.quotes[symbol]["last"])
            quantity = int(D(100) * D("0.2") * 10 / (price * D(contract["quanto_multiplier"])))
            order = exchange.orders[t["entry_id"]]
            assert order["size"] == quantity * (1 if expected["side"] == "long" else -1)
            assert D(order["price"]) == D(expected["price"] or 0)
            assert D(t["margin_budget"]) == 20
            assert exchange.positions[symbol]["lever"] == "10"
            assert exchange.positions[symbol]["pos_margin_mode"] == "isolated"
            assert after[symbol]["size"] == (0 if expected["price"] else order["size"])
            assert {k: D(v) for k, v in t.get("channel_levels", {}).items()} == {
                k: D(v) for k, v in expected.get("levels", {}).items()}
            continue
        alias = expected.get("target") or expected.get("target_if_action")
        original = aliases[alias]
        symbol = original["symbol"]
        affected.add(symbol)
        current = engine.store.trade(original["id"])
        if actual.action == "cancel":
            assert after[symbol]["size"] == before[symbol]["size"]
            assert exchange.orders[original["entry_id"]]["status"] == "finished"
            for name, exit_ in original["exits"].items():
                source = before_stops if name == "sl" else before_orders
                current_orders = exchange.stops if name == "sl" else exchange.orders
                if source is not None and exit_.get("id"):
                    assert current_orders[exit_["id"]] == source[exit_["id"]]
        elif actual.action == "close":
            assert after[symbol]["size"] == 0 and current["status"] == "closed"
            assert not any(o["status"] == "open" and o["contract"] == symbol
                           for o in exchange.orders.values())
            assert not any(o["status"] == "open" and o["initial"]["contract"] == symbol
                           for o in exchange.stops.values())
        elif actual.action in {"tp1", "tp2"}:
            exit_ = original["exits"][actual.action]
            quantity = 0 if exit_.get("done") else abs(before_orders[exit_["id"]]["left"])
            sign = 1 if before[symbol]["size"] > 0 else -1
            assert after[symbol]["size"] == before[symbol]["size"] - sign * quantity
            pending = [o for o in exchange.orders.values()
                       if o["contract"] == symbol and o["status"] == "open"]
            assert sum(o["left"] for o in pending if o["is_reduce_only"]) == abs(after[symbol]["size"])
            stops = [o for o in exchange.stops.values()
                     if o["initial"]["contract"] == symbol and o["status"] == "open"]
            assert bool(stops) == bool(after[symbol]["size"])
            if not after[symbol]["size"]:
                assert not pending
        else:
            assert after[symbol]["size"] == before[symbol]["size"]
            levels = expected.get("levels", {}) or {"sl": expected["sl"]}
            for name, value in levels.items():
                assert D(current["channel_levels"][name]) == D(value)
                exit_ = current["exits"][name]
                if name == "sl":
                    assert exchange.stops[exit_["id"]]["status"] == "open"
                    assert D(exchange.stops[exit_["id"]]["trigger"]["price"]) == D(value)
                else:
                    assert exchange.orders[exit_["id"]]["status"] == "open"
                    assert D(exchange.orders[exit_["id"]]["price"]) == D(value)
            for name, exit_ in original["exits"].items():
                if name in levels or not exit_.get("id"):
                    continue
                old_orders = before_stops if name == "sl" else before_orders
                new_orders = exchange.stops if name == "sl" else exchange.orders
                if old_orders is not None:
                    assert new_orders[exit_["id"]] == old_orders[exit_["id"]]
        for method, path, body, _params in exchange.writes:
            if method == "POST" and path == "/orders" and body["contract"] == symbol:
                assert body.get("reduce_only") is True, "Management must never open/reverse a position"
    for symbol in before.keys() | after.keys():
        if symbol not in affected:
            assert before[symbol] == after[symbol], ("unrelated position changed", symbol)
            if before_orders is not None:
                assert {k: v for k, v in before_orders.items() if v["contract"] == symbol} == {
                    k: v for k, v in exchange.orders.items() if v["contract"] == symbol}
            if before_stops is not None:
                assert {k: v for k, v in before_stops.items() if v["initial"]["contract"] == symbol} == {
                    k: v for k, v in exchange.stops.items() if v["initial"]["contract"] == symbol}
    if not affected:
        assert not exchange.writes, "No-op/review/guarded case must have zero exchange writes"
    assert not engine.store.unknowns()


def assert_initial_plan(trade, planned, exchange):
    """Check native protection against the accepted proposal, not receipt-derived prices."""
    stop = exchange.stops[trade["exits"]["sl"]["id"]]
    assert stop["status"] == "open"
    assert D(stop["trigger"]["price"]) == D(planned["sl"])
    assert stop["trigger"]["rule"] == (2 if trade["side"] == "long" else 1)
    assert stop["trigger"]["price_type"] == 1
    assert stop["initial"]["contract"] == trade["symbol"]
    assert stop["initial"]["is_close"] and stop["initial"]["is_reduce_only"]
    first = int(trade["filled"] * D(planned["tp1_fraction"]))
    for name, size in (("tp1", first), ("tp2", trade["filled"] - first)):
        if size:
            order = exchange.orders[trade["exits"][name]["id"]]
            assert order["status"] == "open"
            assert order["size"] == size * (-1 if trade["side"] == "long" else 1)
            assert order["contract"] == trade["symbol"] and order["is_reduce_only"]
            assert D(order["price"]) == D(planned[name])
        else:
            assert name not in trade["exits"], "Zero allocation must not create an exit order"


async def run_case(spec, interpreter, planner, history, image_cache, run_number):
    store, exchange = Store(":memory:"), FakeExchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    aliases, runner = {}, None
    report = {"case": spec["name"], "run": run_number, "expected": spec["expected"],
              "source_kind": "real_current_revision" if spec["name"].startswith("real_") else "constructed",
              "expectation_scope": spec.get("expectation_scope", "exact_semantics_and_execution"),
              "timing_note": "Signal times include case setup; plan times start at plan claim. model_seconds is legacy end-to-proposal, not pure inference.",
              "semantic_passed": False, "execution_passed": False}
    started = time.monotonic()
    try:
        for fixture in spec["trades"]:
            aliases[fixture["alias"]] = await seed(engine, exchange, fixture)
        for expected in spec["expected"]:
            if expected.get("symbol") and expected["symbol"] + "_USDT" not in exchange.contracts:
                exchange.add_contract(expected["symbol"] + "_USDT",
                                      **spec.get("market", {}).get(expected["symbol"], {}))
        # Control a second unrelated contract even when no action is expected.
        if "SAFE_USDT" not in exchange.contracts:
            exchange.add_contract("SAFE_USDT")
        real_ids = [e for e in spec["events"] if isinstance(e, int)]
        messages = deepcopy({i: m for i, m in history.items() if real_ids and i <= max(real_ids)})
        now = round(time.time() - spec.get("age", 0), 6)
        if real_ids:
            shift = now - max(messages[i]["source_time"] for i in real_ids)
            for m in messages.values():
                m["source_time"] = round(m["source_time"] + shift, 6)
                if m.get("edited_at"):
                    m["edited_at"] = round(m["edited_at"] + shift, 6)
        else:
            for fixture in spec["trades"]:
                messages[fixture["source"]] = message(fixture["source"],
                    f"#{fixture['symbol']} 当前价格开{'多' if fixture['side'] == 'long' else '空'}")
                messages[fixture["source"]]["source_time"] = now - 120
        for extra in spec.get("extra_history", []):
            messages[extra["id"]] = message(extra["id"], extra["text"], extra.get("reply"))
            messages[extra["id"]]["source_time"] = now - 60
        images = dict(image_cache)
        event_ids = []
        for index, event in enumerate(spec["events"]):
            if isinstance(event, int):
                m = messages[event]
                m["edited_at"] = None  # semantic first-arrival simulation of current revision
            else:
                mid = 20000 + index
                m = message(mid, event["text"], event.get("reply"))
                m["source_time"] = round(now - (len(spec["events"]) - index - 1) * .01, 6)
                m["edited_at"] = m["source_time"] if event.get("edited") else None
                if event.get("image"):
                    payload = images.get(event["image"])
                    # Fixture filenames must not disclose the image's meaning to the model.
                    m["media"] = {"id": str(7_000_000_000_000_000_000 + mid), "mime": "image/png",
                                  "size": len(payload[0]) if payload else 123, "grouped_id": None}
                    if payload:
                        images[mid] = payload
                messages[mid] = m
            eid = store.event(CHANNEL, m["message_id"], m["text"], m["reply_to_message_id"],
                              m["source_time"], True, media=m["media"], edited_at=m["edited_at"],
                              entry_eligible=not spec.get("recovered", False))
            event_ids.append(eid)
        channel = SnapshotChannel(messages, images)
        runner = web.AppRunner(make_app(engine, channel, credentials=exchange))
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        os.environ["TELEGRAM_TRADER_URL"] = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
        exchange.writes.clear()
        before = deepcopy(exchange.positions)
        before_orders, before_stops = deepcopy(exchange.orders), deepcopy(exchange.stops)
        report["owned_before"] = aliases
        async with aiohttp.ClientSession() as session:
            job = await interpreter.api(session, "POST", "/signals/claim")
            assert job and len(job["events"]) == len(spec["expected"])
            async with interpreter.interpret_current(job, int(os.environ["ADMIN_USER_ID"]), session) as result:
                report["proposal_ready_seconds"] = round(time.monotonic() - started, 6)
                assert result is not None, "Analysis became stale before an accepted proposal"
                report.update(result=result, model_seconds=round(time.monotonic() - started, 2))
                model = Decisions.model_validate(result)
                mapping = {d.event_id: d for d in model.decisions}
                assert set(mapping) == set(event_ids) and len(mapping) == len(model.decisions)
                decisions = [mapping[eid] for eid in event_ids]
                errors = []
                for actual, expected in zip(decisions, spec["expected"], strict=True):
                    try:
                        assert_decision(actual, expected, aliases, messages)
                    except AssertionError as exc:
                        errors.append(str(exc) or "decision field mismatch")
                report["semantic_errors"] = errors
                report["semantic_passed"] = not errors
                body = {"lease": job["lease"], "result": result}
                await interpreter.api(session, "POST", "/signals/decisions", json=body)
                report["execution_applied_seconds"] = round(time.monotonic() - started, 6)
            for _ in range(3):
                await engine.poll()
            rows = [dict(store.db.execute("SELECT status,detail FROM events WHERE id=?", (eid,)).fetchone())
                    for eid in event_ids]
            report["event_results"] = rows
            writes = deepcopy(exchange.writes)
            await interpreter.api(session, "POST", "/signals/decisions", json=body)
            assert exchange.writes == writes, "Repeated lease caused additional exchange writes"
            assert_effects(spec, decisions, before, exchange.positions, aliases, engine, exchange, rows,
                           before_orders, before_stops)
            if spec["name"] == "derived_fifth_image_explicit_entry":
                assert {"op": "image", "message_id": 20004} in channel.calls, "Fifth source image was not inspected"
            report["execution_passed"] = True
            if spec["name"] in {"real_FIL_market_long", "real_INJ_market_short"} and not errors:
                plan_started = time.monotonic()
                plan_job = await interpreter.api(session, "POST", "/jobs/claim")
                assert plan_job
                async with planner.generate_plan(plan_job, int(os.environ["ADMIN_USER_ID"])) as planned:
                    report["plan_proposal_ready_seconds"] = round(time.monotonic() - plan_started, 6)
                    await interpreter.api(session, "POST", "/plans", json={
                        "trade_id": plan_job["trade_id"], "version": plan_job["version"],
                        "lease": plan_job["lease"], "plan": planned})
                    report["plan_applied_seconds"] = round(time.monotonic() - plan_started, 6)
                trade = store.trade(plan_job["trade_id"])
                assert_initial_plan(trade, planned, exchange)
                report["real_model_exit_plan"] = planned
        report.update(tools=channel.calls, exchange_writes=exchange.writes,
                      positions_after=exchange.positions, owned_after=store.trades())
    except Exception as exc:  # Retain each independent evaluation failure.
        report.update(error=type(exc).__name__ + ": " + str(exc), traceback=traceback.format_exc())
        if "channel" in locals():
            report["tools"] = channel.calls
        if isinstance(exc, restart_errors(interpreter, planner)):
            exc.evaluation_report = report
            raise  # An abandoned model task must not overlap the next case.
    finally:
        report["seconds"] = round(time.monotonic() - started, 2)
        report["passed"] = (report["semantic_passed"] and report["execution_passed"]
                            and report.get("semantic_policy_strict", True) and "error" not in report)
        if runner:
            await runner.cleanup()
        store.close()
    return report


async def main():
    root, snapshot = Path(sys.argv[1]), Path(sys.argv[2])
    selected = sys.argv[3:]
    specs = [c for c in CASES if not selected or c["name"] in selected]
    assert specs and (not selected or set(selected) == {c["name"] for c in specs}), "Unknown/empty selection"
    history = {int(k): v for k, v in json.loads((snapshot / "history.json").read_text()).items()}
    image_cache, manifest = {}, {}
    # Precache requested source images and their reply ancestors before inference.
    ids = {i for c in specs for i in c["events"] if isinstance(i, int)}
    pending = list(ids)
    while pending:
        mid = pending.pop()
        assert mid in history, f"missing frozen source {mid}"
        parent = history[mid].get("reply_to_message_id")
        if parent and parent not in ids:
            ids.add(parent)
            pending.append(parent)
    for mid in sorted(ids):
        m = history[mid]
        manifest[str(mid)] = {"text_sha256": hashlib.sha256(json.dumps(m, sort_keys=True).encode()).hexdigest(),
                              "original_source_time": m["source_time"], "original_edited_at": m.get("edited_at")}
        if m.get("media"):
            current = json.loads(source_get(f"/channel/messages/{mid}")[0])
            assert current["media"] == m["media"] and current["text"] == m["text"], f"snapshot changed {mid}"
            data, mime = source_get(f"/channel/messages/{mid}/image")
            image_cache[mid] = data, mime
            manifest[str(mid)]["image_sha256"] = hashlib.sha256(data).hexdigest()
            (root / f"source-image-{mid}").write_bytes(data)
    for name in ("explicit-short", "commentary"):
        image_cache[name] = (root / "telegram-trader/tests/assets" / (name + ".png")).read_bytes(), "image/png"
    (root / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    from condor.agents.agent import AgentStore
    metadata = {
        "agent_key": AgentStore().get("telegram_agent").agent_key,
        "started_at": time.time(),
        "snapshot_semantics": "Frozen current revisions retimed as first arrivals, not point-in-time history",
        "images": "Selected sources and reply ancestors only; unavailable other historical images fail explicitly",
        "execution": "Real HTTP application, memory-only database and exchange; no live/testnet order access",
        "hashes": {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                   for folder in (root / "telegram-trader/trader", root / "telegram-trader/condor-assets",
                                  root / "telegram-trader/tests")
                   for p in folder.rglob("*") if p.is_file() and p.suffix in {".py", ".md", ".png"}},
    }
    (root / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2))
    os.environ["TRADER_TOKEN"] = "isolated-comprehensive-memory-only"
    interpreter = load("candidate_interpreter", root / "telegram-trader/condor-assets/telegram_agent/routines/signal_interpreter.py")
    planner = load("candidate_planner", root / "telegram-trader/condor-assets/telegram_agent/routines/exit_planner.py")
    reports = []
    for spec in specs:
        try:
            report = await run_case(spec, interpreter, planner, history, image_cache, 1)
        except restart_errors(interpreter, planner) as exc:
            report = exc.evaluation_report
            reports.append(report)
            (root / "results.json").write_text(json.dumps(reports, ensure_ascii=False, indent=2))
            (root / "failure.json").write_text(json.dumps({
                "case": spec["name"], "error": type(exc).__name__,
                "reason": "Model cleanup stalled; evaluation stopped without starting another case",
            }, ensure_ascii=False, indent=2))
            print(json.dumps({"case": spec["name"], "passed": False,
                              "error": type(exc).__name__, "fatal": True}), flush=True)
            # asyncio.run would cancel and await the very SDK cleanup that is
            # already stuck. This is the dedicated evaluation process boundary.
            os._exit(70)
        reports.append(report)
        (root / "results.json").write_text(json.dumps(reports, ensure_ascii=False, indent=2))
        print(json.dumps({k: report.get(k) for k in ("case", "passed", "semantic_passed", "execution_passed", "seconds", "error")}), flush=True)
    (root / "done.json").write_text(json.dumps({"total": len(reports), "passed": sum(r["passed"] for r in reports)}))
    return 0 if all(r["passed"] for r in reports) else 1


if __name__ == "__main__":
    from condor.agents.agent import AgentStore, parse_frontmatter

    os.umask(0o077)
    os.setpriority(os.PRIO_PROCESS, 0, 19)  # keep production workers above evaluation work
    candidate = Path(sys.argv[1]) / "telegram-trader/condor-assets/telegram_agent/AGENT.md"
    _, instructions = parse_frontmatter(candidate.read_text())
    original_get = AgentStore.get

    def candidate_agent(store, slug):
        agent = deepcopy(original_get(store, slug))
        if slug == "telegram_agent" and agent is not None:
            agent.instructions = instructions
        return agent

    with patch.object(AgentStore, "get", candidate_agent):
        raise SystemExit(asyncio.run(main()))
