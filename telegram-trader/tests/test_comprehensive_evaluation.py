"""Test the evaluation oracle itself with deliberately incorrect model/exchange results."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path

import aiohttp
import eval_comprehensive_channel as evaluation
import pytest
from comprehensive_cases import CASES, case, decision, text, trade
from comprehensive_fixtures import FakeExchange
from test_exit_planner import planner as planner  # noqa: PLC0414 -- pytest fixture
from test_interpreter import interpreter as interpreter  # noqa: PLC0414 -- pytest fixture

from trader.engine import Engine
from trader.models import Decision, Signal
from trader.store import Store


@pytest.fixture
def rig():
    store, exchange = Store(":memory:"), FakeExchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield store, exchange, engine
    store.close()


def model_decision(action="open", **fields):
    values = {"event_id": "local-event", "action": action, "reason": "scripted oracle test",
              "evidence_message_ids": [1]}
    values.update(fields)
    return Decision.model_validate(values)


@pytest.mark.parametrize("mutation", [
    {"symbol": "JTO"}, {"side": "long"}, {"price": "11"},
    {"price": None}, {"levels": {"sl": "12"}}, {"evidence_message_ids": [999]},
])
def test_decision_oracle_rejects_wrong_open_parameters_and_unavailable_evidence(mutation):
    expected = {"action": "open", "symbol": "INJ", "side": "short", "price": "10"}
    actual = model_decision(symbol="INJ", side="short", price="10")
    evaluation.assert_decision(actual, expected, {}, {1: {}})
    actual = actual.model_copy(update=mutation)
    with pytest.raises(AssertionError):
        evaluation.assert_decision(actual, expected, {}, {1: {}})


@pytest.mark.parametrize("action", ["close", "cancel", "tp1", "tp2"])
def test_decision_oracle_rejects_wrong_owned_target(action):
    aliases = {"A": {"id": "owned-inj"}, "B": {"id": "owned-jto"}}
    expected = {"action": action, "target": "A"}
    correct = model_decision(action, target_trade_id="owned-inj")
    evaluation.assert_decision(correct, expected, aliases, {1: {}})
    with pytest.raises(AssertionError):
        evaluation.assert_decision(
            correct.model_copy(update={"target_trade_id": "owned-jto"}), expected, aliases, {1: {}}
        )


@pytest.mark.parametrize("mutation", [
    {"levels": {"sl": "11.1", "tp1": "9", "tp2": "8"}},
    {"levels": {"sl": "11", "tp1": "8", "tp2": "9"}},
    {"levels": {"sl": "11", "tp1": "9"}},
])
def test_decision_oracle_rejects_wrong_or_missing_exact_levels(mutation):
    expected = {"action": "targets", "target": "A", "levels": {"sl": "11", "tp1": "9", "tp2": "8"}}
    actual = model_decision("targets", target_trade_id="owned", levels=expected["levels"])
    evaluation.assert_decision(actual, expected, {"A": {"id": "owned"}}, {1: {}})
    with pytest.raises(AssertionError):
        evaluation.assert_decision(actual.model_copy(update=mutation), expected, {"A": {"id": "owned"}}, {1: {}})


@pytest.mark.parametrize("fixture,position,remaining,planned", [
    (trade(), -20, 0, True),
    (trade(limit="10", market="9", fill=0), 0, 20, False),
    (trade(limit="10", market="9", fill=4), -4, 16, True),
    (trade(tp1_done=True), -10, 0, True),
])
async def test_seed_creates_the_declared_owned_state(rig, fixture, position, remaining, planned):
    store, exchange, engine = rig
    owned = await evaluation.seed(engine, exchange, fixture)
    assert owned["channel"] == evaluation.CHANNEL and owned["message"] == fixture["source"]
    assert owned["symbol"] == "INJ_USDT" and owned["side"] == "short"
    assert owned["margin_budget"] == "20.0"
    assert exchange.positions["INJ_USDT"]["size"] == position
    assert exchange.positions["INJ_USDT"]["lever"] == "10"
    assert exchange.positions["INJ_USDT"]["pos_margin_mode"] == "isolated"
    assert exchange.orders[owned["entry_id"]]["left"] == remaining
    assert all(owned["exits"].get(name, {}).get("id") for name in ("sl", "tp1", "tp2")) is planned
    if fixture.get("tp1_done"):
        assert owned["exits"]["tp1"]["done"]
    assert not store.unknowns()


async def effect_fixture(rig, action="sl"):
    store, exchange, engine = rig
    owned = await evaluation.seed(engine, exchange, trade())
    exchange.add_contract("SAFE_USDT")
    before = deepcopy(exchange.positions)
    before_orders, before_stops = deepcopy(exchange.orders), deepcopy(exchange.stops)
    exchange.writes.clear()
    fields = {"price": "10.75"} if action == "sl" else {}
    event_id = store.event(evaluation.CHANNEL, 20000, "scripted management", 100, 1, True)
    event = store.db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    await engine.process(event, Signal(action=action, target_trade_id=owned["id"], **fields))
    actual = model_decision(action, target_trade_id=owned["id"], **fields)
    expected = dict(action=action, target="A", **({"sl": "10.75"} if action == "sl" else {}))
    spec = {"expected": [expected]}
    rows = [dict(store.db.execute("SELECT status,detail FROM events WHERE id=?", (event_id,)).fetchone())]
    args = spec, [actual], before, exchange.positions, {"A": owned}, engine, exchange, rows
    snapshots = {"before_orders": before_orders, "before_stops": before_stops}
    return args, snapshots


async def test_effect_oracle_rejects_tp_reversing_the_remaining_position(rig):
    args, snapshots = await effect_fixture(rig, "tp1")
    evaluation.assert_effects(*args, **snapshots)
    exchange = rig[1]
    assert exchange.positions["INJ_USDT"]["size"] == -10
    exchange.positions["INJ_USDT"]["size"] = 10
    with pytest.raises(AssertionError):
        evaluation.assert_effects(*args, **snapshots)


async def test_tp_quantity_oracle_is_frozen_before_execution(rig):
    args, snapshots = await effect_fixture(rig, "tp1")
    evaluation.assert_effects(*args, **snapshots)
    exchange, original = rig[1], args[4]["A"]
    assert snapshots["before_orders"][original["exits"]["tp1"]["id"]]["left"] == 10
    # Simulate an erroneous five-contract TP and a consistently resized remainder.
    # Reading the expected amount from the mutated old receipt would accept this.
    exchange.orders[original["exits"]["tp1"]["id"]]["size"] = 5
    exchange.positions["INJ_USDT"]["size"] = -15
    exchange.orders[original["exits"]["tp2"]["id"]].update(size=15, left=15)
    with pytest.raises(AssertionError):
        evaluation.assert_effects(*args, **snapshots)


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("mutation", ["sl_price", "tp1_price", "tp2_price", "sl_rule", "tp_side", "contract"])
async def test_initial_plan_oracle_rejects_native_price_direction_or_contract_changes(rig, side, mutation):
    _, exchange, engine = rig
    owned = await evaluation.seed(engine, exchange, trade(side=side))
    evaluation.assert_initial_plan(owned, owned["plan"], exchange)
    stop = exchange.stops[owned["exits"]["sl"]["id"]]
    tp1 = exchange.orders[owned["exits"]["tp1"]["id"]]
    if mutation == "sl_price":
        stop["trigger"]["price"] = "10.75"
    elif mutation in {"tp1_price", "tp2_price"}:
        exchange.orders[owned["exits"][mutation.removesuffix("_price")]["id"]]["price"] = "7.77"
    elif mutation == "sl_rule":
        stop["trigger"]["rule"] = 3 - stop["trigger"]["rule"]
    elif mutation == "tp_side":
        tp1["size"] *= -1
    else:
        tp1["contract"] = "OTHER_USDT"
    with pytest.raises(AssertionError):
        evaluation.assert_initial_plan(owned, owned["plan"], exchange)


async def test_initial_plan_oracle_accepts_zero_first_allocation_but_no_extra_order(rig):
    _, exchange, engine = rig
    owned = await evaluation.seed(engine, exchange, trade(limit="10", market="9", fill=1))
    assert "tp1" not in owned["exits"]
    evaluation.assert_initial_plan(owned, owned["plan"], exchange)
    owned["exits"]["tp1"] = deepcopy(owned["exits"]["tp2"])
    with pytest.raises(AssertionError, match="Zero allocation"):
        evaluation.assert_initial_plan(owned, owned["plan"], exchange)


async def test_effect_oracle_rejects_unrequested_tp_change_during_sl_amendment(rig):
    args, snapshots = await effect_fixture(rig)
    evaluation.assert_effects(*args, **snapshots)
    original = args[4]["A"]
    rig[1].orders[original["exits"]["tp1"]["id"]]["price"] = "8.50"
    with pytest.raises(AssertionError):
        evaluation.assert_effects(*args, **snapshots)


async def test_effect_oracle_rejects_review_status_for_expected_successful_management(rig):
    args, snapshots = await effect_fixture(rig)
    evaluation.assert_effects(*args, **snapshots)
    args[-1][0]["status"] = "review"
    with pytest.raises(AssertionError):
        evaluation.assert_effects(*args, **snapshots)


@pytest.mark.parametrize("kind", ["position", "new_order", "new_stop"])
async def test_effect_oracle_rejects_unrelated_contract_side_effects(rig, kind):
    args, snapshots = await effect_fixture(rig)
    evaluation.assert_effects(*args, **snapshots)
    exchange = rig[1]
    if kind == "position":
        exchange.positions["SAFE_USDT"]["size"] = 1
    elif kind == "new_order":
        await exchange.request("POST", "/orders", {
            "contract": "SAFE_USDT", "size": 1, "price": "9", "tif": "gtc", "reduce_only": False,
        })
    else:
        exchange.stops["untracked-stop"] = {
            "id": "untracked-stop", "initial": {"contract": "SAFE_USDT"}, "status": "open",
        }
    with pytest.raises(AssertionError):
        evaluation.assert_effects(*args, **snapshots)


async def test_effect_oracle_rejects_any_exchange_write_for_noop(rig):
    _, exchange, engine = rig
    exchange.add_contract("SAFE_USDT")
    before = deepcopy(exchange.positions)
    args = ({"expected": [{"action": "ignore"}]}, [model_decision("ignore")],
            before, exchange.positions, {}, engine, exchange, [{"status": "ignore"}])
    evaluation.assert_effects(*args)
    exchange.writes.append(("POST", "/orders", {"contract": "SAFE_USDT"}, None))
    with pytest.raises(AssertionError, match="zero exchange writes"):
        evaluation.assert_effects(*args)


@pytest.mark.parametrize("invalid_row", [
    {"status": "done", "detail": "misreported success"},
    {"status": "ignore", "detail": "wrong execution outcome"},
    {"status": "review", "detail": ""},
])
async def test_guarded_case_requires_explicit_rejection_not_done_or_silent_ignore(rig, invalid_row):
    _, exchange, engine = rig
    exchange.add_contract("INJ_USDT")
    before = deepcopy(exchange.positions)
    spec = {"name": "derived_old_entry_age_guard", "execution_review": True,
            "expected": [{"action": "open", "symbol": "INJ", "side": "short", "price": None}]}
    rows = [{"status": "review", "detail": "entry signal older than 60 seconds; no new entry"}]
    args = spec, [model_decision(symbol="INJ", side="short")], before, exchange.positions, {}, engine, exchange, rows
    evaluation.assert_effects(*args)
    rows[0] = invalid_row
    with pytest.raises(AssertionError):
        evaluation.assert_effects(*args)


async def test_correct_sl_price_is_not_success_when_native_order_is_inactive(rig):
    args, snapshots = await effect_fixture(rig)
    evaluation.assert_effects(*args, **snapshots)
    original = args[4]["A"]
    rig[1].stops[original["exits"]["sl"]["id"]]["status"] = "finished"
    with pytest.raises(AssertionError):
        evaluation.assert_effects(*args, **snapshots)


SMOKES = [
    ("derived_limit_short_exact_price", {"action": "open", "symbol": "INJ", "side": "short", "price": "8.63"}),
    ("derived_cancel_only_unfilled", {"action": "cancel", "target_symbol": "INJ_USDT"}),
    ("derived_amend_SL", {"action": "sl", "target_symbol": "INJ_USDT", "price": "10.75"}),
    ("local_TP1", {"action": "tp1", "target_symbol": "INJ_USDT"}),
    ("derived_negated_close", {"action": "ignore"}),
    ("derived_opposite_entry_without_close", {"action": "review"}),
    ("derived_opposite_entry_without_close", {"action": "open", "symbol": "INJ", "side": "short"}),
    ("derived_close_then_reverse_compound", {"action": "review"}),
    ("derived_unsupported_TP3_price_update", {"action": "review"}),
]


@pytest.mark.parametrize("name,fields", SMOKES)
async def test_run_case_uses_real_http_and_rejects_no_spurious_failures(
    monkeypatch, interpreter, name, fields
):
    monkeypatch.setenv("TRADER_TOKEN", "harness-local-only")
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    # Restore the environment mutated by run_case, even on failures.
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(evaluation, "source_get", lambda path: pytest.fail("unexpected production read"))
    if name == "local_TP1":
        spec = case(name, [text("INJ 当前价格执行TP1", 100)], [decision("tp1", target="A")], [trade()])
    else:
        spec = deepcopy(next(item for item in CASES if item["name"] == name))
    submissions = []

    @asynccontextmanager
    async def scripted_model(job, owner, session):
        values = dict(fields)
        target_symbol = values.pop("target_symbol", None)
        if target_symbol:
            values["target_trade_id"] = next(t["id"] for t in job["trades"] if t["symbol"] == target_symbol)
        item = model_decision(**values).model_dump()
        item.update(event_id=job["events"][0]["event_id"],
                    evidence_message_ids=[job["events"][0]["message_id"]])
        result = {"decisions": [item]}
        await interpreter.api(session, "POST", "/signals/proposals", json={"lease": job["lease"], "result": result})
        submissions.append(result)
        yield result

    monkeypatch.setattr(interpreter, "interpret_current", scripted_model)
    report = await evaluation.run_case(spec, interpreter, None, {}, {}, 1)
    assert report["passed"], report
    assert report["semantic_passed"] and report["execution_passed"]
    assert len(submissions) == 1
    assert any(call["op"] == "authority" for call in report["tools"])
    if name in {"derived_opposite_entry_without_close", "derived_close_then_reverse_compound",
                "derived_unsupported_TP3_price_update"}:
        assert not report["exchange_writes"]
        assert report["positions_after"]["INJ_USDT"]["size"] == (
            -20 if name == "derived_unsupported_TP3_price_update" else 20)
    if name == "derived_opposite_entry_without_close":
        assert report["expectation_scope"] == "safe_rejection_not_semantic_perfection"


async def test_run_case_reports_wrong_model_direction_as_failure(monkeypatch, interpreter):
    monkeypatch.setenv("TRADER_TOKEN", "harness-local-only")
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(evaluation, "source_get", lambda path: pytest.fail("unexpected production read"))
    spec = deepcopy(next(item for item in CASES if item["name"] == "derived_limit_short_exact_price"))

    @asynccontextmanager
    async def wrong_model(job, owner, session):
        actual = model_decision(symbol="INJ", side="long", price="8.63").model_dump()
        actual.update(event_id=job["events"][0]["event_id"],
                      evidence_message_ids=[job["events"][0]["message_id"]])
        result = {"decisions": [actual]}
        await interpreter.api(session, "POST", "/signals/proposals", json={"lease": job["lease"], "result": result})
        yield result

    monkeypatch.setattr(interpreter, "interpret_current", wrong_model)
    report = await evaluation.run_case(spec, interpreter, None, {}, {}, 1)
    assert not report["passed"] and not report["semantic_passed"]
    assert report["semantic_errors"]
    assert not report["execution_passed"]


async def test_image_fixture_names_never_leak_into_model_job(monkeypatch, interpreter):
    monkeypatch.setenv("TRADER_TOKEN", "harness-local-only")
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(evaluation, "source_get", lambda path: pytest.fail("unexpected production read"))
    names = ("explicit-short", "commentary")
    images = {name: ((Path(__file__).parent / "assets" / f"{name}.png").read_bytes(), "image/png")
              for name in names}
    spec = case("local_opaque_media_metadata", [text("", image=name) for name in names],
                [decision("ignore") for _ in names])

    @asynccontextmanager
    async def inspect_job(job, _owner, session):
        encoded = json.dumps(job)
        assert all(name not in encoded for name in names)
        identifiers = [event["media"]["id"] for event in job["events"]]
        assert len(set(identifiers)) == len(names)
        assert all(isinstance(identifier, str) and identifier.isdecimal()
                   and 0 < int(identifier) < 2**63 for identifier in identifiers)
        items = []
        for name, event in zip(names, job["events"], strict=True):
            # Resolve by opaque Telegram message ID; private fixture lookup stays intact.
            async with session.get(
                os.environ["TELEGRAM_TRADER_URL"] + f"/channel/messages/{event['message_id']}/image",
                headers={"Authorization": "Bearer harness-local-only"},
            ) as response:
                assert response.status == 200
                assert await response.read() == images[name][0]
            item = model_decision("ignore").model_dump()
            item.update(event_id=event["event_id"], evidence_message_ids=[event["message_id"]])
            items.append(item)
        result = {"decisions": items}
        await interpreter.api(session, "POST", "/signals/proposals", json={"lease": job["lease"], "result": result})
        yield result

    monkeypatch.setattr(interpreter, "interpret_current", inspect_job)
    report = await evaluation.run_case(spec, interpreter, None, {}, images, 1)
    assert report["passed"], report.get("traceback", report)


@pytest.mark.parametrize("name,fields", [
    ("derived_opposite_entry_without_close", {"action": "close"}),
    ("derived_opposite_entry_without_close", {"action": "open", "symbol": "INJ", "side": "long"}),
    ("derived_close_then_reverse_compound", {"action": "close"}),
    ("derived_unsupported_TP3_price_update", {"action": "targets", "levels": {"tp2": "7.50"}}),
    ("derived_unsupported_TP3_price_update", {"action": "ignore"}),
])
async def test_oracles_reject_wrong_direction_partial_instruction_or_unsupported_target_mapping(
    monkeypatch, interpreter, name, fields,
):
    monkeypatch.setenv("TRADER_TOKEN", "harness-local-only")
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(evaluation, "source_get", lambda path: pytest.fail("unexpected production read"))
    spec = deepcopy(next(item for item in CASES if item["name"] == name))

    @asynccontextmanager
    async def wrong_model(job, _owner, session):
        values = dict(fields)
        if values["action"] in {"close", "targets"}:
            values["target_trade_id"] = job["trades"][0]["id"]
        actual = model_decision(**values).model_dump()
        actual.update(event_id=job["events"][0]["event_id"],
                      evidence_message_ids=[job["events"][0]["message_id"]])
        result = {"decisions": [actual]}
        await interpreter.api(session, "POST", "/signals/proposals", json={"lease": job["lease"], "result": result})
        yield result

    monkeypatch.setattr(interpreter, "interpret_current", wrong_model)
    report = await evaluation.run_case(spec, interpreter, None, {}, {}, 1)
    assert not report["passed"] and not report["semantic_passed"], report


def test_real_profit_only_oracle_never_accepts_the_displayed_position_as_an_entry():
    spec = next(item for item in CASES if item["name"] == "real_JUP_profit_screenshot_only")
    assert spec["events"] == [15361] and not spec["trades"]
    # Frozen caption: "JUP 空單賺錢💸". The side describes performance, not an order.
    ignored = model_decision("ignore", evidence_message_ids=[15361])
    evaluation.assert_decision(ignored, spec["expected"][0], {}, {15361: {}})
    opening = model_decision("open", symbol="JUP", side="short", evidence_message_ids=[15361])
    with pytest.raises(AssertionError):
        evaluation.assert_decision(opening, spec["expected"][0], {}, {15361: {}})


async def test_harness_applies_signal_and_plan_before_each_model_cleanup(
    monkeypatch, interpreter, planner,
):
    monkeypatch.setenv("TRADER_TOKEN", "harness-local-only")
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(evaluation, "source_get", lambda path: pytest.fail("unexpected production read"))
    applied = {name: asyncio.Event() for name in ("signal", "plan")}
    release = {name: asyncio.Event() for name in applied}
    cleaned = dict.fromkeys(applied, False)
    actual_api = interpreter.api

    async def tracked_api(session, method, path, **kwargs):
        response = await actual_api(session, method, path, **kwargs)
        phase = {"/signals/decisions": "signal", "/plans": "plan"}.get(path)
        if phase and not applied[phase].is_set():
            assert not cleaned[phase], "Execution incorrectly waited for model cleanup"
            applied[phase].set()
        return response

    async def generated_decisions(job, _owner, session, *_args):
        item = model_decision(symbol="INJ", side="short").model_dump()
        item.update(event_id=job["events"][0]["event_id"], evidence_message_ids=[15333])
        await actual_api(session, "POST", "/signals/proposals", json={
            "lease": job["lease"], "result": {"decisions": [item]},
        })
        try:
            await asyncio.Event().wait()
        finally:
            await release["signal"].wait()
            cleaned["signal"] = True

    async def generated_plan(job, _owner, *_args):
        async with aiohttp.ClientSession() as session:
            await actual_api(session, "POST", "/plans/proposals", json={
                "trade_id": job["trade_id"], "version": job["version"], "lease": job["lease"],
                "plan": {"sl": "11", "tp1": "9", "tp2": "8", "tp1_fraction": "0.5", "rationale": "local lifecycle self-test"},
            })
        try:
            await asyncio.Event().wait()
        finally:
            await release["plan"].wait()
            cleaned["plan"] = True

    monkeypatch.setattr(interpreter, "api", tracked_api)
    monkeypatch.setattr(interpreter, "generate_decisions", generated_decisions)
    monkeypatch.setattr(planner, "request_plan", generated_plan)
    spec = deepcopy(next(item for item in CASES if item["name"] == "real_INJ_market_short"))
    history = {15333: evaluation.message(15333, "#INJ 当前价格入场一笔空单")}
    task = asyncio.create_task(evaluation.run_case(spec, interpreter, planner, history, {}, 1))
    try:
        for phase in ("signal", "plan"):
            await asyncio.wait_for(applied[phase].wait(), 2)
            assert not cleaned[phase] and not task.done()
            release[phase].set()
        report = await asyncio.wait_for(task, 2)
    finally:
        for event in release.values():
            event.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert report["passed"], report.get("traceback", report)
    assert all(cleaned.values())
    assert report["proposal_ready_seconds"] <= report["execution_applied_seconds"]
    assert report["plan_proposal_ready_seconds"] <= report["plan_applied_seconds"]
    assert report["execution_applied_seconds"] <= report["seconds"] + 0.01
