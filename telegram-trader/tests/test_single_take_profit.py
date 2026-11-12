"""Single four-hour target policy: local SQLite and in-memory exchange only."""

import copy

import pytest
from comprehensive_fixtures import FakeExchange
from test_comprehensive_execution import decision, entry, remaining_targets

from trader.engine import EXIT_POLICY, Engine
from trader.gate import GateError
from trader.models import Plan
from trader.store import Store


@pytest.fixture
def rig():
    store, exchange = Store(":memory:"), FakeExchange(equity="70")
    exchange.add_contract("INJ_USDT")
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield store, exchange, engine
    store.close()


def single_plan(side="short"):
    return Plan(
        sl="9" if side == "long" else "11",
        tp="12" if side == "long" else "8",
        rationale="5m invalidation for SL; 4h resistance/support for the entire position",
    )


async def install(rig):
    store, _exchange, engine = rig
    job = await engine.claim_plan()
    assert job is not None
    plan = single_plan(job["side"])
    args = job["trade_id"], job["version"], job["lease"], plan
    await engine.propose_plan(*args)
    await engine.apply_plan(*args)
    return store.trade(job["trade_id"])


async def settle(engine, count=6):
    for _ in range(count):
        await engine.poll()


@pytest.mark.parametrize("side", ["long", "short"])
async def test_single_target_installs_after_unchanged_full_position_stop(rig, side):
    store, exchange, _engine = rig
    trade = await entry(rig, side)
    assert trade["exit_policy"] == EXIT_POLICY == "single_tp_4h_v1"
    before = len(exchange.writes)
    trade = await install(rig)
    assert list(trade["exits"]) == ["sl", "tp"]
    assert trade["exits"]["tp"]["size"] == 14
    target = exchange.orders[trade["exits"]["tp"]["id"]]
    assert target["size"] == (-14 if side == "long" else 14)
    assert target["price"] == ("12" if side == "long" else "8")
    assert target["is_reduce_only"] and target["tif"] == "gtc"
    stop = exchange.stops[trade["exits"]["sl"]["id"]]
    assert stop["initial"]["close"] and stop["initial"]["reduce_only"]
    assert stop["initial"]["size"] == 0 and stop["initial"]["price"] == "0"
    assert stop["trigger"]["price"] == ("9" if side == "long" else "11")
    assert stop["trigger"]["rule"] == (2 if side == "long" else 1)
    assert stop["trigger"]["price_type"] == 1
    assert [path for method, path, *_ in exchange.writes[before:] if method == "POST"] == [
        "/price_orders", "/orders",
    ]
    assert not store.unknowns() and not trade["plan_needed"]


async def test_claim_uses_single_schema_and_both_timeframes(rig, monkeypatch):
    _store, exchange, engine = rig
    await entry(rig)
    calls = []
    candles = exchange.candles

    async def record(symbol, interval="5m"):
        calls.append((symbol, interval))
        return await candles(symbol, interval=interval)

    monkeypatch.setattr(exchange, "candles", record)
    job = await engine.claim_plan()
    assert job["exit_policy"] == EXIT_POLICY == "single_tp_4h_v1"
    assert set(job["output_schema"]["properties"]) == {"sl", "tp", "rationale"}
    assert set(job["output_schema"]["required"]) == {"sl", "tp", "rationale"}
    assert sorted(calls) == [("INJ_USDT", "4h"), ("INJ_USDT", "5m")]
    assert job["candles_5m"] and job["candles_4h"]


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("stage", ["tp1", "tp2"])
@pytest.mark.parametrize("planned", [False, True])
async def test_current_channel_tp_stage_closes_all_once(rig, side, stage, planned):
    store, exchange, engine = rig
    trade = await entry(rig, side)
    if planned:
        trade = await install(rig)
    assert (await decision(rig, stage, trade=trade))["status"] == "done"
    await settle(engine)
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert store.trade(trade["id"])["status"] == "closed"
    assert not remaining_targets(exchange)
    assert not [stop for stop in exchange.stops.values() if stop["status"] == "open"]
    before = len(exchange.writes)
    await decision(rig, "tp1", trade=trade)
    await decision(rig, "tp2", trade=trade)
    assert len(exchange.writes) == before
    assert not store.unknowns()


@pytest.mark.parametrize("stage", ["tp1", "tp2"])
async def test_current_tp_partial_ioc_finishes_after_restart(rig, stage):
    store, exchange, _engine = rig
    trade = await entry(rig)
    trade = await install(rig)
    exchange.set_next_market_fill("INJ_USDT", 3)
    assert (await decision(rig, stage, trade=trade))["status"] == "done"
    assert abs(exchange.positions["INJ_USDT"]["size"]) == 11
    assert store.trade(trade["id"])["close_requested"]
    await decision(rig, "tp1", trade=trade)
    assert -11 <= exchange.positions["INJ_USDT"]["size"] <= 0
    await settle(Engine(store, exchange, enabled=True))
    assert store.trade(trade["id"])["status"] == "closed"
    assert exchange.positions["INJ_USDT"]["size"] == 0
    reductions = [order for order in exchange.orders.values()
                  if order["is_reduce_only"] and order["price"] == "0"]
    assert sum(abs(order["size"]) - order["left"] for order in reductions) == 14


@pytest.mark.parametrize("name", ["tp1", "tp2"])
@pytest.mark.parametrize("side", ["long", "short"])
async def test_numeric_tp_updates_existing_single_order_only(rig, name, side):
    store, exchange, _engine = rig
    trade = await entry(rig, side)
    trade = await install(rig)
    target_id = trade["exits"]["tp"]["id"]
    stop = copy.deepcopy(exchange.stops[trade["exits"]["sl"]["id"]])
    new_price = "11.5" if side == "long" else "8.5"
    before = len(exchange.writes)
    result = await decision(rig, "targets", trade=trade, levels={name: new_price})
    assert result["status"] == "done", result
    assert exchange.writes[before:] == [("PUT", f"/orders/{target_id}", {"price": new_price}, None)]
    assert store.trade(trade["id"])["exits"]["tp"]["id"] == target_id
    assert exchange.orders[target_id]["price"] == new_price
    assert exchange.orders[target_id]["size"] == (-14 if side == "long" else 14)
    assert exchange.stops[stop["id"]] == stop


async def test_conflicting_channel_targets_do_not_partially_apply_or_replace_stop(rig):
    store, exchange, _engine = rig
    trade = await entry(rig)
    trade = await install(rig)
    before = len(exchange.writes)
    levels = copy.deepcopy(trade["channel_levels"])
    result = await decision(rig, "targets", trade=trade,
                            levels={"sl": "10.5", "tp1": "9", "tp2": "8"})
    assert result["status"] == "error"
    assert len(exchange.writes) == before
    assert store.trade(trade["id"])["channel_levels"] == levels


async def test_equal_named_targets_are_one_numeric_update(rig):
    _store, exchange, _engine = rig
    trade = await entry(rig)
    trade = await install(rig)
    before = len(exchange.writes)
    result = await decision(rig, "targets", trade=trade, levels={"tp1": "8.5", "tp2": "8.5"})
    assert result["status"] == "done", result
    assert len(exchange.writes[before:]) == 1
    assert exchange.orders[trade["exits"]["tp"]["id"]]["price"] == "8.5"


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("at_cost", [False, True])
async def test_numeric_or_cost_stop_amends_only_native_stop(rig, side, at_cost):
    store, exchange, _engine = rig
    trade = await entry(rig, side)
    trade = await install(rig)
    target = copy.deepcopy(exchange.orders[trade["exits"]["tp"]["id"]])
    stop_id = trade["exits"]["sl"]["id"]
    if at_cost:
        exchange.set_market("INJ_USDT", price="11" if side == "long" else "9")
        price = trade["entry_price"]
    else:
        price = "9.5" if side == "long" else "10.5"
    before = len(exchange.writes)
    result = await decision(rig, "sl", trade=trade, price=price,
                            reason="成本损" if at_cost else "SL设置价格")
    assert result["status"] == "done", result
    writes = exchange.writes[before:]
    assert len(writes) == 1
    assert writes[0][:2] == ("PUT", f"/price_orders/amend/{stop_id}")
    assert writes[0][2]["trigger_price"] == price
    assert exchange.orders[target["id"]] == target
    current = store.trade(trade["id"])
    assert current["source_sl"] and current["exits"]["sl"]["price"] == price
    assert not current["close_requested"]
    assert abs(exchange.positions["INJ_USDT"]["size"]) == 14


@pytest.mark.parametrize("side", ["long", "short"])
async def test_immediate_stop_exit_is_full_market_close_not_a_stop_amendment(rig, side):
    store, exchange, engine = rig
    trade = await entry(rig, side)
    trade = await install(rig)
    before = len(exchange.writes)
    result = await decision(rig, "close", trade=trade, reason="SL离场，当前立即退出")
    assert result["status"] == "done", result
    immediate = exchange.writes[before:]
    assert not [write for write in immediate if write[0] == "PUT"]
    reductions = [body for method, path, body, _params in immediate
                  if method == "POST" and path == "/orders"]
    assert len(reductions) == 1
    assert reductions[0]["price"] == "0" and reductions[0]["reduce_only"]
    assert abs(reductions[0]["size"]) == 14
    await settle(engine)
    assert store.trade(trade["id"])["status"] == "closed"
    assert exchange.positions["INJ_USDT"]["size"] == 0


@pytest.mark.parametrize("committed", [False, True])
async def test_single_target_unknown_post_never_duplicates_and_recovers_exact_receipt(
    rig, monkeypatch, committed
):
    store, exchange, engine = rig
    trade = await entry(rig)
    job = await engine.claim_plan()
    plan = single_plan()
    args = job["trade_id"], job["version"], job["lease"], plan
    await engine.propose_plan(*args)
    request = exchange.request
    attempts = 0

    async def lost(method, path, body=None, params=None, **kwargs):
        nonlocal attempts
        if method == "POST" and path == "/orders" and body.get("reduce_only"):
            attempts += 1
            if committed:
                await request(method, path, body=body, params=params, **kwargs)
            raise TimeoutError("offline lost single target receipt")
        return await request(method, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", lost)
    with pytest.raises(TimeoutError):
        await engine.apply_plan(*args)
    assert store.unknowns()
    assert len(exchange.stops) == 1
    monkeypatch.setattr(exchange, "request", request)
    await settle(Engine(store, exchange, enabled=True))
    current = store.trade(trade["id"])
    assert attempts == 1
    targets = [order for order in exchange.orders.values() if order["is_reduce_only"]]
    if committed:
        assert len(targets) == 1 and not store.unknowns()
        assert current["exits"]["tp"]["id"] == targets[0]["id"]
        assert not current["plan_needed"]
    else:
        assert not targets and store.unknowns()
        assert "tp" not in current["exits"]
    assert exchange.stops[current["exits"]["sl"]["id"]]["status"] == "open"
    assert exchange.positions["INJ_USDT"]["size"] == -14


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("racing_fill", [False, True])
async def test_partial_single_tp_fill_closes_tail_and_cancels_pending_entry(
    rig, monkeypatch, side, racing_fill
):
    store, exchange, engine = rig
    trade = await entry(rig, side, limit=True)
    exchange.fill(trade["entry_id"], 4, price="10")
    await engine.poll()
    trade = await install(rig)
    exchange.fill(trade["exits"]["tp"]["id"], 1, price=trade["plan"]["tp"])
    request = exchange.request

    async def race(method, path, body=None, params=None, **kwargs):
        if method == "DELETE" and path == f"/orders/{trade['entry_id']}":
            assert store.trade(trade["id"])["close_requested"]
            if racing_fill:
                exchange.fill(trade["entry_id"], price="10")
        return await request(method, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", race)
    await settle(Engine(store, exchange, enabled=True))
    current = store.trade(trade["id"])
    assert current["close_requested"] and current["status"] == "closed"
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert exchange.orders[trade["entry_id"]]["status"] == "finished"
    assert not remaining_targets(exchange) and not store.unknowns()


@pytest.mark.parametrize("side", ["long", "short"])
async def test_additional_entry_fill_resizes_only_single_target_and_preserves_stop(rig, side):
    store, exchange, engine = rig
    trade = await entry(rig, side, limit=True)
    exchange.fill(trade["entry_id"], 4, price="10")
    await engine.poll()
    trade = await install(rig)
    target_id = trade["exits"]["tp"]["id"]
    stop = copy.deepcopy(exchange.stops[trade["exits"]["sl"]["id"]])
    exchange.fill(trade["entry_id"], 3, price="10")
    before = len(exchange.writes)
    await engine.poll()
    current = store.trade(trade["id"])
    assert current["filled"] == 7 and current["exits"]["tp"]["size"] == 7
    assert current["exits"]["tp"]["id"] == target_id
    assert exchange.orders[target_id]["left"] == 7
    assert exchange.writes[before:] == [
        ("PUT", f"/orders/{target_id}", {"size": -7 if side == "long" else 7}, None),
    ]
    assert exchange.stops[stop["id"]] == stop
    assert not store.unknowns()


@pytest.mark.parametrize("failure", ["timeout", "empty"])
async def test_missing_four_hour_data_does_not_guess_target_or_modify_existing_stop(
    rig, monkeypatch, failure
):
    store, exchange, engine = rig
    trade = await entry(rig)
    assert (await decision(rig, "sl", trade=trade, price="10.5"))["status"] == "done"
    trade = store.trade(trade["id"])
    stop = copy.deepcopy(exchange.stops[trade["exits"]["sl"]["id"]])
    before = len(exchange.writes)
    candles = exchange.candles

    async def unavailable(symbol, interval="5m"):
        if interval == "4h":
            if failure == "timeout":
                raise TimeoutError("offline missing 4h data")
            return []
        return await candles(symbol, interval=interval)

    monkeypatch.setattr(exchange, "candles", unavailable)
    with pytest.raises((TimeoutError, GateError)):
        await engine.claim_plan()
    current = store.trade(trade["id"])
    assert current["plan"] is None and current.get("plan_proposal") is None
    assert current["plan_needed"] and set(current["exits"]) == {"sl"}
    assert exchange.stops[stop["id"]] == stop
    assert len(exchange.writes) == before


async def test_accepted_single_plan_reclaims_without_refetching_candles(rig, monkeypatch):
    store, exchange, engine = rig
    trade = await entry(rig)
    job = await engine.claim_plan()
    plan = single_plan()
    await engine.propose_plan(job["trade_id"], job["version"], job["lease"], plan)
    current = store.trade(trade["id"])
    current["plan_lease"] = 0
    store.save_trade(current)

    async def forbidden(*args, **kwargs):
        raise AssertionError("an accepted proposal must not require fresh market data")

    monkeypatch.setattr(exchange, "candles", forbidden)
    reclaimed = await engine.claim_plan()
    assert reclaimed["accepted_plan"] == plan.model_dump(mode="json")
    await engine.apply_plan(reclaimed["trade_id"], reclaimed["version"], reclaimed["lease"], plan)
    assert set(store.trade(trade["id"])["exits"]) == {"sl", "tp"}
