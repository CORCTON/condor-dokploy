"""Full-position exits: real engine/store, isolated fake exchange only."""

import pytest
from test_comprehensive_execution import decision, entry, remaining_targets
from test_comprehensive_execution import strict_rig as strict_rig  # noqa: PLC0414

from trader.engine import Engine
from trader.models import Plan

POLICY = "single_tp_4h_v1"


def plan_for(side):
    stop, target = ("9", "12") if side == "long" else ("11", "8")
    return Plan(sl=stop, tp=target, rationale="offline full-position take-profit policy regression")


async def apply_current_plan(rig):
    store, _exchange, engine = rig
    job = await engine.claim_plan()
    assert job is not None
    args = job["trade_id"], job["version"], job["lease"], plan_for(job["side"])
    await engine.propose_plan(*args)
    await engine.apply_plan(*args)
    return store.trade(job["trade_id"])


async def settle(rig, count=6):
    for _ in range(count):
        await rig[2].poll()


def market_reductions(exchange):
    return [order for order in exchange.orders.values()
            if order["price"] == "0" and order["is_reduce_only"]]


@pytest.mark.parametrize("side", ["long", "short"])
async def test_new_trade_full_position_target_and_claim_schema(strict_rig, side):
    store, exchange, engine = strict_rig
    trade = await entry(strict_rig, side)
    assert trade["exit_policy"] == POLICY
    job = await engine.claim_plan()
    assert set(job["output_schema"]["properties"]) == {"sl", "tp", "rationale"}
    args = job["trade_id"], job["version"], job["lease"], plan_for(side)
    await engine.propose_plan(*args)
    await engine.apply_plan(*args)
    trade = store.trade(trade["id"])
    assert set(trade["exits"]) == {"sl", "tp"}
    assert trade["exits"]["tp"]["size"] == 14
    assert remaining_targets(exchange) == 14


async def test_single_plan_rejects_partial_fraction_before_writes(strict_rig):
    _store, exchange, engine = strict_rig
    await entry(strict_rig)
    await engine.claim_plan()
    before = len(exchange.writes)
    with pytest.raises(ValueError):
        Plan.model_validate(plan_for("short").model_dump() | {"tp1_fraction": "0.5"})
    assert len(exchange.writes) == before


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("planned", [False, True])
async def test_tp2_first_closes_all_with_or_without_plan_and_repeats_are_noops(
    strict_rig, side, planned
):
    store, exchange, _engine = strict_rig
    trade = await entry(strict_rig, side)
    if planned:
        trade = await apply_current_plan(strict_rig)
    assert (await decision(strict_rig, "tp2", trade=trade))["status"] == "done"
    assert exchange.positions["INJ_USDT"]["size"] == 0
    await settle(strict_rig)
    assert store.trade(trade["id"])["status"] == "closed"
    before = len(exchange.writes)
    await decision(strict_rig, "tp2", trade=trade)
    await decision(strict_rig, "tp1", trade=trade)
    assert len(exchange.writes) == before
    assert not remaining_targets(exchange)
    assert not store.unknowns()


@pytest.mark.parametrize("side", ["long", "short"])
async def test_tp2_partial_ioc_persists_full_exit_and_late_tp1_cannot_reduce_again(strict_rig, side):
    store, exchange, _engine = strict_rig
    trade = await entry(strict_rig, side)
    trade = await apply_current_plan(strict_rig)
    exchange.set_next_market_fill("INJ_USDT", 2)
    await decision(strict_rig, "tp2", trade=trade)
    assert abs(exchange.positions["INJ_USDT"]["size"]) == 12
    assert store.trade(trade["id"])["close_requested"]
    await decision(strict_rig, "tp1", trade=trade)
    assert abs(exchange.positions["INJ_USDT"]["size"]) <= 12
    restarted = Engine(store, exchange, enabled=True)
    await settle((store, exchange, restarted))
    assert store.trade(trade["id"])["status"] == "closed"
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert sum(abs(order["size"]) - order["left"] for order in market_reductions(exchange)) == 14


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("initial_fill,racing_fill", [(14, False), (4, False), (4, True), (1, True)])
async def test_any_native_tp2_fill_persists_full_exit_across_entry_cancel_race(
    strict_rig, monkeypatch, side, initial_fill, racing_fill
):
    store, exchange, _engine = strict_rig
    trade = await entry(strict_rig, side, limit=True)
    exchange.fill(trade["entry_id"], initial_fill, price="10")
    await settle(strict_rig, 1)
    trade = await apply_current_plan(strict_rig)
    exchange.fill(trade["exits"]["tp"]["id"], 1, price=trade["plan"]["tp"])
    request = exchange.request

    async def race(method, path, body=None, params=None, **kwargs):
        if method == "DELETE" and path == f"/orders/{trade['entry_id']}":
            assert store.trade(trade["id"])["close_requested"]
            if racing_fill:
                exchange.fill(trade["entry_id"], price="10")
        return await request(method, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", race)
    await settle(strict_rig)
    current = store.trade(trade["id"])
    assert current["close_requested"]
    assert current["status"] == "closed"
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert exchange.orders[trade["entry_id"]]["status"] == "finished"
    assert not remaining_targets(exchange)
    assert not store.unknowns()


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize("final_exit", ["native_partial_tp2", "native_full_tp2", "partial_close"])
async def test_final_exit_preempts_uncertain_sl_amendment_and_preserves_unknown(
    strict_rig, monkeypatch, side, committed, final_exit
):
    store, exchange, _engine = strict_rig
    trade = await entry(strict_rig, side)
    trade = await apply_current_plan(strict_rig)
    request = exchange.request
    sl_requests = 0

    async def lose_sl_response(method, path, body=None, params=None, **kwargs):
        nonlocal sl_requests
        if method == "PUT" and path.startswith("/price_orders/amend/"):
            sl_requests += 1
            if committed:
                await request(method, path, body=body, params=params, **kwargs)
            raise TimeoutError("offline ambiguous SL amendment before final exit")
        return await request(method, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", lose_sl_response)
    price = "9.5" if side == "long" else "10.5"
    assert (await decision(strict_rig, "sl", trade=trade, price=price))["status"] == "error"
    unknown_keys = [operation["id"] for operation in store.unknowns()]
    assert len(unknown_keys) == 1
    tp2_fill = 0
    if final_exit == "partial_close":
        exchange.set_next_market_fill("INJ_USDT", 2)
        assert (await decision(strict_rig, "close", trade=trade))["status"] == "done"
        assert abs(exchange.positions["INJ_USDT"]["size"]) == 12
        assert store.trade(trade["id"])["close_requested"]
    else:
        tp2_fill = 1 if final_exit == "native_partial_tp2" else 14
        exchange.set_market("INJ_USDT", price=trade["plan"]["tp"])
        exchange.fill(trade["exits"]["tp"]["id"], tp2_fill, price=trade["plan"]["tp"])

    restarted = Engine(store, exchange, enabled=True)
    await settle((store, exchange, restarted))
    current = store.trade(trade["id"])
    assert current["close_requested"] and current["status"] == "closed"
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert sl_requests == 1
    # No exact amendment-receipt reconciliation exists: closure cannot erase this unknown.
    assert [operation["id"] for operation in store.unknowns()] == unknown_keys
    assert restarted.paused()
    assert sum(abs(order["size"]) - order["left"]
               for order in market_reductions(exchange)) + tp2_fill == 14
    assert not remaining_targets(exchange)
    assert all(stop["status"] == "finished" for stop in exchange.stops.values())


@pytest.mark.parametrize("action", ["tp1", "tp2"])
@pytest.mark.parametrize("committed", [False, True])
async def test_tp_unknown_write_never_blindly_resends_and_reconciles_exact_receipt(
    strict_rig, monkeypatch, committed, action
):
    store, exchange, engine = strict_rig
    trade = await entry(strict_rig)
    request = exchange.request
    exchange.set_next_market_fill("INJ_USDT", 3)
    injected = False

    async def lose_response(method, path, body=None, params=None, **kwargs):
        nonlocal injected
        if not injected and method == "POST" and path == "/orders" and body.get("reduce_only"):
            injected = True
            if committed:
                await request(method, path, body=body, params=params, **kwargs)
            raise TimeoutError("offline ambiguous close response")
        return await request(method, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", lose_response)
    result = await decision(strict_rig, action, trade=trade)
    assert result["status"] == "error"
    assert store.trade(trade["id"])["close_requested"]
    assert store.unknowns() and engine.paused()
    before = len(market_reductions(exchange))
    await settle((store, exchange, Engine(store, exchange, enabled=True)))
    if committed:
        assert exchange.positions["INJ_USDT"]["size"] == 0
        assert not store.unknowns()
        assert sum(abs(order["size"]) - order["left"] for order in market_reductions(exchange)) == 14
    else:
        assert abs(exchange.positions["INJ_USDT"]["size"]) == 14
        assert len(market_reductions(exchange)) == before == 0
        assert store.unknowns()


@pytest.mark.parametrize("action", ["tp1", "tp2"])
async def test_tp_crash_before_write_recovers_durable_close_intent(strict_rig, monkeypatch, action):
    store, exchange, engine = strict_rig
    trade = await entry(strict_rig)
    write = engine.write

    async def crash(trade, key, *args, **kwargs):
        if key.endswith(":close:0"):
            raise RuntimeError("offline crash before durable exchange operation")
        return await write(trade, key, *args, **kwargs)

    monkeypatch.setattr(engine, "write", crash)
    with pytest.raises(RuntimeError):
        await decision(strict_rig, action, trade=trade)
    assert store.trade(trade["id"])["close_requested"]
    assert abs(exchange.positions["INJ_USDT"]["size"]) == 14
    await settle((store, exchange, Engine(store, exchange, enabled=True)))
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert store.trade(trade["id"])["status"] == "closed"


@pytest.mark.parametrize("quantity", [1, 2, 3])
async def test_small_positions_keep_full_target_without_zero_size_orders(strict_rig, quantity):
    store, exchange, _engine = strict_rig
    exchange.account["total"] = str(quantity * 5)
    trade = await entry(strict_rig)
    assert trade["filled"] == quantity
    trade = await apply_current_plan(strict_rig)
    assert trade["exits"]["tp"]["size"] == quantity
    assert set(trade["exits"]) == {"sl", "tp"}
    assert all(order["size"] != 0 for order in exchange.orders.values())
    await decision(strict_rig, "tp2", trade=trade)
    await settle(strict_rig)
    assert store.trade(trade["id"])["status"] == "closed"


