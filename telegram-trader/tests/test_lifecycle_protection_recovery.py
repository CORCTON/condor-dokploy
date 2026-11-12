"""Native protection and accepted-plan recovery; in-memory exchange only."""

import copy
import time

import pytest
from comprehensive_fixtures import FakeExchange
from test_comprehensive_execution import decision, entry, exits

from trader.engine import Engine, Review
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


async def settle(engine, count=6):
    for _ in range(count):
        await engine.poll()


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("blocker", ["targets", "sl"])
@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize("stop_state", ["partial_fill", "expired", "renewal_due"])
async def test_native_stop_maintenance_preempts_uncertain_management(
    rig, monkeypatch, side, blocker, committed, stop_state
):
    store, exchange, _engine = rig
    trade = await entry(rig, side)
    trade = await exits(rig)
    request = exchange.request
    attempts = 0

    async def lose_response(method, path, body=None, params=None, **kwargs):
        nonlocal attempts
        is_target = (
            blocker == "targets" and method == "PUT" and path.startswith("/orders/")
        ) or (blocker == "sl" and method == "PUT" and path.startswith("/price_orders/amend/"))
        if is_target:
            attempts += 1
            if committed:
                await request(method, path, body=body, params=params, **kwargs)
            raise TimeoutError("offline lost management response")
        return await request(method, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", lose_response)
    kwargs = ({"price": "9.5" if side == "long" else "10.5"} if blocker == "sl"
              else {"levels": {"tp": "11.5" if side == "long" else "8.5"}})
    assert (await decision(rig, blocker, trade=trade, **kwargs))["status"] == "error"
    monkeypatch.setattr(exchange, "request", request)
    unknown_keys = [op["id"] for op in store.unknowns()]
    assert len(unknown_keys) == attempts == 1
    current = store.trade(trade["id"])
    old_stop = exchange.stops[current["exits"]["sl"]["id"]]
    remaining = exchange.positions["INJ_USDT"]["size"]
    if stop_state == "partial_fill":
        exchange.set_market("INJ_USDT", price="8.5" if side == "long" else "11.5")
        exchange.trigger(old_stop["id"], quantity=3)
    elif stop_state == "expired":
        old_stop.update(status="finished", finish_as="expired")
    else:
        current["exits"]["sl"]["created"] = time.time() - 83000
        store.save_trade(current)

    restarted = Engine(store, exchange, enabled=True)
    await settle(restarted)
    current = store.trade(trade["id"])
    if stop_state == "partial_fill":
        assert exchange.positions["INJ_USDT"]["size"] == 0
        assert current["close_requested"] and current["status"] == "closed"
    else:
        assert exchange.positions["INJ_USDT"]["size"] == remaining
        active = [s for s in exchange.stops.values() if s["status"] == "open"]
        assert len(active) == 1 and active[0]["id"] != old_stop["id"]
        assert not current.get("close_requested")
    # Neither amendment has an exact correlation receipt; protection cannot erase uncertainty.
    assert [op["id"] for op in store.unknowns()] == unknown_keys
    assert restarted.paused()
    assert attempts == 1


async def proposed(rig):
    trade = await entry(rig)
    job = await rig[2].claim_plan()
    plan = Plan(sl="11", tp="8", rationale="accepted once")
    args = job["trade_id"], job["version"], job["lease"]
    await rig[2].propose_plan(*args, plan)
    return trade, args, plan


async def fail_before_stop(rig, monkeypatch):
    trade, args, plan = await proposed(rig)
    exchange, engine = rig[1:]
    get = exchange.get
    reads = 0

    async def timeout_second_position(path, **params):
        nonlocal reads
        if path == "/positions/INJ_USDT":
            reads += 1
            if reads == 2:
                raise TimeoutError("offline pre-stop read failure")
        return await get(path, **params)

    monkeypatch.setattr(exchange, "get", timeout_second_position)
    with pytest.raises(TimeoutError):
        await engine.apply_plan(*args, plan)
    monkeypatch.setattr(exchange, "get", get)
    return trade, args, plan


async def test_started_plan_recovers_stop_before_targets_without_replanning(rig, monkeypatch):
    store, exchange, engine = rig
    trade, args, plan = await fail_before_stop(rig, monkeypatch)
    assert not exchange.stops
    assert not store.trade(trade["id"])["exits"]
    assert not await engine.fail_plan(*args, "TimeoutError")
    current = store.trade(trade["id"])
    current["plan_lease"] = 0
    store.save_trade(current)
    assert await engine.claim_plan() is None
    restarted = Engine(store, exchange, enabled=True)
    before = len(exchange.writes)
    await settle(restarted)
    current = store.trade(trade["id"])
    assert current["plan"] == plan.model_dump(mode="json")
    assert not current["plan_needed"]
    assert list(current["exits"]) == ["sl", "tp"]
    writes = exchange.writes[before:]
    assert [p for m, p, *_ in writes if m == "POST"] == ["/price_orders", "/orders"]
    assert {name: exchange.orders[current["exits"][name]["id"]]["price"]
            for name in ("tp",)} == {"tp": "8"}
    assert len(exchange.stops) == 1
    assert not store.unknowns()


async def test_accepted_unstarted_plan_survives_expired_lease(rig):
    store, exchange, engine = rig
    trade, _args, plan = await proposed(rig)
    current = store.trade(trade["id"])
    current["plan_lease"] = 0
    store.save_trade(current)
    job = await engine.claim_plan()
    assert job["accepted_plan"] == plan.model_dump(mode="json")
    args = job["trade_id"], job["version"], job["lease"]
    changed = plan.model_copy(update={"rationale": "different model plan"})
    with pytest.raises(Review, match="accepted exit plan cannot be changed"):
        await engine.propose_plan(*args, changed)
    await engine.apply_plan(*args, plan)
    assert len(exchange.stops) == 1


async def test_recovery_preserves_new_channel_stop_and_target(rig, monkeypatch):
    store, exchange, engine = rig
    trade, _args, _plan = await fail_before_stop(rig, monkeypatch)
    assert (await decision(rig, "targets", trade=trade,
                           levels={"sl": "10.5", "tp1": "9.5"}))["status"] == "done"
    await settle(engine)
    current = store.trade(trade["id"])
    assert current["source_sl"]
    assert exchange.stops[current["exits"]["sl"]["id"]]["trigger"]["price"] == "10.5"
    assert exchange.orders[current["exits"]["tp"]["id"]]["price"] == "9.5"
    assert not current["plan_needed"]


async def test_reclaimed_accepted_proposal_honors_newer_channel_levels(rig):
    store, exchange, engine = rig
    trade, _args, plan = await proposed(rig)
    assert (await decision(rig, "targets", trade=trade,
                           levels={"sl": "10.5", "tp1": "9.5"}))["status"] == "done"
    current = store.trade(trade["id"])
    current["plan_lease"] = 0
    store.save_trade(current)
    job = await engine.claim_plan()
    assert job["accepted_plan"] == plan.model_dump(mode="json")
    await engine.apply_plan(job["trade_id"], job["version"], job["lease"], plan)
    current = store.trade(trade["id"])
    assert current["source_sl"] and not current["plan_needed"]
    assert exchange.stops[current["exits"]["sl"]["id"]]["trigger"]["price"] == "10.5"
    assert exchange.orders[current["exits"]["tp"]["id"]]["price"] == "9.5"


@pytest.mark.parametrize("committed", [False, True])
async def test_unknown_initial_stop_never_resubmits_after_revision_or_restart(
    rig, monkeypatch, committed
):
    store, exchange, engine = rig
    trade, args, plan = await proposed(rig)
    request = exchange.request
    attempts = 0

    async def lost_stop(method, path, body=None, params=None, **kwargs):
        nonlocal attempts
        if method == "POST" and path == "/price_orders":
            attempts += 1
            if committed:
                await request(method, path, body=body, params=params, **kwargs)
            raise TimeoutError("offline unknown stop")
        return await request(method, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", lost_stop)
    with pytest.raises(TimeoutError):
        await engine.apply_plan(*args, plan)
    monkeypatch.setattr(exchange, "request", request)
    assert (await decision(rig, "sl", trade=trade, price="10.5"))["status"] == "error"
    restarted = Engine(store, exchange, enabled=True)
    await settle(restarted)
    assert attempts == 1
    if committed:
        assert len(exchange.stops) == 1
        assert not store.trade(trade["id"])["plan_needed"]
        assert not store.unknowns()
    else:
        assert not exchange.stops
        assert not store.trade(trade["id"])["exits"]
        assert store.unknowns() and restarted.paused()


async def test_rejected_initial_stop_remains_review_without_targets_or_new_model(rig, monkeypatch):
    store, exchange, engine = rig
    trade, args, plan = await proposed(rig)
    request = exchange.request

    async def reject_stop(method, path, body=None, params=None, **kwargs):
        if method == "POST" and path == "/price_orders":
            raise GateError(400, "INVALID_PRICE")
        return await request(method, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", reject_stop)
    with pytest.raises(GateError):
        await engine.apply_plan(*args, plan)
    monkeypatch.setattr(exchange, "request", request)
    before = copy.deepcopy(exchange.writes)
    await settle(engine)
    assert exchange.writes == before
    assert not store.trade(trade["id"])["exits"]
    assert store.trade(trade["id"])["last_error"]
    assert engine.paused() and await engine.claim_plan() is None


@pytest.mark.parametrize("failed_order", ["entry", "tp2"])
@pytest.mark.parametrize("stop_state", ["partial_fill", "expired", "renewal_due"])
async def test_other_order_read_failure_does_not_hide_native_protection(
    rig, monkeypatch, failed_order, stop_state
):
    store, exchange, engine = rig
    trade = await entry(rig)
    trade = await exits(rig)
    old_stop = exchange.stops[trade["exits"]["sl"]["id"]]
    if stop_state == "partial_fill":
        exchange.set_market("INJ_USDT", price="11.5")
        exchange.trigger(old_stop["id"], quantity=3)
    elif stop_state == "expired":
        old_stop.update(status="finished", finish_as="expired")
    else:
        trade["exits"]["sl"]["created"] = time.time() - 83000
        store.save_trade(trade)
    failed_id = trade["entry_id"] if failed_order == "entry" else trade["exits"]["tp"]["id"]
    get = exchange.get

    async def unavailable(path, **params):
        if path == f"/orders/{failed_id}":
            raise TimeoutError("offline unavailable order detail")
        return await get(path, **params)

    monkeypatch.setattr(exchange, "get", unavailable)
    before = len(exchange.writes)
    await engine.poll()
    current = store.trade(trade["id"])
    assert "TimeoutError" in current["last_error"] and engine.paused()
    reductions = [w for w in exchange.writes[before:]
                  if w[0:2] == ("POST", "/orders") and w[2]["price"] == "0"]
    assert len(reductions) <= 1  # one poll cannot launch a second final-exit IOC
    if stop_state == "partial_fill":
        assert current["close_requested"]
        # An unreadable entry cannot be blindly canceled; persist the exit intent
        # first, then finish once that required cancellation receipt is readable.
        assert exchange.positions["INJ_USDT"]["size"] == (-11 if failed_order == "entry" else 0)
    else:
        assert exchange.positions["INJ_USDT"]["size"] == -14
        assert current["exits"]["sl"]["id"] != old_stop["id"]
        assert sum(s["status"] == "open" for s in exchange.stops.values()) == 1
        assert not reductions
    monkeypatch.setattr(exchange, "get", get)
    await settle(engine)
    if stop_state == "partial_fill":
        assert store.trade(trade["id"])["status"] == "closed"
        assert exchange.positions["INJ_USDT"]["size"] == 0


async def test_protection_fallback_revalidates_fresh_position_ownership(rig, monkeypatch):
    store, exchange, engine = rig
    trade = await entry(rig)
    trade = await exits(rig)
    stop = exchange.stops[trade["exits"]["sl"]["id"]]
    stop.update(status="finished", finish_as="expired")
    exchange.positions["INJ_USDT"]["size"] = -20
    get = exchange.get

    async def entry_unavailable(path, **params):
        if path == f"/orders/{trade['entry_id']}":
            raise TimeoutError("offline unavailable entry")
        return await get(path, **params)

    monkeypatch.setattr(exchange, "get", entry_unavailable)
    before = copy.deepcopy(exchange.writes)
    await engine.poll()
    assert exchange.writes == before
    assert "ownership/mode mismatch" in store.trade(trade["id"])["last_error"]
    assert engine.paused()


async def test_tp2_fill_still_closes_when_stop_detail_read_fails(rig, monkeypatch):
    store, exchange, engine = rig
    trade = await entry(rig)
    trade = await exits(rig)
    exchange.set_market("INJ_USDT", price="8")
    exchange.fill(trade["exits"]["tp"]["id"], 1)
    get = exchange.get

    async def stop_unavailable(path, **params):
        if path == f"/price_orders/{trade['exits']['sl']['id']}":
            raise TimeoutError("offline unavailable stop detail")
        return await get(path, **params)

    monkeypatch.setattr(exchange, "get", stop_unavailable)
    before = len(exchange.writes)
    await engine.poll()
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert store.trade(trade["id"])["close_requested"]
    assert len([w for w in exchange.writes[before:]
                if w[0:2] == ("POST", "/orders") and w[2]["price"] == "0"]) == 1


async def test_failed_native_stop_renewal_is_not_repeated_by_protection_fallback(rig, monkeypatch):
    store, exchange, engine = rig
    trade = await entry(rig)
    trade = await exits(rig)
    exchange.stops[trade["exits"]["sl"]["id"]].update(status="finished", finish_as="expired")
    request = exchange.request
    attempts = 0

    async def lost_renewal(method, path, body=None, params=None, **kwargs):
        nonlocal attempts
        if method == "POST" and path == "/price_orders":
            attempts += 1
            raise TimeoutError("offline uncertain replacement")
        return await request(method, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", lost_renewal)
    await settle(engine)
    assert attempts == 1
    assert len(store.unknowns()) == 1 and engine.paused()
    assert not store.trade(trade["id"]).get("close_requested")
    assert exchange.positions["INJ_USDT"]["size"] == -14


async def test_tp_read_failure_and_unknown_target_amend_cannot_hide_confirmed_sl_fill(rig, monkeypatch):
    store, exchange, engine = rig
    trade = await entry(rig)
    trade = await exits(rig)
    request = exchange.request

    async def lost_amend(method, path, body=None, params=None, **kwargs):
        if method == "PUT" and path.startswith("/orders/"):
            raise TimeoutError("offline unknown target amendment")
        return await request(method, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", lost_amend)
    assert (await decision(rig, "targets", trade=trade, levels={"tp": "8.5"}))["status"] == "error"
    monkeypatch.setattr(exchange, "request", request)
    unknown_keys = [op["id"] for op in store.unknowns()]
    get = exchange.get

    async def missing_tp2(path, **params):
        if path == f"/orders/{trade['exits']['tp']['id']}":
            raise TimeoutError("offline unavailable TP2 detail")
        return await get(path, **params)

    monkeypatch.setattr(exchange, "get", missing_tp2)
    exchange.set_market("INJ_USDT", price="11.5")
    exchange.trigger(trade["exits"]["sl"]["id"], quantity=3)
    before = len(exchange.writes)
    await engine.poll()
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert store.trade(trade["id"])["close_requested"]
    assert [op["id"] for op in store.unknowns()] == unknown_keys
    assert engine.paused()
    assert len([w for w in exchange.writes[before:]
                if w[0:2] == ("POST", "/orders") and w[2]["price"] == "0"]) == 1


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("channel_stop", [False, True])
async def test_additional_entry_fill_invalidates_only_unstarted_proposal(rig, side, channel_stop):
    store, exchange, engine = rig
    trade = await entry(rig, side, limit=True)
    exchange.fill(trade["entry_id"], 1, price="10")
    await engine.poll()
    first_plan = Plan(sl="9" if side == "long" else "11", tp="12" if side == "long" else "8", rationale="first partial fill")
    if channel_stop:
        assert (await decision(rig, "sl", trade=trade, price=str(first_plan.sl)))["status"] == "done"
    first = await engine.claim_plan("1" * 32)
    old_args = first["trade_id"], first["version"], first["lease"]
    await engine.propose_plan(*old_args, first_plan)
    previous_stop = copy.deepcopy(store.trade(trade["id"])["exits"].get("sl"))
    # Both are legal better-priced fills for the original limit order. They
    # change the average enough to invalidate the previous model stop ordering.
    exchange.fill(trade["entry_id"], 13, price="6" if side == "long" else "14")
    await engine.poll()
    current = store.trade(trade["id"])
    assert current["filled"] == 14 and current["entry_price"] != "10"
    assert current["plan"] is None and current["plan_proposal"] is None
    assert current["plan_token"] == first["lease"] and current["plan_lease"] == 0
    assert current["exits"].get("sl") == previous_stop
    before = copy.deepcopy(exchange.writes)
    with pytest.raises(Review, match="stale"):
        await engine.apply_plan(*old_args, first_plan)
    assert await engine.claim_plan(first["lease"]) is None
    assert exchange.writes == before
    job = await engine.claim_plan("2" * 32)
    assert job["accepted_plan"] is None
    assert job["version"] > first["version"]
    assert job["entry"] == current["entry_price"] and job["filled_contracts"] == 14
    assert job["candles_5m"]  # The fresh job takes the regular model-planning path.
    revised = Plan.model_validate(first_plan.model_dump() | {
        "sl": first_plan.sl if channel_stop else ("5" if side == "long" else "15"),
        "rationale": "confirmed final fill basis",
    })
    args = job["trade_id"], job["version"], job["lease"]
    await engine.propose_plan(*args, revised)
    await engine.apply_plan(*args, revised)
    current = store.trade(trade["id"])
    assert not current["plan_needed"]
    assert current["source_sl"] is channel_stop
    assert current["exits"]["tp"]["size"] == 14
    if channel_stop:
        assert current["exits"]["sl"] == previous_stop
