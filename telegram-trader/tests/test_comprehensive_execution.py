"""Extended executor fault matrix. Scripted decisions, local SQLite and fake exchange only."""

import copy
import time

import pytest
from comprehensive_fixtures import FakeExchange
from test_trader import Exchange

from trader.engine import Engine
from trader.models import Plan, Signal
from trader.store import Store


@pytest.fixture
def rig(tmp_path):
    store = Store(tmp_path / "comprehensive.sqlite3")
    exchange = Exchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield store, exchange, engine
    store.close()


@pytest.fixture
def strict_rig(tmp_path):
    store = Store(tmp_path / "strict-execution.sqlite3")
    # 70 equity * 20% margin * 10x / price 10 = 14 contracts for the fault matrix.
    exchange = FakeExchange(equity="70")
    exchange.add_contract("INJ_USDT")
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield store, exchange, engine
    store.close()


async def decision(rig, action, *, trade=None, symbol=None, channel=-100123, **kwargs):
    store, _exchange, engine = rig
    number = store.db.execute("SELECT COALESCE(MAX(message),0)+1 FROM events").fetchone()[0]
    key = store.event(channel, number, f"scripted {action}", None, time.time(), True)
    event = store.db.execute("SELECT * FROM events WHERE id=?", (key,)).fetchone()
    await engine.process(
        event,
        Signal(
            action=action,
            symbol=symbol or (trade["symbol"].removesuffix("_USDT") if trade else None),
            target_trade_id=trade["id"] if trade else None,
            **kwargs,
        ),
    )
    return dict(store.db.execute("SELECT status,detail FROM events WHERE id=?", (key,)).fetchone())


async def entry(rig, side="short", *, limit=False, symbol="INJ"):
    result = await decision(rig, "open", symbol=symbol, side=side, price="10" if limit else None)
    assert result["status"] == "done", result
    return next(t for t in rig[0].trades() if t["symbol"] == symbol + "_USDT")


async def exits(rig):
    store, _exchange, engine = rig
    job = await engine.claim_plan()
    stop, target = ("9", "12") if job["side"] == "long" else ("11", "8")
    plan = Plan(sl=stop, tp=target, rationale="local matrix")
    args = job["trade_id"], job["version"], job["lease"], plan
    await engine.propose_plan(*args)
    await engine.apply_plan(*args)
    return store.trade(job["trade_id"])


def native_fill(exchange, order_id, amount=None):
    order = exchange.orders[order_id]
    amount = abs(order["left"]) if amount is None else amount
    assert 0 <= amount <= abs(order["left"])
    exchange.position["size"] += amount if order["size"] > 0 else -amount
    order["left"] = abs(order["left"]) - amount
    if not order["left"]:
        order.update(status="finished", finish_as="filled")


def remaining_targets(exchange):
    return sum(
        abs(order["left"])
        for order in exchange.orders.values()
        if order["status"] == "open" and order["reduce_only"]
    )


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("filled", [0, 1, 7, 13, 14])
async def test_cancel_limit_at_each_fill_boundary_never_flattens_owned_fill(rig, side, filled):
    store, exchange, engine = rig
    trade = await entry(rig, side, limit=True)
    native_fill(exchange, trade["entry_id"], filled)
    await engine.poll()
    if filled:
        trade = await exits(rig)
    expected_position = filled if side == "long" else -filled
    for _ in range(2):
        assert (await decision(rig, "cancel", trade=trade))["status"] in {"done", "error"}
        await engine.poll()
    assert exchange.orders[trade["entry_id"]]["status"] == "finished"
    assert exchange.position["size"] == expected_position
    assert not store.unknowns()
    if filled:
        assert remaining_targets(exchange) == filled
        assert next(iter(exchange.stops.values()))["status"] == "open"
    else:
        assert store.trade(trade["id"])["status"] == "closed"
        assert not exchange.stops


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("sequence", [("tp1", "tp2"), ("tp2", "tp1")])
@pytest.mark.parametrize("ioc_fill_limit", [None, 2])
async def test_market_tp_ordering_and_repeated_instructions_reduce_only_once(
    rig, side, sequence, ioc_fill_limit
):
    store, exchange, engine = rig
    trade = await entry(rig, side)
    trade = await exits(rig)
    exchange.partial = ioc_fill_limit
    for index, name in enumerate(sequence):
        await decision(rig, name, trade=trade)
        await decision(rig, name, trade=trade)  # duplicate before background reconciliation
        for _ in range(10):
            await engine.poll()
        expected = 0
        assert abs(exchange.position["size"]) == expected
        before = len(exchange.writes)
        await decision(rig, name, trade=trade)  # duplicate after the allocation completed
        assert len(exchange.writes) == before
    assert store.trade(trade["id"])["status"] == "closed"
    assert not remaining_targets(exchange)
    assert not store.unknowns()


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("race_fill", [False, True])
@pytest.mark.parametrize("initial_fill", [1, 4, 13])
async def test_native_tp2_first_cancellation_race_closes_remaining_position(
    strict_rig, monkeypatch, side, race_fill, initial_fill
):
    rig = strict_rig
    store, exchange, engine = rig
    trade = await entry(rig, side, limit=True)
    exchange.fill(trade["entry_id"], initial_fill, price="10")
    await engine.poll()
    trade = await exits(rig)
    exchange.fill(trade["exits"]["tp"]["id"], price=trade["plan"]["tp"])
    request = exchange.request

    async def racing_cancel(method, path, body=None, params=None):
        if race_fill and method == "DELETE" and path == f"/orders/{trade['entry_id']}":
            exchange.fill(trade["entry_id"], price="10")
        return await request(method, path, body, params)

    monkeypatch.setattr(exchange, "request", racing_cancel)
    for _ in range(4):
        await engine.poll()
    current = store.trade(trade["id"])
    assert current["entry_terminal"] and current["close_requested"]
    assert current["status"] == "closed"
    assert exchange.orders[current["exits"]["tp"]["id"]]["status"] == "finished"
    assert current["exits"]["tp"]["id"] == trade["exits"]["tp"]["id"]
    assert len([o for o in exchange.orders.values() if o["price"] == trade["plan"]["tp"]]) == 1
    assert remaining_targets(exchange) == abs(exchange.positions["INJ_USDT"]["size"])
    assert exchange.positions["INJ_USDT"]["size"] == 0


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("partial_fill", [0, 3])
async def test_cancelled_native_tp_preserves_unfilled_review_or_finishes_confirmed_exit(
    strict_rig, side, partial_fill
):
    store, exchange, engine = strict_rig
    await entry(strict_rig, side)
    trade = await exits(strict_rig)
    cancelled = trade["exits"]["tp"]["id"]
    if partial_fill:
        exchange.fill(cancelled, partial_fill, price=trade["plan"]["tp"])
    await exchange.request("DELETE", f"/orders/{cancelled}")
    before = len(exchange.writes)
    for _ in range(4):
        await engine.poll()
    current = store.trade(trade["id"])
    assert exchange.orders[cancelled]["status"] == "finished"
    if partial_fill:
        assert len(exchange.writes) > before
        assert current["close_requested"] and current["status"] == "closed"
        assert exchange.positions["INJ_USDT"]["size"] == 0
        assert not remaining_targets(exchange)
        assert not engine.paused() and not store.unknowns()
    else:
        assert len(exchange.writes) == before
        assert engine.paused() and current["last_error"]
        assert abs(exchange.positions["INJ_USDT"]["size"]) == 14
        assert exchange.stops[current["exits"]["sl"]["id"]]["status"] == "open"


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("close_source", ["channel", "native_sl"])
async def test_cancelled_tp_during_confirmed_full_exit_does_not_block_residual_close(
    strict_rig, side, close_source
):
    store, exchange, engine = strict_rig
    await entry(strict_rig, side)
    trade = await exits(strict_rig)
    if close_source == "channel":
        exchange.set_next_market_fill("INJ_USDT", 3)
        assert (await decision(strict_rig, "close", trade=trade))["status"] == "done"
    else:
        exchange.set_market("INJ_USDT", price="8.9" if side == "long" else "11.1")
        exchange.trigger(trade["exits"]["sl"]["id"], quantity=3)
    await exchange.request("DELETE", f"/orders/{trade['exits']['tp']['id']}")
    restarted = Engine(store, exchange, enabled=True)
    for _ in range(4):
        await restarted.poll()
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert store.trade(trade["id"])["status"] == "closed"
    assert not engine.paused()
    assert not store.unknowns()
    assert not remaining_targets(exchange)


@pytest.mark.parametrize("stop_status", ["expired", "renewal_due"])
async def test_cancelled_tp_review_still_renews_native_stop_protection(strict_rig, stop_status):
    store, exchange, engine = strict_rig
    await entry(strict_rig)
    trade = await exits(strict_rig)
    previous = trade["exits"]["sl"]["id"]
    if stop_status == "expired":
        exchange.stops[previous].update(status="finished", finish_as="expired")
    else:
        trade["exits"]["sl"]["created"] = time.time() - 83000
        store.save_trade(trade)
    await exchange.request("DELETE", f"/orders/{trade['exits']['tp']['id']}")
    await engine.poll()
    current = store.trade(trade["id"])
    assert engine.paused() and "TP closed before" in current["last_error"]
    assert current["exits"]["sl"]["id"] != previous
    assert len([stop for stop in exchange.stops.values() if stop["status"] == "open"]) == 1
    assert exchange.orders[trade["exits"]["tp"]["id"]]["left"] == 14
    before = len(exchange.writes)
    await Engine(store, exchange, enabled=True).poll()
    assert len(exchange.writes) == before
    assert exchange.positions["INJ_USDT"]["size"] == -14


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("stop_status", ["expired", "renewal_due"])
@pytest.mark.parametrize("partial_retry", [False, True])
async def test_close_completion_during_stop_renewal_does_not_pause_future_entries(
    strict_rig, side, stop_status, partial_retry
):
    store, exchange, engine = strict_rig
    await entry(strict_rig, side)
    trade = await exits(strict_rig)
    exchange.set_next_market_fill("INJ_USDT", 3)
    assert (await decision(strict_rig, "close", trade=trade))["status"] == "done"
    trade = store.trade(trade["id"])
    old_stop = trade["exits"]["sl"]["id"]
    if stop_status == "expired":
        exchange.stops[old_stop].update(status="finished", finish_as="expired")
    else:
        trade["exits"]["sl"]["created"] = time.time() - 83000
        store.save_trade(trade)
    if partial_retry:
        exchange.set_next_market_fill("INJ_USDT", 3)
    await engine.poll()
    assert not engine.paused()
    current = store.trade(trade["id"])
    assert not current.get("last_error")
    if partial_retry:
        assert abs(exchange.positions["INJ_USDT"]["size"]) == 8
        assert current["exits"]["sl"]["id"] != old_stop
        assert len([stop for stop in exchange.stops.values() if stop["status"] == "open"]) == 1
    else:
        assert exchange.positions["INJ_USDT"]["size"] == 0
        assert len(exchange.stops) == 1  # no replacement is needed for a flat position
    for _ in range(4):
        await engine.poll()
    assert store.trade(trade["id"])["status"] == "closed"
    assert not engine.paused() and not store.unknowns()
    assert not remaining_targets(exchange)
    exchange.add_contract("JTO_USDT")
    subsequent = await entry(strict_rig, side, symbol="JTO")
    assert subsequent["filled"] == 14


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("repair", ["tp1", "tp2", "close", "amend_cancelled", "sl_trigger"])
async def test_cancelled_tp_review_remains_recoverable_without_recreating_cancelled_order(
    strict_rig, side, repair
):
    store, exchange, engine = strict_rig
    await entry(strict_rig, side)
    trade = await exits(strict_rig)
    cancelled = trade["exits"]["tp"]["id"]
    await exchange.request("DELETE", f"/orders/{cancelled}")
    await engine.poll()
    assert engine.paused() and store.trade(trade["id"])["last_error"]
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    resumed_rig = store, exchange, engine
    if repair in {"tp1", "tp2", "close"}:
        assert (await decision(resumed_rig, repair, trade=trade))["status"] == "done"
    elif repair == "amend_cancelled":
        before = len(exchange.writes)
        result = await decision(resumed_rig, "targets", trade=trade,
                                levels={"tp": "11.50" if side == "long" else "8.50"})
        assert result["status"] == "error"
        assert len(exchange.writes) == before
    else:
        exchange.set_market("INJ_USDT", price="8.9" if side == "long" else "11.1")
        exchange.trigger(trade["exits"]["sl"]["id"])
    for _ in range(4):
        await engine.poll()
    current = store.trade(trade["id"])
    assert current["exits"]["tp"]["id"] == cancelled
    assert exchange.orders[cancelled]["status"] == "finished"
    assert engine.paused()  # repair is not authorization to resume new entries
    assert not store.unknowns()
    if repair == "amend_cancelled":
        assert "TP closed before" in current["last_error"]
        assert abs(exchange.positions["INJ_USDT"]["size"]) == 14
        assert not remaining_targets(exchange)
        assert exchange.stops[current["exits"]["sl"]["id"]]["status"] == "open"
    else:
        assert not current.get("last_error")
        assert current["status"] == "closed"
        assert exchange.positions["INJ_USDT"]["size"] == 0
        assert not remaining_targets(exchange)
        engine.record_heartbeat("interpreter")
        engine.record_heartbeat("planner")
        await engine.resume()
        assert not engine.paused()


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("quantity,terminal", [
    (14, "filled"), (0, "cancelled"), (3, "cancelled"),
    (0, "reduce_out"), (3, "reduce_out"), (0, "position_closed"), (3, "position_closed"),
])
async def test_gate_string_quantity_receipts_distinguish_native_fill_from_cancellation(
    strict_rig, monkeypatch, side, quantity, terminal
):
    store, exchange, engine = strict_rig
    await entry(strict_rig, side)
    trade = await exits(strict_rig)
    order_id = trade["exits"]["tp"]["id"]
    if quantity:
        exchange.fill(order_id, quantity, price=trade["plan"]["tp"])
    if terminal != "filled":
        await exchange.request("DELETE", f"/orders/{order_id}")
        exchange.orders[order_id]["finish_as"] = terminal
    get = exchange.get

    async def response_as_documented(path, **params):
        result = await get(path, **params)
        if path.startswith("/orders/"):
            result["size"] = str(result["size"])
            result["left"] = str(result["left"])
            result.pop("reduce_only", None)
            result.pop("close", None)
        return result

    monkeypatch.setattr(exchange, "get", response_as_documented)
    before = len(exchange.writes)
    for _ in range(4):
        await engine.poll()
    current = store.trade(trade["id"])
    if quantity:
        assert current["status"] == "closed"
        assert exchange.positions["INJ_USDT"]["size"] == 0
        assert not engine.paused() and not current.get("last_error")
        assert not remaining_targets(exchange)
        receipt = exchange.orders[order_id]
        assert abs(receipt["size"]) - receipt["left"] == quantity
        if quantity < 14:
            assert current["exits"]["tp"]["filled"] == quantity
    else:
        assert len(exchange.writes) == before
        assert engine.paused() and current.get("last_error")
        assert exchange.positions["INJ_USDT"]["size"] == (14 if side == "long" else -14)


@pytest.mark.parametrize("action", ["close", "cancel", "tp1", "tp2", "sl", "targets"])
async def test_management_without_an_owned_trade_has_no_exchange_effect(rig, action):
    _store, exchange, _engine = rig
    fields = {"price": "11"} if action == "sl" else {}
    if action == "targets":
        fields["levels"] = {"tp1": "9"}
    result = await decision(rig, action, symbol="JTO", **fields)
    assert result["status"] == "error"
    assert not exchange.writes


@pytest.mark.parametrize("mismatch", ["symbol", "channel", "opposite_position", "extra_position"])
async def test_owned_trade_guard_does_not_manage_a_mismatched_position(rig, mismatch):
    _store, exchange, _engine = rig
    trade = await entry(rig)
    trade = await exits(rig)
    before = len(exchange.writes)
    fields = {}
    if mismatch == "symbol":
        fields["symbol"] = "BTC"
    elif mismatch == "channel":
        fields["channel"] = -100999
    elif mismatch == "opposite_position":
        exchange.position["size"] = 1
    else:
        exchange.position["size"] = -15
    result = await decision(rig, "close", trade=trade, **fields)
    assert result["status"] == "error"
    assert len(exchange.writes) == before


@pytest.mark.parametrize("side", ["long", "short"])
async def test_stop_amend_repeat_and_revert_reuses_native_stop_across_restart(rig, side):
    store, exchange, engine = rig
    trade = await entry(rig, side)
    trade = await exits(rig)
    prices = ("9.50", "9.50", "9.00") if side == "long" else ("10.50", "10.50", "11.00")
    stop_id = trade["exits"]["sl"]["id"]
    for price in prices:
        assert (await decision((store, exchange, engine), "sl", trade=trade, price=price))[
            "status"
        ] == "done"
        engine = Engine(store, exchange, enabled=True)
        await engine.poll()
        assert len(exchange.stops) == 1
        assert exchange.stops[stop_id]["trigger"]["price"] == price
        assert store.trade(trade["id"])["exits"]["sl"]["price"] == price


@pytest.mark.parametrize("side", ["long", "short"])
async def test_target_amend_reuses_order_then_native_fill_exits_and_cannot_be_recreated(rig, side):
    store, exchange, engine = rig
    await entry(rig, side)
    trade = await exits(rig)
    target = trade["exits"]["tp"]["id"]
    price = "11.50" if side == "long" else "8.50"
    assert (await decision(rig, "targets", trade=trade, levels={"tp1": price}))["status"] == "done"
    assert exchange.orders[target]["left"] == 14
    assert exchange.orders[target]["price"] == price
    assert len(exchange.orders) == 2
    native_fill(exchange, target, 3)
    for _ in range(4):
        await engine.poll()
    assert exchange.position["size"] == 0
    assert store.trade(trade["id"])["status"] == "closed"
    before = len(exchange.writes)
    result = await decision(rig, "targets", trade=trade, levels={"tp2": price})
    assert result["status"] == "error"
    assert len(exchange.writes) == before
    assert store.trade(trade["id"])["exits"]["tp"]["id"] == target


@pytest.mark.parametrize("side", ["long", "short"])
async def test_native_stop_partial_fill_finishes_reduce_only_and_cleans_all_exits(rig, side):
    store, exchange, engine = rig
    trade = await entry(rig, side, limit=True)
    native_fill(exchange, trade["entry_id"], 7)
    await engine.poll()
    trade = await exits(rig)
    exchange.stops[trade["exits"]["sl"]["id"]].update(status="finished", finish_as="succeeded")
    exchange.position["size"] = 5 if side == "long" else -5
    exchange.partial = 2
    for _ in range(10):
        await engine.poll()
    assert exchange.position["size"] == 0
    assert store.trade(trade["id"])["status"] == "closed"
    assert exchange.orders[trade["entry_id"]]["status"] == "finished"
    assert not remaining_targets(exchange)
    assert not store.unknowns()
    assert all(
        order["reduce_only"] for key, order in exchange.orders.items() if key != trade["entry_id"]
    )


@pytest.mark.parametrize("finish_as", ["cancelled", "auto_cancelled", "failed"])
async def test_nonexecuted_native_stop_disappearance_pauses_instead_of_guessing_a_close(
    rig, finish_as
):
    store, exchange, engine = rig
    trade = await entry(rig)
    trade = await exits(rig)
    exchange.stops[trade["exits"]["sl"]["id"]].update(status="finished", finish_as=finish_as)
    before = len(exchange.writes)
    await engine.poll()
    await engine.poll()
    assert engine.paused()
    assert "native stop no longer active" in store.trade(trade["id"])["last_error"]
    assert len(exchange.writes) == before
    assert exchange.position["size"] == -14


async def test_lost_close_receipt_recovers_after_sqlite_reopen_without_duplicate_reduction(
    rig, tmp_path
):
    store, exchange, _engine = rig
    trade = await entry(rig)
    trade = await exits(rig)
    exchange.lost_response = True
    assert (await decision(rig, "close", trade=trade))["status"] == "error"
    assert exchange.position["size"] == 0
    assert len(store.unknowns()) == 1
    store.close()
    recovered = Store(tmp_path / "comprehensive.sqlite3")
    try:
        restarted = Engine(recovered, exchange, enabled=True)
        for _ in range(3):
            await restarted.poll()
        assert not recovered.unknowns()
        assert recovered.trade(trade["id"])["status"] == "closed"
        assert restarted.paused()
        assert (
            len([o for o in exchange.orders.values() if o["price"] == "0" and o["reduce_only"]])
            == 1
        )
    finally:
        recovered.close()


async def test_lost_stop_amend_receipt_preserves_native_protection_and_never_blindly_repeats(
    rig, monkeypatch
):
    store, exchange, engine = rig
    trade = await entry(rig)
    trade = await exits(rig)
    request = exchange.request

    async def lose_amend(method, path, body=None, params=None):
        result = await request(method, path, body, params)
        if method == "PUT" and path.startswith("/price_orders/amend/"):
            raise TimeoutError("injected after amend committed")
        return result

    monkeypatch.setattr(exchange, "request", lose_amend)
    assert (await decision(rig, "sl", trade=trade, price="10.50"))["status"] == "error"
    monkeypatch.setattr(exchange, "request", request)
    before = len(exchange.writes)
    for _ in range(3):
        await engine.poll()
    assert len(exchange.writes) == before
    assert exchange.stops[trade["exits"]["sl"]["id"]]["trigger"]["price"] == "10.50"
    assert len(exchange.stops) == 1
    assert engine.paused() and store.unknowns()
    assert store.trade(trade["id"])["last_error"]


async def test_two_owned_contracts_close_and_cleanup_are_strictly_isolated(tmp_path):
    store = Store(tmp_path / "two-contracts.sqlite3")
    exchange = FakeExchange(equity="70")
    exchange.add_contract("INJ_USDT")
    exchange.add_contract("BTC_USDT")
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    rig = store, exchange, engine
    try:
        first = await entry(rig, "short", symbol="INJ")
        await exits(rig)
        second = await entry(rig, "long", symbol="BTC")
        second = await exits(rig)
        other_orders = {
            key: copy.deepcopy(order)
            for key, order in exchange.orders.items()
            if order["contract"] == "BTC_USDT"
        }
        other_stop = copy.deepcopy(exchange.stops[second["exits"]["sl"]["id"]])
        assert (await decision(rig, "close", trade=first))["status"] == "done"
        for _ in range(3):
            await engine.poll()
        assert exchange.positions["INJ_USDT"]["size"] == 0
        assert store.trade(first["id"])["status"] == "closed"
        assert exchange.positions["BTC_USDT"]["size"] == 14
        assert store.trade(second["id"])["status"] == "working"
        assert all(exchange.orders[key] == value for key, value in other_orders.items())
        assert exchange.stops[second["exits"]["sl"]["id"]] == other_stop
    finally:
        store.close()


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("stop_fill", [0, 3, 14])
async def test_realistic_native_trigger_recovers_after_database_reopen(tmp_path, side, stop_fill):
    path = tmp_path / "native-restart.sqlite3"
    store = Store(path)
    exchange = FakeExchange(equity="70")
    exchange.add_contract("INJ_USDT")
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    local_rig = store, exchange, engine
    try:
        await entry(local_rig, side)
        trade = await exits(local_rig)
        exchange.set_market("INJ_USDT", price="8.9" if side == "long" else "11.1")
        receipt = exchange.trigger(trade["exits"]["sl"]["id"], quantity=stop_fill)
        child = exchange.orders[receipt["trade_id"]]
        assert abs(child["size"]) - child["left"] == stop_fill
    finally:
        store.close()
    reopened = Store(path)
    try:
        restarted = Engine(reopened, exchange, enabled=True)
        for _ in range(4):
            await restarted.poll()
        assert exchange.positions["INJ_USDT"]["size"] == 0
        assert reopened.trade(trade["id"])["status"] == "closed"
        assert not reopened.unknowns()
        assert not remaining_targets(exchange)
        assert all(order["status"] == "finished" for order in exchange.stops.values())
        reductions = [
            order
            for order in exchange.orders.values()
            if order["is_reduce_only"] and order["price"] == "0"
        ]
        assert sum(abs(order["size"]) - order["left"] for order in reductions) == 14
        assert len(reductions) == (1 if stop_fill == 14 else 2)
    finally:
        reopened.close()
