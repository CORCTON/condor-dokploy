"""Cross margin verification and manual handoff never adopt external positions."""

import copy
import time

import pytest
from comprehensive_fixtures import FakeExchange

from trader.engine import Engine, cross_position
from trader.models import Plan, Signal
from trader.store import Store


@pytest.mark.parametrize("position,expected", [
    ({"pos_margin_mode": "cross", "mode": "single", "lever": "10", "leverage": "0"}, True),
    ({"pos_margin_mode": "isolated", "mode": "single", "lever": "10"}, False),
    ({"pos_margin_mode": "cross", "mode": "dual_long", "lever": "10"}, False),
    ({"pos_margin_mode": "cross", "mode": "single", "lever": "7"}, False),
    ({"pos_margin_mode": "cross", "mode": "single", "leverage": "10"}, False),
    ({}, False),
])
def test_cross_verification_uses_explicit_margin_mode_and_actual_lever(position, expected):
    assert cross_position(position) is expected


async def test_detached_trade_keeps_native_exits_and_cannot_be_adopted_or_reopened():
    store, exchange = Store(":memory:"), FakeExchange(equity="100")
    exchange.add_contract("RARE_USDT", price="10")
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)

    async def send(number, signal, reply=None):
        key = store.event(123, number, "fixture instruction", reply, time.time(), True)
        row = store.db.execute("SELECT * FROM events WHERE id=?", (key,)).fetchone()
        await engine.process(row, signal)
        return row

    try:
        entry = Signal(action="open", symbol="RARE", side="short")
        original = await send(1, entry)
        trade, = store.trades()
        job = await engine.claim_plan()
        plan = Plan(sl="11", tp="8", rationale="Offline handoff test")
        args = job["trade_id"], job["version"], job["lease"], plan
        await engine.propose_plan(*args)
        await engine.apply_plan(*args)
        native = copy.deepcopy((exchange.positions, exchange.orders, exchange.stops))
        writes = copy.deepcopy(exchange.writes)
        operations = [tuple(r) for r in store.db.execute("SELECT * FROM operations ORDER BY id")]

        # The approved operational handoff archives then removes only this row.
        with store.db:
            store.db.execute("DELETE FROM trades WHERE id=?", (trade["id"],))
        await engine.poll()
        assert not engine.active()
        await send(2, Signal(action="close", target_trade_id=trade["id"]), reply=1)
        await engine.process(original, entry)
        await send(3, entry)  # Same symbol still has the operator's real position.

        assert (exchange.positions, exchange.orders, exchange.stops) == native
        assert exchange.writes == writes
        assert [tuple(r) for r in store.db.execute("SELECT * FROM operations ORDER BY id")] == operations
        assert not engine.active() and not store.unknowns()
    finally:
        store.close()
