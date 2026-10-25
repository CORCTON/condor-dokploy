"""Independent contract checks for the operator's isolated 10x / 20% policy."""

import time

import pytest
from comprehensive_fixtures import FakeExchange

from trader.engine import Engine, Review, size_order
from trader.models import Signal
from trader.store import Store


@pytest.mark.parametrize("side,direction", [("long", 1), ("short", -1)])
@pytest.mark.parametrize("limit_price,expected_lots", [(None, 20), ("8", 25)])
async def test_new_entry_uses_isolated_10x_and_twenty_percent_equity(side, direction, limit_price, expected_lots):
    store, exchange = Store(":memory:"), FakeExchange(equity="100")
    exchange.add_contract("INJ_USDT", price="10")
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    try:
        key = store.event(123, 1, "operator-approved test entry", None, time.time(), True)
        row = store.db.execute("SELECT * FROM events WHERE id=?", (key,)).fetchone()
        await engine.process(row, Signal(action="open", symbol="INJ", side=side, price=limit_price))
        assert store.db.execute("SELECT status FROM events WHERE id=?", (key,)).fetchone()[0] == "done"
        trade = store.trades()[0]
        assert trade["margin_budget"] == "20.0"
        assert trade["lots"] == expected_lots
        assert exchange.writes[0] == (
            "POST", "/positions/INJ_USDT/set_leverage", None,
            {"leverage": "10", "margin_mode": "isolated"},
        )
        assert exchange.orders[trade["entry_id"]]["size"] == direction * expected_lots
        assert exchange.positions["INJ_USDT"]["lever"] == "10"
    finally:
        store.close()


def test_contract_below_ten_x_is_rejected_without_silent_downsize():
    exchange = FakeExchange()
    exchange.add_contract("INJ_USDT")
    contract = exchange.contracts["INJ_USDT"]
    contract["leverage_max"] = "9"
    with pytest.raises(Review, match="does not support 10x"):
        size_order("100", "100", "10", contract)
    assert not exchange.writes


def test_contract_size_rounds_down_without_changing_twenty_percent_margin_budget():
    exchange = FakeExchange()
    exchange.add_contract("INJ_USDT", multiplier="0.3")
    lots, margin = size_order("100", "100", "7", exchange.contracts["INJ_USDT"])
    assert lots == 95
    assert margin == "20.0"
