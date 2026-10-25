"""Validate simulator semantics before using it as an execution oracle."""

import time
from decimal import Decimal

import pytest
from comprehensive_fixtures import FakeExchange

from trader.engine import Engine
from trader.gate import GateError
from trader.models import Plan, Signal
from trader.store import Store


@pytest.fixture
def exchange():
    result = FakeExchange()
    result.add_contract("INJ_USDT", price="10")
    result.add_contract("JTO_USDT", price="2", tick="0.001", multiplier="0.1")
    return result


async def order(exchange, symbol="INJ_USDT", size=10, price="0", *, reduce=False, text=""):
    return await exchange.request("POST", "/orders", {
        "contract": symbol, "size": size, "price": price,
        "tif": "ioc" if price == "0" else "gtc", "reduce_only": reduce, "text": text,
    })


async def stop(exchange, symbol="INJ_USDT", price="9", rule=2):
    return await exchange.request("POST", "/price_orders", {
        "initial": {"contract": symbol, "size": 0, "price": "0", "tif": "ioc",
                    "close": True, "reduce_only": True, "text": "stop-intent"},
        "trigger": {"strategy_type": 0, "price_type": 1, "price": price,
                    "rule": rule, "expiration": 86400},
        "order_type": "plan-close-long-position" if rule == 2 else "plan-close-short-position",
    })


async def test_global_ids_and_contract_positions_are_independent(exchange):
    first = await order(exchange, size=5)
    second = await order(exchange, "JTO_USDT", size=-8)
    conditional = await stop(exchange)
    assert len({first["id"], second["id"], conditional["id"]}) == 3
    assert (await exchange.get("/positions/INJ_USDT"))["size"] == 5
    assert (await exchange.get("/positions/JTO_USDT"))["size"] == -8
    rows = await exchange.get("/positions", contract="JTO_USDT", holding=True)
    assert len(rows) == 1 and rows[0]["size"] == -8
    rows[0]["size"] = 999
    assert exchange.positions["JTO_USDT"]["size"] == -8
    assert (await exchange.get("/contracts/JTO_USDT"))["quanto_multiplier"] == "0.1"


async def test_orders_filter_status_contract_and_paginate_after_filtering(exchange):
    inj = [await order(exchange, price="9", text=f"tag-{i}") for i in range(3)]
    await order(exchange, "JTO_USDT", price="1")
    await exchange.request("DELETE", f"/orders/{inj[1]['id']}")
    rows = await exchange.get("/orders", contract="INJ_USDT", status="open", limit=1)
    assert [row["id"] for row in rows] == [inj[2]["id"]]
    rows = await exchange.get("/orders", contract="INJ_USDT", status="open", offset=1, limit=1)
    assert [row["id"] for row in rows] == [inj[0]["id"]]
    assert await exchange.get("/orders", contract="INJ_USDT", status="open", offset=2) == []
    rows = await exchange.get("/orders", contract="INJ_USDT", status="finished")
    assert len(rows) == 1 and rows[0]["finish_as"] == "cancelled"
    assert (await exchange.get("/orders/tag-0"))["id"] == inj[0]["id"]
    with pytest.raises(GateError, match="INVALID_STATUS"):
        await exchange.get("/orders", status="anything")
    with pytest.raises(GateError, match="INVALID_SIZE"):
        await exchange.get("/orders", offset=-1)


async def test_partial_ioc_is_terminal_and_control_is_per_contract_once(exchange):
    exchange.set_next_market_fill("INJ_USDT", 3)
    partial = await order(exchange)
    other = await order(exchange, "JTO_USDT", size=4)
    next_order = await order(exchange, size=7)
    assert (partial["status"], partial["left"], partial["finish_as"]) == ("finished", 7, "ioc")
    assert other["left"] == next_order["left"] == 0
    assert exchange.positions["INJ_USDT"]["size"] == 10
    assert exchange.positions["JTO_USDT"]["size"] == 4
    with pytest.raises(GateError, match="ORDER_CLOSED"):
        exchange.fill(partial["id"], 1)


async def test_limit_fills_require_explicit_driver_and_keep_weighted_average(exchange):
    pending = await order(exchange, price="12")
    exchange.set_market("INJ_USDT", price="11")
    assert exchange.positions["INJ_USDT"]["size"] == 0
    first = exchange.fill(pending["id"], 2, price="10")
    second = exchange.fill(pending["id"], 3, price="11")
    assert (first["left"], second["left"], second["status"]) == (8, 5, "open")
    assert Decimal(second["fill_price"]) == Decimal("10.6")
    assert Decimal(exchange.positions["INJ_USDT"]["entry_price"]) == Decimal("10.6")
    with pytest.raises(GateError, match="FILL_VIOLATES_LIMIT"):
        exchange.fill(pending["id"], 1, price="13")
    with pytest.raises(GateError, match="FILL_EXCEEDS_REMAINDER"):
        exchange.fill(pending["id"], 6)
    exchange.fill(pending["id"], 5)
    assert (await exchange.get(f"/orders/{pending['id']}"))["finish_as"] == "filled"


async def test_cancel_partial_and_amend_preserve_filled_quantity(exchange):
    pending = await order(exchange, size=-10, price="10")
    exchange.fill(pending["id"], 3)
    changed = await exchange.request("PUT", f"/orders/{pending['id']}", {"size": -12, "price": "11"})
    assert (changed["left"], changed["size"]) == (9, -12)
    with pytest.raises(GateError, match="INVALID_AMENDED_SIZE"):
        await exchange.request("PUT", f"/orders/{pending['id']}", {"size": 12})
    cancelled = await exchange.request("DELETE", f"/orders/{pending['id']}")
    assert cancelled["left"] == 9 and exchange.positions["INJ_USDT"]["size"] == -3
    with pytest.raises(GateError, match="ORDER_CLOSED"):
        await exchange.request("DELETE", f"/orders/{pending['id']}")
    assert (await exchange.get(f"/orders/{pending['id']}"))["finish_as"] == "cancelled"


async def test_reduce_only_cannot_increase_or_reverse_position(exchange):
    await order(exchange, size=5)
    wrong_side = await order(exchange, size=3, reduce=True)
    assert wrong_side["left"] == 3 and exchange.positions["INJ_USDT"]["size"] == 5
    oversized = await order(exchange, size=-8, reduce=True)
    assert oversized["left"] == 3 and exchange.positions["INJ_USDT"]["size"] == 0
    empty = await order(exchange, size=-2, reduce=True)
    assert empty["left"] == 2 and exchange.positions["INJ_USDT"]["size"] == 0


@pytest.mark.parametrize("side,level,rule", [(1, "9", 2), (-1, "11", 1)])
async def test_stop_trigger_is_explicit_uses_mark_and_partially_reduces(exchange, side, level, rule):
    await order(exchange, size=5 * side)
    await order(exchange, "JTO_USDT", size=-4)
    pending = await stop(exchange, price=level, rule=rule)
    with pytest.raises(GateError, match="TRIGGER_NOT_REACHED"):
        exchange.trigger(pending["id"])
    exchange.set_market("INJ_USDT", price=level)
    assert exchange.stops[pending["id"]]["status"] == "open"
    finished = exchange.trigger(pending["id"], quantity=2)
    assert finished["status"] == "finished" and finished["finish_as"] == "succeeded"
    child = await exchange.get(f"/orders/{finished['trade_id']}")
    assert child["is_reduce_only"] and child["left"] == 3
    assert exchange.positions["INJ_USDT"]["size"] == 3 * side
    assert exchange.positions["JTO_USDT"]["size"] == -4
    with pytest.raises(GateError, match="ORDER_CLOSED"):
        exchange.trigger(pending["id"])


async def test_stop_amend_filter_and_pagination_preserve_exact_intent(exchange):
    first = await stop(exchange)
    second = await stop(exchange, price="8")
    await stop(exchange, symbol="JTO_USDT", price="1.500")
    changed = await exchange.request("PUT", f"/price_orders/amend/{first['id']}", {
        "order_id": int(first["id"]), "size": 0, "price": "0", "close": True,
        "trigger_price": "9.50", "price_type": 1,
    })
    assert changed == int(first["id"])
    await exchange.request("DELETE", f"/price_orders/{second['id']}")
    opened = await exchange.get("/price_orders", contract="INJ_USDT", status="open", limit=1)
    assert len(opened) == 1 and opened[0]["trigger"]["price"] == "9.50"
    assert opened[0]["initial"]["is_close"] is True
    closed = await exchange.get("/price_orders", contract="INJ_USDT", status="finished")
    assert [row["id"] for row in closed] == [second["id"]]
    assert await exchange.get("/price_orders", contract="INJ_USDT", status="finished", offset=1) == []


async def test_contract_ticks_snapshot_copies_and_unknown_routes_fail_closed(exchange):
    with pytest.raises(GateError, match="INVALID_PRICE"):
        await order(exchange, price="9.999")
    precise = await order(exchange, "JTO_USDT", price="1.999")
    assert precise["price"] == "1.999"
    with pytest.raises(GateError, match="CONTRACT_NOT_FOUND"):
        await exchange.get("/positions/BTC_USDT")
    with pytest.raises(AssertionError, match="Unsupported"):
        await exchange.request("POST", "https://api.gateio.ws/orders", {})
    rows = await exchange.candles("INJ_USDT")
    assert len(rows) == 120 and rows[-1]["t"] - rows[0]["t"] == 119 * 300
    custom = [{"c": "12"}]
    exchange.set_candles("INJ_USDT", custom)
    custom[0]["c"] = "999"
    assert (await exchange.candles("INJ_USDT"))[0]["c"] == "12"
    assert exchange.ready and exchange.status()["environment"] == "simulation"


async def test_two_contract_engine_lifecycle_and_native_stop_leave_other_trade_untouched(exchange):
    store = Store(":memory:")
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    try:
        for number, symbol in enumerate(("INJ", "JTO"), 1):
            key = store.event(-100123, number, "scripted entry", None, time.time(), True)
            event = store.db.execute("SELECT * FROM events WHERE id=?", (key,)).fetchone()
            await engine.process(event, Signal(action="open", symbol=symbol, side="long"))
        for values in (("9", "11", "12"), ("1.9", "2.1", "2.2")):
            job = await engine.claim_plan()
            proposal = Plan(sl=values[0], tp1=values[1], tp2=values[2],
                            tp1_fraction="0.5", rationale="local fixture protocol verification")
            args = (job["trade_id"], job["version"], job["lease"], proposal)
            await engine.propose_plan(*args)
            await engine.apply_plan(*args)
        trades = {trade["symbol"]: trade for trade in store.trades()}
        other_size = exchange.positions["JTO_USDT"]["size"]
        other_stop = trades["JTO_USDT"]["exits"]["sl"]["id"]
        exchange.set_market("INJ_USDT", price="9")
        exchange.trigger(trades["INJ_USDT"]["exits"]["sl"]["id"])
        await engine.poll()
        await engine.poll()
        assert store.trade(trades["INJ_USDT"]["id"])["status"] == "closed"
        assert not await exchange.get("/orders", contract="INJ_USDT", status="open")
        assert exchange.positions["JTO_USDT"]["size"] == other_size
        assert exchange.stops[other_stop]["status"] == "open"
        assert not store.unknowns()
    finally:
        store.close()
