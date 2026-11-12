"""404 order reads may use only exact, confirmed terminal write receipts.

Offline only: temporary SQLite plus FakeExchange; no credentials or real API.
"""

import pytest
from comprehensive_fixtures import FakeExchange
from test_comprehensive_execution import decision

from trader.engine import Engine
from trader.gate import GateError
from trader.store import Store

ORDER_ID = "42"


@pytest.fixture
def rig(tmp_path):
    store = Store(tmp_path / "terminal-receipts.sqlite3")
    exchange = FakeExchange(equity="70")
    exchange.add_contract("FIL_USDT")
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield store, exchange, engine
    store.close()


def persist(store, method, path, *, key="receipt", operation_status="confirmed", patch=None):
    receipt = {"id": ORDER_ID, "status": "finished", "finish_as": "ioc",
               "contract": "FIL_USDT", "size": "14", "left": "14", "fill_price": "0"}
    receipt.update(patch or {})
    store.intent(key, "owned-trade", method, path, {"body": None, "params": None})
    store.result(key, receipt, operation_status)
    return receipt


@pytest.mark.parametrize("method,path", [
    ("POST", "/orders"), ("PUT", f"/orders/{ORDER_ID}"), ("DELETE", f"/orders/{ORDER_ID}"),
])
@pytest.mark.parametrize("numeric_id", [False, True])
async def test_confirmed_terminal_receipts_for_each_regular_write_method_cover_404(
    rig, method, path, numeric_id
):
    store, exchange, engine = rig
    receipt = persist(store, method, path, patch={"id": 42 if numeric_id else ORDER_ID})
    assert await engine.order(ORDER_ID) == receipt
    assert not exchange.writes


@pytest.mark.parametrize("method,path,operation_status,patch", [
    ("POST", "/orders", "confirmed", {"status": "open"}),
    ("PUT", f"/orders/{ORDER_ID}", "confirmed", {"status": "open"}),
    ("DELETE", f"/orders/{ORDER_ID}", "confirmed", {"status": "open"}),
    ("POST", "/orders", "unknown", {}),
    ("PUT", f"/orders/{ORDER_ID}", "unknown", {}),
    ("DELETE", f"/orders/{ORDER_ID}", "unknown", {}),
    ("POST", "/orders", "rejected", {}),
    ("PUT", f"/orders/{ORDER_ID}", "rejected", {}),
    ("DELETE", f"/orders/{ORDER_ID}", "rejected", {}),
    ("POST", "/orders", "confirmed", {"id": "43"}),
    ("PUT", f"/orders/{ORDER_ID}", "confirmed", {"id": "43"}),
    ("DELETE", f"/orders/{ORDER_ID}", "confirmed", {"id": "43"}),
    ("PUT", "/orders/43", "confirmed", {}),
    ("DELETE", "/orders/43", "confirmed", {}),
    ("POST", f"/orders/{ORDER_ID}", "confirmed", {}),
    ("PUT", "/orders", "confirmed", {}),
    ("DELETE", "/orders", "confirmed", {}),
    ("POST", "/price_orders", "confirmed", {}),
    ("PUT", f"/price_orders/{ORDER_ID}", "confirmed", {}),
    ("DELETE", f"/price_orders/{ORDER_ID}", "confirmed", {}),
    ("GET", f"/orders/{ORDER_ID}", "confirmed", {}),
])
async def test_nonterminal_unconfirmed_wrong_identity_or_wrong_route_never_resolves_404(
    rig, method, path, operation_status, patch
):
    store, exchange, engine = rig
    persist(store, method, path, operation_status=operation_status, patch=patch)
    with pytest.raises(GateError) as error:
        await engine.order(ORDER_ID)
    assert error.value.status == 404
    assert not exchange.writes
    assert store.operation("receipt")["status"] == operation_status


async def test_no_terminal_receipt_keeps_missing_order_unknown(rig):
    _store, exchange, engine = rig
    with pytest.raises(GateError) as error:
        await engine.order(ORDER_ID)
    assert error.value.status == 404
    assert not exchange.writes


async def test_newer_unrelated_post_does_not_hide_older_exact_terminal_receipt(rig):
    store, exchange, engine = rig
    expected = persist(store, "POST", "/orders", key="older-target")
    persist(store, "POST", "/orders", key="newer-other", patch={"id": "43"})
    with store.db:
        store.db.execute("UPDATE operations SET created=100 WHERE id='older-target'")
        store.db.execute("UPDATE operations SET created=200 WHERE id='newer-other'")
    assert await engine.order(ORDER_ID) == expected
    assert not exchange.writes


@pytest.mark.parametrize("error", [
    GateError(400, "ORDER_NOT_FOUND"), GateError(401, "INVALID_KEY"),
    GateError(403, "FORBIDDEN"), GateError(429, "TOO_MANY_REQUESTS"),
    GateError(500, "INTERNAL_SERVER_ERROR"), TimeoutError("offline timeout"),
    OSError("offline transport failure"),
])
async def test_terminal_receipt_never_masks_non404_query_errors(rig, monkeypatch, error):
    store, exchange, engine = rig
    persist(store, "POST", "/orders", key="post")
    persist(store, "DELETE", f"/orders/{ORDER_ID}", key="delete")
    original = exchange.get

    async def fail(path, **params):
        if path == f"/orders/{ORDER_ID}":
            raise error
        return await original(path, **params)

    monkeypatch.setattr(exchange, "get", fail)
    with pytest.raises(type(error)) as caught:
        await engine.order(ORDER_ID)
    assert caught.value is error
    assert not exchange.writes


async def test_successful_live_order_read_takes_precedence_over_cached_receipt(rig, monkeypatch):
    store, exchange, engine = rig
    old = persist(store, "POST", "/orders")
    current = old | {"status": "open", "left": "10"}
    original = exchange.get

    async def actual(path, **params):
        if path == f"/orders/{ORDER_ID}":
            return current
        return await original(path, **params)

    monkeypatch.setattr(exchange, "get", actual)
    assert await engine.order(ORDER_ID) == current
    assert not exchange.writes


@pytest.mark.parametrize("side", ["long", "short"])
async def test_zero_fill_ioc_hidden_after_post_cleans_up_across_restart_without_resend(
    rig, monkeypatch, side
):
    store, exchange, _engine = rig
    exchange.set_next_market_fill("FIL_USDT", 0)
    original = exchange.get

    async def hide_terminal_orders(path, **params):
        if path.startswith("/orders/"):
            raise GateError(404, "ORDER_NOT_FOUND")
        return await original(path, **params)

    monkeypatch.setattr(exchange, "get", hide_terminal_orders)
    result = await decision(rig, "open", symbol="FIL", side=side)
    assert result["status"] == "done", result
    trade = store.trades()[0]
    assert trade["filled"] == 0 and trade["entry_terminal"]
    assert exchange.positions["FIL_USDT"]["size"] == 0
    before = len(exchange.writes)
    restarted = Engine(store, exchange, enabled=True)
    for _ in range(4):
        await restarted.poll()
    current = store.trade(trade["id"])
    assert current["status"] == "closed"
    assert not current["plan_needed"] and not current["exits"]
    assert not current.get("last_error")
    assert len(exchange.writes) == before  # no resubmission or speculative cancellation
    assert len([write for write in exchange.writes if write[0:2] == ("POST", "/orders")]) == 1
    assert len(exchange.orders) == 1
    assert not store.unknowns() and not restarted.paused()
