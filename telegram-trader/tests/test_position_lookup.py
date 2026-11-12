"""Initial missing-position records are not errors that may hide owned exposure.

Every exchange response below is injected into the in-memory Gate fixture. These
tests never create a network session, read credentials, or address a real account.
"""

import pytest
from comprehensive_fixtures import FakeExchange
from test_comprehensive_execution import decision

from trader.engine import Engine
from trader.gate import GateError
from trader.models import Plan
from trader.store import Store

SYMBOL = "FIL_USDT"
POSITION = f"/positions/{SYMBOL}"


@pytest.fixture
def rig(tmp_path):
    store = Store(tmp_path / "position-lookup.sqlite3")
    exchange = FakeExchange(equity="70")
    exchange.add_contract(SYMBOL)
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield store, exchange, engine
    store.close()


def entry_writes(exchange):
    return [write for write in exchange.writes if write[0:2] == ("POST", "/orders")]


@pytest.mark.parametrize("side", ["long", "short"])
async def test_first_contract_missing_record_allows_entry_only_after_real_mode_verification(
    rig, monkeypatch, side
):
    store, exchange, _engine = rig
    original = exchange.get
    reads = []

    async def position_not_created_yet(path, **params):
        if path == POSITION:
            reads.append(len(exchange.writes))
            if len(reads) == 1:
                raise GateError(400, "POSITION_NOT_FOUND")
            assert any(write[1] == POSITION + "/set_leverage" for write in exchange.writes)
        return await original(path, **params)

    monkeypatch.setattr(exchange, "get", position_not_created_yet)
    result = await decision(rig, "open", symbol="FIL", side=side)
    assert result["status"] == "done", result
    assert reads[0] == 0 and reads[1] == 1
    assert len(entry_writes(exchange)) == 1
    assert exchange.writes[0][1] == POSITION + "/set_leverage"
    assert exchange.writes[0][3] == {"leverage": "10", "margin_mode": "isolated"}
    assert exchange.positions[SYMBOL]["size"] == (14 if side == "long" else -14)
    assert store.trades()[0]["filled"] == 14
    assert not store.unknowns()


@pytest.mark.parametrize("status,label", [
    (400, "INVALID_PARAM_VALUE"), (400, "USER_NOT_FOUND"), (400, "CONTRACT_NOT_FOUND"),
    (401, "POSITION_NOT_FOUND"), (403, "POSITION_NOT_FOUND"), (404, "POSITION_NOT_FOUND"),
    (429, "POSITION_NOT_FOUND"), (500, "POSITION_NOT_FOUND"), (503, "POSITION_NOT_FOUND"),
])
async def test_only_exact_initial_400_position_not_found_may_be_normalized(
    rig, monkeypatch, status, label
):
    store, exchange, _engine = rig
    original = exchange.get

    async def fail(path, **params):
        if path == POSITION:
            raise GateError(status, label)
        return await original(path, **params)

    monkeypatch.setattr(exchange, "get", fail)
    result = await decision(rig, "open", symbol="FIL", side="short")
    assert result["status"] == "error"
    assert result["detail"] == f"Gate {status}: {label}"
    assert not exchange.writes
    assert not store.trades() and not store.unknowns()


@pytest.mark.parametrize("error", [TimeoutError("offline timeout"), OSError("offline transport")])
async def test_initial_position_transport_failure_is_never_treated_as_zero(rig, monkeypatch, error):
    store, exchange, _engine = rig
    original = exchange.get

    async def fail(path, **params):
        if path == POSITION:
            raise error
        return await original(path, **params)

    monkeypatch.setattr(exchange, "get", fail)
    assert (await decision(rig, "open", symbol="FIL", side="short"))["status"] == "error"
    assert not exchange.writes
    assert not store.trades()


@pytest.mark.parametrize("route", ["/accounts", "/orders", "/price_orders", f"/contracts/{SYMBOL}"])
async def test_same_missing_label_from_other_preflight_routes_is_not_normalized(
    rig, monkeypatch, route
):
    store, exchange, _engine = rig
    original = exchange.get

    async def wrong_route_error(path, **params):
        if path == route:
            raise GateError(400, "POSITION_NOT_FOUND")
        return await original(path, **params)

    monkeypatch.setattr(exchange, "get", wrong_route_error)
    result = await decision(rig, "open", symbol="FIL", side="short")
    assert result == {"status": "error", "detail": "Gate 400: POSITION_NOT_FOUND"}
    assert not exchange.writes and not store.trades()


@pytest.mark.parametrize("size", [-3, 3])
async def test_existing_manual_position_still_blocks_initial_entry_without_writes(rig, size):
    store, exchange, _engine = rig
    exchange.positions[SYMBOL]["size"] = size
    result = await decision(rig, "open", symbol="FIL", side="short")
    assert result == {"status": "error", "detail": "unmanaged position exists"}
    assert exchange.positions[SYMBOL]["size"] == size
    assert not exchange.writes and not store.trades()


@pytest.mark.parametrize("order_route", ["/orders", "/price_orders"])
async def test_missing_initial_position_does_not_bypass_existing_orders(rig, monkeypatch, order_route):
    store, exchange, _engine = rig
    original = exchange.get

    async def missing_with_manual_order(path, **params):
        if path == POSITION:
            raise GateError(400, "POSITION_NOT_FOUND")
        if path == order_route:
            return [{"id": "manual-order", "status": "open"}]
        return await original(path, **params)

    monkeypatch.setattr(exchange, "get", missing_with_manual_order)
    result = await decision(rig, "open", symbol="FIL", side="short")
    expected = "unmanaged open orders exist" if order_route == "/orders" else "unmanaged trigger orders exist"
    assert result == {"status": "error", "detail": expected}
    assert not exchange.writes and not store.trades()


async def test_missing_position_after_leverage_write_still_forbids_order(rig, monkeypatch):
    store, exchange, _engine = rig
    original = exchange.get

    async def always_missing(path, **params):
        if path == POSITION:
            raise GateError(400, "POSITION_NOT_FOUND")
        return await original(path, **params)

    monkeypatch.setattr(exchange, "get", always_missing)
    result = await decision(rig, "open", symbol="FIL", side="short")
    assert result == {"status": "error", "detail": "Gate 400: POSITION_NOT_FOUND"}
    assert len(exchange.writes) == 1
    assert exchange.writes[0][1] == POSITION + "/set_leverage"
    assert not entry_writes(exchange)
    assert exchange.positions[SYMBOL]["size"] == 0
    assert store.trades()[0]["status"] == "rejected"
    assert store.operation(store.trades()[0]["id"] + ":entry") is None


@pytest.mark.parametrize("invalid", [
    {"lever": "7"}, {"pos_margin_mode": "cross"}, {"mode": "dual"}, {"size": 2},
])
async def test_initial_missing_never_fabricates_successful_post_leverage_mode(
    rig, monkeypatch, invalid
):
    store, exchange, _engine = rig
    original = exchange.get
    initial = True

    async def invalid_verification(path, **params):
        nonlocal initial
        if path == POSITION and initial:
            initial = False
            raise GateError(400, "POSITION_NOT_FOUND")
        result = await original(path, **params)
        return result | invalid if path == POSITION else result

    monkeypatch.setattr(exchange, "get", invalid_verification)
    result = await decision(rig, "open", symbol="FIL", side="short")
    assert result == {"status": "error", "detail": "isolated 10x could not be verified"}
    assert len(exchange.writes) == 1
    assert exchange.writes[0][1] == POSITION + "/set_leverage"
    assert not entry_writes(exchange)
    assert store.trades()[0]["status"] == "rejected"


async def test_owned_position_missing_is_not_zero_and_does_not_remove_protection(rig, monkeypatch):
    store, exchange, engine = rig
    assert (await decision(rig, "open", symbol="FIL", side="short"))["status"] == "done"
    trade = store.trades()[0]
    job = await engine.claim_plan()
    args = job["trade_id"], job["version"], job["lease"], Plan(sl="11", tp="8", rationale="offline protection fixture")
    await engine.propose_plan(*args)
    await engine.apply_plan(*args)
    trade = store.trade(trade["id"])
    original = exchange.get

    async def missing_owned_record(path, **params):
        if path == POSITION:
            raise GateError(400, "POSITION_NOT_FOUND")
        return await original(path, **params)

    monkeypatch.setattr(exchange, "get", missing_owned_record)
    with pytest.raises(GateError) as error:
        await engine.owned_position(trade)
    assert error.value.status == 400 and error.value.label == "POSITION_NOT_FOUND"
    before = len(exchange.writes)
    for _ in range(3):
        await engine.poll()
    current = store.trade(trade["id"])
    assert current["status"] == "working"
    assert current["position_size"] == 14
    assert not current.get("zero_reads")
    assert "POSITION_NOT_FOUND" in current["last_error"]
    assert engine.paused()
    assert len(exchange.writes) == before
    assert exchange.positions[SYMBOL]["size"] == -14
    assert exchange.stops[current["exits"]["sl"]["id"]]["status"] == "open"
    assert all(exchange.orders[current["exits"][name]["id"]]["status"] == "open"
               for name in ("tp",))
