"""Malformed successful Gate replies must fail closed without losing owned intent.

The real Gate parser and Engine run against an in-memory HTTP-shaped adapter.
No aiohttp session, credentials, sockets, or real exchange requests are used.
"""

import copy
import json
import time
from urllib.parse import parse_qsl, urlsplit

import pytest
from comprehensive_fixtures import FakeExchange
from test_comprehensive_execution import decision, entry
from test_take_profit_policy import apply_current_plan

from trader.engine import Engine
from trader.gate import Gate, GateError
from trader.store import Store


class MemoryResponse:
    def __init__(self, session, method, url, data):
        self.session, self.method = session, method
        parsed = urlsplit(url)
        assert parsed.hostname == "in-memory.invalid", "Only the memory adapter is permitted"
        prefix = "/api/v4/futures/usdt"
        assert parsed.path.startswith(prefix)
        self.path = parsed.path.removeprefix(prefix)
        self.params = dict(parse_qsl(parsed.query))
        self.body = json.loads(data) if data else None
        self.status = 200

    async def __aenter__(self):
        try:
            self.payload = await self.session.exchange.request(
                self.method, self.path, body=self.body, params=self.params
            )
        except GateError as error:
            self.status = error.status
            self.payload = {"label": error.label, "message": "in-memory rejection"}
        else:
            self.payload = self.session.rewrite(self.method, self.path, self.payload)
        return self

    async def __aexit__(self, *_args):
        pass

    async def json(self, **_kwargs):
        return copy.deepcopy(self.payload)


class MemorySession:
    def __init__(self, exchange):
        self.exchange = exchange
        self.rewrite = lambda _method, _path, payload: payload

    def request(self, method, url, *, data=None, **_kwargs):
        return MemoryResponse(self, method, url, data)


@pytest.fixture
def rig():
    store = Store(":memory:")
    exchange = FakeExchange(equity="70")
    exchange.add_contract("INJ_USDT")
    session = MemorySession(exchange)
    gate = Gate(session, "offline-key", "offline-secret")
    gate.base = "http://in-memory.invalid"
    engine = Engine(store, gate, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield (store, exchange, engine), session
    store.close()


def stop_body(symbol="INJ_USDT", text="t-offline-manual-stop"):
    return {
        "initial": {"contract": symbol, "size": 0, "price": "0", "tif": "ioc",
                    "close": True, "reduce_only": True, "text": text},
        "trigger": {"strategy_type": 0, "price_type": 1, "price": "11", "rule": 1,
                    "expiration": 86400},
        "order_type": "plan-close-short-position",
    }


def entry_posts(exchange):
    return [write for write in exchange.writes if write[:2] == ("POST", "/orders")
            and not write[2].get("reduce_only")]


async def test_null_order_status_cannot_leave_an_entry_live_after_reporting_close(rig):
    state, session = rig
    store, exchange, engine = state
    trade = await entry(state, side="long", limit=True)
    exchange.fill(trade["entry_id"], 4, price="10")
    await engine.poll()
    trade = store.trade(trade["id"])
    before = len(exchange.writes)
    session.rewrite = lambda method, path, payload: (
        payload | {"status": None}
        if method == "GET" and path == f"/orders/{trade['entry_id']}" else payload
    )

    result = await decision(state, "close", trade=trade)
    assert result["status"] == "error"
    assert len(exchange.writes) == before  # no false cancellation proof or premature close IOC
    assert exchange.positions["INJ_USDT"]["size"] == 4
    assert exchange.orders[trade["entry_id"]]["status"] == "open"
    assert store.trade(trade["id"])["close_requested"]

    session.rewrite = lambda _method, _path, payload: payload
    for _ in range(4):
        await engine.poll()
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert exchange.orders[trade["entry_id"]]["status"] == "finished"
    assert store.trade(trade["id"])["status"] == "closed"
    with pytest.raises(GateError, match="ORDER_CLOSED"):
        exchange.fill(trade["entry_id"], price="10")
    assert len(entry_posts(exchange)) == 1


@pytest.mark.parametrize("patch", [
    {"left": -15}, {"left": 15}, {"size": "14.5"}, {"left": "13.5"},
])
async def test_impossible_tp2_remaining_quantity_never_becomes_fill_evidence(rig, patch):
    state, session = rig
    store, exchange, engine = state
    await entry(state)
    trade = await apply_current_plan(state)
    order_id = trade["exits"]["tp"]["id"]
    assert exchange.orders[order_id]["size"] == 14
    assert exchange.orders[order_id]["left"] == 14
    before = len(exchange.writes)
    session.rewrite = lambda method, path, payload: (
        payload | patch
        if method == "GET" and path == f"/orders/{order_id}" else payload
    )

    await engine.poll()
    current = store.trade(trade["id"])
    assert not current["close_requested"]
    assert current["last_error"] and engine.paused()
    assert exchange.positions["INJ_USDT"]["size"] == -14
    assert len(exchange.writes) == before


@pytest.mark.parametrize("path", ["/orders", "/price_orders"])
async def test_null_open_order_list_never_means_no_unmanaged_orders(rig, path):
    state, session = rig
    _store, exchange, _engine = state
    body = ({"contract": "INJ_USDT", "size": 1, "price": "9", "tif": "gtc",
             "text": "t-offline-manual"} if path == "/orders" else stop_body())
    manual = await exchange.request("POST", path, body=body)
    before = len(exchange.writes)
    session.rewrite = lambda method, route, payload: (
        None if method == "GET" and route == path else payload
    )

    result = await decision(state, "open", symbol="INJ", side="long")
    assert result["status"] == "error"
    assert len(exchange.writes) == before
    assert exchange.positions["INJ_USDT"]["size"] == 0
    collection = exchange.orders if path == "/orders" else exchange.stops
    assert collection[manual["id"]]["status"] == "open"


@pytest.mark.parametrize("failed_read", [1, 2])
async def test_http_200_null_position_is_never_a_valid_missing_or_verified_position(rig, failed_read):
    state, session = rig
    _store, exchange, _engine = state
    position_reads = 0

    def null_position(method, path, payload):
        nonlocal position_reads
        if method == "GET" and path == "/positions/INJ_USDT":
            position_reads += 1
            if position_reads == failed_read:
                return None
        return payload

    session.rewrite = null_position
    result = await decision(state, "open", symbol="INJ", side="long")
    assert result["status"] == "error"
    assert position_reads == failed_read
    assert not entry_posts(exchange)
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert len(exchange.writes) == failed_read - 1  # only the second case may set leverage


@pytest.mark.parametrize("bad_reply", [
    {}, {"id": True}, {"id": 1.0}, {"id": "1.0"},
    {"contract": "BTC_USDT"}, {"text": "t-wrong-intent"}, {"size": -14},
])
async def test_invalid_entry_reply_is_unknown_then_exact_lookup_recovers_without_resend(rig, bad_reply):
    state, session = rig
    store, exchange, engine = state
    session.rewrite = lambda method, path, payload: (
        (payload | bad_reply if bad_reply else {})
        if (method, path) == ("POST", "/orders") else payload
    )
    result = await decision(state, "open", symbol="INJ", side="long")
    trade = store.trades()[0]
    key = trade["id"] + ":entry"
    assert result["status"] == "error"
    assert exchange.positions["INJ_USDT"]["size"] == 14
    assert store.operation(key)["status"] == "unknown"
    assert engine.paused()

    session.rewrite = lambda _method, _path, payload: payload
    for _ in range(3):
        await engine.poll()
    current = store.trade(trade["id"])
    assert store.operation(key)["status"] == "confirmed" and not store.unknowns()
    assert current["entry_id"] in exchange.orders
    assert current["status"] == "working" and current["filled"] == 14
    assert current["plan_needed"]
    assert engine.paused()  # recovering the receipt does not silently resume new entries
    assert len(entry_posts(exchange)) == 1


@pytest.mark.parametrize("resource", ["order", "position", "trigger"])
async def test_successful_detail_reply_must_belong_to_requested_resource(rig, resource):
    state, session = rig
    _store, exchange, engine = state
    await entry(state)
    trade = await apply_current_plan(state)
    path = {
        "order": f"/orders/{trade['exits']['tp']['id']}",
        "position": "/positions/INJ_USDT",
        "trigger": f"/price_orders/{trade['exits']['sl']['id']}",
    }[resource]
    patch = {"contract": "BTC_USDT"} if resource == "position" else {"id": 99999}
    session.rewrite = lambda method, route, payload: (
        payload | patch if method == "GET" and route == path else payload
    )
    before = len(exchange.writes)
    with pytest.raises(GateError):
        await engine.gate.get(path)
    assert len(exchange.writes) == before
    assert exchange.positions["INJ_USDT"]["size"] == -14


@pytest.mark.parametrize("response_shape", ["integer", "object"])
async def test_trigger_amend_wrong_receipt_id_stays_unknown_without_resend(rig, response_shape):
    state, session = rig
    store, exchange, engine = state
    await entry(state)
    trade = await apply_current_plan(state)
    route = f"/price_orders/amend/{trade['exits']['sl']['id']}"
    session.rewrite = lambda method, path, payload: (
        (99999 if response_shape == "integer" else {"id": 99999})
        if method == "PUT" and path == route else payload
    )
    result = await decision(state, "sl", trade=trade, price="10.5")
    assert result["status"] == "error"
    unknown = store.unknowns()
    assert len(unknown) == 1 and unknown[0]["path"] == route
    assert engine.paused()
    for _ in range(3):
        await engine.poll()
    assert store.operation(unknown[0]["id"])["status"] == "unknown"
    assert len([write for write in exchange.writes if write[:2] == ("PUT", route)]) == 1


@pytest.mark.parametrize("patch", [
    {"contract": "BTC_USDT"}, {"text": "t-another-intent"}, {"size": -14},
    {"size": 15}, {"price": "9"}, {"tif": "gtc"},
    {"is_reduce_only": True, "reduce_only": True},
])
async def test_unknown_regular_receipt_must_match_original_intent_before_confirmation(rig, patch):
    state, session = rig
    store, exchange, engine = state
    session.rewrite = lambda method, path, payload: (
        {} if (method, path) == ("POST", "/orders") else payload
    )
    await decision(state, "open", symbol="INJ", side="long")
    trade = store.trades()[0]
    key = trade["id"] + ":entry"
    body = json.loads(store.operation(key)["body"])["body"]
    session.rewrite = lambda method, path, payload: (
        payload | patch if method == "GET" and path == f"/orders/{body['text']}" else payload
    )
    for _ in range(2):
        await engine.poll()
    assert store.operation(key)["status"] == "unknown"
    assert store.trade(trade["id"])["entry_id"] is None
    assert engine.paused() and len(entry_posts(exchange)) == 1
    assert exchange.positions["INJ_USDT"]["size"] == 14

    session.rewrite = lambda _method, _path, payload: payload
    await engine.poll()
    assert store.operation(key)["status"] == "confirmed"
    assert store.trade(trade["id"])["filled"] == 14
    assert not store.unknowns() and len(entry_posts(exchange)) == 1


@pytest.mark.parametrize("maintenance", ["close", "renew_stop"])
async def test_malformed_trigger_list_does_not_starve_another_owned_trade(rig, maintenance):
    state, session = rig
    store, exchange, engine = state
    trade = await entry(state)
    trade = await apply_current_plan(state)
    old_stop = trade["exits"]["sl"]["id"]
    if maintenance == "close":
        exchange.set_next_market_fill("INJ_USDT", 2)
        await decision(state, "close", trade=trade)
        assert exchange.positions["INJ_USDT"]["size"] == -12
    else:
        trade["exits"]["sl"]["created"] = time.time() - 83000
        store.save_trade(trade)
    exchange.add_contract("BTC_USDT")
    other = await entry(state, symbol="BTC")
    body = stop_body("BTC_USDT", "t-unknown-stop")
    store.intent("unknown-stop", other["id"], "POST", "/price_orders",
                 {"body": body, "params": None})
    session.rewrite = lambda method, path, payload: (
        None if method == "GET" and path == "/price_orders" else payload
    )

    for _ in range(4):
        await engine.poll()
    assert store.operation("unknown-stop")["status"] == "unknown"
    assert engine.paused()
    assert not any(stop["initial"]["text"] == "t-unknown-stop"
                   for stop in exchange.stops.values())
    current = store.trade(trade["id"])
    if maintenance == "close":
        assert current["status"] == "closed" and exchange.positions["INJ_USDT"]["size"] == 0
    else:
        assert current["exits"]["sl"]["id"] != old_stop
        assert exchange.stops[old_stop]["status"] == "finished"
        assert exchange.stops[current["exits"]["sl"]["id"]]["status"] == "open"
        assert exchange.positions["INJ_USDT"]["size"] == -14


@pytest.mark.parametrize("receipt_mode", ["legacy_full", "id_only", "id_and_string"])
async def test_valid_trigger_receipt_shapes_and_terminal_order_reads_still_work(rig, receipt_mode):
    state, session = rig
    store, exchange, engine = state

    def trigger_receipt(method, path, payload):
        if receipt_mode == "legacy_full":
            return payload
        if (method, path) == ("POST", "/price_orders"):
            order_id = int(payload["id"])
        elif method == "PUT" and path.startswith("/price_orders/amend/"):
            order_id = int(payload)
        else:
            return payload
        result = {"id": order_id}
        if receipt_mode == "id_and_string":
            result["id_string"] = str(order_id)
        return result

    session.rewrite = trigger_receipt
    trade = await entry(state)
    trade = await apply_current_plan(state)
    result = await decision(state, "sl", trade=trade, price="10.5")
    assert result["status"] == "done"
    trade = store.trade(trade["id"])
    assert trade["exits"]["sl"]["price"] == "10.5"
    assert (await decision(state, "close", trade=trade))["status"] == "done"
    for _ in range(3):
        await engine.poll()
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert store.trade(trade["id"])["status"] == "closed"
    assert not store.unknowns()
