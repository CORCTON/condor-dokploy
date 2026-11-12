"""Gate response protocol tests; fake sessions never connect to an exchange."""

import copy

import pytest

from trader.engine import Engine, Review
from trader.gate import Gate, GateError
from trader.store import Store

MISSING_TRIGGER = {
    "code": "1034", "label": "AUTO_ORDER_NOT_FOUND", "message": "no orderID match",
}


class Response:
    def __init__(self, status, payload):
        self.status, self.payload = status, payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        pass

    async def json(self, **_kwargs):
        if isinstance(self.payload, ValueError):
            raise self.payload
        return copy.deepcopy(self.payload)


class Session:
    def __init__(self, status, payload):
        self.status, self.payload, self.calls = status, payload, []

    def request(self, method, url, **_kwargs):
        self.calls.append((method, url))
        return Response(self.status, self.payload)


@pytest.mark.parametrize("status", [200, 205])
async def test_http_2xx_trigger_error_is_not_a_successful_lookup(status):
    session = Session(status, MISSING_TRIGGER)
    gate = Gate(session, "test-key", "test-secret")
    with pytest.raises(GateError) as raised:
        await gate.get("/price_orders/999999999999999999")
    assert (raised.value.status, raised.value.label) == (status, "AUTO_ORDER_NOT_FOUND")


@pytest.mark.parametrize("method,path", [
    ("POST", "/orders"), ("POST", "/price_orders"),
    ("PUT", "/price_orders/amend/123"), ("DELETE", "/price_orders/123"),
])
@pytest.mark.parametrize("status", [200, 205])
async def test_http_2xx_error_write_stays_unknown_and_cannot_be_blindly_retried(method, path, status):
    session = Session(status, MISSING_TRIGGER)
    store = Store(":memory:")
    engine = Engine(store, Gate(session, "test-key", "test-secret"), enabled=True)
    store.set("paused", False)
    try:
        with pytest.raises(GateError):
            await engine.write({"id": "trade"}, "trade:probe", method, path)
        assert store.operation("trade:probe")["status"] == "unknown"
        assert engine.paused()
        with pytest.raises(Review, match="no blind retry"):
            await engine.write({"id": "trade"}, "trade:probe", method, path)
        assert len(session.calls) == 1
    finally:
        store.close()


@pytest.mark.parametrize("payload", [
    [], [{"id": 123, "status": "open"}], 123,
    {"contract": "FIL_USDT", "size": "0"},
    {"id": 123, "status": "finished", "size": "12", "left": "0"},
    {"code": "0", "message": "success"},
])
async def test_success_payload_shapes_are_preserved(payload):
    gate = Gate(Session(200, payload), "test-key", "test-secret")
    assert await gate.get("/fixture") == payload


@pytest.mark.parametrize("status,payload,label", [
    (400, {"label": "POSITION_NOT_FOUND", "message": "not found"}, "POSITION_NOT_FOUND"),
    (401, {"label": "INVALID_KEY"}, "INVALID_KEY"),
    (429, {"label": "TOO_MANY_REQUESTS"}, "TOO_MANY_REQUESTS"),
    (503, None, "HTTP_ERROR"),
    (502, [], "HTTP_ERROR"),
])
async def test_http_errors_keep_original_status(status, payload, label):
    gate = Gate(Session(status, payload), "test-key", "test-secret")
    with pytest.raises(GateError) as raised:
        await gate.get("/fixture")
    assert (raised.value.status, raised.value.label) == (status, label)


@pytest.mark.parametrize("status", [200, 400, 502, 503])
async def test_non_json_response_is_a_lookup_failure_not_an_order_receipt(status):
    gate = Gate(Session(status, ValueError("invalid JSON")), "test-key", "test-secret")
    with pytest.raises(GateError) as raised:
        await gate.get("/orders/123")
    assert (raised.value.status, raised.value.label) == (status, "INVALID_JSON_RESPONSE")


async def test_http_200_non_json_write_remains_unknown():
    session = Session(200, ValueError("invalid JSON"))
    store = Store(":memory:")
    engine = Engine(store, Gate(session, "test-key", "test-secret"), enabled=True)
    store.set("paused", False)
    try:
        with pytest.raises(GateError, match="INVALID_JSON_RESPONSE"):
            await engine.write({"id": "trade"}, "trade:probe", "POST", "/orders")
        assert store.operation("trade:probe")["status"] == "unknown"
        assert engine.paused() and len(session.calls) == 1
    finally:
        store.close()
