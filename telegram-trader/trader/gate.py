import hashlib
import hmac
import json
import time
from decimal import Decimal
from typing import Annotated, Literal
from urllib.parse import urlencode

import aiohttp
from pydantic import BaseModel, Field, TypeAdapter, ValidationError, model_validator

_GateId = Annotated[int, Field(strict=True, gt=0)] | Annotated[
    str, Field(pattern=r"^[1-9][0-9]*$")
]


class _OrderReceipt(BaseModel):
    id: _GateId
    status: Literal["open", "finished"]
    contract: str = Field(min_length=1)
    size: Decimal
    left: Decimal
    fill_price: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def valid_remainder(self):
        if abs(self.left) > abs(self.size):
            raise ValueError("unfilled quantity exceeds order size")
        return self


class _TriggerReceipt(BaseModel):
    id: _GateId
    status: Literal["open", "finished", "inactive", "invalid"]
    initial: dict
    trigger: dict


class _IdReceipt(BaseModel):
    id: _GateId


class _PositionReceipt(BaseModel):
    contract: str = Field(min_length=1)
    size: Decimal


_ORDER_LIST = TypeAdapter(list[_OrderReceipt])
_TRIGGER_LIST = TypeAdapter(list[_TriggerReceipt])
_TRIGGER_AMENDMENT = TypeAdapter(_GateId | _IdReceipt)


def validate_response(method, path, result, status):
    """Validate consumed Gate fields, not an invented replacement exchange schema.

    Keep the original payload (including extra fields) and HTTP status. A malformed
    2xx write is uncertain, never a confirmed receipt or a proven rejection.
    """
    validator = None
    if path == "/orders":
        validator = _ORDER_LIST if method == "GET" else _OrderReceipt
    elif path.startswith("/orders/"):
        validator = _OrderReceipt
    elif path == "/price_orders":
        validator = _TRIGGER_LIST if method == "GET" else _IdReceipt
    elif path.startswith("/price_orders/amend/"):
        validator = _TRIGGER_AMENDMENT
    elif path.startswith("/price_orders/"):
        validator = _TriggerReceipt
    elif method == "GET" and path.startswith("/positions/"):
        validator = _PositionReceipt
    if validator is not None:
        try:
            if isinstance(validator, TypeAdapter):
                validator.validate_python(result)
            else:
                parsed = validator.model_validate(result)
                if validator is _OrderReceipt and any(
                    value != value.to_integral_value() for value in (parsed.size, parsed.left)
                ):
                    raise ValueError("owned orders require whole contracts")
            if path.startswith("/orders/"):
                identifier = path.rsplit("/", 1)[1]
                actual = result.get("text") if identifier.startswith("t-") else str(result["id"])
                if actual != identifier:
                    raise ValueError("order identity mismatch")
            elif method == "GET" and path.startswith("/positions/"):
                if result["contract"] != path.rsplit("/", 1)[1]:
                    raise ValueError("position contract mismatch")
            elif path.startswith("/price_orders/"):
                actual = result["id"] if isinstance(result, dict) else result
                if str(actual) != path.rsplit("/", 1)[1]:
                    raise ValueError("trigger identity mismatch")
        except (ValidationError, ValueError):
            # Do not expose raw response values through Pydantic's diagnostic.
            raise GateError(status, "INVALID_RESPONSE_SCHEMA") from None


def order_matches_intent(result, body):
    """Custom text locates an order; it does not prove its original intent."""
    if not isinstance(result, dict) or not isinstance(body, dict):
        return False
    for key, expected in body.items():
        actual = result.get({"reduce_only": "is_reduce_only", "close": "is_close"}.get(key, key))
        if key in {"size", "price"}:
            try:
                if Decimal(str(actual)) != Decimal(str(expected)):
                    return False
            except ArithmeticError:
                return False
        elif isinstance(expected, bool):
            if actual is not expected:
                return False
        elif actual != expected:
            return False
    return True


class GateError(Exception):
    def __init__(self, status, label):
        self.status, self.label = status, label
        super().__init__(f"Gate {status}: {label}")


class Gate:
    def __init__(self, session, key, secret, testnet=True):
        self.session, self.key, self.secret = session, key, secret
        self.base = "https://api-testnet.gateapi.io" if testnet else "https://api.gateio.ws"

    @property
    def ready(self):
        return bool(self.key and self.secret)

    async def request(self, method, path, body=None, params=None, private=True):
        route = path
        path = "/api/v4/futures/usdt" + path
        query = urlencode(params or {})
        raw = json.dumps(body, separators=(",", ":")) if body is not None else ""
        headers = {"Content-Type": "application/json"}
        if private:
            if not self.ready:
                raise GateError(503, "CREDENTIALS_UNAVAILABLE")
            timestamp = str(int(time.time()))
            payload = "\n".join(
                [method, path, query, hashlib.sha512(raw.encode()).hexdigest(), timestamp]
            )
            headers.update(
                KEY=self.key,
                Timestamp=timestamp,
                SIGN=hmac.new(self.secret.encode(), payload.encode(), hashlib.sha512).hexdigest(),
            )
        async with self.session.request(
            method,
            self.base + path + ("?" + query if query else ""),
            data=raw or None,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=6),
        ) as response:
            try:
                result = await response.json(content_type=None)
            except ValueError as exc:
                raise GateError(response.status, "INVALID_JSON_RESPONSE") from exc
            label = result.get("label") if isinstance(result, dict) else None
            # Some trigger-order lookups return an error envelope with HTTP 200.
            # Preserve the HTTP status: a write must not become a confirmed
            # success, nor be treated as a proven 4xx rejection that can be retried.
            error_envelope = (
                isinstance(result, dict)
                and isinstance(label, str)
                and bool(label)
                and "message" in result
                and set(result) <= {"code", "label", "message"}
            )
            if response.status >= 400 or error_envelope:
                raise GateError(response.status, label or "HTTP_ERROR")
            validate_response(method, route, result, response.status)
            if method == "POST" and route == "/orders" and not order_matches_intent(result, body):
                raise GateError(response.status, "ORDER_INTENT_MISMATCH")
            return result

    async def get(self, path, **params):
        return await self.request("GET", path, params=params)

    async def market(self, symbol):
        return await self.request("GET", "/tickers", params={"contract": symbol}, private=False)

    async def candles(self, symbol, interval="5m"):
        return await self.request(
            "GET",
            "/candlesticks",
            params={"contract": symbol, "interval": interval, "limit": 120},
            private=False,
        )
