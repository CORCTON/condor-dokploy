"""In-memory Gate protocol fixture; no network, funding, fees or margin accounting.

Market IOC orders execute immediately (optionally with an explicit partial fill).
Limit orders NEVER auto-match: drive them with ``fill``. ``set_market`` only moves
quotes; drive conditional orders separately with ``trigger``. This separation
makes arrival/fill/cancel races reproducible instead of guessing exchange timing.
"""

import time
from copy import deepcopy
from decimal import Decimal

from trader.gate import GateError
from trader.models import decimal


class FakeExchange:
    ready = True
    error = ""

    def __init__(self, equity="100"):
        self.account = {
            "currency": "USDT", "in_dual_mode": False, "margin_mode": 0,
            "position_mode": "single", "total": str(decimal(equity)),
            "unrealised_pnl": "0", "available": str(decimal(equity)),
        }
        self.contracts, self.positions, self.quotes = {}, {}, {}
        self.orders, self.stops, self.candle_data = {}, {}, {}
        self.writes, self.reads = [], []
        self.next_market_fills = {}
        self._next_id = 1

    def status(self):
        """Also usable as make_app's credential-status fixture; no real keys."""
        return {"source": "in-memory-fixture", "environment": "simulation",
                "ready": self.ready, "error": self.error}

    def add_contract(self, symbol, *, price="10", tick="0.01", multiplier="1"):
        if symbol in self.contracts:
            raise ValueError("contract already configured")
        if min(decimal(tick), decimal(multiplier), decimal(price)) <= 0:
            raise ValueError("positive contract values required")
        self.contracts[symbol] = {
            "name": symbol, "quanto_multiplier": str(decimal(multiplier)),
            "leverage_max": "100", "order_size_min": 1, "order_size_max": 100000,
            "taker_fee_rate": "0.0005", "order_price_round": str(decimal(tick)),
            "enable_decimal": False, "in_delisting": False,
        }
        self.positions[symbol] = {
            "contract": symbol, "size": 0, "leverage": "0", "lever": "10",
            "pos_margin_mode": "isolated", "mode": "single", "entry_price": "0",
            "mark_price": str(decimal(price)),
        }
        self.set_market(symbol, price=price)

    def _contract(self, symbol):
        if symbol not in self.contracts:
            raise GateError(404, "CONTRACT_NOT_FOUND")
        return self.contracts[symbol]

    def _price(self, symbol, value, *, market=False):
        result = decimal(value)
        tick = decimal(self._contract(symbol)["order_price_round"])
        if result == 0 and market:
            return result
        if result <= 0 or result % tick:
            raise GateError(400, "INVALID_PRICE")
        return result

    @staticmethod
    def _integer(value, *, minimum=None):
        number = decimal(value)
        if number != number.to_integral_value() or (minimum is not None and number < minimum):
            raise GateError(400, "INVALID_SIZE")
        return int(number)

    def set_market(self, symbol, *, price=None, bid=None, ask=None):
        self._contract(symbol)
        old = self.quotes.get(symbol, {})
        reference_bid = price if price is not None else old["highest_bid"]
        reference_ask = price if price is not None else old["lowest_ask"]
        bid = self._price(symbol, bid if bid is not None else reference_bid)
        ask = self._price(symbol, ask if ask is not None else reference_ask)
        if bid > ask:
            raise ValueError("bid must not exceed ask")
        mark = decimal(price) if price is not None else (bid + ask) / 2
        self.quotes[symbol] = {
            "contract": symbol, "highest_bid": str(bid), "lowest_ask": str(ask),
            "last": str(mark), "mark_price": str(mark),
        }
        self.positions[symbol]["mark_price"] = str(mark)

    def set_next_market_fill(self, symbol, quantity):
        self._contract(symbol)
        self.next_market_fills[symbol] = self._integer(quantity, minimum=0)

    def set_candles(self, symbol, rows):
        self._contract(symbol)
        self.candle_data[symbol] = deepcopy(rows)

    def _id(self):
        value = str(self._next_id)
        self._next_id += 1
        return value

    @staticmethod
    def _lookup(collection, key):
        if str(key) in collection:
            return collection[str(key)]
        matches = [order for order in collection.values() if order.get("text") == key]
        if len(matches) == 1:
            return matches[0]
        raise GateError(404 if not matches else 409, "ORDER_NOT_FOUND" if not matches else "AMBIGUOUS_TEXT")

    @staticmethod
    def _open(order):
        if order["status"] != "open":
            raise GateError(400, "ORDER_CLOSED")

    def _new_order(self, body, *, fill_quantity=None):
        symbol = body["contract"]
        self._contract(symbol)
        size = self._integer(body["size"])
        close = bool(body.get("close", False))
        reduce_only = bool(body.get("reduce_only", False) or close)
        if close:
            if size != 0:
                raise GateError(400, "INVALID_CLOSE_SIZE")
            size = -self.positions[symbol]["size"]
        if size == 0:
            raise GateError(400, "EMPTY_POSITION" if close else "INVALID_SIZE")
        price = self._price(symbol, body["price"], market=True)
        if body.get("tif") != ("ioc" if price == 0 else "gtc"):
            raise GateError(400, "UNSUPPORTED_TIF_IN_FIXTURE")
        order = {
            **deepcopy(body), "id": self._id(), "size": size, "price": str(price),
            "left": abs(size), "fill_price": "0", "status": "open",
            "is_reduce_only": reduce_only, "is_close": close, "create_time": time.time(),
        }
        self.orders[order["id"]] = order
        if price == 0:
            limit = fill_quantity
            if limit is None:
                limit = self.next_market_fills.pop(symbol, abs(size))
            self.fill(order["id"], min(abs(size), limit))
            order.update(status="finished", finish_as="filled" if not order["left"] else "ioc",
                         finish_time=time.time())
        return deepcopy(order)

    def fill(self, order_id, quantity=None, *, price=None):
        """Apply a confirmed fill, respecting limit price, remainder and reduce-only."""
        order = self._lookup(self.orders, order_id)
        self._open(order)
        symbol, direction = order["contract"], 1 if order["size"] > 0 else -1
        amount = order["left"] if quantity is None else self._integer(quantity, minimum=0)
        if amount > order["left"]:
            raise GateError(400, "FILL_EXCEEDS_REMAINDER")
        quote = self.quotes[symbol]["lowest_ask" if direction > 0 else "highest_bid"]
        price = self._price(symbol, quote if price is None else price)
        limit = decimal(order["price"])
        if limit and ((direction > 0 and price > limit) or (direction < 0 and price < limit)):
            raise GateError(400, "FILL_VIOLATES_LIMIT")
        position = self.positions[symbol]
        before = position["size"]
        if order["is_reduce_only"]:
            amount = min(amount, abs(before)) if before * direction < 0 else 0
        filled_before = abs(order["size"]) - order["left"]
        if amount:
            order["fill_price"] = str(
                (decimal(order["fill_price"]) * filled_before + price * amount)
                / (filled_before + amount)
            )
            after = before + direction * amount
            if before == 0 or before * direction > 0:
                position["entry_price"] = str(
                    (decimal(position["entry_price"]) * abs(before) + price * amount)
                    / abs(after)
                )
            elif after == 0:
                position["entry_price"] = "0"
            elif before * after < 0:
                position["entry_price"] = str(price)
            position["size"] = after
            order["left"] -= amount
        if not order["left"]:
            order.update(status="finished", finish_as="filled", finish_time=time.time())
        return deepcopy(order)

    def trigger(self, stop_id, *, quantity=None):
        """Trigger a native conditional close only after its mark-price rule is met."""
        stop = self._lookup(self.stops, stop_id)
        self._open(stop)
        initial, trigger = stop["initial"], stop["trigger"]
        mark = decimal(self.positions[initial["contract"]]["mark_price"])
        level = decimal(trigger["price"])
        if not (mark >= level if trigger["rule"] == 1 else mark <= level):
            raise GateError(400, "TRIGGER_NOT_REACHED")
        if quantity is not None:
            quantity = self._integer(quantity, minimum=0)
        order = self._new_order(initial, fill_quantity=quantity)
        stop.update(status="finished", finish_as="succeeded", trade_id=order["id"],
                    finish_time=time.time())
        return deepcopy(stop)

    async def request(self, method, path, body=None, params=None, private=True):
        if method == "GET":
            return await self.get(path, **(params or {}))
        self.writes.append((method, path, deepcopy(body), deepcopy(params)))
        if method == "POST" and path.startswith("/positions/") and path.endswith("/set_leverage"):
            symbol = path.split("/")[2]
            contract = self._contract(symbol)
            leverage = self._integer(params["leverage"], minimum=1)
            if leverage > decimal(contract["leverage_max"]) or params["margin_mode"] != "isolated":
                raise GateError(400, "INVALID_LEVERAGE_OR_MARGIN")
            self.positions[symbol].update(lever=str(leverage), pos_margin_mode="isolated")
            return deepcopy(self.positions[symbol])
        if method == "POST" and path == "/orders":
            return self._new_order(body)
        if method == "POST" and path == "/price_orders":
            initial, trigger = body["initial"], body["trigger"]
            self._price(initial["contract"], trigger["price"])
            if (initial.get("size") != 0 or initial.get("close") is not True
                    or initial.get("reduce_only") is not True or initial.get("price") != "0"
                    or trigger.get("rule") not in {1, 2} or trigger.get("price_type") != 1):
                raise GateError(400, "UNSUPPORTED_TRIGGER_IN_FIXTURE")
            result = {**deepcopy(body), "id": self._id(), "status": "open",
                      "create_time": time.time()}
            result["initial"].update(is_close=True, is_reduce_only=True)
            self.stops[result["id"]] = result
            return deepcopy(result)
        if method == "PUT" and path.startswith("/price_orders/amend/"):
            stop = self._lookup(self.stops, path.rsplit("/", 1)[1])
            self._open(stop)
            if (body.get("price") != "0" or body.get("close") is not True
                    or body.get("size") != 0 or body.get("price_type") != 1
                    or str(body.get("order_id")) != stop["id"]):
                raise GateError(400, "INVALID_TRIGGER_AMENDMENT")
            price = self._price(stop["initial"]["contract"], body["trigger_price"])
            stop["trigger"]["price"] = str(price)
            return int(stop["id"])
        if method == "PUT" and path.startswith("/orders/"):
            order = self._lookup(self.orders, path.rsplit("/", 1)[1])
            self._open(order)
            if set(body) - {"price", "size"}:
                raise GateError(400, "UNSUPPORTED_ORDER_AMENDMENT")
            size = self._integer(body.get("size", order["size"]))
            filled = abs(order["size"]) - order["left"]
            if size * order["size"] <= 0 or abs(size) <= filled:
                raise GateError(400, "INVALID_AMENDED_SIZE")
            price = self._price(order["contract"], body.get("price", order["price"]))
            order.update(size=size, left=abs(size) - filled, price=str(price))
            return deepcopy(order)
        if method == "DELETE" and path.startswith(("/orders/", "/price_orders/")):
            collection = self.stops if path.startswith("/price_orders/") else self.orders
            order = self._lookup(collection, path.rsplit("/", 1)[1])
            self._open(order)
            order.update(status="finished", finish_as="cancelled", finish_time=time.time())
            return deepcopy(order)
        raise AssertionError(f"Unsupported in-memory request: {method} {path}")

    async def get(self, path, **params):
        self.reads.append((path, deepcopy(params)))
        if path == "/accounts":
            return deepcopy(self.account)
        if path.startswith("/contracts/"):
            return deepcopy(self._contract(path.rsplit("/", 1)[1]))
        if path.startswith("/positions/"):
            symbol = path.rsplit("/", 1)[1]
            self._contract(symbol)
            return deepcopy(self.positions[symbol])
        if path in {"/orders", "/price_orders", "/positions"}:
            collection = {"/orders": self.orders, "/price_orders": self.stops,
                          "/positions": self.positions}[path]
            rows = list(collection.values())
            if path != "/positions":
                status = params.get("status", "open")
                if status not in {"open", "finished"}:
                    raise GateError(400, "INVALID_STATUS")
                rows = [row for row in rows if row["status"] == status]
                rows.sort(key=lambda row: int(row["id"]), reverse=True)
            elif params.get("holding") in {True, "true"}:
                rows = [row for row in rows if row["size"]]
            if params.get("contract"):
                rows = [row for row in rows if (
                    row["initial"]["contract"] if path == "/price_orders" else row["contract"]
                ) == params["contract"]]
            offset = self._integer(params.get("offset", 0), minimum=0)
            limit = self._integer(params.get("limit", 100), minimum=1)
            return deepcopy(rows[offset:offset + limit])
        if path.startswith(("/orders/", "/price_orders/")):
            collection = self.stops if path.startswith("/price_orders/") else self.orders
            return deepcopy(self._lookup(collection, path.rsplit("/", 1)[1]))
        if path == "/tickers":
            return await self.market(params["contract"])
        if path == "/candlesticks":
            return await self.candles(params["contract"])
        raise AssertionError(f"Unsupported in-memory read: {path}")

    async def market(self, symbol):
        self._contract(symbol)
        return [deepcopy(self.quotes[symbol])]

    async def candles(self, symbol):
        self._contract(symbol)
        if symbol in self.candle_data:
            return deepcopy(self.candle_data[symbol])
        mark = Decimal(self.positions[symbol]["mark_price"])
        tick = Decimal(self.contracts[symbol]["order_price_round"])
        now = int(time.time()) // 300 * 300
        return [{"t": now - (120 - i) * 300, "o": str(mark), "c": str(mark),
                 "h": str(mark + tick * 2), "l": str(max(tick, mark - tick * 2)), "v": "1000"}
                for i in range(120)]
