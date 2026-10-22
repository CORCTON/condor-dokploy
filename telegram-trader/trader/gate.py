import hashlib
import hmac
import json
import time
from urllib.parse import urlencode

import aiohttp


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
            result = await response.json(content_type=None)
            if response.status >= 400:
                raise GateError(response.status, result.get("label", "HTTP_ERROR"))
            return result

    async def get(self, path, **params):
        return await self.request("GET", path, params=params)

    async def market(self, symbol):
        return await self.request("GET", "/tickers", params={"contract": symbol}, private=False)

    async def candles(self, symbol):
        return await self.request(
            "GET",
            "/candlesticks",
            params={"contract": symbol, "interval": "5m", "limit": 120},
            private=False,
        )
