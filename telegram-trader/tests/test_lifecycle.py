"""Isolated simulation, NOT Gate TestNet acceptance or live latency evidence."""

import hashlib
import hmac
import json

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from test_trader import Exchange, signal

from trader.app import make_app
from trader.engine import Engine
from trader.gate import Gate
from trader.store import Store


@pytest.mark.parametrize("side,limit", [("long", False), ("short", False), ("short", True)])
async def test_full_lifecycle_over_http(tmp_path, monkeypatch, side, limit):
    """Parser -> signed HTTP -> fill -> plan API -> amendments -> TP -> restart -> close."""
    exchange = Exchange()
    secret = "simulation-only-secret"

    async def exchange_http(request):
        raw = await request.text()
        if request.headers.get("KEY"):
            payload = "\n".join(
                [
                    request.method,
                    request.path,
                    request.query_string,
                    hashlib.sha512(raw.encode()).hexdigest(),
                    request.headers["Timestamp"],
                ]
            )
            expected = hmac.new(secret.encode(), payload.encode(), hashlib.sha512).hexdigest()
            assert hmac.compare_digest(request.headers["SIGN"], expected)
        else:
            assert request.method == "GET" and request.path.endswith(("/tickers", "/candlesticks"))
        path = request.path.removeprefix("/api/v4/futures/usdt")
        if path == "/tickers":
            result = await exchange.market(request.query["contract"])
        elif path == "/candlesticks":
            result = await exchange.candles(request.query["contract"])
        else:
            result = await exchange.request(
                request.method, path, json.loads(raw) if raw else None, dict(request.query)
            )
        return web.json_response(result)

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", exchange_http)
    monkeypatch.setenv("TRADER_TOKEN", "simulation-only-token")
    headers = {"Authorization": "Bearer simulation-only-token"}
    database = tmp_path / "lifecycle.sqlite3"
    store = Store(database)
    try:
        async with TestServer(app) as server, aiohttp.ClientSession() as session:
            gate = Gate(session, "simulation-only-key", secret)
            gate.base = str(server.make_url("/")).rstrip("/")
            engine = Engine(store, gate, enabled=True)
            engine.listener_ready = True
            store.set("paused", False)
            rig = store, exchange, engine
            direction = "多" if side == "long" else "空"
            text = f"#INJ {'在10限价' if limit else '当前价格'}入场一笔{direction}单"
            trade = await signal(rig, text)
            await signal(rig, text)  # duplicate delivery must not create another entry
            assert len(exchange.orders) == 1
            sign = 1 if side == "long" else -1
            entry = exchange.orders[trade["entry_id"]]
            if limit:
                entry["left"] = trade["lots"] - 4
                exchange.position["size"] = sign * 4
                await engine.poll()
            assert not exchange.stops  # entry is deliberately not blocked on AI
            values = (
                {"sl": "9", "tp1": "11", "tp2": "12"}
                if side == "long"
                else {"sl": "11", "tp1": "9", "tp2": "8"}
            )
            async with TestClient(TestServer(make_app(engine))) as client:
                job_response = await client.post("/jobs/claim", headers=headers)
                job = await job_response.json()
                assert job["filled_contracts"] > 0 and job["candles_5m"]
                body = {k: job[k] for k in ("trade_id", "version", "lease")}
                body["plan"] = dict(values, tp1_fraction="0.5", rationale="synthetic test plan")
                response = await client.post("/plans", headers=headers, json=body)
                assert response.status == 200, await response.text()
                before = len(exchange.writes)
                assert (await client.post("/plans", headers=headers, json=body)).status == 409
                assert len(exchange.writes) == before
            if limit:
                entry.update(left=0, status="finished")
                exchange.position["size"] = sign * trade["lots"]
                await engine.poll()
            new_sl, new_tp = ("9.50", "11.50") if side == "long" else ("10.50", "8.50")
            await signal(rig, f"INJ SL: {new_sl} TP1: {new_tp}", message=2)
            assert next(iter(exchange.stops.values()))["trigger"]["price"] == new_sl
            assert exchange.orders["2"]["price"] == new_tp
            await signal(rig, "INJ 当前价格做TP1", message=3)
            await engine.poll()
            remaining = exchange.position["size"]
            assert 0 < abs(remaining) < trade["lots"]
            await signal(rig, "INJ 当前价格做TP1", message=4)
            assert exchange.position["size"] == remaining
            before = len(exchange.writes)
            await signal(rig, "INJ 利润不错", message=5)
            assert len(exchange.writes) == before
            store.close()
            store = Store(database)  # genuine SQLite close/reopen, not just a new Engine
            restarted = Engine(store, gate, enabled=True)
            await restarted.poll()
            assert len(exchange.writes) == before
            restarted.pause("operator test pause")
            await signal((store, exchange, restarted), "全部平仓", message=6, reply=1)
            for _ in range(3):
                await restarted.poll()
            assert exchange.position["size"] == 0
            assert store.trade(trade["id"])["status"] == "closed"
            assert not await exchange.get("/orders", status="open")
            assert not await exchange.get("/price_orders", status="open")
            assert not store.unknowns()
            assert len([o for o in exchange.orders.values() if not o["reduce_only"]]) == 1
    finally:
        store.close()
