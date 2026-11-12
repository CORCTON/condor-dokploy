"""Uncertain-receipt read failures must not starve other owned positions.

Real Store/Engine with an in-memory exchange only; no HTTP or exchange credentials.
"""

import time

import pytest
from aiohttp import ClientError
from test_comprehensive_execution import decision, entry
from test_comprehensive_execution import strict_rig as strict_rig  # noqa: PLC0414
from test_take_profit_policy import apply_current_plan, plan_for

from trader.gate import GateError


@pytest.mark.parametrize("operation_path", ["/orders", "/price_orders"])
@pytest.mark.parametrize("error_type", [TimeoutError, OSError, ClientError])
@pytest.mark.parametrize("maintenance", ["close", "renew_stop"])
async def test_uncertain_receipt_read_error_does_not_starve_other_owned_trade(
    strict_rig, monkeypatch, operation_path, error_type, maintenance
):
    store, exchange, engine = strict_rig
    managed = await entry(strict_rig)
    managed = await apply_current_plan(strict_rig)
    exchange.add_contract("BTC_USDT")
    other = await entry(strict_rig, symbol="BTC")
    old_stop = managed["exits"]["sl"]["id"]
    if maintenance == "close":
        exchange.set_next_market_fill("INJ_USDT", 2)
        assert (await decision(strict_rig, "close", trade=managed))["status"] == "done"
        assert abs(exchange.positions["INJ_USDT"]["size"]) == 12
    else:
        managed["exits"]["sl"]["created"] = time.time() - 83000
        store.save_trade(managed)

    text = "t-offline-unknown-receipt"
    unknown_key = f"{other['id']}:offline-unknown"
    body = {"contract": "BTC_USDT", "size": 1, "price": "0", "tif": "ioc",
            "reduce_only": True, "text": text}
    if operation_path == "/price_orders":
        body = {"initial": body, "trigger": {"price": "11", "rule": 1},
                "order_type": "plan-close-short-position"}
    store.intent(unknown_key, other["id"], "POST", operation_path, {"body": body})
    lookup = exchange.get
    failed_reads = 0

    async def unavailable_receipt(path, **params):
        nonlocal failed_reads
        if path == (f"/orders/{text}" if operation_path == "/orders" else "/price_orders"):
            failed_reads += 1
            raise error_type("offline receipt query failure")
        return await lookup(path, **params)

    monkeypatch.setattr(exchange, "get", unavailable_receipt)
    for _ in range(4):
        await engine.poll()

    assert failed_reads == 4
    assert store.operation(unknown_key)["status"] == "unknown"
    assert [operation["id"] for operation in store.unknowns()] == [unknown_key]
    assert engine.paused()
    assert not any(order.get("text") == text for order in exchange.orders.values())
    assert not any(stop["initial"].get("text") == text for stop in exchange.stops.values())
    assert abs(exchange.positions["BTC_USDT"]["size"]) == other["filled"]
    current = store.trade(managed["id"])
    if maintenance == "close":
        assert current["close_requested"] and current["status"] == "closed"
        assert exchange.positions["INJ_USDT"]["size"] == 0
    else:
        assert current["exits"]["sl"]["id"] != old_stop
        assert exchange.stops[current["exits"]["sl"]["id"]]["status"] == "open"
        assert exchange.stops[old_stop]["status"] == "finished"
        assert abs(exchange.positions["INJ_USDT"]["size"]) == managed["filled"]


@pytest.mark.parametrize("error", [
    GateError(503, "SERVICE_UNAVAILABLE"), TimeoutError("offline timeout"),
    OSError("offline transport failure"), ClientError("offline client failure"),
])
async def test_failed_candle_read_releases_only_its_unproposed_lease_and_can_reclaim(
    strict_rig, monkeypatch, error
):
    store, exchange, engine = strict_rig
    trade = await entry(strict_rig)
    candles = exchange.candles
    claimed = {}
    before = len(exchange.writes)

    async def unavailable(_symbol, interval="5m"):
        claimed.update(store.trade(trade["id"]))
        assert claimed["plan_token"] and claimed["plan_lease"] > time.time()
        raise error

    monkeypatch.setattr(exchange, "candles", unavailable)
    with pytest.raises(type(error)) as raised:
        await engine.claim_plan()
    assert raised.value is error
    failed = store.trade(trade["id"])
    assert failed["plan_token"] is None and failed["plan_lease"] == 0
    assert failed["plan_needed"] and failed["plan"] is None
    assert failed.get("plan_proposal") is None
    assert failed["plan_error"] == "market data unavailable: " + type(error).__name__
    assert not failed["exits"] and not store.unknowns()

    monkeypatch.setattr(exchange, "candles", candles)
    job = await engine.claim_plan()
    assert job is not None and job["trade_id"] == trade["id"]
    assert job["lease"] != claimed["plan_token"]
    assert job["version"] == claimed["version"]
    assert len(exchange.writes) == before  # read recovery never submits protection or a close


@pytest.mark.parametrize("changed_field", ["plan_token", "version"])
async def test_stale_candle_failure_cannot_clear_a_changed_job(
    strict_rig, monkeypatch, changed_field
):
    store, exchange, engine = strict_rig
    trade = await entry(strict_rig)
    newer = {}
    before = len(exchange.writes)

    async def changed_during_read(_symbol, interval="5m"):
        current = store.trade(trade["id"])
        current[changed_field] = (
            "newer-lease-token" if changed_field == "plan_token" else current["version"] + 1
        )
        current["plan_lease"] = time.time() + 240
        store.save_trade(current)
        newer.update(current)
        raise TimeoutError("old market-data request failed")

    monkeypatch.setattr(exchange, "candles", changed_during_read)
    with pytest.raises(TimeoutError):
        await engine.claim_plan()
    assert store.trade(trade["id"]) == newer
    assert await engine.claim_plan() is None
    assert len(exchange.writes) == before


@pytest.mark.parametrize("guard", ["accepted_proposal", "unknown_write"])
async def test_candle_failure_preserves_accepted_proposal_or_uncertain_operation(
    strict_rig, monkeypatch, guard
):
    store, exchange, engine = strict_rig
    trade = await entry(strict_rig)
    guarded = {}
    before = len(exchange.writes)

    async def guard_during_read(_symbol, interval="5m"):
        current = store.trade(trade["id"])
        if guard == "accepted_proposal":
            await engine.propose_plan(
                current["id"], current["version"], current["plan_token"], plan_for(current["side"])
            )
        else:
            store.intent("uncertain-protection", trade["id"], "POST", "/price_orders", {})
        guarded.update(store.trade(trade["id"]))
        raise ClientError("market-data reply unavailable")

    monkeypatch.setattr(exchange, "candles", guard_during_read)
    with pytest.raises(ClientError):
        await engine.claim_plan()
    current = store.trade(trade["id"])
    assert current["plan_token"] == guarded["plan_token"]
    assert current["plan_lease"] == guarded["plan_lease"]
    assert current["plan_needed"] and current["plan"] is None
    assert current.get("plan_proposal") == guarded.get("plan_proposal")
    if guard == "accepted_proposal":
        assert current["plan_proposal"] is not None
        assert not store.unknowns()
    else:
        assert store.operation("uncertain-protection")["status"] == "unknown"
        assert engine.paused()
    assert await engine.claim_plan() is None
    assert len(exchange.writes) == before

