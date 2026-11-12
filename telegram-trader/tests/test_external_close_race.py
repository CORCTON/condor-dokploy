"""App closure can cancel native protection between independent Gate reads."""

import pytest
from test_comprehensive_execution import entry, exits
from test_comprehensive_execution import strict_rig as strict_rig  # noqa: PLC0414

from trader.gate import GateError


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("finish_as", ["auto_cancelled", "cancelled", "failed"])
@pytest.mark.parametrize("operator_paused", [False, True])
async def test_external_close_between_position_and_stop_does_not_latch_fault(
    strict_rig, monkeypatch, side, finish_as, operator_paused
):
    store, exchange, engine = strict_rig
    await entry(strict_rig, side)
    trade = await exits(strict_rig)
    if operator_paused:
        engine.pause("operator requested pause")
    read = exchange.get
    closed = False

    async def close_after_position_snapshot(path, **params):
        nonlocal closed
        result = await read(path, **params)
        if path == "/positions/INJ_USDT" and not closed:
            closed = True
            exchange.positions["INJ_USDT"]["size"] = 0
            exchange.stops[trade["exits"]["sl"]["id"]].update(
                status="finished", finish_as=finish_as
            )
            exchange.orders[trade["exits"]["tp"]["id"]].update(
                status="finished", finish_as="position_closed"
            )
        return result

    monkeypatch.setattr(exchange, "get", close_after_position_snapshot)
    writes = list(exchange.writes)
    await engine.poll()
    assert store.trade(trade["id"])["status"] == "working"  # still require two zero polls
    assert not store.trade(trade["id"]).get("last_error")
    for _ in range(3):
        await engine.poll()
    assert store.trade(trade["id"])["status"] == "closed"
    assert engine.paused() is operator_paused
    assert exchange.writes == writes
    assert not store.unknowns()
    if operator_paused:
        assert engine.pause_reason() == "operator requested pause"


@pytest.mark.parametrize("fresh_position", ["nonzero", "unavailable", "foreign"])
async def test_inactive_stop_requires_fresh_verified_zero_before_ignoring(
    strict_rig, monkeypatch, fresh_position
):
    store, exchange, engine = strict_rig
    await entry(strict_rig)
    trade = await exits(strict_rig)
    exchange.stops[trade["exits"]["sl"]["id"]].update(
        status="finished", finish_as="auto_cancelled"
    )
    read = exchange.get
    positions = 0

    async def changed_position(path, **params):
        nonlocal positions
        if path == "/positions/INJ_USDT":
            positions += 1
            if positions > 1:
                if fresh_position == "unavailable":
                    raise GateError(503, "SERVICE_UNAVAILABLE")
                if fresh_position == "foreign":
                    exchange.positions["INJ_USDT"]["size"] = 1
        return await read(path, **params)

    monkeypatch.setattr(exchange, "get", changed_position)
    writes = list(exchange.writes)
    await engine.poll()
    assert engine.paused()
    assert store.trade(trade["id"])["status"] == "working"
    assert store.trade(trade["id"]).get("last_error")
    assert exchange.writes == writes
    assert store.get("paused") is (fresh_position != "unavailable")
