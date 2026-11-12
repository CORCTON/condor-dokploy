"""Unknown amendments serialize only their owned target; no network is used."""

import copy
import time

import pytest
from test_comprehensive_execution import decision, entry, exits
from test_comprehensive_execution import strict_rig as strict_rig  # noqa: PLC0414

from trader.engine import Engine
from trader.gate import GateError


@pytest.fixture
def rig(strict_rig):
    return strict_rig


async def partial_trade(rig, side="short"):
    _store, exchange, engine = rig
    trade = await entry(rig, side, limit=True)
    exchange.fill(trade["entry_id"], 4, price="10")
    await engine.poll()
    return await exits(rig)


def lose_first_amend(monkeypatch, exchange, target_id, *, method="PUT"):
    request = exchange.request
    attempts = []

    async def delayed(verb, path, body=None, params=None, **kwargs):
        if verb == method and path == f"/orders/{target_id}":
            attempts.append(copy.deepcopy(body))
            if len(attempts) == 1:
                raise TimeoutError("offline first amendment may still arrive later")
        return await request(verb, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", delayed)
    return request, attempts


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("first,following", [
    ("size", "size"), ("price", "price"), ("size", "price"), ("price", "size"),
])
async def test_unknown_target_amend_blocks_later_amend_for_same_order(
    rig, monkeypatch, side, restart, first, following
):
    store, exchange, engine = rig
    trade = await partial_trade(rig, side)
    target_id = trade["exits"]["tp"]["id"]
    request, attempts = lose_first_amend(monkeypatch, exchange, target_id)
    if first == "size":
        exchange.fill(trade["entry_id"], 3, price="10")
        await engine.poll()
    else:
        result = await decision(rig, "targets", trade=trade,
                                levels={"tp": "11.5" if side == "long" else "8.5"})
        assert result["status"] == "error"
    unknown = store.unknowns()
    assert len(unknown) == len(attempts) == 1
    if restart:
        engine = Engine(store, exchange, enabled=True)
    state = store, exchange, engine
    if following == "size":
        exchange.fill(trade["entry_id"], price="10")
        await engine.poll()
    else:
        result = await decision(state, "targets", trade=trade,
                                levels={"tp": "11.7" if side == "long" else "8.7"})
        assert result["status"] == "error"
    assert len(attempts) == 1, "a later PUT can be overwritten by the first delayed PUT"
    assert [op["id"] for op in store.unknowns()] == [op["id"] for op in unknown]
    # The first request finally commits after its response timeout. It remains
    # unknown: merely reading these values cannot prove all pending writes ended.
    await request("PUT", f"/orders/{target_id}", body=attempts[0])
    await engine.poll()
    assert len(attempts) == 1
    assert engine.paused()
    assert exchange.positions["INJ_USDT"]["size"] != 0
    current = store.trade(trade["id"])
    assert exchange.stops[current["exits"]["sl"]["id"]]["status"] == "open"


@pytest.mark.parametrize("expiry", ["expired", "renewal_due"])
async def test_unknown_target_resize_does_not_block_native_stop_renewal(rig, monkeypatch, expiry):
    store, exchange, engine = rig
    trade = await partial_trade(rig)
    _request, attempts = lose_first_amend(monkeypatch, exchange, trade["exits"]["tp"]["id"])
    exchange.fill(trade["entry_id"], 3, price="10")
    await engine.poll()
    current = store.trade(trade["id"])
    old_stop = current["exits"]["sl"]["id"]
    if expiry == "expired":
        exchange.stops[old_stop].update(status="finished", finish_as="expired")
    else:
        current["exits"]["sl"]["created"] = time.time() - 83000
        store.save_trade(current)
    exchange.fill(trade["entry_id"], price="10")
    await Engine(store, exchange, enabled=True).poll()
    current = store.trade(trade["id"])
    assert current["exits"]["sl"]["id"] != old_stop
    assert sum(stop["status"] == "open" for stop in exchange.stops.values()) == 1
    assert exchange.positions["INJ_USDT"]["size"] == -14
    assert len(attempts) == len(store.unknowns()) == 1


@pytest.mark.parametrize("action", ["close", "tp1", "tp2"])
async def test_unknown_target_amend_does_not_block_explicit_full_close(rig, monkeypatch, action):
    store, exchange, engine = rig
    trade = await partial_trade(rig)
    target_id = trade["exits"]["tp"]["id"]
    request, attempts = lose_first_amend(monkeypatch, exchange, target_id)
    assert (await decision(rig, "targets", trade=trade, levels={"tp": "8.5"}))["status"] == "error"
    unknown = store.unknowns()
    assert (await decision(rig, action, trade=trade))["status"] == "done"
    for _ in range(4):
        await engine.poll()
    assert store.trade(trade["id"])["status"] == "closed"
    assert exchange.positions["INJ_USDT"]["size"] == 0
    assert exchange.orders[target_id]["status"] == "finished"
    assert exchange.orders[trade["entry_id"]]["status"] == "finished"
    assert len(attempts) == 1
    assert [op["id"] for op in store.unknowns()] == [op["id"] for op in unknown]
    with pytest.raises(GateError, match="ORDER_CLOSED"):
        await request("PUT", f"/orders/{target_id}", body=attempts[0])


async def test_unknown_target_amend_does_not_block_other_owned_target_or_sl(rig, monkeypatch):
    store, exchange, _engine = rig
    first = await partial_trade(rig)
    exchange.add_contract("JTO_USDT")
    second = await entry(rig, symbol="JTO")
    second = await exits(rig)
    _request, attempts = lose_first_amend(monkeypatch, exchange, first["exits"]["tp"]["id"])
    assert (await decision(rig, "targets", trade=first, levels={"tp": "8.5"}))["status"] == "error"
    assert (await decision(rig, "targets", trade=second, levels={"tp": "8.7"}))["status"] == "done"
    assert (await decision(rig, "sl", trade=first, price="10.5"))["status"] == "done"
    assert exchange.orders[second["exits"]["tp"]["id"]]["price"] == "8.7"
    assert exchange.stops[first["exits"]["sl"]["id"]]["trigger"]["price"] == "10.5"
    assert len(attempts) == len(store.unknowns()) == 1


async def test_confirmed_rejection_of_amend_does_not_block_fresh_channel_price(rig, monkeypatch):
    _store, exchange, _engine = rig
    trade = await partial_trade(rig)
    request = exchange.request
    rejected = False

    async def reject_first(method, path, body=None, params=None, **kwargs):
        nonlocal rejected
        if method == "PUT" and path == f"/orders/{trade['exits']['tp']['id']}" and not rejected:
            rejected = True
            raise GateError(400, "INVALID_PRICE")
        return await request(method, path, body=body, params=params, **kwargs)

    monkeypatch.setattr(exchange, "request", reject_first)
    assert (await decision(rig, "targets", trade=trade, levels={"tp": "8.5"}))["status"] == "error"
    assert (await decision(rig, "targets", trade=trade, levels={"tp": "8.7"}))["status"] == "done"
    assert exchange.orders[trade["exits"]["tp"]["id"]]["price"] == "8.7"


async def test_unknown_target_cancellation_blocks_price_amend(rig, monkeypatch):
    store, exchange, engine = rig
    trade = await partial_trade(rig)
    target_id = trade["exits"]["tp"]["id"]
    _request, attempts = lose_first_amend(monkeypatch, exchange, target_id, method="DELETE")
    with pytest.raises(TimeoutError):
        await engine.cancel_regular(trade, target_id, "offline-cancel")
    before = copy.deepcopy(exchange.writes)
    assert (await decision(rig, "targets", trade=trade, levels={"tp": "8.7"}))["status"] == "error"
    assert exchange.writes == before
    assert len(attempts) == len(store.unknowns()) == 1
