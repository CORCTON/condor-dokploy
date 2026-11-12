"""Independent fault injections for partial-fill exit coverage; no external access."""

import datetime
import time
from types import SimpleNamespace

from aiohttp.test_utils import TestClient, TestServer
from test_trader import plan, signal
from test_trader import rig as rig  # noqa: PLC0414 -- re-export the pytest fixture

from trader.app import make_app
from trader.channel import ChannelReader
from trader.engine import Engine


async def partial_limit_with_plan(rig, quantity):
    store, exchange, engine = rig
    trade = await signal(rig, "#INJ 在10限价入场空单")
    exchange.orders[trade["entry_id"]]["left"] = trade["lots"] - quantity
    exchange.position["size"] = -quantity
    await engine.poll()
    job = await engine.claim_plan()
    args = job["trade_id"], job["version"], job["lease"], plan()
    await engine.propose_plan(*args)
    await engine.apply_plan(*args)
    return store.trade(trade["id"])


def pending_tp_size(exchange):
    return sum(
        abs(order["left"])
        for order in exchange.orders.values()
        if order["reduce_only"] and order["status"] == "open"
    )


async def test_later_fills_resize_single_contract_target_to_full_owned_quantity(rig):
    store, exchange, engine = rig
    trade = await partial_limit_with_plan(rig, 1)
    assert trade["exits"]["tp"]["size"] == 1
    target_id = trade["exits"]["tp"]["id"]
    exchange.orders[trade["entry_id"]].update(left=0, status="finished")
    exchange.position["size"] = -trade["lots"]
    await engine.poll()
    current = store.trade(trade["id"])
    assert current["filled"] == trade["lots"]
    assert current["exits"]["tp"]["id"] == target_id
    assert current["exits"]["tp"]["size"] == trade["lots"]
    assert pending_tp_size(exchange) == abs(exchange.position["size"])


async def test_source_edit_not_yet_pushed_is_rechecked_before_an_entry(rig, monkeypatch):
    store, exchange, engine = rig
    published = time.time()
    event_id = store.event(-100123, 1, "INJ 当前开空", None, published, True)
    current = SimpleNamespace(
        id=1, message="取消，不开了", reply_to_msg_id=None,
        date=datetime.datetime.fromtimestamp(published, datetime.UTC),
        edit_date=datetime.datetime.fromtimestamp(published + 1, datetime.UTC),
        grouped_id=None, media=None, photo=None, document=None, file=None,
    )

    class ChannelClient:
        async def get_messages(self, peer, ids):
            return [current for _ in ids] if isinstance(ids, list) else current

    channel = ChannelReader()
    channel.client, channel.peer = ChannelClient(), "allowed"
    monkeypatch.setenv("TRADER_TOKEN", "audit-only")
    headers = {"Authorization": "Bearer audit-only"}
    async with TestClient(TestServer(make_app(engine, channel))) as client:
        job = await (await client.post("/signals/claim", headers=headers)).json()
        # The exchange command must not rely only on an old local watermark when
        # Telegram's authoritative message has already been edited to cancel.
        result = {"decisions": [{
            "event_id": event_id, "action": "open", "symbol": "INJ", "side": "short",
            "reason": "fixture decision against delayed local source",
            "evidence_message_ids": [1],
        }]}
        body = {"lease": job["lease"], "result": result}
        await client.post("/signals/proposals", headers=headers, json=body)
        await client.post("/signals/decisions", headers=headers, json=body)
    assert not exchange.orders


async def test_native_tp_cancellation_race_does_not_orphan_later_entry_fill(
    rig, monkeypatch
):
    store, exchange, engine = rig
    trade = await partial_limit_with_plan(rig, 4)
    entry = exchange.orders[trade["entry_id"]]
    for name in ("tp",):
        order = exchange.orders[trade["exits"][name]["id"]]
        exchange.position["size"] += abs(order["left"])
        order.update(left=0, status="finished")
    request = exchange.request

    async def fill_before_cancel(method, path, body=None, params=None):
        if method == "DELETE" and path == f"/orders/{entry['id']}":
            exchange.position["size"] -= abs(entry["left"])
            entry["left"] = 0
        return await request(method, path, body=body, params=params)

    monkeypatch.setattr(exchange, "request", fill_before_cancel)
    await engine.poll()
    await engine.poll()
    current = store.trade(trade["id"])
    assert current["entry_terminal"]
    assert entry["status"] != "open"
    # The fill racing a native exit must either be reduced or remain covered by
    # acknowledged reduce-only exits, not silently left with no remaining TP.
    assert pending_tp_size(exchange) == abs(exchange.position["size"])


async def test_partial_market_tp_crash_before_next_intent_preserves_remaining_allocation(
    rig, monkeypatch
):
    store, exchange, engine = rig
    trade = await signal(rig)
    job = await engine.claim_plan()
    args = job["trade_id"], job["version"], job["lease"], plan()
    await engine.propose_plan(*args)
    await engine.apply_plan(*args)
    exchange.partial = 3
    await signal(rig, "INJ 当前价格做TP1", message=2)
    original = engine.write

    async def crash_before_intent(trade, key, *args, **kwargs):
        if key.endswith(":close:1"):
            raise RuntimeError("injected crash after take progress commit, before new intent")
        return await original(trade, key, *args, **kwargs)

    monkeypatch.setattr(engine, "write", crash_before_intent)
    await engine.poll()
    assert exchange.position["size"] == -11  # only 3 of 14 contracts filled
    exchange.partial = None
    restarted = Engine(store, exchange, enabled=True)
    for _ in range(4):
        await restarted.poll()
    assert exchange.position["size"] == 0  # resume only the remaining 11 contracts
    assert store.trade(trade["id"])["status"] == "closed"
    reductions = [o for o in exchange.orders.values() if o["price"] == "0" and o["reduce_only"]]
    assert sum(abs(o["size"]) - o["left"] for o in reductions) == 14
