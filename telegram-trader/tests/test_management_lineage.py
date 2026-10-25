"""Known explicit reply ownership must survive an incorrect model trade ID.

All arrivals, fills and model proposals are scripted in memory; no external API
is called. Evidence IDs are context, not a substitute for explicit reply links.
"""

import time
from copy import deepcopy
from types import SimpleNamespace

import pytest
from comprehensive_fixtures import FakeExchange

from trader.engine import Engine, Review
from trader.models import Decisions
from trader.signals import SignalInbox
from trader.store import Store

CHANNEL = -1001793451069


@pytest.fixture
def rig():
    store, exchange = Store(":memory:"), FakeExchange()
    for symbol in ("FIL", "INJ"):
        exchange.add_contract(symbol + "_USDT")
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield SimpleNamespace(store=store, exchange=exchange, engine=engine, inbox=SignalInbox(engine))
    store.close()


def receive(rig, message, text, reply=None, *, channel=CHANNEL, context=False):
    event_id = rig.store.event(channel, message, text, reply, time.time(), True)
    if context:
        rig.store.finish(event_id, "ignore")
    return event_id


def answer(job, *, evidence=None, **fields):
    assert len(job["events"]) == 1
    event = job["events"][0]
    return Decisions.model_validate({"decisions": [{
        "event_id": event["event_id"],
        "reason": "scripted lineage regression",
        "evidence_message_ids": evidence or [event["message_id"]],
        **fields,
    }]})


async def apply(rig, job, result):
    await rig.inbox.propose(job["lease"], result)
    await rig.inbox.apply(job["lease"], result)


async def open_trade(rig, message, symbol="FIL"):
    receive(rig, message, f"#{symbol} 当前价格入场一笔多单")
    job = await rig.inbox.claim()
    await apply(rig, job, answer(job, action="open", symbol=symbol, side="long"))
    return next(t for t in rig.engine.active() if t["symbol"] == symbol + "_USDT")


async def close_old_and_open_new(rig):
    old = await open_trade(rig, 15368)
    receive(rig, 15369, "FIL先行离场", old["message"])
    job = await rig.inbox.claim()
    await apply(rig, job, answer(job, action="close", target_trade_id=old["id"]))
    await rig.engine.poll()
    await rig.engine.poll()
    assert rig.store.trade(old["id"])["status"] == "closed"
    new = await open_trade(rig, 15370)
    rig.exchange.writes.clear()
    return old, new


@pytest.mark.parametrize("action,fields", [
    ("close", {}), ("cancel", {}), ("tp1", {}), ("tp2", {}),
    ("sl", {"price": "9"}), ("targets", {"levels": {"tp1": "11"}}),
])
async def test_old_explicit_reply_rejects_a_valid_new_trade_id_before_any_write(rig, action, fields):
    old, new = await close_old_and_open_new(rig)
    receive(rig, 15375, "FIL 這單先行離場", old["message"])
    job = await rig.inbox.claim()
    before = deepcopy(rig.exchange.positions), deepcopy(rig.store.trades())
    with pytest.raises(Review, match="reply.*different owned trade"):
        await rig.inbox.propose(job["lease"], answer(
            job, action=action, symbol="FIL", target_trade_id=new["id"], **fields,
        ))
    assert rig.store.analysis(job["lease"])["result"] is None
    assert (rig.exchange.positions, rig.store.trades()) == before
    assert not rig.exchange.writes


async def test_indirect_pnl_reply_keeps_closed_trade_lineage(rig):
    old, new = await close_old_and_open_new(rig)
    receive(rig, 15371, "FIL利润不错", old["message"], context=True)
    receive(rig, 15372, "这单剩余仓位", 15371, context=True)
    receive(rig, 15375, "全部止盈", 15372)
    job = await rig.inbox.claim()
    with pytest.raises(Review, match="reply.*different owned trade"):
        await rig.inbox.propose(job["lease"], answer(
            job, action="close", target_trade_id=new["id"],
        ))
    assert not rig.exchange.writes
    assert rig.exchange.positions["FIL_USDT"]["size"] == new["filled"]


@pytest.mark.parametrize("reply_kind", ["direct", "indirect", "none", "unowned_context", "missing"])
async def test_valid_current_management_is_not_rejected_by_unrelated_evidence(rig, reply_kind):
    old, new = await close_old_and_open_new(rig)
    unrelated = await open_trade(rig, 15371, "INJ")
    reply = {
        "direct": new["message"], "indirect": 15372, "none": None,
        "unowned_context": 15373, "missing": 42,
    }[reply_kind]
    if reply_kind == "indirect":
        receive(rig, 15372, "FIL浮盈", new["message"], context=True)
    elif reply_kind == "unowned_context":
        receive(rig, 15373, "今天行情很活跃", context=True)
    receive(rig, 15375, "FIL当前这单全部平仓", reply)
    job = await rig.inbox.claim()
    before_inj = deepcopy(rig.exchange.positions["INJ_USDT"])
    rig.exchange.writes.clear()
    await apply(rig, job, answer(
        job, action="close", symbol="FIL", target_trade_id=new["id"],
        evidence=[old["message"], unrelated["message"], 15375],
    ))
    assert rig.exchange.positions["FIL_USDT"]["size"] == 0
    assert rig.exchange.positions["INJ_USDT"] == before_inj
    assert all(body["contract"] == "FIL_USDT" for method, path, body, _ in rig.exchange.writes
               if method == "POST" and path == "/orders")


async def test_reply_lookup_never_uses_another_channels_same_message_id(rig):
    new = await open_trade(rig, 15370)
    old_other_channel = dict(new, id="other-channel-old", channel=CHANNEL - 1,
                             message=15368, status="closed")
    rig.store.save_trade(old_other_channel)
    receive(rig, 15375, "FIL当前仓位平掉", 15368)
    job = await rig.inbox.claim()
    await apply(rig, job, answer(job, action="close", target_trade_id=new["id"]))
    assert rig.exchange.positions["FIL_USDT"]["size"] == 0


async def test_apply_rechecks_lineage_discovered_after_proposal(rig):
    new = await open_trade(rig, 15370)
    receive(rig, 15375, "FIL 這單先行離場", 15368)
    job = await rig.inbox.claim()
    result = answer(job, action="close", target_trade_id=new["id"])
    await rig.inbox.propose(job["lease"], result)
    # A historical owned record is recovered without changing the current live
    # trade fingerprint or channel watermark. Apply still must validate lineage.
    rig.store.save_trade(dict(new, id="recovered-old", message=15368, status="closed"))
    assert rig.inbox.fresh(job["lease"])
    rig.exchange.writes.clear()
    with pytest.raises(Review, match="reply.*different owned trade"):
        await rig.inbox.apply(job["lease"], result)
    assert not rig.exchange.writes
    assert rig.exchange.positions["FIL_USDT"]["size"] == new["filled"]


async def test_model_can_correct_the_wrong_target_to_ignore_on_the_same_lease(rig):
    old, new = await close_old_and_open_new(rig)
    receive(rig, 15375, "FIL 這單先行離場", old["message"])
    job = await rig.inbox.claim()
    with pytest.raises(Review, match="reply.*different owned trade"):
        await rig.inbox.propose(job["lease"], answer(job, action="close", target_trade_id=new["id"]))
    assert rig.inbox.fresh(job["lease"])
    await apply(rig, job, answer(job, action="ignore"))
    assert not rig.exchange.writes
    assert rig.exchange.positions["FIL_USDT"]["size"] == new["filled"]
