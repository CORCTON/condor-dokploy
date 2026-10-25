"""Offline protocol scenarios using verbatim messages from the two-month window.

Source: comprehensive-evidence-20261025/validated-10x-9jj218rm/history.json,
channel -1001793451069, source_time >= 1787587200 (2026-08-25 00:00 Asia/Shanghai).
These are scripted decisions and synthetic arrival/fill/edit schedules, NOT new
Gemini samples or a historical backtest. The source is a current-revision snapshot;
the original pre-edit text is unavailable. First-arrival scenarios intentionally
retime that text and omit its historical edit marker. Images are not interpreted.

Tests named ``characterization`` document existing availability costs or trust
boundaries; passing them does not mean those side effects are desirable or fixed.
All execution uses the real Store/SignalInbox/Engine and an in-memory FakeExchange.
No provider, Telegram, live exchange, or testnet connection is made.
"""

import time
from copy import deepcopy
from types import SimpleNamespace

import pytest
from comprehensive_fixtures import FakeExchange

from trader.engine import Engine, Review
from trader.gate import Gate
from trader.models import Decisions
from trader.signals import SignalInbox
from trader.store import Store

CHANNEL = -1001793451069
CUTOFF = 1787587200
SOURCES = {
    15333: ("#INJ\n\n當前價格入場一筆空單", None, 1792378808.0, 1792378856.0),
    15342: ("#JTO\n\n當前價格入場一筆多單，進行突破博弈", None, 1792468078.0, 1792468202.0),
    15347: ("JTO 很舒服的上漲，當前價格做TP1💰", 15342, 1792505415.0, 1792548529.0),
    15368: ("#FIL\n\n當前價格入場一筆多單📈", None, 1792723020.0, 1792723186.0),
    15375: ("FIL 這單先行離場", 15368, 1792772265.0, 1792776132.0),
    15247: ("撤單吧，沒有給到入場機會", 15246, 1791128239.0, 1791129036.0),
}


@pytest.fixture
def rig(tmp_path, monkeypatch):
    async def forbidden_exchange(*_args, **_kwargs):
        pytest.fail("offline regression must never instantiate a real exchange transport")

    monkeypatch.setattr(Gate, "request", forbidden_exchange)
    store, exchange = Store(tmp_path / "isolated.sqlite3"), FakeExchange()
    for symbol in ("INJ", "JTO", "FIL"):
        exchange.add_contract(symbol + "_USDT")
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    assert engine.gate is exchange
    yield SimpleNamespace(store=store, exchange=exchange, engine=engine,
                          inbox=SignalInbox(engine), source_times={})
    store.close()


def receive(rig, source_id, *, message_id=None, text=None, edited=False):
    original, reply, historical_time, _historical_edit = SOURCES[source_id]
    assert historical_time >= CUTOFF
    message_id = source_id if message_id is None else message_id
    source_time = rig.source_times.setdefault(message_id, time.time() - 1)
    return rig.store.event(
        CHANNEL, message_id, original if text is None else text, reply, source_time, True,
        edited_at=source_time + .5 if edited else None,
    )


def decisions(job, *values):
    assert len(values) == len(job["events"])
    return Decisions.model_validate({"decisions": [
        {"event_id": event["event_id"], "reason": "offline scripted protocol scenario",
         "evidence_message_ids": [event["message_id"]], **value}
        for event, value in zip(job["events"], values, strict=True)
    ]})


async def apply(rig, job, value):
    await rig.inbox.propose(job["lease"], value)
    await rig.inbox.apply(job["lease"], value)


async def open_trade(rig, source_id, symbol, side, *, message_id=None):
    receive(rig, source_id, message_id=message_id)
    job = await rig.inbox.claim()
    await apply(rig, job, decisions(job, {"action": "open", "symbol": symbol, "side": side}))
    trade = next(t for t in rig.engine.active() if t["symbol"] == symbol + "_USDT")
    assert trade["filled"] and rig.exchange.positions[trade["symbol"]]["size"]
    return trade


@pytest.mark.parametrize("source_id,symbol,side", [
    (15333, "INJ", "short"), (15368, "FIL", "long"),
])
async def test_nonmaterial_edit_before_apply_reanalyzes_without_losing_first_entry(
    rig, source_id, symbol, side
):
    """The latest eligible revision can open once when no entry intent exists."""
    original_id = receive(rig, source_id)
    original = await rig.inbox.claim()
    result = decisions(original, {"action": "open", "symbol": symbol, "side": side})
    await rig.inbox.propose(original["lease"], result)

    # Constructed punctuation-only edit; not a claim about the unknown old revision.
    edited_id = receive(rig, source_id, text=SOURCES[source_id][0] + "。", edited=True)
    assert edited_id != original_id
    with pytest.raises(Review, match="stale"):
        await rig.inbox.apply(original["lease"], result)
    edited = await rig.inbox.claim()
    assert edited["events"][0]["edited"] is True
    await apply(rig, edited, decisions(edited, {"action": "open", "symbol": symbol, "side": side}))

    rows = {row["id"]: dict(row) for row in rig.store.db.execute("SELECT * FROM events")}
    assert rows[original_id]["status"] == "superseded"
    assert rows[edited_id]["status"] == "done"
    assert len(rig.store.trades()) == len(rig.exchange.orders) == 1
    entry, = rig.exchange.orders.values()
    assert entry["contract"] == symbol + "_USDT"
    assert (entry["size"] > 0) == (side == "long")


async def test_characterization_independent_coin_fill_state_stales_accepted_entry(rig):
    """A JTO close invalidates INJ analysis despite an unchanged INJ message."""
    jto = await open_trade(rig, 15342, "JTO", "long")
    receive(rig, 15333)
    job = await rig.inbox.claim()
    result = decisions(job, {"action": "open", "symbol": "INJ", "side": "short"})
    await rig.inbox.propose(job["lease"], result)
    watermark = rig.store.watermark(CHANNEL)

    # Simulate an exchange-side confirmed close, then let real maintenance observe it.
    await rig.exchange.request("POST", "/orders", {
        "contract": "JTO_USDT", "size": -jto["filled"], "price": "0", "tif": "ioc",
        "reduce_only": True,
    })
    await rig.engine.poll()
    await rig.engine.poll()  # Closure requires two independent zero-position observations.
    rig.exchange.writes.clear()
    assert rig.store.trade(jto["id"])["status"] == "closed"
    assert rig.store.watermark(CHANNEL) == watermark
    assert not rig.inbox.fresh(job["lease"])
    with pytest.raises(Review, match="stale"):
        await rig.inbox.apply(job["lease"], result)
    assert not rig.exchange.writes
    assert not any(t["symbol"] == "INJ_USDT" for t in rig.store.trades())

    # The entry is retriable while still fresh; this is delay, not an automatic loss.
    retry = await rig.inbox.claim()
    assert retry["lease"] != job["lease"]
    await apply(rig, retry, decisions(retry, {"action": "open", "symbol": "INJ", "side": "short"}))
    assert rig.exchange.positions["INJ_USDT"]["size"] < 0


@pytest.mark.parametrize("wrong_target", ["unowned", "different_symbol"])
async def test_valid_entry_and_wrong_management_target_reject_entire_batch(rig, wrong_target):
    """Atomic proposal validation is safe, but one bad target delays valid siblings."""
    fil = await open_trade(rig, 15368, "FIL", "long")
    jto = await open_trade(rig, 15342, "JTO", "long")
    receive(rig, 15333)
    receive(rig, 15375)
    job = await rig.inbox.claim()
    target = "not-owned" if wrong_target == "unowned" else jto["id"]
    result = decisions(job,
                       {"action": "open", "symbol": "INJ", "side": "short"},
                       {"action": "close", "symbol": "FIL", "target_trade_id": target})
    before = deepcopy(rig.exchange.positions)
    rig.exchange.writes.clear()
    with pytest.raises(Review, match="owned trade|symbol and target trade disagree"):
        await rig.inbox.propose(job["lease"], result)
    assert rig.store.analysis(job["lease"])["result"] is None
    assert rig.exchange.positions == before and not rig.exchange.writes
    assert all(row[0] == "analyzing" for row in rig.store.db.execute(
        "SELECT status FROM events WHERE id IN (?,?)",
        tuple(event["event_id"] for event in job["events"]),
    ))
    # Tool-validation recovery can correct the bad sibling on the same fresh lease.
    corrected = decisions(job,
                          {"action": "open", "symbol": "INJ", "side": "short"},
                          {"action": "close", "symbol": "FIL", "target_trade_id": fil["id"]})
    await apply(rig, job, corrected)
    assert rig.exchange.positions["INJ_USDT"]["size"] < 0
    assert rig.exchange.positions["FIL_USDT"]["size"] == 0
    assert rig.exchange.positions["JTO_USDT"] == before["JTO_USDT"]


@pytest.mark.parametrize("source_id,symbol,side", [
    (15333, "INJ", "short"), (15368, "FIL", "long"),
])
async def test_duplicate_delivery_and_completed_lease_do_not_open_twice(rig, source_id, symbol, side):
    receive(rig, source_id)
    job = await rig.inbox.claim()
    result = decisions(job, {"action": "open", "symbol": symbol, "side": side})
    await apply(rig, job, result)
    before = deepcopy(rig.exchange.positions), deepcopy(rig.exchange.writes)
    assert receive(rig, source_id) is None
    await rig.inbox.apply(job["lease"], result)
    assert await rig.inbox.claim() is None
    assert len(rig.store.trades()) == 1
    assert (rig.exchange.positions, rig.exchange.writes) == before
    assert not rig.store.unknowns()


async def old_and_new_fil(rig):
    old = await open_trade(rig, 15368, "FIL", "long")
    receive(rig, 15375)
    job = await rig.inbox.claim()
    await apply(rig, job, decisions(job, {"action": "close", "target_trade_id": old["id"]}))
    await rig.engine.poll()
    await rig.engine.poll()
    assert rig.store.trade(old["id"])["status"] == "closed"
    # Deliberately synthetic later entry, reusing the real opening wording.
    new = await open_trade(rig, 15368, "FIL", "long", message_id=9_000_001)
    assert old["id"] != new["id"]
    # A synthetic later revision of the real old exit retains its old reply ID.
    receive(rig, 15375, text=SOURCES[15375][0] + "。", edited=True)
    return old, new, await rig.inbox.claim()


async def test_closed_old_trade_id_cannot_close_a_new_same_coin_position(rig):
    old, new, job = await old_and_new_fil(rig)
    result = decisions(job, {"action": "close", "symbol": "FIL", "target_trade_id": old["id"]})
    before = deepcopy(rig.exchange.positions)
    rig.exchange.writes.clear()
    assert job["events"][0]["reply_to_message_id"] == old["message"]
    with pytest.raises(Review, match="exact owned trade"):
        await rig.inbox.propose(job["lease"], result)
    assert rig.exchange.positions == before and not rig.exchange.writes
    assert rig.store.trade(new["id"])["status"] == "working"


async def test_wrong_model_lineage_with_valid_current_id_is_rejected(rig):
    """Known reply ownership rejects an injected bad target, with no exchange writes."""
    old, new, job = await old_and_new_fil(rig)
    assert job["events"][0]["reply_to_message_id"] == old["message"]
    rig.exchange.writes.clear()
    before = deepcopy(rig.exchange.positions)
    with pytest.raises(Review, match="explicit reply references a different owned trade"):
        await apply(rig, job, decisions(job, {
            "action": "close", "symbol": "FIL", "target_trade_id": new["id"],
        }))
    assert rig.store.trade(new["id"])["status"] == "working"
    assert rig.exchange.positions == before and not rig.exchange.writes


@pytest.mark.parametrize("source_id,action,symbol", [
    (15375, "close", "FIL"), (15347, "tp1", "JTO"), (15247, "cancel", "ETH"),
])
async def test_unowned_management_is_noop_or_rejected_without_exchange_writes(
    rig, source_id, action, symbol
):
    receive(rig, source_id)
    job = await rig.inbox.claim()
    with pytest.raises(Review, match="exact owned trade"):
        await rig.inbox.propose(job["lease"], decisions(job, {
            "action": action, "symbol": symbol, "target_trade_id": "not-owned",
        }))
    await apply(rig, job, decisions(job, {"action": "ignore"}))
    assert not rig.exchange.writes and not rig.store.trades() and not rig.store.unknowns()
    assert rig.store.db.execute("SELECT COUNT(*) FROM notices").fetchone()[0] == 0
