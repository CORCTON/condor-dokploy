"""Current revisions may make a first entry, never resubmit a prior entry intent.

Scripted decisions and in-memory exchange only; no model or exchange API calls.
All age checks use the original publication time, never the recent edit time.
"""

import hashlib
import time
from copy import deepcopy

import pytest
from comprehensive_fixtures import FakeExchange
from test_signals import answer, apply

from trader.engine import Engine, Review
from trader.gate import Gate, GateError
from trader.signals import SignalInbox
from trader.store import Store

CHANNEL = -100123
TEXT = "#INJ\n當前價格入場一筆空單"


@pytest.fixture
def inbox(tmp_path, monkeypatch):
    monkeypatch.setattr(Gate, "__init__", lambda *_args, **_kwargs:
                        pytest.fail("entry revision tests must not use a real exchange"))
    store, exchange = Store(tmp_path / "revisions.sqlite3"), FakeExchange()
    for symbol in ("INJ_USDT", "JTO_USDT"):
        exchange.add_contract(symbol)
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    value = SignalInbox(engine)
    value.published = time.time() - 3
    yield value
    store.close()


def receive(inbox, *, text=TEXT, edited=False, media=None, live=True, eligible=True, age=None):
    return inbox.store.event(
        CHANNEL, 1, text, None, inbox.published if age is None else time.time() - age, live,
        media=media, edited_at=inbox.published + 2 if edited else None,
        entry_eligible=eligible,
    )


async def decide(inbox, *, symbol="INJ", side="short", price=None):
    job = await inbox.claim()
    assert job and len(job["events"]) == 1
    result = answer(job, symbol=symbol, side=side, price=price)
    await apply(inbox, job["lease"], result)
    return job


def event_result(inbox, event_id):
    return dict(inbox.store.db.execute(
        "SELECT status,detail FROM events WHERE id=?", (event_id,),
    ).fetchone())


@pytest.mark.parametrize("stage", ["queued", "proposed"])
@pytest.mark.parametrize("revision,symbol,side,media", [
    (TEXT, "INJ", "short", None),
    (TEXT + "。", "INJ", "short", None),
    ("#INJ 當前價格入場一筆多單", "INJ", "long", None),
    ("#JTO 當前價格入場一筆空單", "JTO", "short", None),
    (TEXT, "INJ", "short", {"id": "new-photo", "mime": "image/png", "size": 3}),
])
async def test_latest_fresh_revision_can_make_exactly_one_first_entry(
    inbox, stage, revision, symbol, side, media,
):
    original_id = receive(inbox)
    if stage == "proposed":
        original = await inbox.claim()
        old_result = answer(original, symbol="INJ", side="short")
        await inbox.propose(original["lease"], old_result)
    current_id = receive(inbox, text=revision, edited=True, media=media)
    assert current_id != original_id
    if stage == "proposed":
        with pytest.raises(Review, match="stale"):
            await inbox.apply(original["lease"], old_result)
    assert not inbox.engine.gate.writes

    current = await decide(inbox, symbol=symbol, side=side)
    assert current["events"][0]["edited"] and current["events"][0]["entry_eligible"]
    assert current["events"][0]["source_time"] == inbox.published
    assert event_result(inbox, original_id)["status"] == "superseded"
    assert event_result(inbox, current_id)["status"] == "done"
    trade, = inbox.store.trades()
    assert (trade["symbol"], trade["side"]) == (symbol + "_USDT", side)
    assert len(inbox.engine.gate.orders) == 1
    assert inbox.store.operation(trade["id"] + ":entry")["status"] == "confirmed"


async def test_first_seen_revision_after_fast_edit_can_open_without_original_delivery(inbox):
    event_id = receive(inbox, edited=True)
    await decide(inbox)
    assert event_result(inbox, event_id)["status"] == "done"
    assert len(inbox.engine.gate.orders) == 1


@pytest.mark.parametrize("guard", ["old", "recovered", "offline", "activation", "paused", "restart"])
async def test_edit_never_renews_original_age_or_overrides_entry_eligibility(inbox, guard):
    event_id = receive(inbox, edited=True, age=61 if guard == "old" else None,
                       eligible=guard != "recovered", live=guard != "offline")
    if guard == "activation":
        inbox.store.set("entries_started_after", inbox.published + 1)
    elif guard == "paused":
        inbox.engine.pause("test operator pause")
    elif guard == "restart":
        inbox.store.recover_analysis()
    await decide(inbox)
    assert event_result(inbox, event_id)["status"] == "review"
    assert not inbox.engine.gate.writes and not inbox.store.trades()
    if guard == "old":
        assert "older than 60 seconds" in event_result(inbox, event_id)["detail"]


@pytest.mark.parametrize("previous", ["filled", "pending", "cancelled", "closed", "rejected", "unknown"])
@pytest.mark.parametrize("symbol", ["INJ", "JTO"])
async def test_revision_cannot_resubmit_existing_entry_in_any_terminal_or_uncertain_state(
    inbox, monkeypatch, previous, symbol,
):
    exchange = inbox.engine.gate
    real_request = exchange.request

    async def entry_outcome(method, path, body=None, params=None):
        if method == "POST" and path == "/orders":
            if previous == "rejected":
                raise GateError(400, "ORDER_REJECTED")
            if previous == "unknown":
                await real_request(method, path, body, params)
                raise TimeoutError("entry accepted but response lost")
        return await real_request(method, path, body, params)

    monkeypatch.setattr(exchange, "request", entry_outcome)
    receive(inbox)
    await decide(inbox, price="9" if previous in {"pending", "cancelled"} else None)
    monkeypatch.setattr(exchange, "request", real_request)
    trade, = inbox.store.trades()
    if previous == "cancelled":
        await inbox.engine.cancel_entry(trade)
        await inbox.engine.poll()
        await inbox.engine.poll()
    elif previous == "closed":
        await inbox.engine.close_trade(trade)
        await inbox.engine.poll()
        await inbox.engine.poll()
    if previous in {"closed", "cancelled"}:
        assert inbox.store.trade(trade["id"])["status"] == "closed"
    if previous == "rejected":
        assert inbox.store.operation(trade["id"] + ":entry")["status"] == "rejected"
    if previous == "unknown":
        assert inbox.store.unknowns()
        inbox.store.set("paused", False)  # Unknown intent must still independently block entries.
    before = deepcopy(exchange.orders), deepcopy(exchange.positions), deepcopy(exchange.writes)
    event_id = receive(inbox, text=f"#{symbol} 當前價格入場一筆多單", edited=True)
    await decide(inbox, symbol=symbol, side="long")
    assert event_result(inbox, event_id)["status"] == "review"
    assert (exchange.orders, exchange.positions, exchange.writes) == before
    assert len(inbox.store.trades()) == 1


@pytest.mark.parametrize("status", ["unknown", "confirmed", "rejected"])
async def test_prior_durable_entry_intent_blocks_revision_even_without_trade_row(inbox, status):
    trade_id = hashlib.sha256(f"{CHANNEL}:1".encode()).hexdigest()[:24]
    inbox.store.intent(trade_id + ":entry", trade_id, "POST", "/orders",
                       {"body": {"contract": "INJ_USDT"}, "params": None})
    if status != "unknown":
        inbox.store.result(trade_id + ":entry", {"id": "prior-entry"}, status)
    event_id = receive(inbox, edited=True)
    await decide(inbox)
    assert event_result(inbox, event_id)["status"] == "review"
    assert not inbox.engine.gate.writes and not inbox.store.trades()
    assert inbox.store.operation(trade_id + ":entry")["status"] == status


@pytest.mark.parametrize("phase", ["preflight", "after_leverage"])
@pytest.mark.parametrize("symbol,side", [("INJ", "short"), ("INJ", "long"), ("JTO", "short")])
async def test_edit_arriving_during_entry_checks_requires_new_decision_before_order(
    inbox, monkeypatch, phase, symbol, side,
):
    original_id = receive(inbox)
    exchange = inbox.engine.gate
    real_get, changed = exchange.get, False

    async def during_get(path, **params):
        nonlocal changed
        leverage_written = any(item[1].endswith("set_leverage") for item in exchange.writes)
        if not changed and ((phase == "preflight" and path == "/accounts") or (
            phase == "after_leverage" and path == "/positions/INJ_USDT" and leverage_written
        )):
            changed = True
            receive(inbox, text=f"#{symbol} 當前價格入場一筆{'多' if side == 'long' else '空'}單。",
                    edited=True)
        return await real_get(path, **params)

    monkeypatch.setattr(exchange, "get", during_get)
    await decide(inbox)
    assert changed and not exchange.orders
    assert event_result(inbox, original_id)["status"] == "superseded"
    job = await decide(inbox, symbol=symbol, side=side)
    assert job["events"][0]["edited"]
    if phase == "after_leverage" and symbol == "JTO":
        # Existing write protocol deliberately refuses different arguments under
        # a confirmed leverage intent. This narrow race stays a safe refusal;
        # changing that write protocol is outside the first-entry edit fix.
        row = event_result(inbox, job["events"][0]["event_id"])
        assert row["status"] == "review"
        assert "operation key reused with different intent" in row["detail"]
        assert not exchange.orders
        assert len(exchange.writes) == 1
        assert exchange.writes[0][1] == "/positions/INJ_USDT/set_leverage"
        return
    assert len(exchange.orders) == 1
    order, = exchange.orders.values()
    assert order["contract"] == symbol + "_USDT"
    assert (order["size"] > 0) == (side == "long")
    assert sum(path.endswith("set_leverage") for _method, path, *_rest in exchange.writes) == 1
    assert len(inbox.store.trades()) == 1
    assert not inbox.store.unknowns()


async def test_fresh_edit_does_not_override_previously_recorded_rejected_trade(inbox):
    # A failed pre-submit attempt may have no :entry intent; it still requires review.
    receive(inbox)
    inbox.engine.gate.positions["INJ_USDT"]["pos_margin_mode"] = "cross"
    real_request = inbox.engine.gate.request

    async def leverage_rejected(method, path, body=None, params=None):
        if path.endswith("set_leverage"):
            raise GateError(400, "LEVERAGE_REJECTED")
        return await real_request(method, path, body, params)

    inbox.engine.gate.request = leverage_rejected
    await decide(inbox)
    trade, = inbox.store.trades()
    assert trade["status"] == "rejected" and inbox.store.operation(trade["id"] + ":entry") is None
    inbox.engine.gate.request = real_request
    event_id = receive(inbox, edited=True)
    before = deepcopy(inbox.engine.gate.writes)
    await decide(inbox)
    assert event_result(inbox, event_id)["status"] == "review"
    assert inbox.engine.gate.writes == before and not inbox.engine.gate.orders
