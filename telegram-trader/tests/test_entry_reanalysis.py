"""An invalidated, unsubmitted entry must be reconsidered, never blindly retried."""

import json
import time

import pytest
from test_trader import Exchange

from trader.engine import Engine
from trader.models import Decisions, Signal
from trader.signals import SignalInbox
from trader.store import Store


@pytest.fixture
def inbox(tmp_path):
    store = Store(tmp_path / "reanalysis.sqlite3")
    engine = Engine(store, Exchange(), enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield SignalInbox(engine)
    store.close()


def receive(inbox, message, text):
    return inbox.store.event(-100123, message, text, None, time.time(), True)


def test_single_letter_channel_asset_is_valid():
    assert Signal(action="open", symbol="T", side="short").symbol == "T"
    with pytest.raises(ValueError):
        Signal(action="open", symbol="", side="short")


def decisions(job, opens=(1,)):
    return Decisions.model_validate({"decisions": [
        {
            "event_id": event["event_id"],
            "action": "open" if event["message_id"] in opens else "ignore",
            "symbol": "INJ" if event["message_id"] in opens else None,
            "side": "short" if event["message_id"] in opens else None,
            "reason": "scripted fresh-context decision",
            "evidence_message_ids": [event["message_id"]],
        }
        for event in job["events"]
    ]})


async def apply(inbox, job, result):
    await inbox.propose(job["lease"], result)
    await inbox.apply(job["lease"], result)


def inject_message_during_preflight(inbox, phase, text="我的开仓价：10"):
    gate = inbox.engine.gate
    original_get, original_request = gate.get, gate.request
    fired = False

    async def get(path, **params):
        nonlocal fired
        result = await original_get(path, **params)
        if phase == "account" and path == "/accounts" and not fired:
            fired = True
            receive(inbox, 2, text)
        return result

    async def request(method, path, body=None, params=None):
        nonlocal fired
        result = await original_request(method, path, body=body, params=params)
        if phase == "leverage" and path.endswith("set_leverage") and not fired:
            fired = True
            receive(inbox, 2, text)
        return result

    gate.get, gate.request = get, request


@pytest.mark.parametrize("phase", ["account", "leverage"])
async def test_followup_during_preflight_requeues_original_then_opens_once(inbox, phase):
    receive(inbox, 1, "INJ 当前开空")
    job = await inbox.claim()
    inject_message_during_preflight(inbox, phase)
    await apply(inbox, job, decisions(job))
    assert not inbox.engine.gate.orders
    assert not inbox.engine.paused()
    assert not inbox.engine.active()
    fresh = await inbox.claim()
    assert [event["message_id"] for event in fresh["events"]] == [1, 2]
    await apply(inbox, fresh, decisions(fresh))
    assert inbox.engine.gate.position["size"] == -14
    assert len(inbox.engine.gate.orders) == 1
    assert len([write for write in inbox.engine.gate.writes if write[1].endswith("set_leverage")]) == 1
    assert not inbox.store.queued()


@pytest.mark.parametrize("phase", ["account", "leverage"])
async def test_cancellation_during_preflight_is_seen_and_prevents_entry(inbox, phase):
    receive(inbox, 1, "INJ 当前开空")
    job = await inbox.claim()
    inject_message_during_preflight(inbox, phase, "别开了，取消")
    await apply(inbox, job, decisions(job))
    fresh = await inbox.claim()
    assert len(fresh["events"]) == 2
    await apply(inbox, fresh, decisions(fresh, opens=()))
    assert not inbox.engine.gate.orders
    assert not inbox.engine.active()
    assert not inbox.store.queued()


async def test_reanalysis_does_not_refresh_entry_age(inbox):
    key = receive(inbox, 1, "INJ 当前开空")
    job = await inbox.claim()
    inject_message_during_preflight(inbox, "leverage")
    await apply(inbox, job, decisions(job))
    row = inbox.store.db.execute("SELECT payload FROM events WHERE id=?", (key,)).fetchone()
    payload = json.loads(row[0])
    payload["source_time"] -= 61
    with inbox.store.db:
        inbox.store.db.execute("UPDATE events SET payload=? WHERE id=?", (json.dumps(payload), key))
    fresh = await inbox.claim()
    await apply(inbox, fresh, decisions(fresh))
    assert not inbox.engine.gate.orders
    assert "60 seconds" in inbox.store.db.execute("SELECT detail FROM events WHERE id=?", (key,)).fetchone()[0]


async def test_post_submission_followup_never_replays_confirmed_entry(inbox):
    receive(inbox, 1, "INJ 当前开空")
    job = await inbox.claim()
    gate = inbox.engine.gate
    original = gate.request

    async def request(method, path, body=None, params=None):
        result = await original(method, path, body=body, params=params)
        if method == "POST" and path == "/orders":
            receive(inbox, 2, "我的开仓价：10")
        return result

    gate.request = request
    await apply(inbox, job, decisions(job))
    fresh = await inbox.claim()
    assert [event["message_id"] for event in fresh["events"]] == [2]
    await apply(inbox, fresh, decisions(fresh, opens=()))
    assert len(gate.orders) == 1
    assert gate.position["size"] == -14


async def test_unknown_entry_result_never_returns_to_entry_queue(inbox):
    receive(inbox, 1, "INJ 当前开空")
    job = await inbox.claim()
    inbox.engine.gate.lost_response = True
    await apply(inbox, job, decisions(job))
    assert len(inbox.engine.gate.orders) == 1
    assert inbox.store.unknowns()
    assert inbox.engine.paused()
    assert await inbox.claim() is None


async def test_completed_events_in_invalidated_batch_are_not_requeued(inbox):
    receive(inbox, 0, "早上好")
    receive(inbox, 1, "INJ 当前开空")
    job = await inbox.claim()
    inject_message_during_preflight(inbox, "account")
    await apply(inbox, job, decisions(job))
    fresh = await inbox.claim()
    assert [event["message_id"] for event in fresh["events"]] == [1, 2]
    await apply(inbox, fresh, decisions(fresh))
    assert len(inbox.engine.gate.orders) == 1


async def test_superseded_event_snapshot_cannot_execute(inbox):
    key = receive(inbox, 1, "INJ 当前开空")
    job = await inbox.claim()
    event = inbox.store.db.execute("SELECT * FROM events WHERE id=?", (key,)).fetchone()
    inbox.store.finish(key, "superseded", "source deleted")
    await inbox.engine.execute_decision(
        event, decisions(job).decisions[0], inbox.store.watermark(-100123)
    )
    assert not inbox.engine.gate.writes
    row = inbox.store.db.execute("SELECT status FROM events WHERE id=?", (key,)).fetchone()
    assert row[0] == "superseded"
