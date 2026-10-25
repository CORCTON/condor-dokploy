"""Ingress/recovery regressions use only an in-memory database and exchange simulator."""

import datetime
import json
import time

import pytest
from test_channel import message
from test_signals import answer, apply, open_trade, receive
from test_trader import Exchange

from trader.app import (
    collect_new_messages,
    receive_current_message,
    receive_message,
    refresh_known_messages,
)
from trader.engine import Engine, Review
from trader.models import Decisions
from trader.signals import SignalInbox
from trader.store import Store


@pytest.fixture
def inbox():
    store = Store(":memory:")
    engine = Engine(store, Exchange(), enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield SignalInbox(engine)
    store.close()


def edited(message_id, text, revision):
    result = message(message_id, text)
    result.date = datetime.datetime.fromtimestamp(time.time() - 30, datetime.UTC)
    result.edit_date = datetime.datetime.fromtimestamp(revision, datetime.UTC)
    return result


async def test_history_edit_before_push_cannot_become_a_new_entry(inbox):
    now = time.time()
    post = edited(101, "INJ 当前价格入场一笔空单", now - 1)

    class Client:
        async def iter_messages(self, peer, **kwargs):
            yield post

        async def get_messages(self, peer, ids):
            return post

    await collect_new_messages(Client(), "allowed", 100, inbox.engine, -100123, now - 60)
    receive_message(inbox.engine, -100123, post, now - 60, edited=True)
    job = await inbox.claim()
    assert len(job["events"]) == 1
    assert job["events"][0]["edited"] is True
    assert job["events"][0]["source_time"] == post.date.timestamp()
    await apply(inbox, job["lease"], answer(job, symbol="INJ", side="short"))
    assert not inbox.engine.gate.orders


async def test_newer_edit_supersedes_pending_revision_and_rejects_delayed_old_push(inbox):
    now = time.time()
    first = edited(101, "INJ 当前开空", now - 2)
    final = edited(101, "取消，不开了", now - 1)
    receive_message(inbox.engine, -100123, first, now - 60, edited=True)
    old_job = await inbox.claim()
    receive_message(inbox.engine, -100123, final, now - 60, edited=True)
    receive_message(inbox.engine, -100123, first, now - 60, edited=True)
    assert not inbox.fresh(old_job["lease"])
    inbox.store.release_analysis(old_job["lease"])
    inbox.store.finish(old_job["events"][0]["event_id"], "queued")
    job = await inbox.claim()
    assert [e["text"] for e in job["events"]] == ["取消，不开了"]
    assert inbox.store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2


async def test_late_same_second_edit_push_reads_current_authoritative_message(inbox):
    now = time.time()
    old = edited(101, "INJ 当前开空", int(now) - 1)
    current = edited(101, "取消，不开了", int(now) - 1)
    receive_message(inbox.engine, -100123, current, now - 60, edited=True)

    class Client:
        async def get_messages(self, peer, ids):
            assert ids == 101
            return current

    await receive_current_message(
        Client(), "allowed", inbox.engine, -100123, old, now - 60, edited=True
    )
    job = await inbox.claim()
    assert [e["text"] for e in job["events"]] == ["取消，不开了"]
    assert inbox.store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1


def test_upgrade_does_not_replay_completed_legacy_edited_messages(inbox):
    now = int(time.time())
    post = edited(101, "INJ SL放在11", now - 1)
    # Legacy records used the edit timestamp as source_time and had no edited_at.
    key = inbox.store.event(-100123, 101, post.message, post.reply_to_msg_id, now - 1, True, True)
    inbox.store.finish(key, "done")
    receive_message(inbox.engine, -100123, post, now, edited=True)
    assert not inbox.store.queued()
    assert inbox.store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1


@pytest.mark.parametrize("same_second", [False, True])
async def test_edit_reverting_to_prior_sl_is_a_new_action(inbox, same_second):
    trade = await open_trade(inbox)
    now = time.time()
    for index, price in enumerate(("11", "12", "11")):
        post = edited(2, "INJ SL放在" + price, now if same_second else now + index)
        receive_message(inbox.engine, -100123, post, now - 60, edited=True)
        job = await inbox.claim()
        assert job is not None
        await apply(
            inbox, job["lease"], answer(job, "sl", price=price, target_trade_id=trade["id"])
        )
        assert inbox.engine.gate.stops["100"]["trigger"]["price"] == price


async def test_restart_reanalyzes_received_close_but_not_historical_entries(inbox):
    trade = await open_trade(inbox)
    receive(inbox, "INJ 全部平仓", message=2, reply=1)
    inbox.store.recover_analysis()
    job = await inbox.claim()
    assert job["events"][0]["live"]
    assert not job["events"][0]["entry_eligible"]
    await apply(inbox, job["lease"], answer(job, "close", target_trade_id=trade["id"]))
    assert inbox.engine.gate.position["size"] == 0


async def test_reconnect_cursor_recovers_gap_before_claiming_any_message(inbox):
    now = time.time()
    inbox.engine.listener_ready = False
    posts = [message(102, "INJ 平仓"), message(103, "INJ 当前开空")]
    for post in posts:
        post.date = datetime.datetime.fromtimestamp(now - 10, datetime.UTC)

    class Client:
        async def iter_messages(self, peer, **kwargs):
            assert kwargs["min_id"] == 101
            for post in posts:
                yield post

    cursor = await collect_new_messages(Client(), "allowed", 101, inbox.engine, -100123, now)
    assert cursor == inbox.store.get("telegram_cursor:-100123") == 103
    assert await inbox.claim() is None
    inbox.engine.listener_ready = True
    job = await inbox.claim()
    assert [e["message_id"] for e in job["events"]] == [102, 103]
    assert all(e["live"] and not e["entry_eligible"] for e in job["events"])


async def test_new_push_during_startup_waits_for_recovery_without_losing_entry(inbox):
    live_since = time.time() - 1
    inbox.engine.listener_ready = False
    receive_message(inbox.engine, -100123, message(101, "INJ 当前开空"), live_since)
    assert await inbox.claim() is None
    inbox.engine.listener_ready = True
    job = await inbox.claim()
    assert job["events"][0]["entry_eligible"]
    await apply(inbox, job["lease"], answer(job, symbol="INJ", side="short"))
    assert len(inbox.engine.gate.orders) == 1


async def test_source_deleted_during_analysis_invalidates_without_requeue(inbox):
    now = time.time()
    post = edited(101, "INJ 当前开空", now)
    receive_message(inbox.engine, -100123, post, now - 60)
    job = await inbox.claim()

    class Client:
        async def get_messages(self, peer, ids):
            return None

    await receive_current_message(Client(), "allowed", inbox.engine, -100123, post, now - 60)
    assert not inbox.fresh(job["lease"])
    assert await inbox.claim() is None
    assert not inbox.engine.gate.writes


async def test_recovered_management_includes_later_completed_channel_context(inbox):
    receive(inbox, "INJ SL放在11", message=2)
    completed = receive(inbox, "INJ SL改为12，以这个为准", message=3)
    inbox.store.finish(completed, "done")
    inbox.store.recover_analysis()
    job = await inbox.claim()
    assert job["events"][0]["context_recovered"]
    assert [e["message_id"] for e in job["events"]] == [2]
    assert [e["message_id"] for e in job["context_events"]] == [3]


async def test_reconnect_refreshes_only_relevant_edits_and_drops_deleted_pending(inbox):
    trade = await open_trade(inbox)
    now = time.time()
    original = edited(2, "INJ SL放在11", now - 2)
    receive_message(inbox.engine, -100123, original, now - 60, edited=True)
    job = await inbox.claim()
    await apply(inbox, job["lease"], answer(job, "sl", price="11", target_trade_id=trade["id"]))
    receive(inbox, "INJ 当前开空", message=3)
    inbox.engine.listener_ready = False

    class Client:
        async def get_messages(self, peer, ids):
            if ids == 2:
                return edited(2, "INJ SL放在12", now - 1)
            assert ids == [1, 2, 3]
            return [None, edited(2, "INJ SL放在12", now - 1), None]

    await refresh_known_messages(Client(), "allowed", inbox.engine, -100123, now)
    inbox.engine.listener_ready = True
    job = await inbox.claim()
    assert [e["message_id"] for e in job["events"]] == [2]
    assert not job["events"][0]["entry_eligible"]
    await apply(inbox, job["lease"], answer(job, "sl", price="12", target_trade_id=trade["id"]))
    assert inbox.engine.gate.stops["100"]["trigger"]["price"] == "12"
    assert inbox.store.db.execute("SELECT status FROM events WHERE message=3").fetchone()[0] == "superseded"


async def test_five_images_do_not_split_and_discard_a_valid_entry(inbox):
    for number in range(1, 6):
        receive(inbox, "INJ 当前开空" if number == 1 else "走势", message=number,
                media={"id": str(number), "mime": "image/jpeg"})
    job = await inbox.claim()
    assert len(job["events"]) == 5 and not job["more_queued"]
    decisions = answer(job, "ignore")
    decisions.decisions[0].action = "open"
    decisions.decisions[0].symbol = "INJ"
    decisions.decisions[0].side = "short"
    await apply(inbox, job["lease"], decisions)
    assert len(inbox.engine.gate.orders) == 1


async def test_large_backlog_defers_open_then_supplies_later_cancellation_context(inbox):
    for number in range(1, 52):
        text = "INJ 当前开空" if number == 1 else "取消INJ开仓" if number == 51 else "走势"
        receive(inbox, text, message=number)
    job = await inbox.claim()
    decisions = answer(job, "ignore")
    decisions.decisions[0].action = "open"
    decisions.decisions[0].symbol = "INJ"
    decisions.decisions[0].side = "short"
    await apply(inbox, job["lease"], decisions)
    assert not inbox.engine.gate.writes
    assert inbox.store.db.execute("SELECT status FROM events WHERE message=1").fetchone()[0] == "deferred"
    next_job = await inbox.claim()
    assert [e["message_id"] for e in next_job["events"]] == [51]
    await apply(inbox, next_job["lease"], answer(next_job, "ignore"))
    final_job = await inbox.claim()
    assert [e["message_id"] for e in final_job["events"]] == [1]
    assert final_job["events"][0]["backlog_deferred"]
    assert any(e["text"] == "取消INJ开仓" for e in final_job["context_events"])
    assert all(e["context_only"] for e in final_job["context_events"])
    await apply(inbox, final_job["lease"], answer(final_job, "ignore"))
    assert await inbox.claim() is None
    assert not inbox.engine.gate.writes


async def test_execution_requires_matching_immutable_proposal(inbox):
    receive(inbox, "INJ 当前开空")
    job = await inbox.claim()
    opening = answer(job, symbol="INJ", side="short")
    ignored = answer(job, "ignore")
    with pytest.raises(Review, match="accepted proposal"):
        await inbox.apply(job["lease"], opening)
    await inbox.propose(job["lease"], ignored)
    await inbox.propose(job["lease"], Decisions.model_validate(json.loads(ignored.model_dump_json())))
    with pytest.raises(Review, match="cannot be changed"):
        await inbox.propose(job["lease"], opening)
    with pytest.raises(Review, match="accepted proposal"):
        await inbox.apply(job["lease"], opening)
    await inbox.apply(job["lease"], ignored)
    assert not inbox.engine.gate.writes
