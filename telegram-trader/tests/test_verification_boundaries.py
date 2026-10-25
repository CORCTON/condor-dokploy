"""Local source-verification boundaries; scripted decisions, no remote services."""

import datetime
import json
import time
from copy import deepcopy

import pytest
from aiohttp.test_utils import TestClient, TestServer
from test_channel import message
from test_comprehensive_ingress import channel_with
from test_signals import answer
from test_signals import inbox as inbox  # noqa: PLC0414 -- pytest fixture

from trader.app import make_app, receive_message
from trader.gate import Gate

CHANNEL = -100123
HEADERS = {"Authorization": "Bearer verification-boundaries-local-only"}


@pytest.fixture(autouse=True)
def local_only(monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "verification-boundaries-local-only")
    monkeypatch.setattr(
        Gate, "__init__",
        lambda *_args, **_kwargs: pytest.fail("verification tests must not use a real exchange"),
    )


def source(message_id, published, *, edited_at=None):
    post = message(message_id, "INJ 当前开空")
    post.reply_to_msg_id = None
    post.date = datetime.datetime.fromtimestamp(published, datetime.UTC)
    if edited_at is not None:
        post.edit_date = datetime.datetime.fromtimestamp(edited_at, datetime.UTC)
    return post


async def submit(client, job, decision):
    return await client.post("/signals/decisions", headers=HEADERS, json={
        "lease": job["lease"], "result": decision.model_dump(),
    })


@pytest.mark.parametrize("guard", [
    "missing_eligibility", "offline", "recovered", "paused", "disabled", "reactivated",
])
async def test_verification_edit_cannot_restore_a_blocked_entry(inbox, guard):
    published = int(time.time()) - 3
    post = source(1, published)
    if guard == "offline":
        inbox.store.event(CHANNEL, 1, post.message, None, published, False)
    else:
        receive_message(inbox.engine, CHANNEL, post,
                        published + 1 if guard == "recovered" else published - 1)
    if guard == "missing_eligibility":
        row = inbox.store.db.execute("SELECT id,payload FROM events").fetchone()
        payload = json.loads(row["payload"])
        del payload["entry_eligible"]
        with inbox.store.db:
            inbox.store.db.execute("UPDATE events SET payload=? WHERE id=?",
                                   (json.dumps(payload), row["id"]))
    job = await inbox.claim()
    decision = answer(job, symbol="INJ", side="short")
    await inbox.propose(job["lease"], decision)
    post.edit_date = datetime.datetime.fromtimestamp(published + 1, datetime.UTC)

    class Authority:
        calls = 0

        async def get_messages(self, peer, ids):
            assert ids == [1]
            self.calls += 1
            if self.calls == 1:
                if guard == "paused":
                    inbox.engine.pause("operator pause during source verification")
                elif guard == "disabled":
                    inbox.engine.enabled = False
                elif guard == "reactivated":
                    inbox.engine.gate.ready = True
                    inbox.engine.record_heartbeat("interpreter")
                    inbox.engine.record_heartbeat("planner")
                    inbox.engine.pause("operator starts a new activation")
                    await inbox.engine.resume()
                    assert inbox.store.get("entries_started_after") > published
                    assert not inbox.engine.paused()
            return [post]

    async with TestClient(TestServer(make_app(inbox.engine, channel_with(Authority())))) as client:
        response = await submit(client, job, decision)
        assert response.status == 409
        assert not inbox.engine.gate.writes
        fresh = await inbox.claim()
        assert fresh and fresh["lease"] != job["lease"]
        event, = fresh["events"]
        eligible = guard not in {"missing_eligibility", "offline", "recovered"}
        assert event["source_time"] == published and event["edited"]
        assert event["entry_eligible"] is eligible
        assert event["context_recovered"] is not eligible
        replacement = answer(fresh, symbol="INJ", side="short")
        await inbox.propose(fresh["lease"], replacement)
        response = await submit(client, fresh, replacement)
        assert response.status == 200
        assert not inbox.engine.gate.writes and not inbox.store.trades()
        row = inbox.store.db.execute(
            "SELECT status FROM events WHERE id=?", (event["event_id"],)
        ).fetchone()
        assert row["status"] == "review"
        assert inbox.store.db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0


@pytest.mark.parametrize("first_eligible", [False, True])
async def test_verification_keeps_mixed_batch_eligibility_bound_to_each_message(
    inbox, first_eligible,
):
    published = int(time.time()) - 3
    eligibility = {1: first_eligible, 2: not first_eligible}
    posts = {message_id: source(message_id, published) for message_id in eligibility}
    for message_id, post in posts.items():
        receive_message(inbox.engine, CHANNEL, post, published - 1,
                        entry_eligible=eligibility[message_id])
    job = await inbox.claim()
    decision = answer(job, symbol="INJ", side="short")
    await inbox.propose(job["lease"], decision)
    for post in posts.values():
        post.edit_date = datetime.datetime.fromtimestamp(published + 1, datetime.UTC)

    class Authority:
        async def get_messages(self, peer, ids):
            assert ids == [1, 2]
            return [posts[message_id] for message_id in ids]

    async with TestClient(TestServer(make_app(inbox.engine, channel_with(Authority())))) as client:
        response = await submit(client, job, decision)
        assert response.status == 409 and not inbox.engine.gate.writes
        fresh = await inbox.claim()
        assert {event["message_id"]: event["entry_eligible"] for event in fresh["events"]} == eligibility
        replacement = answer(fresh, symbol="INJ", side="short")
        await inbox.propose(fresh["lease"], replacement)
        response = await submit(client, fresh, replacement)
        assert response.status == 200
        trade, = inbox.store.trades()
        assert trade["message"] == (1 if first_eligible else 2)
        assert len(inbox.engine.gate.orders) == 1
        assert not inbox.store.unknowns()


@pytest.mark.parametrize("eligible", [False, True])
async def test_older_edit_delivery_cannot_replace_current_revision_eligibility(inbox, eligible):
    published = int(time.time()) - 4
    newest = source(1, published, edited_at=published + 2)
    newest.message += "。"
    receive_message(inbox.engine, CHANNEL, newest, published - 1, entry_eligible=eligible)
    job = await inbox.claim()
    decision = answer(job, symbol="INJ", side="short")
    await inbox.propose(job["lease"], decision)
    older = deepcopy(newest)
    older.message = "INJ 当前开空"
    older.edit_date = datetime.datetime.fromtimestamp(published + 1, datetime.UTC)
    receive_message(inbox.engine, CHANNEL, older, time.time(), via="late-read",
                    entry_eligible=not eligible)
    assert inbox.fresh(job["lease"])
    assert inbox.store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1

    class Authority:
        async def get_messages(self, peer, ids):
            assert ids == [1]
            return [newest]

    async with TestClient(TestServer(make_app(inbox.engine, channel_with(Authority())))) as client:
        response = await submit(client, job, decision)
        assert response.status == 200
        row = inbox.store.db.execute("SELECT payload,status FROM events").fetchone()
        payload = json.loads(row["payload"])
        assert payload["text"] == newest.message and payload["entry_eligible"] is eligible
        assert payload["source_time"] == published
        if eligible:
            assert row["status"] == "done" and len(inbox.engine.gate.orders) == 1
        else:
            assert row["status"] == "review" and not inbox.engine.gate.writes
        assert not inbox.store.unknowns()
