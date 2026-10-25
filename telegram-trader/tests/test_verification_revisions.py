"""Source verification must preserve, never grant, a signal's entry eligibility."""

import datetime
import json
import time

import pytest
from aiohttp.test_utils import TestClient, TestServer
from test_channel import message
from test_comprehensive_ingress import channel_with
from test_signals import answer
from test_signals import inbox as inbox  # noqa: PLC0414 -- pytest fixture

from trader.app import make_app, receive_message


@pytest.mark.parametrize("revision", ["metadata", "text"])
@pytest.mark.parametrize("guard", [None, "recovered", "expired", "activation"])
async def test_verification_revision_retains_original_entry_boundary(
    inbox, monkeypatch, revision, guard
):
    monkeypatch.setenv("TRADER_TOKEN", "verification-local-only")
    headers = {"Authorization": "Bearer verification-local-only"}
    published = int(time.time()) - (61 if guard == "expired" else 2)
    post = message(1, "INJ 当前开空")
    post.date = datetime.datetime.fromtimestamp(published, datetime.UTC)
    receive_message(inbox.engine, -100123, post,
                    published + 1 if guard == "recovered" else published - 1)
    if guard == "activation":
        inbox.store.set("entries_started_after", published + 1)
    job = await inbox.claim()
    decision = answer(job, symbol="INJ", side="short")
    await inbox.propose(job["lease"], decision)
    post.edit_date = datetime.datetime.fromtimestamp(published + 1, datetime.UTC)
    if revision == "text":
        post.message += "。"

    class Authority:
        async def get_messages(self, peer, ids):
            assert ids == [1]
            return [post]

    async with TestClient(TestServer(make_app(inbox.engine, channel_with(Authority())))) as client:
        response = await client.post("/signals/decisions", headers=headers, json={
            "lease": job["lease"], "result": decision.model_dump(),
        })
        assert response.status == 409
        assert not inbox.engine.gate.writes
        fresh = await inbox.claim()
        assert fresh and fresh["lease"] != job["lease"]
        row = inbox.store.db.execute("SELECT * FROM events ORDER BY rowid DESC LIMIT 1").fetchone()
        payload = json.loads(row["payload"])
        assert payload["edited"] and payload["source_time"] == published
        assert payload["entry_eligible"] == (guard != "recovered")
        assert payload["context_recovered"] == (guard == "recovered")
        replacement = answer(fresh, symbol="INJ", side="short")
        await inbox.propose(fresh["lease"], replacement)
        response = await client.post("/signals/decisions", headers=headers, json={
            "lease": fresh["lease"], "result": replacement.model_dump(),
        })
        assert response.status == 200
        if guard is None:
            assert len(inbox.engine.gate.orders) == 1
            assert inbox.store.trades()[0]["status"] == "working"
        else:
            assert not inbox.engine.gate.writes
            latest = inbox.store.db.execute(
                "SELECT status FROM events ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            assert latest[0] == "review"
        assert not inbox.store.unknowns()
