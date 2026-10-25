"""Additional ingress fault combinations; local HTTP, scripted decisions, no network trading.

These check delivery/execution contracts, not Gemini's semantic accuracy.
"""

import asyncio
import datetime
import hashlib
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer
from test_channel import message
from test_interpreter import interpreter as interpreter  # noqa: PLC0414 -- pytest fixture
from test_signals import answer, apply, open_trade, receive
from test_trader import Exchange

from trader.app import collect_new_messages, make_app, receive_message
from trader.channel import MAX_IMAGE_BYTES, ChannelReader
from trader.engine import Engine, Review
from trader.signals import SignalInbox
from trader.store import Store

CHANNEL = -100123
HEADERS = {"Authorization": "Bearer ingress-local-only"}


@pytest.fixture
def inbox(monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "ingress-local-only")
    store = Store(":memory:")
    engine = Engine(store, Exchange(), enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield SignalInbox(engine)
    store.close()


def channel_with(client):
    reader = ChannelReader()
    reader.client, reader.peer = client, "allowlisted-channel"
    return reader


async def accepted_entry(client):
    response = await client.post("/signals/claim", headers=HEADERS)
    assert response.status == 200
    job = await response.json()
    body = {
        "lease": job["lease"],
        "result": answer(job, symbol="INJ", side="short").model_dump(),
    }
    response = await client.post("/signals/proposals", headers=HEADERS, json=body)
    assert response.status == 200
    return job, body


@pytest.mark.parametrize("error", [TimeoutError, OSError])
async def test_authoritative_read_failure_cannot_trade_and_same_proposal_can_retry(inbox, error):
    post = message(1, "INJ 当前开空")
    receive_message(inbox.engine, CHANNEL, post, time.time() - 1)

    class Client:
        calls = 0

        async def get_messages(self, peer, ids):
            self.calls += 1
            assert ids == [1]
            if self.calls == 1:
                raise error("injected Telegram authority failure")
            return [post]

    reader = channel_with(Client())
    async with TestClient(TestServer(make_app(inbox.engine, reader))) as client:
        job, body = await accepted_entry(client)
        response = await client.post("/signals/decisions", headers=HEADERS, json=body)
        assert response.status == (503 if error is TimeoutError else 500)
        assert not inbox.engine.gate.writes
        assert inbox.store.db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0
        assert inbox.store.analysis(job["lease"])["status"] == "analyzing"
        response = await client.post("/signals/decisions", headers=HEADERS, json=body)
        assert response.status == 200
        assert len(inbox.engine.gate.orders) == 1
        assert not inbox.engine.paused()


async def test_authoritative_deletion_rejects_http_apply_and_does_not_resurrect(inbox):
    receive(inbox, "INJ 当前开空")

    class Client:
        async def get_messages(self, peer, ids):
            return [None] * len(ids)

    async with TestClient(TestServer(make_app(inbox.engine, channel_with(Client())))) as client:
        job, body = await accepted_entry(client)
        response = await client.post("/signals/decisions", headers=HEADERS, json=body)
        assert response.status == 409
        assert "stale" in await response.text()
        assert inbox.store.db.execute("SELECT status FROM events").fetchone()[0] == "superseded"
        inbox.store.recover_analysis()
        assert await inbox.claim() is None
        assert not inbox.engine.gate.writes
        assert inbox.store.analysis(job["lease"])["status"] == "stale"


@pytest.mark.parametrize("age_after_failure", [1, 61])
async def test_actual_worker_reanalyzes_authority_failure_without_resetting_source_age(
    inbox, interpreter, monkeypatch, age_after_failure
):
    clock = [time.time()]
    published = clock[0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    post = message(1, "INJ 当前开空")
    post.date = datetime.datetime.fromtimestamp(published, datetime.UTC)
    receive_message(inbox.engine, CHANNEL, post, published - 1)
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TRADER_LLM_READY", "true")
    completed = asyncio.Event()
    calls, jobs, sleeps = [], [], []
    real_sleep, api = asyncio.sleep, interpreter.api

    class Client:
        count = 0

        async def get_messages(self, peer, ids):
            self.count += 1
            if self.count == 1:
                clock[0] = published + age_after_failure
                raise TimeoutError("source unavailable before any exchange write")
            return [post]

    async def fast_sleep(delay):
        sleeps.append(delay)
        await real_sleep(0 if delay == 5 else delay)

    async def tracked_api(session, method, path, **kwargs):
        calls.append(path)
        result = await api(session, method, path, **kwargs)
        if path == "/signals/decisions":
            completed.set()
        return result

    @asynccontextmanager
    async def scripted_model(job, owner, session):
        jobs.append(job)
        result = answer(job, symbol="INJ", side="short").model_dump()
        await api(session, "POST", "/signals/proposals", json={"lease": job["lease"], "result": result})
        yield result

    monkeypatch.setattr(interpreter, "api", tracked_api)
    monkeypatch.setattr(interpreter, "interpret_current", scripted_model)
    monkeypatch.setattr(
        interpreter, "asyncio", SimpleNamespace(sleep=fast_sleep, CancelledError=asyncio.CancelledError)
    )
    async with TestClient(TestServer(make_app(inbox.engine, channel_with(Client())))) as client:
        monkeypatch.setenv("TELEGRAM_TRADER_URL", str(client.make_url("")))
        task = asyncio.create_task(interpreter.run_worker())
        try:
            await asyncio.wait_for(completed.wait(), 3)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    assert len(jobs) == 2
    assert calls.count("/signals/fail") == 1
    assert sleeps.count(5) == 1  # Existing worker backoff, not an immediate retry storm.
    assert jobs[0]["lease"] != jobs[1]["lease"]
    assert jobs[0]["events"][0]["event_id"] == jobs[1]["events"][0]["event_id"]
    assert jobs[0]["events"][0]["source_time"] == jobs[1]["events"][0]["source_time"]
    assert inbox.store.analysis(jobs[0]["lease"])["status"] == "failed"
    assert inbox.store.analysis(jobs[1]["lease"])["status"] == "done"
    assert len(inbox.engine.gate.orders) == (1 if age_after_failure == 1 else 0)
    if age_after_failure == 61:
        row = inbox.store.db.execute("SELECT status,detail FROM events").fetchone()
        assert row["status"] == "review" and "60 seconds" in row["detail"]


async def test_late_failure_for_old_lease_cannot_release_a_newly_claimed_analysis(inbox):
    receive(inbox, "INJ 当前开空")
    async with TestClient(TestServer(make_app(inbox.engine))) as client:
        old, _ = await accepted_entry(client)
        old_failure = {"lease": old["lease"], "error": "injected source query failure"}
        response = await client.post("/signals/fail", headers=HEADERS, json=old_failure)
        assert response.status == 200
        new, body = await accepted_entry(client)
        assert old["lease"] != new["lease"]
        response = await client.post("/signals/fail", headers=HEADERS, json=old_failure)
        assert response.status == 200
        assert inbox.fresh(new["lease"])
        assert inbox.store.db.execute("SELECT status FROM events").fetchone()[0] == "analyzing"
        response = await client.post("/signals/decisions", headers=HEADERS, json=body)
        assert response.status == 200
    assert len(inbox.engine.gate.orders) == 1


async def test_simultaneous_duplicate_http_apply_submits_one_order(inbox):
    post = message(1, "INJ 当前开空")
    receive_message(inbox.engine, CHANNEL, post, time.time() - 1)

    class Client:
        async def get_messages(self, peer, ids):
            await asyncio.sleep(0)
            return [post]

    async with TestClient(TestServer(make_app(inbox.engine, channel_with(Client())))) as client:
        _, body = await accepted_entry(client)
        responses = await asyncio.gather(*(
            client.post("/signals/decisions", headers=HEADERS, json=body) for _ in range(2)
        ))
        assert [response.status for response in responses] == [200, 200]
        assert len(inbox.engine.gate.orders) == 1
        assert sum(path == "/orders" for _, path, _, _ in inbox.engine.gate.writes) == 1


@pytest.mark.parametrize("interruption", ["pause", "expires"])
async def test_pause_or_sixty_seconds_expiry_during_authority_wait_blocks_entry(
    inbox, monkeypatch, interruption
):
    clock = [time.time()]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    post = message(1, "INJ 当前开空")
    post.date = datetime.datetime.fromtimestamp(clock[0] - 59, datetime.UTC)
    receive_message(inbox.engine, CHANNEL, post, clock[0] - 60)
    entered, release = asyncio.Event(), asyncio.Event()

    class Client:
        async def get_messages(self, peer, ids):
            entered.set()
            await release.wait()
            return [post]

    async with TestClient(TestServer(make_app(inbox.engine, channel_with(Client())))) as client:
        _, body = await accepted_entry(client)
        task = asyncio.create_task(client.post("/signals/decisions", headers=HEADERS, json=body))
        await asyncio.wait_for(entered.wait(), 2)
        try:
            if interruption == "pause":
                # Pause must not wait for the source read or engine execution lock.
                response = await asyncio.wait_for(client.post("/pause", headers=HEADERS), 2)
                assert response.status == 200
                assert (await response.json())["paused"]
            else:
                clock[0] += 2
        finally:
            release.set()
        assert (await asyncio.wait_for(task, 2)).status == 200
    row = inbox.store.db.execute("SELECT status,detail FROM events").fetchone()
    assert row["status"] == "review"
    assert ("paused" if interruption == "pause" else "60 seconds") in row["detail"]
    assert not inbox.engine.gate.writes


async def test_partial_history_failure_late_push_and_repeated_reconnect_preserve_every_id(inbox):
    now = time.time()
    posts = {i: message(i, f"消息 {i}") for i in range(101, 105)}
    cursors = []
    inbox.store.set("telegram_cursor:-100123", 100)
    inbox.store.set("entries_started_after", now - 5)

    class Client:
        fail_once = True

        async def iter_messages(self, peer, **kwargs):
            cursors.append(kwargs["min_id"])
            for number, post in posts.items():
                if number > kwargs["min_id"]:
                    yield post
                    if self.fail_once:
                        self.fail_once = False
                        raise OSError("lost connection after one persisted message")

    client = Client()
    with pytest.raises(OSError):
        await collect_new_messages(client, "allowed", 100, inbox.engine, CHANNEL, now - 1)
    assert inbox.store.get("telegram_cursor:-100123") == 101
    receive_message(inbox.engine, CHANNEL, posts[104], now - 1)
    assert inbox.store.get("telegram_cursor:-100123") == 101
    for _ in range(2):
        inbox.engine.listener_ready = False
        await collect_new_messages(
            client, "allowed", inbox.store.get("telegram_cursor:-100123"),
            inbox.engine, CHANNEL, now - 1,
        )
        assert await inbox.claim() is None
        inbox.engine.listener_ready = True
    assert cursors == [100, 101, 104]
    assert inbox.store.get("telegram_cursor:-100123") == 104
    assert inbox.store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 4
    job = await inbox.claim()
    assert {event["message_id"] for event in job["events"]} == set(posts)
    assert all(event["entry_eligible"] for event in job["events"])
    await apply(inbox, job["lease"], answer(job, "ignore"))
    assert await inbox.claim() is None
    assert not inbox.engine.paused()
    assert inbox.store.get("entries_started_after") == now - 5
    assert not inbox.engine.gate.writes


async def test_repeated_restart_preserves_pause_and_recovers_only_management_once(inbox):
    trade = await open_trade(inbox)
    inbox.engine.pause("operator pause must survive all restarts")
    cutoff = time.time() - 5
    inbox.store.set("entries_started_after", cutoff)
    receive(inbox, "INJ 全部平仓", message=2, reply=1)
    receive(inbox, "JTO 当前开多", message=3)
    abandoned = []
    for _ in range(3):
        job = await inbox.claim()
        abandoned.append(job["lease"])
        inbox.store.recover_analysis()
        assert inbox.engine.paused()
        assert inbox.store.get("entries_started_after") == cutoff
        assert len(inbox.store.queued()) == 2
    job = await inbox.claim()
    assert all(event["live"] and not event["entry_eligible"] for event in job["events"])
    decisions = answer(job, "open", symbol="JTO", side="long")
    decisions.decisions[0].action = "close"
    decisions.decisions[0].symbol = "INJ"
    decisions.decisions[0].side = None
    decisions.decisions[0].target_trade_id = trade["id"]
    await apply(inbox, job["lease"], decisions)
    assert inbox.engine.gate.position["size"] == 0
    assert len(inbox.engine.gate.orders) == 2  # one initial open, one recovered close
    inbox.store.recover_analysis()
    assert await inbox.claim() is None
    assert all(inbox.store.analysis(lease)["status"] == "abandoned" for lease in abandoned)
    assert inbox.engine.paused()


@pytest.mark.parametrize("target,symbol", [(None, None), ("jto", "INJ"), ("elsewhere", None)])
async def test_multiple_positions_require_exact_channel_and_symbol_association(inbox, target, symbol):
    for trade_id, asset, channel in (
        ("inj", "INJ", CHANNEL), ("jto", "JTO", CHANNEL), ("elsewhere", "INJ", CHANNEL - 1)
    ):
        inbox.store.save_trade({
            "id": trade_id, "symbol": asset + "_USDT", "channel": channel,
            "status": "working", "version": 1, "filled": 14, "position_size": -14,
            "entry_terminal": True,
        })
    receive(inbox, "INJ 平掉", message=10)
    job = await inbox.claim()
    assert {trade["id"] for trade in job["trades"]} == {"inj", "jto", "elsewhere"}
    with pytest.raises(Review, match="exact owned trade|disagree"):
        await inbox.propose(
            job["lease"], answer(job, "close", target_trade_id=target, symbol=symbol)
        )
    assert inbox.store.analysis(job["lease"])["result"] is None
    await inbox.propose(job["lease"], answer(job, "close", target_trade_id="inj", symbol="INJ"))
    assert not inbox.engine.gate.writes  # Do not execute against the single-position simulator.


async def test_full_long_deferred_context_and_image_references_survive_http_claim(inbox):
    texts = {}
    for number in range(1, 52):
        texts[number] = ("走势原文。" * 1200) + f" END-{number}"
        if number == 1:
            texts[number] += " INJ 当前开空"
        if number == 51:
            texts[number] += " 最终通知：撤销 INJ，不要开仓。"
        receive(inbox, texts[number], message=number, reply=1 if number > 1 else None,
                media={"id": f"photo-{number}", "mime": "image/png", "size": 8000})
    async with TestClient(TestServer(make_app(inbox.engine))) as client:
        first = await (await client.post("/signals/claim", headers=HEADERS)).json()
        assert len(first["events"]) == 50 and first["more_queued"]
        assert all(event["text"] == texts[event["message_id"]] for event in first["events"])
        decisions = answer(first, "ignore")
        decisions.decisions[0].action = "open"
        decisions.decisions[0].symbol, decisions.decisions[0].side = "INJ", "short"
        await apply(inbox, first["lease"], decisions)
        second = await (await client.post("/signals/claim", headers=HEADERS)).json()
        assert second["events"][0]["text"] == texts[51]
        await apply(inbox, second["lease"], answer(second, "ignore"))
        deferred = await (await client.post("/signals/claim", headers=HEADERS)).json()
        assert deferred["events"][0]["text"] == texts[1]
        assert len(deferred["context_events"]) == 50
        for event in deferred["context_events"]:
            assert event["context_only"] and event["text"] == texts[event["message_id"]]
            assert event["reply"] == 1
            assert event["media"]["id"] == f"photo-{event['message_id']}"
        assert deferred["context_events"][-1]["text"].endswith("撤销 INJ，不要开仓。")
        # The scripted model resolves the cancellation; transport did not drop its tail.
        await apply(inbox, deferred["lease"], answer(deferred, "ignore"))
    assert not inbox.engine.gate.writes


@pytest.mark.parametrize(
    "declared,actual,downloads",
    [(MAX_IMAGE_BYTES + 1, 3, 0), (3, MAX_IMAGE_BYTES + 1, 1), (0, 3, 0)],
)
async def test_oversize_or_unknown_size_images_fail_explicitly_without_truncation(
    declared, actual, downloads
):
    post = message(1, photo=True)
    post.file.size = declared

    class Client:
        count = 0

        async def get_messages(self, peer, ids):
            return post

        async def download_media(self, msg, file):
            self.count += 1
            return b"x" * actual

    client = Client()
    with pytest.raises(ValueError, match="limit|too large"):
        await channel_with(client).image(1)
    assert client.count == downloads


async def test_eight_mib_image_is_returned_byte_for_byte_over_http(inbox):
    data = bytes(range(256)) * (MAX_IMAGE_BYTES // 256)
    post = message(1, photo=True)
    post.file.size = len(data)

    class Client:
        async def get_messages(self, peer, ids):
            return post

        async def download_media(self, msg, file):
            return data

    async with TestClient(TestServer(make_app(inbox.engine, channel_with(Client())))) as client:
        response = await client.get("/channel/messages/1/image", headers=HEADERS)
        assert response.status == 200
        delivered = await response.read()
    assert len(delivered) == MAX_IMAGE_BYTES
    assert hashlib.sha256(delivered).digest() == hashlib.sha256(data).digest()
    assert not inbox.store.queued() and not inbox.engine.gate.writes
