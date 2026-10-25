"""Bounded Telegram reads using production HTTP cancellation semantics; no live trading."""

import asyncio
import datetime
import time
from types import SimpleNamespace

import pytest
from aiohttp import ClientSession, ClientTimeout, web
from test_channel import message
from test_signals import answer
from test_trader import Exchange

from trader import app as app_module
from trader import channel as channel_module
from trader.app import (
    collect_new_messages,
    make_app,
    receive_current_message,
    receive_live_message,
    receive_message,
    refresh_known_messages,
)
from trader.channel import ChannelReader
from trader.engine import Engine
from trader.signals import SignalInbox
from trader.store import Store

CHANNEL = -100123
HEADERS = {"Authorization": "Bearer local-deadline-test"}


@pytest.fixture
def context(monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "local-deadline-test")
    monkeypatch.setattr(app_module, "READ_TIMEOUT_SECONDS", 0.08)
    monkeypatch.setattr(channel_module, "READ_TIMEOUT_SECONDS", 0.08)
    store = Store(":memory:")
    engine = Engine(store, Exchange(), enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    reader = ChannelReader()
    reader.peer = "allowlisted-channel"
    reader.client = SimpleNamespace()
    yield SimpleNamespace(engine=engine, store=store, reader=reader, inbox=SignalInbox(engine))
    store.close()


@pytest.fixture
async def server(context):
    # Unlike TestServer, production AppRunner does NOT cancel handlers on client
    # disconnect. Our deadline must release read locks without canceling writes.
    runner = web.AppRunner(make_app(context.engine, context.reader))
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    try:
        yield f"http://127.0.0.1:{runner.addresses[0][1]}"
    finally:
        await runner.cleanup()


async def entry_proposal(context):
    post = message(1, "INJ 当前开空")
    receive_message(context.engine, CHANNEL, post, time.time() - 1)
    job = await context.inbox.claim()
    decision = answer(job, symbol="INJ", side="short")
    await context.inbox.propose(job["lease"], decision)
    return post, {"lease": job["lease"], "result": decision.model_dump()}


def assert_no_execution(context):
    assert not context.engine.gate.writes
    assert context.store.db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0


async def test_disconnected_verification_releases_lock_without_executing(context, server):
    post, body = await entry_proposal(context)
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def authority(peer, ids):
        if not entered.is_set():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        return [post]

    context.reader.client.get_messages = authority
    async with ClientSession() as session:
        with pytest.raises(TimeoutError):
            async with session.post(
                server + "/signals/decisions", headers=HEADERS, json=body,
                timeout=ClientTimeout(total=0.02),
            ) as response:
                await response.read()
        assert entered.is_set() and context.reader.ingress_lock.locked()
        await asyncio.wait_for(cancelled.wait(), 0.5)
        await asyncio.sleep(0)
        assert not context.reader.ingress_lock.locked()
        assert_no_execution(context)
        assert context.store.analysis(body["lease"])["status"] == "analyzing"
        async with session.post(
            server + "/signals/decisions", headers=HEADERS, json=body
        ) as response:
            assert response.status == 200
        assert len(context.engine.gate.orders) == 1
        # A lost response/repeated submission must not duplicate the accepted write.
        async with session.post(
            server + "/signals/decisions", headers=HEADERS, json=body
        ) as response:
            assert response.status == 200
        assert len(context.engine.gate.orders) == 1


async def test_verification_deadline_covers_lock_wait_without_unlocking_owner(context, server):
    _, body = await entry_proposal(context)
    await context.reader.ingress_lock.acquire()
    try:
        async with ClientSession() as session, session.post(
            server + "/signals/decisions", headers=HEADERS, json=body
        ) as response:
            assert response.status == 503
            assert "source verification" in await response.text()
        assert context.reader.ingress_lock.locked()
        assert_no_execution(context)
    finally:
        context.reader.ingress_lock.release()


async def test_source_deadline_and_client_disconnect_do_not_cancel_exchange_write(context, server):
    post, body = await entry_proposal(context)
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = context.engine.gate.request

    async def authority(peer, ids):
        return [post]

    async def exchange_write(method, path, body=None, params=None):
        if method == "POST" and path == "/orders":
            entered.set()
            await release.wait()
            result = await original(method, path, body, params)
            finished.set()
            return result
        return await original(method, path, body, params)

    context.reader.client.get_messages = authority
    context.engine.gate.request = exchange_write
    async with ClientSession() as session:
        try:
            with pytest.raises(TimeoutError):
                async with session.post(
                    server + "/signals/decisions", headers=HEADERS, json=body,
                    timeout=ClientTimeout(total=0.02),
                ) as response:
                    await response.read()
            assert entered.is_set()
            await asyncio.sleep(0.1)  # Beyond the Telegram read deadline.
            assert not context.reader.ingress_lock.locked()
            assert context.store.analysis(body["lease"])["status"] == "applying"
        finally:
            release.set()
        await asyncio.wait_for(finished.wait(), 0.5)
        await asyncio.sleep(0)
    assert len(context.engine.gate.orders) == 1
    assert context.store.analysis(body["lease"])["status"] == "done"


@pytest.mark.parametrize("endpoint", ["/channel/history", "/channel/messages/1"])
async def test_context_read_has_total_server_deadline_and_no_partial_history(
    context, server, endpoint
):
    cancelled = asyncio.Event()

    async def authority(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def history(*args, **kwargs):
        yield message(1)
        await authority()

    context.reader.client.get_messages = authority
    context.reader.client.iter_messages = history
    async with ClientSession() as session, session.get(
        server + endpoint, headers=HEADERS
    ) as response:
        assert response.status == 503
        assert "temporarily unavailable" in await response.text()
    assert cancelled.is_set()
    assert not context.store.queued()
    assert_no_execution(context)


async def test_timed_out_edited_push_releases_lock_and_retries_latest_source(context, monkeypatch):
    old = message(1, "INJ 当前开空")
    old.edit_date = datetime.datetime.now(datetime.UTC)
    final = message(1, "取消，不开了")
    final.date, final.edit_date = old.date, old.edit_date
    entered, cancelled, retry = asyncio.Event(), asyncio.Event(), asyncio.Event()
    delays = []

    async def authority(peer, ids):
        if not entered.is_set():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        return final

    async def sleep(seconds):
        delays.append(seconds)
        assert not context.reader.ingress_lock.locked()
        await retry.wait()

    context.reader.client.get_messages = authority
    monkeypatch.setattr(asyncio, "sleep", sleep)
    task = asyncio.create_task(receive_live_message(
        context.reader, context.reader.client, context.reader.peer, context.engine, CHANNEL,
        old, time.time() - 1, context.reader.ingress_lock, edited=True,
    ))
    try:
        await asyncio.wait_for(cancelled.wait(), 0.5)
        assert not context.reader.ingress_lock.locked()
        # Other messages can enter while this one waits for a read-only retry.
        await receive_current_message(
            context.reader.client, context.reader.peer, context.engine, CHANNEL,
            message(2, "后续消息"), time.time() - 1, context.reader.ingress_lock,
        )
        retry.set()
        await asyncio.wait_for(task, 0.5)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert delays == [3]
    job = await context.inbox.claim()
    assert {row["text"] for row in job["events"]} == {"取消，不开了", "后续消息"}
    assert_no_execution(context)


async def test_push_retry_stops_when_connection_is_replaced(context, monkeypatch):
    post = message(1)
    post.edit_date = datetime.datetime.now(datetime.UTC)
    calls = []

    async def authority(*args, **kwargs):
        calls.append(1)
        raise TimeoutError()

    async def sleep(seconds):
        context.reader.client = None

    client = context.reader.client
    client.get_messages = authority
    monkeypatch.setattr(asyncio, "sleep", sleep)
    await receive_live_message(
        context.reader, client, context.reader.peer, context.engine, CHANNEL,
        post, time.time() - 1, context.reader.ingress_lock, edited=True,
    )
    assert calls == [1]
    assert not context.reader.ingress_lock.locked()
    assert not context.store.queued()


async def test_cancelled_edit_does_not_retry_or_leave_a_lock(context):
    post = message(1)
    post.edit_date = datetime.datetime.now(datetime.UTC)
    entered = asyncio.Event()

    async def authority(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    context.reader.client.get_messages = authority
    task = asyncio.create_task(receive_live_message(
        context.reader, context.reader.client, context.reader.peer, context.engine, CHANNEL,
        post, time.time() - 1, context.reader.ingress_lock, edited=True,
    ))
    await asyncio.wait_for(entered.wait(), 0.5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not context.reader.ingress_lock.locked()
    assert not context.store.queued()


@pytest.mark.parametrize("phase", ["iterator", "edited_read", "lock_wait"])
async def test_history_timeout_never_skips_failed_id_and_resumes_committed_prefix(context, phase):
    posts = {i: message(i, f"message {i}") for i in (101, 102, 103)}
    if phase == "edited_read":
        posts[102].edit_date = datetime.datetime.now(datetime.UTC)
    cursors, first = [], [True]
    context.store.set(f"telegram_cursor:{CHANNEL}", 100)
    held = False

    async def authority(peer, ids):
        if first[0]:
            await asyncio.Event().wait()
        return posts[ids]

    async def history(peer, **kwargs):
        nonlocal held
        cursors.append(kwargs["min_id"])
        for message_id, post in posts.items():
            if message_id <= kwargs["min_id"]:
                continue
            if first[0] and message_id == 102:
                if phase == "iterator":
                    await asyncio.Event().wait()
                elif phase == "lock_wait":
                    await context.reader.ingress_lock.acquire()
                    held = True
            yield post

    context.reader.client.get_messages = authority
    context.reader.client.iter_messages = history
    try:
        with pytest.raises(TimeoutError):
            await collect_new_messages(
                context.reader.client, context.reader.peer, 100, context.engine, CHANNEL,
                time.time() - 1, context.reader.ingress_lock,
            )
        assert context.store.get(f"telegram_cursor:{CHANNEL}") == 101
        assert [r["message"] for r in context.store.queued()] == [101]
    finally:
        if held:
            context.reader.ingress_lock.release()
    # A later push must not let the failed 102 disappear behind the cursor.
    receive_message(context.engine, CHANNEL, posts[103], time.time() - 1)
    assert context.store.get(f"telegram_cursor:{CHANNEL}") == 101
    first[0] = False
    result = await collect_new_messages(
        context.reader.client, context.reader.peer, 100, context.engine, CHANNEL,
        time.time() - 1, context.reader.ingress_lock,
    )
    assert cursors == [100, 101]  # Caller still has 100 after a failed invocation.
    assert result == context.store.get(f"telegram_cursor:{CHANNEL}") == 103
    assert {r["message"] for r in context.store.queued()} == {101, 102, 103}
    assert context.store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 3
    assert_no_execution(context)


async def test_recovery_batch_timeout_preserves_pending_messages_for_retry(context):
    post = message(1, "INJ 当前开空")
    receive_message(context.engine, CHANNEL, post, time.time() - 1)
    first = [True]

    async def authority(peer, ids):
        if first[0]:
            await asyncio.Event().wait()
        return [post]

    context.reader.client.get_messages = authority
    with pytest.raises(TimeoutError):
        await refresh_known_messages(
            context.reader.client, context.reader.peer, context.engine, CHANNEL,
            time.time() - 1, context.reader.ingress_lock,
        )
    assert len(context.store.queued()) == 1
    assert not context.reader.ingress_lock.locked()
    first[0] = False
    await refresh_known_messages(
        context.reader.client, context.reader.peer, context.engine, CHANNEL,
        time.time() - 1, context.reader.ingress_lock,
    )
    assert len(context.store.queued()) == 1
    assert_no_execution(context)
