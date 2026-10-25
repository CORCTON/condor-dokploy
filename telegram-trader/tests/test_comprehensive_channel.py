"""Snapshot protocol checks use no Telegram, model provider or exchange connection."""

import asyncio
import datetime
from copy import deepcopy

import pytest
from aiohttp.test_utils import TestClient, TestServer
from comprehensive_channel import SnapshotChannel
from test_channel import message
from test_signals import answer
from test_trader import Exchange

from trader.app import make_app, receive_message
from trader.channel import describe_message
from trader.engine import Engine
from trader.store import Store


def row(message_id, text="完整原文。", *, photo=False, edited=False):
    post = message(message_id, text, photo=photo)
    post.date = datetime.datetime(2026, 10, 25, 3, 2, 1, 123456, tzinfo=datetime.UTC)
    if edited:
        post.edit_date = datetime.datetime(2026, 10, 25, 3, 3, 1, tzinfo=datetime.UTC)
    if photo:
        post.grouped_id = 9876543210123
    return describe_message(post)


@pytest.mark.parametrize("photo,edited", [(False, False), (True, False), (False, True), (True, True)])
async def test_current_revision_roundtrips_without_retiming_or_content_changes(photo, edited):
    original = row(7, "INJ SL 改为 8.750\n当前版本，不是原版。", photo=photo, edited=edited)
    reader = SnapshotChannel({7: original}, {7: (b"png", "image/png")} if photo else {})
    assert reader.client is reader and reader.peer
    assert isinstance(reader.ingress_lock, asyncio.Lock)
    reader.connected()
    assert await reader.read(7) == original
    current = await reader.get_messages(reader.peer, ids=[7])
    assert describe_message(current[0]) == original
    assert (await reader.history())["messages"] == [original]
    assert {call["op"] for call in reader.calls} == {"read", "authority", "history"}


async def test_snapshot_isolated_from_caller_and_return_value_mutations():
    original = row(2, photo=True, edited=True)
    messages, images = {2: deepcopy(original)}, {2: (b"png", "image/png")}
    reader = SnapshotChannel(messages, images)
    messages[2]["text"] = "future edit must not leak into this snapshot"
    messages[2]["media"]["id"] = "changed"
    images[2] = (b"future-image", "image/png")
    fetched = await reader.read(2)
    fetched["text"], fetched["media"]["id"] = "mutated", "mutated"
    raw = await reader.get_messages(reader.peer, ids=2)
    raw.message, raw.photo.id = "mutated", "mutated"
    assert await reader.read(2) == original
    assert await reader.image(2) == (b"png", "image/png")


async def test_missing_and_future_ids_or_images_never_fall_back():
    reader = SnapshotChannel({1: row(1, photo=True), 4: row(4)}, {99: (b"future", "image/png")})
    for message_id in (2, 99):
        with pytest.raises(ValueError, match="unavailable"):
            await reader.read(message_id)
        with pytest.raises(ValueError, match="unavailable"):
            await reader.image(message_id)
    with pytest.raises(ValueError, match="image unavailable in this snapshot"):
        await reader.image(1)
    current = await reader.get_messages(reader.peer, ids=[1, 2, 99])
    assert current[0].id == 1 and current[1:] == [None, None]
    assert [item["message_id"] for item in (await reader.history())["messages"]] == [4, 1]
    assert {call["op"] for call in reader.calls} == {"read", "image", "authority", "download", "history"}


async def test_history_paginates_only_snapshot_and_preserves_full_payloads():
    messages = {i: row(i, f"INJ item {i}\n" + "完整正文" * 2000, photo=i == 3) for i in (1, 3, 8)}
    reader = SnapshotChannel(messages, {})
    first = await reader.history(limit=2, query="inj")
    assert first["messages"] == [messages[8], messages[3]]
    assert first["next_before_message_id"] == 3
    second = await reader.history(before_message_id=3, limit=2, query="INJ")
    assert second["messages"] == [messages[1]]
    assert second["next_before_message_id"] is None
    assert (await reader.history(query="absent"))["messages"] == []
    assert reader.calls[0] == {"op": "history", "before_message_id": 0, "limit": 2, "query": "inj"}


@pytest.mark.parametrize("arguments", [
    {"limit": 0}, {"limit": 51}, {"before_message_id": -1}, {"query": "x" * 201},
])
async def test_history_uses_official_parameter_limits(arguments):
    reader = SnapshotChannel({1: row(1)}, {})
    with pytest.raises(ValueError, match="invalid history query"):
        await reader.history(**arguments)
    assert reader.calls[0]["op"] == "history"


async def test_images_keep_exact_bytes_and_reject_metadata_mismatch():
    source = row(1, photo=True)
    reader = SnapshotChannel({1: source}, {1: (b"png", "image/png")})
    assert await reader.image(1) == (b"png", "image/png")
    assert [call["op"] for call in reader.calls] == ["image", "authority", "download"]
    mismatch = SnapshotChannel({1: source}, {1: (b"jpeg", "image/jpeg")})
    with pytest.raises(ValueError, match="MIME disagrees"):
        await mismatch.image(1)


async def test_real_http_authority_reread_preserves_current_event_and_executes_once(monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "snapshot-local-only")
    headers = {"Authorization": "Bearer snapshot-local-only"}
    post = message(1, "INJ 当前开空", photo=True)
    reader = SnapshotChannel({1: describe_message(post)}, {1: (b"png", "image/png")})
    store = Store(":memory:")
    exchange = Exchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    receive_message(engine, -100123, post, post.date.timestamp() - 1)
    original = dict(store.queued()[0])
    try:
        async with TestClient(TestServer(make_app(engine, reader))) as client:
            job = await (await client.post("/signals/claim", headers=headers)).json()
            body = {"lease": job["lease"],
                    "result": answer(job, symbol="INJ", side="short").model_dump()}
            assert (await client.post("/signals/proposals", headers=headers, json=body)).status == 200
            assert (await client.post("/signals/decisions", headers=headers, json=body)).status == 200
        persisted = store.db.execute("SELECT * FROM events").fetchall()
        assert len(persisted) == 1
        assert persisted[0]["id"] == original["id"]
        assert persisted[0]["payload"] == original["payload"]
        assert persisted[0]["status"] == "done"
        assert len(exchange.orders) == 1
        assert {"op": "authority", "ids": [1]} in reader.calls
    finally:
        store.close()
