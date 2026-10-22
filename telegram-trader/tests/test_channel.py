import datetime
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer

from trader.app import make_app
from trader.channel import ChannelReader, describe_message
from trader.engine import Engine
from trader.store import Store


def message(message_id, text="full source text", photo=False):
    return SimpleNamespace(
        id=message_id,
        message=text,
        reply_to_msg_id=message_id - 1 if message_id > 1 else None,
        date=datetime.datetime.now(datetime.UTC),
        edit_date=None,
        grouped_id=None,
        media=photo,
        photo=SimpleNamespace(id=1234) if photo else None,
        document=None,
        file=SimpleNamespace(mime_type="image/png", size=3) if photo else None,
    )


class TelegramFixture:
    def __init__(self):
        self.calls = []
        self.messages = {
            i: message(i, "A" * 5000 if i == 2 else "text", photo=i == 3) for i in range(1, 4)
        }

    async def get_messages(self, peer, ids):
        self.calls.append(("get", peer, ids))
        return self.messages.get(ids)

    async def iter_messages(self, peer, limit, offset_id, search):
        self.calls.append(("history", peer, limit, offset_id, search))
        rows = [
            m
            for i, m in sorted(self.messages.items(), reverse=True)
            if (not offset_id or i < offset_id) and (not search or search in m.message)
        ]
        for m in rows[:limit]:
            yield m

    async def download_media(self, msg, file):
        self.calls.append(("download", msg.id))
        assert file is bytes
        return b"png"


@pytest.fixture
def reader():
    channel = ChannelReader()
    channel.client, channel.peer = TelegramFixture(), "allowlisted-channel"
    return channel


async def test_history_paginates_without_truncating_or_queueing(reader):
    result = await reader.history(limit=2)
    assert len(result["messages"][1]["text"]) == 5000
    assert result["next_before_message_id"] == 2
    next_page = await reader.history(before_message_id=2)
    assert [m["message_id"] for m in next_page["messages"]] == [1]
    assert all(m["context_only"] for m in result["messages"])
    assert all(call[1] == "allowlisted-channel" for call in reader.client.calls)


async def test_message_and_original_image(reader):
    result = await reader.read(3)
    assert result["reply_to_message_id"] == 2
    assert result["media"]["id"] == "1234"
    assert await reader.image(3) == (b"png", "image/png")
    with pytest.raises(ValueError, match="no supported"):
        await reader.image(1)
    with pytest.raises(ValueError, match="unavailable"):
        await reader.read(99)


async def test_disconnected_context_is_error_not_empty_history():
    with pytest.raises(ValueError, match="disconnected"):
        await ChannelReader().history()


async def test_context_api_requires_auth_and_never_replays_history(reader, monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "context-test-only")
    store = Store(":memory:")
    engine = Engine(store, None)
    try:
        async with TestClient(TestServer(make_app(engine, reader))) as client:
            assert (await client.get("/channel/history")).status == 401
            headers = {"Authorization": "Bearer context-test-only"}
            response = await client.get("/channel/history", headers=headers)
            assert len((await response.json())["messages"]) == 3
            image = await client.get("/channel/messages/3/image", headers=headers)
            assert await image.read() == b"png"
            assert (await client.post("/channel/messages/3", headers=headers)).status == 405
            assert not store.queued() and not store.trades()
    finally:
        store.close()


def test_media_change_creates_new_event_revision():
    store = Store(":memory:")
    try:
        first = describe_message(message(3, photo=True))
        kwargs = {
            "channel": 1,
            "message": 3,
            "text": "same caption",
            "reply": None,
            "source_time": first["source_time"],
            "live": True,
            "media": first["media"],
        }
        assert store.event(**kwargs)
        assert store.event(**kwargs) is None
        kwargs["media"] = dict(first["media"], id="different-image")
        assert store.event(**kwargs)
    finally:
        store.close()
