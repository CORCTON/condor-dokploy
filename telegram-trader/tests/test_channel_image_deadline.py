"""Image deadlines include lock wait and release work after a client disconnect."""

import asyncio

import pytest
from aiohttp import ClientSession, web
from test_channel import reader as reader  # noqa: PLC0414 -- shared source fixture

from trader import channel as channel_module
from trader.app import make_app
from trader.engine import Engine
from trader.store import Store


@pytest.fixture
async def image_server(reader, monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "image-deadline-only")
    store = Store(":memory:")
    # TestServer turns handler_cancellation on, unlike production's AppRunner.
    # Use the real production setting so a disconnected client does not hide a
    # stuck Telegram download that still owns the image lock on the server.
    runner = web.AppRunner(make_app(Engine(store, None), reader))
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    try:
        yield f"http://127.0.0.1:{runner.addresses[0][1]}/channel/messages/3/image"
    finally:
        await runner.cleanup()
        store.close()


async def test_disconnected_client_does_not_leave_image_lock_owned_forever(
    reader, image_server, monkeypatch
):
    monkeypatch.setattr(channel_module, "IMAGE_TIMEOUT_SECONDS", 0.3)
    entered, cancelled = asyncio.Event(), asyncio.Event()
    download = reader.client.download_media

    async def first_download_hangs(*args, **kwargs):
        if not entered.is_set():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        return await download(*args, **kwargs)

    monkeypatch.setattr(reader.client, "download_media", first_download_hangs)
    headers = {"Authorization": "Bearer image-deadline-only"}
    async with ClientSession() as session:
        async def request():
            async with session.get(image_server, headers=headers) as response:
                await response.read()

        # Disconnect only after the server owns the lock. A 20 ms client timeout
        # could expire during connection setup on a busy test runner instead.
        pending = asyncio.create_task(request())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            assert reader.image_lock.locked()
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await asyncio.wait_for(cancelled.wait(), 1)
        await asyncio.sleep(0)
        assert not reader.image_lock.locked()
        async with session.get(image_server, headers=headers) as response:
            assert response.status == 200
            assert await response.read() == b"png"
            assert response.headers["Content-Type"] == "image/png"


async def test_deadline_covers_waiting_for_another_image_without_releasing_its_lock(
    reader, monkeypatch
):
    monkeypatch.setattr(channel_module, "IMAGE_TIMEOUT_SECONDS", 0.01)
    await reader.image_lock.acquire()
    try:
        with pytest.raises(TimeoutError):
            await reader.image(3)
        assert reader.image_lock.locked()
        assert not reader.client.calls  # The waiter never reached Telegram.
    finally:
        reader.image_lock.release()
    assert await reader.image(3) == (b"png", "image/png")


@pytest.mark.parametrize("phase", ["authority", "download"])
async def test_server_returns_safe_retryable_error_for_each_blocking_phase(
    reader, image_server, monkeypatch, phase
):
    monkeypatch.setattr(channel_module, "IMAGE_TIMEOUT_SECONDS", 0.01)
    cancelled = asyncio.Event()

    async def hanging(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    name = "get_messages" if phase == "authority" else "download_media"
    original = getattr(reader.client, name)
    monkeypatch.setattr(reader.client, name, hanging)
    async with ClientSession() as session, session.get(
        image_server, headers={"Authorization": "Bearer image-deadline-only"}
    ) as response:
        assert response.status == 503
        assert await response.text() == "channel image temporarily unavailable"
    assert cancelled.is_set() and not reader.image_lock.locked()
    monkeypatch.setattr(reader.client, name, original)
    assert await reader.image(3) == (b"png", "image/png")


async def test_deadline_does_not_weaken_mime_size_or_complete_image_checks(reader, monkeypatch):
    source = reader.client.messages[3]
    source.file.mime_type = "application/octet-stream"
    with pytest.raises(ValueError, match="supported image"):
        await reader.image(3)
    source.file.mime_type = "image/png"
    source.file.size = channel_module.MAX_IMAGE_BYTES + 1
    with pytest.raises(ValueError, match="8 MiB"):
        await reader.image(3)
    source.file.size = channel_module.MAX_IMAGE_BYTES
    payload = b"x" * channel_module.MAX_IMAGE_BYTES

    async def full_image(*args, **kwargs):
        return payload

    monkeypatch.setattr(reader.client, "download_media", full_image)
    assert await reader.image(3) == (payload, "image/png")
    assert not reader.image_lock.locked()
