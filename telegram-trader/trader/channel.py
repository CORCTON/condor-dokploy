"""Read-only access to the single configured Telegram channel. No replay queue writes."""

import asyncio

MAX_IMAGE_BYTES = 8 * 1024 * 1024
IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif"}


def describe_message(message):
    media = None
    file = getattr(message, "file", None)
    if getattr(message, "media", None):
        item = getattr(message, "photo", None) or getattr(message, "document", None)
        media = {
            "id": str(getattr(item, "id", "unsupported")),
            "mime": getattr(file, "mime_type", None),
            "size": getattr(file, "size", None),
            "grouped_id": str(message.grouped_id) if message.grouped_id else None,
        }
    return {
        "message_id": message.id,
        "text": message.message or "",
        "reply_to_message_id": message.reply_to_msg_id,
        "source_time": message.date.timestamp(),
        "edited_at": message.edit_date.timestamp() if message.edit_date else None,
        "media": media,
        "context_only": True,
    }


class ChannelReader:
    def __init__(self):
        self.client, self.peer = None, None
        self.image_lock = asyncio.Lock()
        self.ingress_lock = asyncio.Lock()

    def connected(self):
        if self.client is None or self.peer is None:
            raise ValueError("channel context unavailable: Telegram is disconnected")

    async def message(self, message_id):
        self.connected()
        if message_id <= 0:
            raise ValueError("positive message ID required")
        message = await self.client.get_messages(self.peer, ids=message_id)
        if message is None or not getattr(message, "date", None):
            raise ValueError("channel message deleted or unavailable")
        return message

    async def read(self, message_id):
        return describe_message(await self.message(message_id))

    async def history(self, before_message_id=0, limit=30, query=""):
        self.connected()
        if before_message_id < 0 or not 1 <= limit <= 50 or len(query) > 200:
            raise ValueError("invalid history query")
        messages = [
            describe_message(m)
            async for m in self.client.iter_messages(
                self.peer, limit=limit, offset_id=before_message_id, search=query or None
            )
            if getattr(m, "date", None)
        ]
        return {
            "messages": messages,
            "next_before_message_id": messages[-1]["message_id"]
            if len(messages) == limit
            else None,
            "context_only": True,
        }

    async def image(self, message_id):
        async with self.image_lock:
            message = await self.message(message_id)
            file = getattr(message, "file", None)
            mime = getattr(file, "mime_type", None)
            if mime not in IMAGE_TYPES:
                raise ValueError("this message has no supported image")
            if not file.size or file.size > MAX_IMAGE_BYTES:
                raise ValueError("image exceeds the 8 MiB transport limit; no image was truncated")
            data = await self.client.download_media(message, file=bytes)
            if not data or len(data) > MAX_IMAGE_BYTES:
                raise ValueError("image unavailable or too large")
            return data, mime
