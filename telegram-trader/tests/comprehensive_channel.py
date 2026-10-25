"""Read-only Telegram snapshot for evaluation; never falls back to a live channel."""

from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

from trader.channel import ChannelReader


class SnapshotChannel(ChannelReader):
    """Expose exactly the supplied, already-retimed message revisions and images."""

    def __init__(self, messages, images):
        super().__init__()
        self._messages, self._images = deepcopy(messages), deepcopy(images)
        if any(key <= 0 or row["message_id"] != key for key, row in self._messages.items()):
            raise ValueError("snapshot keys must match positive message IDs")
        self.client, self.peer = self, "snapshot-channel"
        self.calls = []

    def _message(self, message_id):
        row = self._messages.get(message_id)
        if row is None:
            return None
        media = row["media"]
        return SimpleNamespace(
            id=message_id, message=row["text"], reply_to_msg_id=row["reply_to_message_id"],
            date=datetime.fromtimestamp(row["source_time"], UTC),
            edit_date=datetime.fromtimestamp(row["edited_at"], UTC)
            if row["edited_at"] is not None else None,
            grouped_id=media.get("grouped_id") if media else None,
            media=bool(media), photo=SimpleNamespace(id=media["id"]) if media else None,
            document=None,
            file=SimpleNamespace(mime_type=media["mime"], size=media["size"]) if media else None,
        )

    async def get_messages(self, peer, ids):
        self.calls.append({"op": "authority", "ids": deepcopy(ids)})
        if isinstance(ids, (list, tuple)):
            return [self._message(message_id) for message_id in ids]
        return self._message(ids)

    async def iter_messages(self, peer, *, limit, offset_id, search):
        count = 0
        for message_id, row in sorted(self._messages.items(), reverse=True):
            if offset_id and message_id >= offset_id:
                continue
            if search and search.casefold() not in row["text"].casefold():
                continue
            yield self._message(message_id)
            count += 1
            if count >= limit:
                return

    async def download_media(self, message, file):
        self.calls.append({"op": "download", "message_id": message.id})
        if file is not bytes:
            raise ValueError("snapshot images support only in-memory reads")
        if message.id not in self._messages or message.id not in self._images:
            raise ValueError("image unavailable in this snapshot")
        data, mime = self._images[message.id]
        if mime != message.file.mime_type:
            raise ValueError("snapshot image MIME disagrees with source metadata")
        return data

    async def read(self, message_id):
        self.calls.append({"op": "read", "message_id": message_id})
        return await super().read(message_id)

    async def history(self, before_message_id=0, limit=30, query=""):
        self.calls.append({"op": "history", "before_message_id": before_message_id,
                           "limit": limit, "query": query})
        return await super().history(before_message_id, limit, query)

    async def image(self, message_id):
        self.calls.append({"op": "image", "message_id": message_id})
        return await super().image(message_id)
