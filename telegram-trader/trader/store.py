import hashlib
import json
import sqlite3
import time


class Store:
    """One process owns this database; side-effect intents commit BEFORE network writes."""

    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY, channel INTEGER, message INTEGER, revision TEXT,
                received REAL, payload TEXT, status TEXT NOT NULL, detail TEXT);
            CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS operations (
                id TEXT PRIMARY KEY, trade_id TEXT, method TEXT, path TEXT,
                body TEXT, status TEXT, result TEXT, created REAL);
            CREATE TABLE IF NOT EXISTS notices (
                id INTEGER PRIMARY KEY, text TEXT NOT NULL, sent INTEGER DEFAULT 0);
        """)

    def get(self, key, default=None):
        row = self.db.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key, value):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO kv VALUES (?,?)", (key, json.dumps(value)))

    def event(self, channel, message, text, reply, source_time, live, edited=False):
        revision = hashlib.sha256(text.encode()).hexdigest()[:16]
        key = f"{channel}:{message}:{revision}"
        payload = {
            "text": text,
            "reply": reply,
            "source_time": source_time,
            "live": live,
            "edited": edited,
        }
        with self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?)",
                (key, channel, message, revision, time.time(), json.dumps(payload), "queued", ""),
            )
        return key if cursor.rowcount else None

    def finish(self, key, status, detail=""):
        with self.db:
            self.db.execute(
                "UPDATE events SET status=?, detail=? WHERE id=?", (status, detail, key)
            )

    def queued(self):
        return self.db.execute(
            "SELECT * FROM events WHERE status='queued' ORDER BY received"
        ).fetchall()

    def save_trade(self, trade):
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO trades VALUES (?,?)", (trade["id"], json.dumps(trade))
            )

    def trades(self):
        return [json.loads(row[0]) for row in self.db.execute("SELECT data FROM trades")]

    def trade(self, key):
        row = self.db.execute("SELECT data FROM trades WHERE id=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def reply_trade(self, channel, message):
        seen = set()
        while message and message not in seen and len(seen) < 20:
            seen.add(message)
            matches = [
                t for t in self.trades() if t["channel"] == channel and t["message"] == message
            ]
            if matches:
                return matches[0]
            row = self.db.execute(
                "SELECT payload FROM events WHERE channel=? AND message=? "
                "ORDER BY received DESC LIMIT 1",
                (channel, message),
            ).fetchone()
            message = json.loads(row[0]).get("reply") if row else None
        return None

    def operation(self, key):
        row = self.db.execute("SELECT * FROM operations WHERE id=?", (key,)).fetchone()
        return dict(row) if row else None

    def intent(self, key, trade_id, method, path, body):
        with self.db:
            self.db.execute(
                "INSERT INTO operations VALUES (?,?,?,?,?,?,?,?)",
                (key, trade_id, method, path, json.dumps(body), "unknown", "null", time.time()),
            )

    def result(self, key, result, status="confirmed"):
        with self.db:
            self.db.execute(
                "UPDATE operations SET result=?, status=? WHERE id=?",
                (json.dumps(result), status, key),
            )

    def unknowns(self):
        return [dict(r) for r in self.db.execute("SELECT * FROM operations WHERE status='unknown'")]

    def notice(self, text):
        with self.db:
            self.db.execute("INSERT INTO notices(text) VALUES (?)", (text,))

    def close(self):
        self.db.close()
