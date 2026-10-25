"""Durable model inbox. No natural-language interpretation or fallback parser."""

import json
import time
import uuid

from .engine import Reanalyze, Review
from .models import Decisions


class SignalInbox:
    def __init__(self, engine):
        self.engine, self.store = engine, engine.store

    def trade_state(self):
        fields = ("id", "version", "status", "filled", "position_size", "entry_terminal")
        return json.dumps(
            [{k: t.get(k) for k in fields} for t in sorted(self.engine.active(), key=lambda t: t["id"])],
            sort_keys=True,
        )

    def fresh(self, lease):
        job = self.store.analysis(lease)
        return bool(
            job
            and self.engine.listener_ready
            and job["status"] == "analyzing"
            and job["expires"] > time.time()
            and job["watermark"] == self.store.watermark(job["channel"])
            and job["trade_state"] == self.trade_state()
            and all(
                self.store.db.execute("SELECT status FROM events WHERE id=?", (event_id,))
                .fetchone()[0] == "analyzing"
                for event_id in json.loads(job["event_ids"])
            )
        )

    async def claim(self):
        async with self.engine.lock:
            if not self.engine.listener_ready:
                return None
            for old in self.store.db.execute(
                "SELECT id FROM analyses WHERE status='analyzing'"
            ).fetchall():
                if self.fresh(old["id"]):
                    return None
                self.store.release_analysis(old["id"])
            rows = self.store.queued()
            if not rows:
                with self.store.db:
                    self.store.db.execute("UPDATE events SET status='queued' WHERE status='deferred'")
                rows = self.store.queued()
            if not rows:
                return None
            channel = rows[0]["channel"]
            rows = [row for row in rows if row["channel"] == channel][:50]
            lease = uuid.uuid4().hex
            ids = [r["id"] for r in rows]
            watermark = self.store.watermark(channel)
            contextual = [
                r for r in rows
                if any(json.loads(r["payload"]).get(k) for k in ("backlog_deferred", "context_recovered"))
            ]
            context = []
            if contextual:
                earliest = min(r["message"] for r in contextual)
                context = [
                    {
                        "message_id": r["message"],
                        **json.loads(r["payload"]),
                        "context_only": True,
                    }
                    for r in self.store.db.execute(
                        "SELECT e.* FROM events e WHERE channel=? AND message>=? "
                        "AND rowid<=? AND status<>'superseded' "
                        "AND NOT EXISTS (SELECT 1 FROM events n WHERE n.channel=e.channel "
                        "AND n.message=e.message AND n.rowid>e.rowid) ORDER BY received,rowid",
                        (channel, earliest, watermark),
                    ).fetchall()
                    if r["id"] not in ids
                ]
            with self.store.db:
                self.store.db.execute(
                    "INSERT INTO analyses VALUES (?,?,?,?,?,'analyzing',?,NULL)",
                    (
                        lease,
                        channel,
                        watermark,
                        json.dumps(ids),
                        self.trade_state(),
                        time.time() + 180,
                    ),
                )
                for event_id in ids:
                    self.store.finish(event_id, "analyzing")
            return {
                "lease": lease,
                "channel_id": channel,
                "events": [
                    {
                        "event_id": r["id"],
                        "message_id": r["message"],
                        "reply_to_message_id": json.loads(r["payload"])["reply"],
                        **json.loads(r["payload"]),
                    }
                    for r in rows
                ],
                "trades": self.engine.active(),
                "execution_enabled": self.engine.enabled,
                "new_entries_paused": self.engine.paused(),
                "context_events": context,
                "deferred_after_message_ids": [r["message"] for r in contextual],
                "more_queued": len([r for r in self.store.queued() if r["channel"] == channel]) > 0,
                "output_schema": Decisions.model_json_schema(),
            }

    async def apply(self, lease, decisions):
        async with self.engine.lock:
            job = self.store.analysis(lease)
            if job and job["status"] == "done":
                if job["result"] != decisions.model_dump_json():
                    raise Review("completed lease cannot be changed")
                return
            if not self.fresh(lease):
                self.store.release_analysis(lease)
                raise Review("stale signal analysis; read the new messages/state first")
            expected = self.validate(job, decisions)
            if job["result"] != decisions.model_dump_json():
                raise Review("execution must match the accepted proposal")
            with self.store.db:
                self.store.db.execute(
                    "UPDATE analyses SET status='applying',result=? WHERE id=?",
                    (decisions.model_dump_json(), lease),
                )
            mapping = {d.event_id: d for d in decisions.decisions}
            for event_id in expected:
                event = self.store.db.execute(
                    "SELECT * FROM events WHERE id=?", (event_id,)
                ).fetchone()
                if mapping[event_id].action == "open" and self.store.queued():
                    payload = json.loads(event["payload"])
                    payload["backlog_deferred"] = True
                    with self.store.db:
                        self.store.db.execute(
                            "UPDATE events SET status='deferred',payload=? "
                            "WHERE id=? AND status='analyzing'",
                            (json.dumps(payload), event_id),
                        )
                    continue
                try:
                    await self.engine.execute_decision(event, mapping[event_id], job["watermark"])
                except Reanalyze:
                    self.store.release_analysis(lease)
                    return
                except Exception:
                    self.engine.pause("unexpected model decision execution failure")
                    self.store.finish(
                        event_id, "review", "execution failed; inspect durable intents"
                    )
                    raise
            with self.store.db:
                self.store.db.execute("UPDATE analyses SET status='done' WHERE id=?", (lease,))

    def validate(self, job, decisions):
        expected = json.loads(job["event_ids"])
        actual = [d.event_id for d in decisions.decisions]
        if len(actual) != len(set(actual)) or set(actual) != set(expected):
            raise Review("model must decide exactly once for every claimed event")
        active = {t["id"]: t for t in self.engine.active()}
        for decision in decisions.decisions:
            if decision.action not in {"open", "ignore", "review"}:
                trade = active.get(decision.target_trade_id)
                if not trade or trade["channel"] != job["channel"]:
                    raise Review("management requires an exact owned trade from this channel")
                if decision.symbol and decision.symbol + "_USDT" != trade["symbol"]:
                    raise Review("symbol and target trade disagree")
        return expected

    async def propose(self, lease, decisions):
        async with self.engine.lock:
            if not self.fresh(lease):
                raise Review("stale analysis: no proposal accepted")
            self.validate(self.store.analysis(lease), decisions)
            existing = self.store.analysis(lease)["result"]
            if existing is not None and existing != decisions.model_dump_json():
                raise Review("accepted proposal cannot be changed")
            with self.store.db:
                self.store.db.execute(
                    "UPDATE analyses SET result=? WHERE id=?", (decisions.model_dump_json(), lease)
                )

    def proposal(self, lease):
        if not self.fresh(lease):
            raise Review("stale analysis: no executable proposal")
        raw = self.store.analysis(lease)["result"]
        return json.loads(raw) if raw else None

    def failed(self, lease, error):
        if not self.fresh(lease):
            self.store.release_analysis(lease)
            return
        # Model errors never authorize a regex fallback or an exchange write.
        self.store.release_analysis(lease, "failed")
        if self.store.get("signal_error") != error:
            self.store.set("signal_error", error)
            self.store.notice(
                f"Channel interpretation unavailable: {error}. No fallback execution."
            )
