"""No approval queue; transient reads must not become permanent operator pauses.

Only local SQLite, the real service and a memory exchange are used here.
"""

import asyncio
import time
from types import SimpleNamespace

import pytest
from aiohttp import ClientError, ServerDisconnectedError
from aiohttp.test_utils import TestClient, TestServer
from test_comprehensive_execution import decision, entry
from test_comprehensive_execution import strict_rig as strict_rig  # noqa: PLC0414
from test_signals import answer, apply, receive
from test_signals import inbox as inbox  # noqa: PLC0414
from test_take_profit_policy import apply_current_plan

from trader import app as app_module
from trader.app import make_app, reconcile
from trader.engine import Engine, Review
from trader.gate import GateError
from trader.models import Decision, Decisions


def test_new_tool_schema_has_no_review_action_and_old_worker_fails_to_ignore():
    assert "review" not in Decision.model_json_schema()["properties"]["action"]["enum"]
    signal = Decision(event_id="old-worker", action="review", reason="uncertain reference",
                      evidence_message_ids=[1])
    assert signal.action == "ignore"


async def test_uncertain_message_does_not_block_independent_clear_entry_or_create_review(inbox):
    receive(inbox, "JTO 右侧布局，等等看")
    receive(inbox, "INJ 当前开空", message=2)
    job = await inbox.claim()
    values = answer(job, symbol="INJ", side="short").model_dump()
    values["decisions"][0].update(action="ignore", symbol=None, side=None,
                                  reason="No unconditional entry instruction")
    result = Decisions.model_validate(values)
    await apply(inbox, job["lease"], result)
    rows = inbox.store.db.execute("SELECT status,detail FROM events ORDER BY rowid").fetchall()
    assert [row["status"] for row in rows] == ["ignore", "done"]
    assert rows[0]["detail"] == "No unconditional entry instruction"
    assert len(inbox.engine.gate.orders) == 1
    assert not inbox.engine.paused()
    assert await inbox.claim() is None
    assert not any("review" in row[0].lower() for row in inbox.store.db.execute("SELECT text FROM notices"))


@pytest.mark.parametrize("error", [
    GateError(502, "INVALID_JSON_RESPONSE"), GateError(503, "SERVICE_UNAVAILABLE"),
    GateError(429, "TOO_MANY_REQUESTS"), TimeoutError(), OSError(), ServerDisconnectedError(),
])
async def test_read_outage_recovers_without_start_or_historic_signal_replay(strict_rig, monkeypatch, error):
    store, exchange, engine = strict_rig
    trade = await entry(strict_rig)
    await apply_current_plan(strict_rig)
    exchange.add_contract("JTO_USDT")
    store.set("entries_started_after", time.time() - 20)
    boundary = store.get("entries_started_after")
    read = exchange.get
    before = len(exchange.writes)

    async def unavailable(path, **params):
        if path == f"/orders/{trade['entry_id']}":
            raise error
        return await read(path, **params)

    monkeypatch.setattr(exchange, "get", unavailable)
    for _ in range(3):
        await engine.poll()
        assert engine.paused() and store.get("paused") is False
        assert store.trade(trade["id"])["last_error"]
    assert len(exchange.writes) == before and not store.unknowns()
    missed = await decision(strict_rig, "open", symbol="JTO", side="long")
    assert missed["status"] == "error" and "new entry blocked" in missed["detail"]
    assert len(exchange.writes) == before

    monkeypatch.setattr(exchange, "get", read)
    await engine.poll()
    assert not engine.paused() and not engine.pause_reason()
    assert not store.trade(trade["id"]).get("last_error")
    assert store.get("entries_started_after") == boundary
    assert len(exchange.writes) == before  # recovery never replays the missed opening
    assert (await decision(strict_rig, "open", symbol="JTO", side="long"))["status"] == "done"


async def test_operator_pause_survives_read_recovery_and_restart(strict_rig, monkeypatch):
    store, exchange, engine = strict_rig
    trade = await entry(strict_rig)
    read = exchange.get

    async def unavailable(path, **params):
        if path == f"/orders/{trade['entry_id']}":
            raise ClientError()
        return await read(path, **params)

    monkeypatch.setattr(exchange, "get", unavailable)
    await engine.poll()
    engine.pause("operator requested pause")
    monkeypatch.setattr(exchange, "get", read)
    await engine.poll()
    assert not engine.entry_blockers()
    assert engine.paused() and engine.pause_reason() == "operator requested pause"
    assert Engine(store, exchange, enabled=True).paused()


async def test_preflight_client_failure_never_creates_intent_or_persistent_pause(inbox, monkeypatch):
    read = inbox.engine.gate.get

    async def unavailable(path, **params):
        if path == "/accounts":
            raise ServerDisconnectedError()
        return await read(path, **params)

    monkeypatch.setattr(inbox.engine.gate, "get", unavailable)
    receive(inbox, "INJ 当前开空")
    job = await inbox.claim()
    await apply(inbox, job["lease"], answer(job, symbol="INJ", side="short"))
    assert not inbox.engine.paused()
    assert not inbox.engine.gate.writes and not inbox.store.unknowns()
    assert inbox.store.db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0
    assert inbox.store.db.execute("SELECT status FROM events").fetchone()[0] == "error"
    assert await inbox.claim() is None


async def test_protection_failure_cannot_be_masked_by_temporary_read_outage(strict_rig, monkeypatch):
    store, exchange, engine = strict_rig
    trade = await entry(strict_rig)
    trade = await apply_current_plan(strict_rig)
    exchange.stops[trade["exits"]["sl"]["id"]].update(status="finished", finish_as="cancelled")
    read = exchange.get

    async def unavailable(path, **params):
        if path == f"/orders/{trade['entry_id']}":
            raise GateError(502, "INVALID_JSON_RESPONSE")
        return await read(path, **params)

    monkeypatch.setattr(exchange, "get", unavailable)
    before = len(exchange.writes)
    await engine.poll()
    assert store.get("paused") is True
    assert "native stop no longer active" in engine.pause_reason()
    assert len(exchange.writes) == before
    engine.pause("later generic reconciliation failure")
    assert "native stop no longer active" in engine.pause_reason()


async def test_unknown_write_stays_blocked_even_after_successful_position_read(strict_rig):
    store, _exchange, engine = strict_rig
    trade = await entry(strict_rig)
    store.intent("unknown-amend", trade["id"], "PUT", "/orders/unknown", {"body": {}})
    engine.pause("exchange write outcome unknown")
    await engine.poll()
    assert store.unknowns() and engine.paused()
    assert engine.pause_reason() == "exchange write outcome unknown"
    assert not store.trade(trade["id"]).get("last_error")


@pytest.mark.parametrize("error,persistent", [(TimeoutError(), False), (Review("invariant broken"), True)])
async def test_outer_reconcile_recovery_respects_fault_type(strict_rig, monkeypatch, error, persistent):
    store, exchange, engine = strict_rig
    exchange.ready = True
    steps = 0

    async def poll():
        nonlocal steps
        steps += 1
        if steps == 1:
            raise error

    async def sleep(_):
        if steps == 1:
            assert engine.paused() and store.get("paused") is persistent
        else:
            raise asyncio.CancelledError()

    monkeypatch.setattr(engine, "poll", poll)
    monkeypatch.setattr(app_module, "asyncio", SimpleNamespace(sleep=sleep))
    with pytest.raises(asyncio.CancelledError):
        await reconcile(engine)
    assert steps == 2 and not engine._entry_blockers
    assert engine.paused() is persistent


async def test_status_explains_ignored_and_failed_events_without_approval_api(inbox, monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "offline-only")
    ignored = receive(inbox, "含糊消息")
    inbox.store.finish(ignored, "ignore", "no clear current instruction")
    failed = receive(inbox, "历史执行失败", message=2)
    inbox.store.finish(failed, "error", "entry expired")
    async with TestClient(TestServer(make_app(inbox.engine))) as client:
        headers = {"Authorization": "Bearer offline-only"}
        body = await (await client.get("/status", headers=headers)).json()
        assert "last_review" not in body
        assert body["last_ignored"]["message_id"] == 1
        assert body["last_issue"]["message_id"] == 2
        assert not body["pause_latched"] and not body["temporary_entry_blocks"]
        assert (await client.post("/approve", headers=headers)).status == 404
    assert not inbox.engine.gate.writes


async def test_listener_reconnect_clears_only_temporary_disconnect(inbox, monkeypatch):
    import telethon

    engine = inbox.engine
    engine.store.set("telegram_cursor:-100123", 0)
    monkeypatch.setenv("TG_CHANNEL_ID", "-100123")
    monkeypatch.setenv("TG_API_ID", "123")
    monkeypatch.setenv("TG_API_HASH", "offline-only")
    monkeypatch.setattr(app_module, "missing_config", list)
    monkeypatch.setattr(telethon.utils, "get_peer_id", lambda _: -100123)
    connects = 0
    recovered = asyncio.Event()

    class Client:
        async def connect(self):
            nonlocal connects
            connects += 1
            if connects == 1:
                raise TimeoutError("offline disconnect")
            assert engine.entry_blockers() and not engine.store.get("paused")

        async def is_user_authorized(self):
            return True

        async def get_entity(self, _):
            return object()

        def add_event_handler(self, *args):
            pass

        async def run_until_disconnected(self):
            assert engine.listener_ready and not engine.entry_blockers()
            assert not engine.paused()
            recovered.set()
            await asyncio.Event().wait()

        async def disconnect(self):
            pass

    async def no_op(*args):
        return 0

    async def follow(*args):
        await asyncio.Event().wait()

    async def sleep(_):
        assert connects == 1

    monkeypatch.setattr(telethon, "TelegramClient", lambda *a, **kw: Client())
    monkeypatch.setattr(telethon.events, "NewMessage", lambda **kw: object())
    monkeypatch.setattr(telethon.events, "MessageEdited", lambda **kw: object())
    monkeypatch.setattr(app_module, "refresh_known_messages", no_op)
    monkeypatch.setattr(app_module, "collect_new_messages", no_op)
    monkeypatch.setattr(app_module, "follow_new_messages", follow)
    monkeypatch.setattr(app_module, "asyncio", SimpleNamespace(
        sleep=sleep, create_task=asyncio.create_task, wait=asyncio.wait, gather=asyncio.gather,
        FIRST_COMPLETED=asyncio.FIRST_COMPLETED, CancelledError=asyncio.CancelledError))
    task = asyncio.create_task(app_module.listen(engine, SimpleNamespace(ingress_lock=asyncio.Lock())))
    try:
        await asyncio.wait_for(recovered.wait(), 1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert connects == 2 and engine.store.get("paused") is False
