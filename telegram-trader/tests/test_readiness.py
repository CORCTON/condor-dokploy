"""Read-only business health checks; no exchange, Telegram or model traffic."""

import asyncio
import json
import math
import time
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer
from test_trader import Exchange

from trader import app as app_module
from trader.app import business_readiness, collect_new_messages, make_app, reconcile
from trader.credentials import GateCredentials
from trader.engine import Engine
from trader.store import Store


@pytest.fixture
def rig(tmp_path, monkeypatch):
    for key in ("TG_API_ID", "TG_API_HASH", "TG_CHANNEL_ID", "TRADER_TOKEN"):
        monkeypatch.setenv(key, "fixture")
    session = tmp_path / "telegram.session"
    session.touch()
    monkeypatch.setenv("TG_SESSION_PATH", str(session))
    store = Store(tmp_path / "readiness.sqlite3")
    exchange = Exchange()
    exchange.ready = True
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    for worker in ("interpreter", "planner"):
        engine.record_heartbeat(worker)
    engine.last_channel_scan = engine.last_reconcile = time.monotonic()
    credentials = GateCredentials(
        source="env", environment="testnet", key="fixture-key-private", secret="fixture-secret-private"
    )
    yield store, exchange, engine, credentials
    store.close()


async def test_ready_is_authenticated_read_only_and_independent_of_execution_lock(rig, monkeypatch):
    store, exchange, engine, credentials = rig

    async def forbidden(*args, **kwargs):
        pytest.fail("readiness must not probe any external service")

    monkeypatch.setattr(exchange, "get", forbidden)
    monkeypatch.setattr(exchange, "request", forbidden)
    changes = store.db.total_changes
    async with TestClient(TestServer(make_app(engine, credentials=credentials))) as client:
        assert (await client.get("/ready")).status == 401
        assert (await client.get("/health")).status == 200
        async with engine.lock:
            response = await asyncio.wait_for(
                client.get("/ready", headers={"Authorization": "Bearer fixture"}), timeout=1
            )
            body = await response.json()
        assert response.status == 200
        assert body["ready"] and body["state"] == "ready"
        assert body["reasons"] == []
        assert not body["paused"]
        assert all(math.isfinite(value) for value in body["ages_seconds"].values())
        assert "fixture-key-private" not in json.dumps(body)
        assert "fixture-secret-private" not in json.dumps(body)
    assert store.db.total_changes == changes
    assert not exchange.writes


async def test_operator_pause_is_healthy_and_explicit(rig):
    _, _, engine, credentials = rig
    engine.pause("operator requested pause")
    async with TestClient(TestServer(make_app(engine, credentials=credentials))) as client:
        response = await client.get("/ready", headers={"Authorization": "Bearer fixture"})
        body = await response.json()
        assert response.status == 200
        assert body["ready"] and body["paused"]
        assert body["state"] == "ready"


@pytest.mark.parametrize("fault,reason", [
    ("credentials", "configuration_missing"),
    ("configuration", "configuration_missing"),
    ("listener", "listener_disconnected"),
    ("interpreter", "interpreter_stale"),
    ("planner", "planner_stale"),
    ("scan", "channel_scan_stale"),
    ("reconcile", "reconcile_stale"),
    ("unknown", "exchange_write_unconfirmed"),
    ("position", "position_requires_review"),
])
async def test_business_fault_returns_503_without_changing_controls(rig, monkeypatch, fault, reason):
    store, exchange, engine, credentials = rig
    if fault == "credentials":
        exchange.ready = False
    elif fault == "configuration":
        monkeypatch.delenv("TG_API_HASH")
    elif fault == "listener":
        engine.listener_ready = False
    elif fault in {"interpreter", "planner"}:
        engine._worker_heartbeats[fault] = (time.time() - 61, time.monotonic() - 61)
    elif fault == "scan":
        engine.last_channel_scan = time.monotonic() - 61
    elif fault == "reconcile":
        engine.last_reconcile = time.monotonic() - 181
    elif fault == "unknown":
        store.intent("unconfirmed", "test", "POST", "/orders", {})
    elif fault == "position":
        store.save_trade({"id": "test", "status": "working", "last_error": "private details"})
    before = store.db.total_changes
    async with TestClient(TestServer(make_app(engine, credentials=credentials))) as client:
        response = await client.get("/ready", headers={"Authorization": "Bearer fixture"})
        body = await response.json()
        assert response.status == 503
        assert body["state"] == "degraded" and reason in body["reasons"]
        assert not body["ready"]
        assert "private details" not in json.dumps(body)
        assert (await client.get("/health")).status == 200
    assert store.db.total_changes == before
    assert store.get("paused") is False  # observation never pauses or resumes trading


def test_restart_requires_current_process_progress_not_old_database_heartbeats(rig):
    store, exchange, _, credentials = rig
    store.set("planner_seen", time.time())
    store.set("interpreter_seen", time.time())
    restarted = Engine(store, exchange, enabled=True)
    restarted.listener_ready = True
    result = business_readiness(restarted, credentials)
    assert not result["ready"]
    assert all(value is None for value in result["ages_seconds"].values())
    assert set(result["reasons"]) == {
        "interpreter_stale", "planner_stale", "channel_scan_stale", "reconcile_stale"
    }


async def test_disabled_is_explicitly_healthy_even_without_running_components(rig):
    store, exchange, _, credentials = rig
    exchange.ready = False
    disabled = Engine(store, exchange, enabled=False)
    async with TestClient(TestServer(make_app(disabled, credentials=credentials))) as client:
        response = await client.get("/ready", headers={"Authorization": "Bearer fixture"})
        body = await response.json()
        assert response.status == 200
        assert body["state"] == "disabled" and body["ready"]
        assert not body["enabled"] and not body["reasons"]


@pytest.mark.parametrize("timestamp", [float("nan"), float("inf"), -float("inf")])
def test_invalid_progress_age_is_not_serialized_as_nonfinite_json(rig, timestamp):
    _, _, engine, credentials = rig
    engine.last_channel_scan = timestamp
    result = business_readiness(engine, credentials)
    assert result["ages_seconds"]["channel_scan"] is None
    assert "channel_scan_stale" in result["reasons"]
    json.dumps(result, allow_nan=False)


async def test_empty_complete_channel_scan_records_progress_without_database_write(rig):
    store, _, engine, _ = rig

    async def empty(*args, **kwargs):
        if False:
            yield

    engine.last_channel_scan = None
    before = store.db.total_changes
    cursor = await collect_new_messages(
        SimpleNamespace(iter_messages=empty), object(), 123, engine, -100123, time.time()
    )
    assert cursor == 123
    assert 0 <= time.monotonic() - engine.last_channel_scan < 1
    assert store.db.total_changes == before


async def test_incomplete_channel_scan_does_not_refresh_readiness(rig, monkeypatch):
    _, _, engine, _ = rig

    async def stuck(*args, **kwargs):
        await asyncio.Event().wait()
        yield  # asynchronous generator, never reached

    engine.last_channel_scan = previous = time.monotonic() - 61
    monkeypatch.setattr(app_module, "READ_TIMEOUT_SECONDS", 0.01)
    with pytest.raises(TimeoutError):
        await collect_new_messages(
            SimpleNamespace(iter_messages=stuck), object(), 123, engine, -100123, time.time()
        )
    assert engine.last_channel_scan == previous


@pytest.mark.parametrize("fails", [False, True])
async def test_reconcile_timestamp_records_completed_poll_only(rig, monkeypatch, fails):
    _, _, engine, _ = rig
    engine.last_reconcile = previous = time.monotonic() - 181
    reached_sleep = asyncio.Event()

    async def poll():
        if fails:
            raise TimeoutError("test simulated unavailable exchange")

    async def sleep(seconds):
        reached_sleep.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(engine, "poll", poll)
    monkeypatch.setattr(app_module, "asyncio", SimpleNamespace(sleep=sleep))
    task = asyncio.create_task(reconcile(engine))
    try:
        await asyncio.wait_for(reached_sleep.wait(), timeout=1)
        if fails:
            assert engine.last_reconcile == previous
        else:
            assert 0 <= time.monotonic() - engine.last_reconcile < 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
