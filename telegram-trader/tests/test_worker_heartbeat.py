"""Process-local liveness must not fsync, survive a restart, or cancel trading work."""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestClient, TestServer
from test_exit_planner import planner as planner  # noqa: PLC0414 -- shared module fixture
from test_interpreter import interpreter as interpreter  # noqa: PLC0414 -- shared module fixture
from test_model_runtime import runtime as runtime  # noqa: PLC0414 -- shared module fixture
from test_trader import Exchange

from trader import engine as engine_module
from trader.app import make_app
from trader.engine import Engine, Review
from trader.store import Store

HEADERS = {"Authorization": "Bearer heartbeat-local-test"}


@pytest.fixture
def rig(monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "heartbeat-local-test")
    store, exchange = Store(":memory:"), Exchange()
    exchange.ready = True
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    yield engine
    store.close()


def test_thousand_heartbeats_do_not_touch_sqlite_and_use_monotonic_age(rig, monkeypatch):
    wall, monotonic = [1000.0], [10.0]
    monkeypatch.setattr(engine_module, "time", SimpleNamespace(
        time=lambda: wall[0], monotonic=lambda: monotonic[0],
    ))
    queries = []
    rig.store.db.set_trace_callback(queries.append)
    before = rig.store.db.total_changes
    for _ in range(1000):
        rig.record_heartbeat("interpreter")
        rig.record_heartbeat("planner")
    assert queries == []
    assert rig.store.db.total_changes == before
    assert rig.worker_seen("interpreter") == 1000.0
    wall[0] = -999999.0  # NTP/wall-clock correction cannot make a stale process fresh.
    monotonic[0] = 191.0
    assert rig.worker_age("interpreter") == 181.0
    assert rig.worker_seen("interpreter") == 1000.0


async def test_http_heartbeats_and_idle_claims_never_write_heartbeat_state(rig):
    before = rig.store.db.total_changes
    async with TestClient(TestServer(make_app(rig))) as client:
        assert (await client.post("/workers/interpreter/heartbeat")).status == 401
        assert (await client.post("/workers/unknown/heartbeat", headers=HEADERS)).status == 400
        for _ in range(1000):
            response = await client.post("/workers/interpreter/heartbeat", headers=HEADERS)
            assert response.status == 200
        for endpoint in ("/signals/claim", "/jobs/claim"):
            response = await client.post(endpoint, headers=HEADERS)
            assert await response.json() is None
        response = await client.get("/status", headers=HEADERS)
        status = await response.json()
        assert status["interpreter_seen"] == rig.worker_seen("interpreter")
        assert status["planner_seen"] == rig.worker_seen("planner")
    assert rig.store.db.total_changes == before
    assert rig.store.get("interpreter_seen") is None
    assert rig.store.get("planner_seen") is None
    assert not rig.gate.writes


async def test_restart_ignores_even_recent_legacy_database_heartbeats(rig):
    for name in ("interpreter_seen", "planner_seen"):
        rig.store.set(name, time.time() + 100000)
    assert rig.worker_seen("interpreter") is None
    assert rig.worker_age("planner") is None
    with pytest.raises(Review, match="Routine"):
        await rig.resume()
    rig.record_heartbeat("interpreter")
    rig.record_heartbeat("planner")
    await rig.resume()
    cutoff = rig.store.get("entries_started_after")
    restarted = Engine(rig.store, rig.gate, enabled=True)
    restarted.listener_ready = True
    assert not restarted.paused()  # Durable operator controls remain unchanged.
    assert restarted.store.get("entries_started_after") == cutoff
    with pytest.raises(Review, match="Routine"):
        await restarted.resume()
    assert not rig.gate.writes


async def test_resume_rechecks_heartbeat_age_after_account_wait(rig, monkeypatch):
    monotonic = [10.0]
    monkeypatch.setattr(engine_module, "time", SimpleNamespace(
        time=time.time, monotonic=lambda: monotonic[0],
    ))
    rig.record_heartbeat("interpreter")
    rig.record_heartbeat("planner")

    async def delayed_account(*args, **kwargs):
        monotonic[0] = 191.0
        return rig.gate.account

    rig.gate.get = delayed_account
    with pytest.raises(Review, match="Routine"):
        await rig.resume()
    assert rig.paused() and not rig.gate.writes


def test_trade_intents_and_operator_controls_still_persist_with_full_durability(tmp_path):
    path = tmp_path / "durable.sqlite3"
    store = Store(path)
    engine = Engine(store, Exchange(), enabled=True)
    engine.record_heartbeat("planner")
    engine.pause("operator requested pause")
    store.intent("intent-1", "trade-1", "POST", "/orders", {"size": 1})
    assert store.db.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL
    assert store.db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    store.close()
    restored = Store(path)
    try:
        restarted = Engine(restored, Exchange(), enabled=True)
        assert restarted.paused()
        assert restored.get("pause_reason") == "operator requested pause"
        assert restored.operation("intent-1")["status"] == "unknown"
        assert restarted.worker_seen("planner") is None
    finally:
        restored.close()


def heartbeat_tasks():
    return [task for task in asyncio.all_tasks() if task.get_name().startswith("trader-")
            and task.get_name().endswith("-heartbeat") and not task.done()]


@pytest.mark.parametrize("kind", ["interpreter", "planner"])
async def test_real_worker_heartbeats_during_long_model_turn_and_reaps_on_stop(
    rig, interpreter, planner, monkeypatch, kind
):
    module = interpreter if kind == "interpreter" else planner
    entered, stopped, beat_four = asyncio.Event(), asyncio.Event(), asyncio.Event()
    claims, beats = [], []
    monkeypatch.setenv("TRADER_LLM_READY", "true")
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    heartbeat_globals = module.runtime["send_heartbeats"].__globals__
    monkeypatch.setitem(heartbeat_globals, "HEARTBEAT_INTERVAL_SECONDS", 0.01)
    monkeypatch.setitem(heartbeat_globals, "HEARTBEAT_TIMEOUT_SECONDS", 0.1)

    @asynccontextmanager
    async def model_turn(*args):
        entered.set()
        try:
            await asyncio.Event().wait()
            yield  # Deliberately no executable proposal.
        finally:
            stopped.set()

    async def claim(request):
        claims.append(request.path)
        return web.json_response({"lease": "long-inference"})

    async def heartbeat(request):
        assert request.headers["Authorization"] == HEADERS["Authorization"]
        assert request.match_info["worker"] == kind
        rig.record_heartbeat(kind)
        beats.append(rig.worker_seen(kind))
        if len(beats) >= 4:
            beat_four.set()
        return web.json_response({"recorded": True})

    monkeypatch.setattr(module, "interpret_current" if kind == "interpreter" else "generate_plan",
                        model_turn)
    app = web.Application()
    app.router.add_post("/signals/claim" if kind == "interpreter" else "/jobs/claim", claim)
    app.router.add_post("/workers/{worker}/heartbeat", heartbeat)
    async with TestServer(app) as server:
        monkeypatch.setenv("TELEGRAM_TRADER_URL", str(server.make_url("/")))
        task = asyncio.create_task(module.run_worker())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            await asyncio.wait_for(beat_four.wait(), 1)
            assert len(claims) == 1
            assert rig.worker_age(kind) < 0.1
            assert not rig.gate.writes
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert stopped.is_set() and not heartbeat_tasks()
        count = len(beats)
        await asyncio.sleep(0.04)
        assert len(beats) == count


@pytest.mark.parametrize("ready", [None, "false", "TRUE"])
async def test_unconfigured_worker_never_sends_heartbeat(runtime, monkeypatch, ready, caplog):
    caplog.set_level(logging.INFO)
    if ready is None:
        monkeypatch.delenv("TRADER_LLM_READY", raising=False)
    else:
        monkeypatch.setenv("TRADER_LLM_READY", ready)
    monkeypatch.delenv("TRADER_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_TRADER_URL", raising=False)
    monkeypatch.setattr(runtime, "HEARTBEAT_INTERVAL_SECONDS", 0.01)

    def forbidden(*args, **kwargs):
        pytest.fail("unconfigured worker sent a heartbeat")

    async with runtime.worker_heartbeat(SimpleNamespace(post=forbidden), "planner"):
        await asyncio.sleep(0.04)
    assert not heartbeat_tasks()
    assert sum("state=waiting" in row.message for row in caplog.records) == 1


async def test_heartbeat_timeout_and_errors_recover_with_state_only_logs(runtime, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setenv("TRADER_LLM_READY", "true")
    monkeypatch.setenv("TRADER_TOKEN", "SECRET-heartbeat-token")
    monkeypatch.setattr(runtime, "HEARTBEAT_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(runtime, "HEARTBEAT_TIMEOUT_SECONDS", 0.02)
    calls, recovered = [], asyncio.Event()

    async def heartbeat(request):
        calls.append(1)
        if len(calls) == 1:
            await asyncio.Event().wait()
        if len(calls) <= 3:
            raise web.HTTPServiceUnavailable(text="SECRET-response-detail")
        if len(calls) >= 5:
            recovered.set()
        return web.json_response({"recorded": True})

    app = web.Application()
    app.router.add_post("/workers/planner/heartbeat", heartbeat)
    async with TestServer(app) as server:
        monkeypatch.setenv("TELEGRAM_TRADER_URL", str(server.make_url("/")))
        async with ClientSession() as session, runtime.worker_heartbeat(session, "planner"):
            await asyncio.wait_for(recovered.wait(), 1)
            await asyncio.sleep(0)
    logs = [row.message for row in caplog.records if "Trader worker heartbeat" in row.message]
    assert logs == ["Trader worker heartbeat worker=planner state=unavailable",
                    "Trader worker heartbeat worker=planner state=ok"]
    assert all("SECRET" not in line for line in logs)
    assert not heartbeat_tasks()


async def test_stopping_heartbeat_cancels_inflight_read_only_request(runtime, monkeypatch):
    monkeypatch.setenv("TRADER_LLM_READY", "true")
    monkeypatch.setenv("TRADER_TOKEN", "local-only")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1/")
    entered, cancelled = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def pending(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
            yield
        finally:
            cancelled.set()

    async with runtime.worker_heartbeat(SimpleNamespace(post=pending), "planner"):
        await asyncio.wait_for(entered.wait(), 1)
    assert cancelled.is_set() and not heartbeat_tasks()
