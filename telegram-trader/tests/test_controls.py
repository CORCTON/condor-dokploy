import asyncio
import importlib.util
import json
import sys
import time
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer
from test_trader import Exchange, plan, signal

from trader.app import make_app
from trader.credentials import GateCredentials
from trader.engine import Engine, Review
from trader.store import Store


@pytest.fixture
def controlled(tmp_path, monkeypatch):
    for key in ("TG_API_ID", "TG_API_HASH", "TG_CHANNEL_ID", "TRADER_TOKEN"):
        monkeypatch.setenv(key, "fixture")
    session = tmp_path / "telegram.session"
    session.touch()
    monkeypatch.setenv("TG_SESSION_PATH", str(session))
    monkeypatch.setenv("ADMIN_USER_ID", "123")
    store = Store(tmp_path / "trader.sqlite3")
    exchange = Exchange()
    exchange.ready = True
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    engine.pause("operator requested pause")
    for key in ("interpreter_seen", "planner_seen"):
        store.set(key, time.time())
    yield store, exchange, engine
    store.close()


def ready_credentials():
    return GateCredentials(source="env", environment="testnet", key="fixture", secret="fixture")


async def test_start_is_read_only_idempotent_and_pause_survives_restart(controlled):
    store, exchange, engine = controlled
    await engine.resume()
    cutoff = store.get("entries_started_after")
    assert not engine.paused() and cutoff
    await engine.resume()
    assert store.get("entries_started_after") == cutoff
    assert not exchange.writes
    engine.pause("operator requested pause")
    restarted = Engine(store, exchange, enabled=True)
    assert restarted.paused() and not exchange.writes


@pytest.mark.parametrize(
    "blocker",
    [
        "disabled",
        "credentials",
        "listener",
        "unknown",
        "planner",
        "interpreter",
        "account",
        "auth",
        "trade_error",
    ],
)
async def test_start_fails_closed(controlled, blocker):
    store, exchange, engine = controlled
    if blocker == "disabled":
        engine.enabled = False
    elif blocker == "credentials":
        exchange.ready = False
    elif blocker == "listener":
        engine.listener_ready = False
    elif blocker == "unknown":
        store.intent("uncertain", "trade", "POST", "/orders", {})
    elif blocker in {"planner", "interpreter"}:
        store.set(blocker + "_seen", time.time() - 181)
    elif blocker == "account":
        exchange.account["margin_mode"] = 1
    elif blocker == "auth":

        async def denied(*args, **kwargs):
            raise TimeoutError("fixture exchange unavailable")

        exchange.get = denied
    elif blocker == "trade_error":
        store.save_trade({"id": "test", "status": "working", "last_error": "needs review"})
    with pytest.raises((Review, TimeoutError)):
        await engine.resume()
    assert engine.paused() and not exchange.writes


async def test_pause_wins_over_in_flight_start(controlled):
    _, exchange, engine = controlled
    entered, release = asyncio.Event(), asyncio.Event()

    async def account(*args, **kwargs):
        entered.set()
        await release.wait()
        return exchange.account

    exchange.get = account
    task = asyncio.create_task(engine.resume())
    await entered.wait()
    engine.pause("newer operator pause")
    release.set()
    with pytest.raises(Review, match="暂停指令"):
        await task
    assert engine.paused() and not exchange.writes


async def test_newer_pause_also_wins_over_start_waiting_for_engine_lock(controlled):
    _, exchange, engine = controlled
    async with engine.lock:
        task = asyncio.create_task(engine.resume())
        await asyncio.sleep(0)
        engine.pause("newer operator pause")
    with pytest.raises(Review, match="暂停指令"):
        await task
    assert engine.paused() and not exchange.writes


async def test_old_signal_not_replayed_but_new_signal_executes_in_simulator(controlled):
    store, exchange, engine = controlled
    key = store.event(-100123, 1, "#INJ 当前价格入场一笔空单", None, time.time(), True)
    event = store.db.execute("SELECT * FROM events WHERE id=?", (key,)).fetchone()
    from legacy_signal_fixture import parse

    await engine.resume()
    await engine.process(event, parse(json.loads(event["payload"])["text"]))
    assert not exchange.writes
    trade = await signal(controlled, message=2)
    assert trade["filled"] > 0


async def test_delayed_source_signal_not_opened_after_start(controlled):
    store, exchange, engine = controlled
    old_time = time.time() - 2
    await engine.resume()
    key = store.event(-100123, 1, "#INJ 当前价格入场一笔空单", None, old_time, True)
    event = store.db.execute("SELECT * FROM events WHERE id=?", (key,)).fetchone()
    from legacy_signal_fixture import parse

    await engine.process(event, parse(json.loads(event["payload"])["text"]))
    assert not exchange.writes


@pytest.mark.parametrize("phase", ["checks", "leverage"])
async def test_pause_before_entry_submission_prevents_order(controlled, phase):
    _, exchange, engine = controlled
    await engine.resume()
    original = exchange.get
    calls = 0

    async def get(path, **params):
        nonlocal calls
        result = await original(path, **params)
        if path.startswith("/positions/"):
            calls += 1
            if (phase == "checks" and calls == 1) or (phase == "leverage" and calls == 2):
                engine.pause("operator requested pause")
        return result

    exchange.get = get
    await signal(controlled)
    assert not exchange.orders
    assert not [w for w in exchange.writes if w[1] == "/orders"]


async def test_pause_keeps_owned_exit_management_working(controlled):
    _, exchange, engine = controlled
    await engine.resume()
    trade = await signal(controlled)
    engine.pause("operator requested pause")
    job = await engine.claim_plan()
    await engine.propose_plan(trade["id"], job["version"], job["lease"], plan())
    await engine.apply_plan(trade["id"], job["version"], job["lease"], plan())
    assert exchange.stops
    await signal(controlled, "INJ 全部平仓", message=2)
    assert exchange.position["size"] == 0
    assert engine.paused()


async def test_successful_reconciliation_clears_transient_blocker_but_does_not_resume(controlled):
    store, _, engine = controlled
    await engine.resume()
    trade = await signal(controlled)
    trade["last_error"] = "TimeoutError"
    store.save_trade(trade)
    engine.pause("reconciliation requires review")
    with pytest.raises(Review, match="核查"):
        await engine.resume()
    await engine.poll()
    assert not store.trade(trade["id"]).get("last_error")
    assert engine.paused()
    await engine.resume()
    assert not engine.paused()


async def test_control_http_auth_status_and_idempotency(controlled):
    _, exchange, engine = controlled
    async with TestClient(TestServer(make_app(engine, credentials=ready_credentials()))) as client:
        assert (await client.post("/start")).status == 401
        headers = {"Authorization": "Bearer fixture"}
        result = await (await client.post("/start", headers=headers)).json()
        assert result["new_entries_enabled"] and not result["paused"]
        result = await (await client.post("/pause", headers=headers)).json()
        assert result["paused"] and not result["new_entries_enabled"]
        engine.listener_ready = False
        assert (await client.post("/start", headers=headers)).status == 409
        assert engine.paused() and not exchange.writes


async def test_status_exposes_rejected_signal_even_with_healthy_heartbeat(controlled):
    store, _, engine = controlled
    key = store.event(1, 15368, "#FIL 当前价格入场一笔多单", None, time.time(), True)
    store.finish(key, "review", "fixture rejection")
    async with TestClient(TestServer(make_app(engine, credentials=ready_credentials()))) as client:
        response = await client.get("/status", headers={"Authorization": "Bearer fixture"})
        state = await response.json()
        assert state["listener_ready"]
        assert state["last_review"]["message_id"] == 15368
        assert state["last_review"]["reason"] == "fixture rejection"


def routine(monkeypatch):
    base = ModuleType("routines.base")
    base.RoutineResult = SimpleNamespace
    monkeypatch.setitem(sys.modules, "routines.base", base)
    path = Path(__file__).parents[1] / "condor-assets/telegram_agent/routines/trader_status.py"
    spec = importlib.util.spec_from_file_location("operator_control", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_actual_routine_calls_service_and_enforces_administrator(controlled, monkeypatch):
    _, exchange, engine = controlled
    module = routine(monkeypatch)
    async with TestServer(make_app(engine, credentials=ready_credentials())) as server:
        monkeypatch.setenv("TELEGRAM_TRADER_URL", str(server.make_url("/")).rstrip("/"))
        for action in ("status", "start", "pause"):
            with pytest.raises(PermissionError):
                await module.run(module.Config(action=action), SimpleNamespace(_chat_id=999))
        assert engine.paused()
        for action, paused in (("start", False), ("pause", True), ("status", True)):
            result = await module.run(module.Config(action=action), SimpleNamespace(_chat_id=123))
            body = json.loads(result.text)
            assert body["ok"] and body["paused"] is paused
        engine.listener_ready = False
        result = await module.run(module.Config(action="start"), SimpleNamespace(_chat_id=123))
        assert json.loads(result.text)["ok"] is False
        assert engine.paused() and not exchange.writes
