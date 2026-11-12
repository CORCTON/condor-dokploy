"""Lost claim replies use existing durable leases; no exchange or model network."""

import asyncio
import time
import uuid
from contextlib import asynccontextmanager

import pytest
from aiohttp import ClientSession, ClientTimeout, web
from comprehensive_cases import trade
from comprehensive_fixtures import FakeExchange
from eval_comprehensive_channel import seed
from test_exit_planner import planner as planner  # noqa: PLC0414
from test_interpreter import interpreter as interpreter  # noqa: PLC0414
from test_signals import answer

from trader.app import make_app
from trader.engine import Engine
from trader.gate import Gate
from trader.models import Plan
from trader.signals import SignalInbox
from trader.store import Store


@pytest.fixture
def rig(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("Only memory FakeExchange may be used")

    monkeypatch.setattr(Gate, "__init__", forbidden)
    monkeypatch.setenv("TRADER_TOKEN", "claim-recovery-fixture")
    store, exchange = Store(":memory:"), FakeExchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    yield store, exchange, engine
    store.close()


@asynccontextmanager
async def server(engine):
    # Match production: disconnect does NOT globally cancel an executing handler.
    runner = web.AppRunner(make_app(engine))
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        yield "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
    finally:
        await runner.cleanup()


@pytest.mark.parametrize("worker", ["interpreter", "planner"])
async def test_client_disconnect_before_lock_release_reclaims_original_lease(rig, worker):
    store, exchange, engine = rig
    owned = None
    if worker == "planner":
        owned = await seed(engine, exchange, dict(trade(), plan=False))
    else:
        store.event(1, 100, "#INJ 当前市价开空", None, time.time(), True)
    exchange.writes.clear()
    request_id = uuid.uuid4().hex
    headers = {"Authorization": "Bearer claim-recovery-fixture", "X-Claim-ID": request_id}
    route = "/jobs/claim" if owned else "/signals/claim"
    async with server(engine) as url, ClientSession() as client:
        await engine.lock.acquire()
        try:
            async def dropped_response():
                async with client.post(url + route, headers=headers,
                                       timeout=ClientTimeout(total=0.1)) as response:
                    return await response.json()

            request = asyncio.create_task(dropped_response())
            async with asyncio.timeout(2):
                while engine.worker_seen(worker) is None:
                    await asyncio.sleep(0)
            with pytest.raises(TimeoutError):
                await request
        finally:
            engine.lock.release()
        async with asyncio.timeout(2):
            while True:
                existing = (store.trade(owned["id"]).get("plan_token") if owned else
                            store.db.execute("SELECT id FROM analyses").fetchone())
                if existing:
                    break
                await asyncio.sleep(0)
        async with client.post(url + route, headers=headers) as response:
            assert response.status == 200
            recovered = await response.json()
        assert recovered is not None, "A lost claim response stranded its still-valid lease"
        assert recovered["lease"] == request_id
        if owned:
            assert recovered["trade_id"] == owned["id"]
            assert store.trade(owned["id"])["plan_token"] == request_id
        else:
            assert store.db.execute("SELECT COUNT(*) FROM analyses").fetchone()[0] == 1
        assert exchange.writes == []


@pytest.mark.parametrize("worker", ["interpreter", "planner"])
async def test_worker_reuses_request_id_after_lost_reply(rig, worker, interpreter, planner, monkeypatch):
    module = interpreter if worker == "interpreter" else planner
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TRADER_LLM_READY", "true")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    ids = []

    @asynccontextmanager
    async def heartbeat(*_args):
        yield

    async def pause(_seconds):
        pass

    async def claimed(request_id):
        ids.append(request_id)
        if len(ids) == 1:
            raise TimeoutError("synthetic dropped claim reply")
        if len(ids) == 2:
            return
        raise asyncio.CancelledError

    async def signal_api(_session, _method, path, **kwargs):
        assert path == "/signals/claim"
        return await claimed(kwargs.get("claim_id"))

    async def plan_claim(_session, request_id):
        return await claimed(request_id)

    monkeypatch.setitem(module.runtime, "worker_heartbeat", heartbeat)
    monkeypatch.setattr(module.asyncio, "sleep", pause)
    if worker == "interpreter":
        monkeypatch.setattr(module, "api", signal_api)
    else:
        monkeypatch.setattr(module, "claim_job", plan_claim)
    with pytest.raises(asyncio.CancelledError):
        await module.run_worker()
    assert len(ids) == 3 and ids[0] and ids[0] == ids[1] and ids[2] != ids[1]


@pytest.mark.parametrize("worker", ["interpreter", "planner"])
async def test_concurrent_retry_keeps_one_original_lease_without_extending_it(rig, worker):
    store, exchange, engine = rig
    if worker == "planner":
        owned = await seed(engine, exchange, dict(trade(), plan=False))
        claim = engine.claim_plan
        expiry = lambda: store.trade(owned["id"])["plan_lease"]
    else:
        store.event(1, 100, "#INJ 当前市价开空", None, time.time(), True)
        claim = SignalInbox(engine).claim
        expiry = lambda: store.analysis(request_id)["expires"]
    exchange.writes.clear()
    request_id = uuid.uuid4().hex
    first = await claim(request_id)
    original_expiry = expiry()
    recovered, repeated, competing = await asyncio.gather(
        claim(request_id), claim(request_id), claim(uuid.uuid4().hex))
    assert recovered == repeated == first
    assert competing is None
    assert expiry() == original_expiry
    assert exchange.writes == []


@pytest.mark.parametrize("changed", ["expired", "new_message", "edit", "trade_state", "finished"])
async def test_signal_retry_does_not_revive_stale_or_completed_lease(rig, changed):
    store, exchange, engine = rig
    inbox = SignalInbox(engine)
    store.event(1, 100, "#INJ 当前市价开空", None, time.time(), True)
    request_id = uuid.uuid4().hex
    job = await inbox.claim(request_id)
    if changed == "expired":
        store.db.execute("UPDATE analyses SET expires=0 WHERE id=?", (request_id,))
    elif changed == "new_message":
        store.event(1, 101, "撤了", 100, time.time(), True)
    elif changed == "edit":
        store.event(1, 100, "#INJ 仅观察，不开仓", None, time.time(), True)
    elif changed == "trade_state":
        await seed(engine, exchange, dict(trade(), plan=False))
    else:
        result = answer(job, "ignore")
        await inbox.propose(request_id, result)
        await inbox.apply(request_id, result)
    exchange.writes.clear()
    assert await inbox.claim(request_id) is None
    assert store.analysis(request_id)["status"] != "analyzing"
    assert store.db.execute("SELECT COUNT(*) FROM analyses").fetchone()[0] == 1
    assert exchange.writes == []
    next_job = await inbox.claim(uuid.uuid4().hex)
    assert (next_job is None) == (changed == "finished")


@pytest.mark.parametrize("changed", ["expired", "version", "closing", "closed", "disabled"])
async def test_plan_retry_does_not_upgrade_or_revive_original_version(rig, changed):
    store, exchange, engine = rig
    owned = await seed(engine, exchange, dict(trade(), plan=False))
    request_id = uuid.uuid4().hex
    job = await engine.claim_plan(request_id)
    row = store.trade(owned["id"])
    assert row["plan_version"] == job["version"]
    if changed == "expired":
        row["plan_lease"] = 0
    elif changed == "version":
        row["version"] += 1
    elif changed == "closing":
        row["close_requested"] = True
    elif changed == "closed":
        row["status"] = "closed"
    else:
        engine.enabled = False
    store.save_trade(row)
    exchange.writes.clear()
    assert await engine.claim_plan(request_id) is None
    assert store.trade(owned["id"]) == row
    assert exchange.writes == []


@pytest.mark.parametrize("route", ["/signals/claim", "/jobs/claim"])
@pytest.mark.parametrize("invalid", ["not-a-uuid", "a" * 31, "A" * 32])
async def test_invalid_claim_header_is_rejected_without_allocating(rig, route, invalid):
    store, exchange, engine = rig
    owned = await seed(engine, exchange, dict(trade(), plan=False))
    store.event(1, 200, "#JTO 市价开多", None, time.time(), True)
    before = store.trade(owned["id"])
    exchange.writes.clear()
    async with (
        server(engine) as url, ClientSession() as client,
        client.post(url + route, headers={
            "Authorization": "Bearer claim-recovery-fixture", "X-Claim-ID": invalid,
        }) as response,
    ):
        assert response.status == 400
    assert store.trade(owned["id"]) == before
    assert not store.db.execute("SELECT id FROM analyses").fetchall()
    assert exchange.writes == []


async def test_recovered_accepted_plan_is_reused_without_market_or_model_call(rig, planner, monkeypatch):
    store, exchange, engine = rig
    owned = await seed(engine, exchange, dict(trade(), plan=False))
    job = await engine.claim_plan(uuid.uuid4().hex)
    plan = Plan(sl="11", tp="8", rationale="fixture")
    await engine.propose_plan(job["trade_id"], job["version"], job["lease"], plan)
    row = store.trade(owned["id"])
    row["plan_lease"] = 0
    store.save_trade(row)
    exchange.writes.clear()

    async def forbidden(*_args):
        raise AssertionError("Accepted plan must not be regenerated")

    monkeypatch.setattr(exchange, "candles", forbidden)
    monkeypatch.setattr(planner, "generate_plan", forbidden)
    recovered = await engine.claim_plan(uuid.uuid4().hex)
    assert recovered["accepted_plan"] == row["plan_proposal"]
    async with planner.plan_for_job(recovered, 1) as actual:
        assert actual == row["plan_proposal"]
    assert exchange.writes == []


async def test_signal_retry_preserves_an_already_accepted_proposal(rig):
    store, exchange, engine = rig
    store.event(1, 100, "早安", None, time.time(), True)
    inbox = SignalInbox(engine)
    request_id = uuid.uuid4().hex
    job = await inbox.claim(request_id)
    result = answer(job, "ignore")
    await inbox.propose(request_id, result)
    before = store.analysis(request_id)
    assert await inbox.claim(request_id) == job
    assert store.analysis(request_id) == before
    assert inbox.proposal(request_id) == result.model_dump(mode="json")
    assert exchange.writes == []


async def test_late_failed_claim_read_cannot_invalidate_successfully_recovered_job(rig, monkeypatch):
    store, exchange, engine = rig
    await seed(engine, exchange, dict(trade(), plan=False))
    entered, finish = asyncio.Event(), asyncio.Event()
    calls = 0

    async def candles(_symbol, interval="5m"):
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await finish.wait()
            raise TimeoutError("original claim outlived its disconnected client")
        return [{"t": time.time() - 14400, "o": "10", "h": "11", "l": "9", "c": "10"}]

    monkeypatch.setattr(exchange, "candles", candles)
    request_id = uuid.uuid4().hex
    first = asyncio.create_task(engine.claim_plan(request_id))
    await entered.wait()
    recovered = await engine.claim_plan(request_id)
    finish.set()
    with pytest.raises(TimeoutError):
        await first
    assert engine.plan_trade(recovered["trade_id"], recovered["version"], request_id)
    assert store.trade(recovered["trade_id"])["plan_token"] == request_id


async def test_failed_nonce_claim_market_read_is_retryable_without_lease_extension(rig, monkeypatch):
    store, exchange, engine = rig
    owned = await seed(engine, exchange, dict(trade(), plan=False))
    request_id = uuid.uuid4().hex
    normal_candles = exchange.candles

    async def unavailable(_symbol, interval="5m"):
        raise TimeoutError("synthetic market read failure")

    monkeypatch.setattr(exchange, "candles", unavailable)
    with pytest.raises(TimeoutError):
        await engine.claim_plan(request_id)
    original = store.trade(owned["id"])
    assert original["plan_token"] == request_id
    monkeypatch.setattr(exchange, "candles", normal_candles)
    recovered = await engine.claim_plan(request_id)
    assert recovered["lease"] == request_id
    assert store.trade(owned["id"])["plan_lease"] == original["plan_lease"]
