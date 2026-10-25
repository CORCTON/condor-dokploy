"""Known pre-apply planner failures can retry without touching exchange intents."""

import asyncio
import copy
import time

import pytest
from aiohttp.test_utils import TestClient, TestServer
from test_trader import Exchange, plan, signal

from trader.app import make_app
from trader.engine import Engine
from trader.store import Store


@pytest.fixture
async def planned_job(monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "plan-recovery-only")
    store, exchange = Store(":memory:"), Exchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    await signal((store, exchange, engine))
    job = await engine.claim_plan()
    yield engine, job
    store.close()


async def test_http_failure_releases_unproposed_job_and_old_failure_cannot_release_new_job(
    planned_job
):
    engine, job = planned_job
    before = copy.deepcopy(engine.gate.writes)
    payload = {key: job[key] for key in ("trade_id", "version", "lease")}
    payload["error"] = "MCPInitTimeout"
    headers = {"Authorization": "Bearer plan-recovery-only"}
    async with TestClient(TestServer(make_app(engine))) as client:
        response = await client.post("/jobs/fail", headers=headers, json=payload)
        assert await response.json() == {"recorded": True, "released": True}
        current = engine.store.trade(job["trade_id"])
        assert current["plan_token"] is None and current["plan_lease"] == 0
        assert current["plan_needed"] and not engine.paused()
        new = await engine.claim_plan()
        assert new["lease"] != job["lease"] and new["version"] == job["version"]
        new_state = engine.store.trade(job["trade_id"])
        response = await client.post("/jobs/fail", headers=headers, json=payload)
        assert (await response.json())["released"] is False
        assert engine.store.trade(job["trade_id"]) == new_state
    assert engine.gate.writes == before
    assert engine.store.db.execute("SELECT COUNT(*) FROM notices WHERE text LIKE ?",
                                   ("Exit planning failed%",)).fetchone()[0] == 1


@pytest.mark.parametrize("condition", [
    "wrong_token", "wrong_version", "missing_version", "closed", "closing",
    "already_applied", "apply_started", "unknown_write",
])
async def test_failure_does_not_release_an_unsafe_or_different_job(planned_job, condition):
    engine, job = planned_job
    trade = engine.store.trade(job["trade_id"])
    token, version = job["lease"], job["version"]
    if condition == "wrong_token":
        token = "another-token"
    elif condition == "wrong_version":
        version += 1
    elif condition == "missing_version":
        version = None
    elif condition == "closed":
        trade["status"] = "closed"
    elif condition == "closing":
        trade["close_requested"] = True
    elif condition == "already_applied":
        trade["plan_needed"] = False
    elif condition == "apply_started":
        trade["plan"] = plan().model_dump(mode="json")
    elif condition == "unknown_write":
        engine.store.intent("unknown-fixture", trade["id"], "POST", "/price_orders", {})
    engine.store.save_trade(trade)
    before = copy.deepcopy(engine.gate.writes)
    assert not await engine.fail_plan(trade["id"], version, token, "TimeoutError")
    current = engine.store.trade(trade["id"])
    assert current["plan_token"] == trade["plan_token"]
    assert current["plan_lease"] == trade["plan_lease"] > time.time()
    assert current.get("plan") == trade.get("plan")
    assert engine.gate.writes == before


async def test_failure_keeps_an_accepted_proposal_and_its_original_lease(planned_job):
    engine, job = planned_job
    await engine.propose_plan(job["trade_id"], job["version"], job["lease"], plan())
    before = engine.store.trade(job["trade_id"])
    writes = copy.deepcopy(engine.gate.writes)
    for _ in range(2):
        assert not await engine.fail_plan(
            job["trade_id"], job["version"], job["lease"], "LostProposalResponse"
        )
    current = engine.store.trade(job["trade_id"])
    assert current["plan_proposal"] == before["plan_proposal"]
    assert current["plan_token"] == before["plan_token"]
    assert current["plan_lease"] == before["plan_lease"]
    assert await engine.claim_plan() is None
    assert engine.gate.writes == writes
    assert engine.plan_proposal(job["trade_id"], job["version"], job["lease"]) == before[
        "plan_proposal"
    ]


async def test_failure_waits_for_inflight_apply_and_never_releases_or_replays_it(
    planned_job, monkeypatch
):
    engine, job = planned_job
    await engine.propose_plan(job["trade_id"], job["version"], job["lease"], plan())
    entered, release = asyncio.Event(), asyncio.Event()
    request = engine.gate.request

    async def gated_request(method, path, body=None, params=None):
        if method == "POST" and path == "/price_orders":
            entered.set()
            await release.wait()
        return await request(method, path, body, params)

    monkeypatch.setattr(engine.gate, "request", gated_request)
    applying = asyncio.create_task(engine.apply_plan(
        job["trade_id"], job["version"], job["lease"], plan()
    ))
    failed = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert engine.store.trade(job["trade_id"])["plan"] is not None
        failed = asyncio.create_task(engine.fail_plan(
            job["trade_id"], job["version"], job["lease"], "LostApplyResponse"
        ))
        await asyncio.sleep(0)
        assert not failed.done()
    finally:
        release.set()
        await applying
        if failed is not None:
            assert await failed is False
    current = engine.store.trade(job["trade_id"])
    assert not current["plan_needed"]
    assert current["plan_token"] == job["lease"]
    assert len(engine.gate.orders) == 3 and len(engine.gate.stops) == 1
    writes = copy.deepcopy(engine.gate.writes)
    assert not await engine.fail_plan(
        job["trade_id"], job["version"], job["lease"], "LostApplyResponse"
    )
    assert await engine.claim_plan() is None
    assert engine.gate.writes == writes
