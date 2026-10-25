"""Accepted tools, not final model prose, determine whether a plan is available."""

import asyncio
import importlib.util
import json
import sys
import time
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from aiohttp import ClientResponseError, ClientSession, web
from aiohttp.test_utils import TestServer
from test_trader import Exchange, signal

from trader.app import make_app
from trader.engine import Engine
from trader.models import Plan
from trader.store import Store


@pytest.fixture
def planner():
    path = Path(__file__).parents[1] / "condor-assets/telegram_agent/routines/exit_planner.py"
    spec = importlib.util.spec_from_file_location("test_exit_planner_candidate", path)
    planner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(planner)
    return planner


class PromptDone:
    def __init__(self, reason="end_turn"):
        self.stop_reason = reason


PLAN = {"sl": "11", "tp1": "9", "tp2": "8", "tp1_fraction": "0.5", "rationale": "fixture"}
JOB = {"trade_id": "trade", "version": 1, "lease": "lease", "output_schema": {}}


def install_client(monkeypatch, client):
    modules = {
        "condor": {},
        "condor.acp": {},
        "condor.agents": {},
        "condor.acp.client": {"PromptDone": PromptDone},
        "condor.acp.pydantic_ai_client": {"is_pydantic_ai_model": lambda key: True},
        "condor.agents.agent": {
            "AgentStore": lambda: SimpleNamespace(
                get=lambda slug: SimpleNamespace(agent_key="fixture-model")
            )
        },
        "condor.runtime": {"context": SimpleNamespace(build_agent_context=lambda *args: args[2])},
        "condor.runtime.llm_client": {"build_llm_client": lambda *args, **kwargs: client},
    }
    for name, members in modules.items():
        module = ModuleType(name)
        module.__dict__.update(members)
        monkeypatch.setitem(sys.modules, name, module)


async def test_valid_json_text_without_a_tool_call_is_rejected(planner, monkeypatch):
    class Client:
        async def start(self):
            pass

        async def stop(self):
            pass

        async def prompt_stream(self, prompt):
            yield SimpleNamespace(text=json.dumps(PLAN))
            yield PromptDone()

    install_client(monkeypatch, Client())

    async def no_proposal(request):
        return web.json_response(None)

    app = web.Application()
    app.router.add_get("/plans/trade/proposal", no_proposal)
    async with TestServer(app) as server:
        monkeypatch.setenv("TELEGRAM_TRADER_URL", str(server.make_url("/")).rstrip("/"))
        monkeypatch.setenv("TRADER_TOKEN", "fixture-only")
        with pytest.raises(ValueError, match="without a validated exit-plan tool call"):
            async with planner.generate_plan(JOB, 1):
                pytest.fail("plain JSON text must not yield an executable plan")


@pytest.mark.parametrize("ending", ["end_turn", "error", "timeout", "hang"])
async def test_accepted_plan_survives_final_summary_failure(planner, monkeypatch, ending):
    accepted = False
    closed = stopped = False

    class Client:
        async def start(self):
            pass

        async def stop(self):
            nonlocal stopped
            stopped = True

        async def prompt_stream(self, prompt):
            nonlocal accepted, closed
            try:
                yield SimpleNamespace(status="completed")  # not evidence of accepted tool work
                accepted = True
                yield SimpleNamespace(status="completed")
                if ending == "hang":
                    await asyncio.Event().wait()
                yield PromptDone(ending)
            finally:
                closed = True

    install_client(monkeypatch, Client())

    async def proposal(request):
        assert dict(request.query) == {"version": "1", "lease": "lease"}
        return web.json_response(PLAN if accepted else None)

    app = web.Application()
    app.router.add_get("/plans/trade/proposal", proposal)
    async with TestServer(app) as server:
        monkeypatch.setenv("TELEGRAM_TRADER_URL", str(server.make_url("/")).rstrip("/"))
        monkeypatch.setenv("TRADER_TOKEN", "fixture-only")
        async with asyncio.timeout(2), planner.generate_plan(JOB, 1) as plan:
            assert plan == PLAN
    assert accepted and closed and stopped


async def test_plan_accepted_during_empty_proposal_response_is_read_after_model_failure(
    planner, monkeypatch
):
    finish = asyncio.Event()
    accepted = False
    reads = 0

    async def request_plan(_job, _owner, cleanup_started):
        nonlocal accepted
        await finish.wait()
        accepted = True
        cleanup_started.set()
        raise RuntimeError("final prose failed after tool acceptance")

    async def read_proposal(*args):
        nonlocal reads
        reads += 1
        if reads == 1:
            # The server sampled None, then the model accepted the tool and
            # failed before that already-empty response reached the worker.
            finish.set()
            await asyncio.sleep(0)
            assert accepted
            return None
        return PLAN if accepted else None

    monkeypatch.setattr(planner, "request_plan", request_plan)
    monkeypatch.setattr(planner, "read_proposal", read_proposal)
    async with asyncio.timeout(2), planner.generate_plan(JOB, 1) as plan:
        assert plan == PLAN
    assert reads == 2


@pytest.mark.parametrize("status", [409, 503])
async def test_unavailable_or_stale_proposal_cancels_model(planner, monkeypatch, status):
    cancelled = asyncio.Event()

    async def request_plan(*args):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def read_proposal(*args):
        raise ClientResponseError(None, (), status=status)

    monkeypatch.setattr(planner, "request_plan", request_plan)
    monkeypatch.setattr(planner, "read_proposal", read_proposal)
    with pytest.raises(ClientResponseError) as error:
        async with planner.generate_plan(JOB, 1):
            pytest.fail("stale or unavailable proposal must not be yielded")
    assert error.value.status == status and cancelled.is_set()


async def test_timeout_without_tool_proposal_never_returns_a_plan(planner, monkeypatch):
    cancelled = asyncio.Event()

    async def request_plan(*args):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def read_proposal(*args):
        return None

    timeout = asyncio.timeout
    monkeypatch.setattr(planner, "request_plan", request_plan)
    monkeypatch.setattr(planner, "read_proposal", read_proposal)
    monkeypatch.setattr(planner.asyncio, "timeout", lambda seconds: timeout(0.01))
    with pytest.raises(TimeoutError):
        async with planner.generate_plan(JOB, 1):
            pytest.fail("timed-out request must not yield a plan")
    assert cancelled.is_set()


@pytest.mark.parametrize("ending", ["end_turn", "hang"])
async def test_worker_applies_native_exits_before_waiting_for_slow_model_cleanup(
    planner, monkeypatch, ending
):
    store, exchange = Store(":memory:"), Exchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    trade = await signal((store, exchange, engine))
    cleanup_started, cleanup_release, cleanup_done, applied = (asyncio.Event() for _ in range(4))
    model_starts = 0

    class Client:
        async def start(self):
            nonlocal model_starts
            model_starts += 1

        async def stop(self):
            cleanup_started.set()
            await cleanup_release.wait()
            cleanup_done.set()

        async def prompt_stream(self, prompt):
            job = json.loads(prompt[prompt.index("{") :])
            await engine.propose_plan(
                job["trade_id"], job["version"], job["lease"], Plan.model_validate(PLAN)
            )
            if ending == "hang":
                await asyncio.Event().wait()
            yield PromptDone()

    original_apply = engine.apply_plan

    async def observed_apply(*args):
        await original_apply(*args)
        applied.set()

    install_client(monkeypatch, Client())
    monkeypatch.setattr(engine, "apply_plan", observed_apply)
    monkeypatch.setenv("TRADER_TOKEN", "fixture-only")
    monkeypatch.setenv("TRADER_LLM_READY", "true")
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    async with TestServer(make_app(engine)) as server:
        monkeypatch.setenv("TELEGRAM_TRADER_URL", str(server.make_url("/")).rstrip("/"))
        worker = asyncio.create_task(planner.run_worker())
        try:
            await asyncio.wait_for(applied.wait(), 2)
            await asyncio.wait_for(cleanup_started.wait(), 2)
            assert not cleanup_done.is_set() and not worker.done()
            assert len(exchange.stops) == 1 and len(exchange.orders) == 3
            assert not store.trade(trade["id"])["plan_needed"]
            assert model_starts == 1
            cleanup_release.set()
            await asyncio.wait_for(cleanup_done.wait(), 2)
        finally:
            cleanup_release.set()
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker
            store.close()


@pytest.mark.parametrize(
    "outcome", ["normal", "body_error", "cancel_body", "cancel_cleanup", "cancel_twice"]
)
async def test_generate_plan_scope_waits_for_cleanup_and_leaves_no_request_task(
    planner, monkeypatch, outcome
):
    accepted, cleanup_started, cleanup_release, cleanup_done = (asyncio.Event() for _ in range(4))
    consumed = asyncio.Event()

    class Client:
        async def start(self):
            pass

        async def stop(self):
            cleanup_started.set()
            await cleanup_release.wait()
            cleanup_done.set()

        async def prompt_stream(self, prompt):
            accepted.set()
            yield PromptDone()

    async def proposal(*args):
        return PLAN if accepted.is_set() else None

    install_client(monkeypatch, Client())
    monkeypatch.setattr(planner, "read_proposal", proposal)

    async def consume():
        async with planner.generate_plan(JOB, 1) as plan:
            assert plan == PLAN
            consumed.set()
            if outcome == "body_error":
                raise ValueError("apply rejected")
            if outcome == "cancel_body":
                await asyncio.Event().wait()

    consumer = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(consumed.wait(), 2)
        await asyncio.wait_for(cleanup_started.wait(), 2)
        assert not consumer.done() and not cleanup_done.is_set()
        if outcome.startswith("cancel"):
            consumer.cancel()
            await asyncio.sleep(0)
            if outcome == "cancel_twice":
                consumer.cancel()
                await asyncio.sleep(0)
            assert not consumer.done() and not cleanup_done.is_set()
        cleanup_release.set()
        if outcome.startswith("cancel"):
            with pytest.raises(asyncio.CancelledError):
                await consumer
        elif outcome == "body_error":
            with pytest.raises(ValueError, match="apply rejected"):
                await consumer
        else:
            await consumer
        assert cleanup_done.is_set()
        assert not [
            task
            for task in asyncio.all_tasks()
            if task.get_coro().__qualname__.endswith("request_plan")
        ]
    finally:
        cleanup_release.set()
        if not consumer.done():
            consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)


@pytest.mark.parametrize("mutation", ["expired", "changed_version", "different_plan"])
async def test_yielded_plan_still_requires_current_lease_and_exact_accepted_proposal(
    planner, monkeypatch, mutation
):
    store, exchange = Store(":memory:"), Exchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    trade = await signal((store, exchange, engine))
    job = await engine.claim_plan()
    await engine.propose_plan(
        job["trade_id"], job["version"], job["lease"], Plan.model_validate(PLAN)
    )
    cleaned = asyncio.Event()

    async def request_plan(_job, _owner, cleanup_started):
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            cleaned.set()

    monkeypatch.setattr(planner, "request_plan", request_plan)
    monkeypatch.setenv("TRADER_TOKEN", "fixture-only")
    headers = {"Authorization": "Bearer fixture-only"}
    try:
        async with TestServer(make_app(engine)) as server, ClientSession() as session:
            monkeypatch.setenv("TELEGRAM_TRADER_URL", str(server.make_url("/")).rstrip("/"))
            with pytest.raises(ClientResponseError) as error:
                async with planner.generate_plan(job, 1) as plan:
                    assert len(exchange.orders) == 1 and not exchange.stops
                    current = store.trade(trade["id"])
                    if mutation == "expired":
                        current["plan_lease"] = time.time() - 1
                    elif mutation == "changed_version":
                        current["version"] += 1
                    else:
                        plan = {**plan, "sl": "10.90"}
                    store.save_trade(current)
                    async with session.post(
                        server.make_url("/plans"),
                        headers=headers,
                        json={key: job[key] for key in ("trade_id", "version", "lease")}
                        | {"plan": plan},
                    ) as response:
                        response.raise_for_status()
            assert error.value.status == 409
            assert cleaned.is_set()
            assert len(exchange.orders) == 1 and not exchange.stops
    finally:
        store.close()
