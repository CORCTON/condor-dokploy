"""Accepted tools, not final model prose, determine whether a plan is available."""

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from aiohttp import ClientResponseError, web
from aiohttp.test_utils import TestServer


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
            await planner.generate_plan(JOB, 1)


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
        assert await asyncio.wait_for(planner.generate_plan(JOB, 1), 2) == PLAN
    assert accepted and closed and stopped


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
        await planner.generate_plan(JOB, 1)
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
        await planner.generate_plan(JOB, 1)
    assert cancelled.is_set()
