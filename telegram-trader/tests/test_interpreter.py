"""Cancellation and failure semantics without a provider or exchange."""

import asyncio
import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from aiohttp import ClientResponseError


@pytest.fixture
def interpreter():
    path = Path(__file__).parents[1] / "condor-assets/telegram_agent/routines/signal_interpreter.py"
    spec = importlib.util.spec_from_file_location("test_signal_interpreter", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_new_event_cancels_model_without_submitting_a_decision(interpreter, monkeypatch):
    cancelled = asyncio.Event()

    async def generate(*args):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def api(session, method, path):
        assert method == "GET" and path == "/signals/job/proposal"
        raise ClientResponseError(None, (), status=409)

    monkeypatch.setattr(interpreter, "generate_decisions", generate)
    monkeypatch.setattr(interpreter, "api", api)
    assert await interpreter.interpret_current({"lease": "job"}, 1, None) is None
    assert cancelled.is_set()


async def test_model_failure_is_not_replaced_by_a_guessed_decision(interpreter, monkeypatch):
    async def generate(*args):
        raise ValueError("invalid model output")

    async def api(*args):
        return None

    monkeypatch.setattr(interpreter, "generate_decisions", generate)
    monkeypatch.setattr(interpreter, "api", api)
    with pytest.raises(ValueError, match="invalid model output"):
        await interpreter.interpret_current({"lease": "job"}, 1, None)


async def test_routine_owns_and_reaps_separate_worker_process(interpreter, monkeypatch):
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TRADER_LLM_READY", "false")
    created = asyncio.Event()
    processes = []
    spawn = asyncio.create_subprocess_exec

    async def tracked_spawn(*args, **kwargs):
        assert kwargs["start_new_session"] is True
        process = await spawn(*args, **kwargs)
        processes.append(process)
        created.set()
        return process

    monkeypatch.setattr(interpreter.asyncio, "create_subprocess_exec", tracked_spawn)
    task = asyncio.create_task(interpreter.run(interpreter.Config(), None))
    try:
        await asyncio.wait_for(created.wait(), 5)
        process = processes[0]
        assert process.pid != os.getpid()
        assert os.getpgid(process.pid) == process.pid
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert process.returncode is not None
    with pytest.raises(ProcessLookupError):
        os.kill(process.pid, 0)


@pytest.mark.parametrize("tool_proposal", [True, False])
async def test_validated_tool_proposal_finishes_without_final_model_request(
    interpreter, monkeypatch, tool_proposal
):
    class PromptDone:
        stop_reason = "end_turn"

    class ToolCallUpdate:
        status = "completed"

    proposal = {"decisions": [{"action": "ignore"}]}
    stored = None
    closed = stopped = False

    class Client:
        async def start(self):
            pass

        async def stop(self):
            nonlocal stopped
            stopped = True

        async def prompt_stream(self, *args, **kwargs):
            nonlocal stored, closed
            try:
                yield ToolCallUpdate()  # preliminary completion is NOT acceptance
                if tool_proposal:
                    stored = proposal
                    yield ToolCallUpdate()  # actual validated tool result
                    await asyncio.Event().wait()  # remaining model generation must be cancelled
                yield SimpleNamespace(text='{"decisions":[{"action":"ignore"}]}')
                yield PromptDone()
            finally:
                closed = True

    modules = {
        "condor": {}, "condor.acp": {}, "condor.agents": {}, "condor.runtime": {},
        "condor.acp.client": {"PromptDone": PromptDone, "ToolCallUpdate": ToolCallUpdate},
        "condor.acp.pydantic_ai_client": {"is_pydantic_ai_model": lambda key: True},
        "condor.agents.agent": {"AgentStore": lambda: SimpleNamespace(
            get=lambda slug: SimpleNamespace(agent_key="fixture-model"))},
        "condor.runtime.llm_client": {"build_llm_client": lambda *args, **kwargs: Client()},
    }
    for name, members in modules.items():
        module = ModuleType(name)
        module.__dict__.update(members)
        monkeypatch.setitem(sys.modules, name, module)

    async def api(session, method, path):
        assert path == "/signals/job/proposal"
        return stored

    monkeypatch.setattr(interpreter, "api", api)
    job = {"lease": "job", "events": [], "output_schema": {}}
    if tool_proposal:
        assert await asyncio.wait_for(interpreter.interpret_current(job, 1, None), 2) == proposal
    else:
        with pytest.raises(ValueError, match="without a validated decision tool call"):
            await interpreter.interpret_current(job, 1, None)
    assert closed and stopped
