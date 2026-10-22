"""Cancellation and failure semantics without a provider or exchange."""

import asyncio
import importlib.util
import os
from pathlib import Path

import pytest


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
        assert method == "GET" and path == "/signals/job/fresh"
        return {"fresh": False}

    monkeypatch.setattr(interpreter, "generate_decisions", generate)
    monkeypatch.setattr(interpreter, "api", api)
    assert await interpreter.interpret_current({"lease": "job"}, 1, None) is None
    assert cancelled.is_set()


async def test_model_failure_is_not_replaced_by_a_guessed_decision(interpreter, monkeypatch):
    async def generate(*args):
        raise ValueError("invalid model output")

    async def api(*args):
        return {"fresh": True}

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
