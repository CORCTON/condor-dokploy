"""Cancellation and failure semantics without a provider or exchange."""

import asyncio
import importlib.util
import json
import os
import sys
import time
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


@pytest.mark.parametrize("routine", ["signal_interpreter", "exit_planner"])
async def test_routine_owns_and_reaps_separate_worker_process(interpreter, monkeypatch, routine):
    if routine != "signal_interpreter":
        path = Path(interpreter.__file__).with_name(routine + ".py")
        spec = importlib.util.spec_from_file_location("test_worker_" + routine, path)
        interpreter = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(interpreter)
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TRADER_LLM_READY", "false")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("TRADER_TOKEN", "fixture-only")
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
        await asyncio.sleep(0.1)
        assert process.returncode is None
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


async def test_full_batch_retains_unattached_images_and_explicit_followup_levels(
    interpreter, monkeypatch
):
    from test_trader import Exchange

    from trader.engine import Engine
    from trader.models import Decisions
    from trader.signals import SignalInbox
    from trader.store import Store

    class PromptDone:
        stop_reason = "end_turn"

    store = Store(":memory:")
    engine = Engine(store, Exchange(), enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    inbox = SignalInbox(engine)
    texts = ["#INJ 当前价格入场空单", "SL 11，TP1 9，TP2 8", "图三", "图四", "图五"]
    for number, text in enumerate(texts, 1):
        store.event(1, number, text, 1 if number == 2 else None,
                    time.time(), True, media={"id": str(number)})
    job = await inbox.claim()
    assert len(job["events"]) == 5
    received = {}

    class Client:
        async def start(self):
            pass

        async def stop(self):
            pass

        async def prompt_stream(self, prompt, images):
            received.update(json.loads(prompt))
            assert len(images) == 4
            decisions = []
            for index, event in enumerate(received["events"]):
                decisions.append({
                    "event_id": event["event_id"], "action": "ignore" if index else "open",
                    "symbol": "INJ", "side": "short",
                    "reason": "已将后续止盈止损并入开仓" if index else "明确开空",
                    "levels": {} if index else {"sl": "11", "tp1": "9", "tp2": "8"},
                    "evidence_message_ids": [1, 2] if index == 0 else [event["message_id"]],
                })
            await inbox.propose(job["lease"], Decisions.model_validate({"decisions": decisions}))
            yield PromptDone()

    def build(*args, **kwargs):
        assert "SAME batch" in kwargs["system_prompt"]
        assert "on_demand_image_message_ids" in kwargs["system_prompt"]
        return Client()

    modules = {
        "condor": {}, "condor.acp": {}, "condor.agents": {}, "condor.runtime": {},
        "condor.acp.client": {"PromptDone": PromptDone},
        "condor.acp.pydantic_ai_client": {"is_pydantic_ai_model": lambda key: True},
        "condor.agents.agent": {"AgentStore": lambda: SimpleNamespace(
            get=lambda slug: SimpleNamespace(agent_key="fixture-model"))},
        "condor.runtime.llm_client": {"build_llm_client": build},
    }
    for name, members in modules.items():
        module = ModuleType(name)
        module.__dict__.update(members)
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setenv("TRADER_TOKEN", "fixture-only")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://fixture")
    fetched = []

    class Response:
        def __init__(self):
            self.headers = {"Content-Type": "image/png"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def raise_for_status(self):
            pass

        async def read(self):
            return b"fixture-image"

    class Session:
        def get(self, url, **kwargs):
            fetched.append(url)
            return Response()

    async def api(*args):
        return inbox.proposal(job["lease"])

    monkeypatch.setattr(interpreter, "api", api)
    try:
        result = await interpreter.interpret_current(job, 1, Session())
        assert len(fetched) == 4
        assert received["attached_images_in_order"] == [1, 2, 3, 4]
        assert received["on_demand_image_message_ids"] == [5]
        assert len(received["events"]) == 5
        await inbox.apply(job["lease"], Decisions.model_validate(result))
        trade = store.trades()[0]
        assert trade["channel_levels"] == {"sl": "11", "tp1": "9", "tp2": "8"}
        assert trade["source_sl"] and len(engine.gate.stops) == 1
    finally:
        store.close()
