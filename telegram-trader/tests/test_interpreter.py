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


@pytest.fixture
def delayed_client(monkeypatch):
    """Actual interpreter lifecycle with an explicitly gated, non-instant MCP stop."""
    class PromptDone:
        stop_reason = "end_turn"

    class Client:
        def __init__(self):
            self.cleanup_started = asyncio.Event()
            self.cleanup_release = asyncio.Event()
            self.stopped = False
            self.cleanup_cancelled = False
            self.prompt_finishes = False
            self.on_prompt = None

        async def start(self):
            pass

        async def stop(self):
            self.cleanup_started.set()
            try:
                await self.cleanup_release.wait()
            except asyncio.CancelledError:
                self.cleanup_cancelled = True
                raise
            self.stopped = True

        async def prompt_stream(self, *args, **kwargs):
            await self.on_prompt()
            if not self.prompt_finishes:
                await asyncio.Event().wait()
            yield PromptDone()

    client = Client()
    modules = {
        "condor": {}, "condor.acp": {}, "condor.agents": {}, "condor.runtime": {},
        "condor.acp.client": {"PromptDone": PromptDone},
        "condor.acp.pydantic_ai_client": {"is_pydantic_ai_model": lambda key: True},
        "condor.agents.agent": {"AgentStore": lambda: SimpleNamespace(
            get=lambda slug: SimpleNamespace(agent_key="fixture-model"))},
        "condor.runtime.llm_client": {"build_llm_client": lambda *args, **kwargs: client},
    }
    for name, members in modules.items():
        module = ModuleType(name)
        module.__dict__.update(members)
        monkeypatch.setitem(sys.modules, name, module)
    return client


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
    async with interpreter.interpret_current({"lease": "job"}, 1, None) as result:
        assert result is None
    assert cancelled.is_set()


async def test_model_failure_is_not_replaced_by_a_guessed_decision(interpreter, monkeypatch):
    async def generate(*args):
        raise ValueError("invalid model output")

    async def api(*args):
        return None

    monkeypatch.setattr(interpreter, "generate_decisions", generate)
    monkeypatch.setattr(interpreter, "api", api)
    with pytest.raises(ValueError, match="invalid model output"):
        async with interpreter.interpret_current({"lease": "job"}, 1, None):
            pytest.fail("invalid model output must not yield a proposal")


@pytest.mark.parametrize("error", [ValueError, RuntimeError])
async def test_accepted_proposal_survives_final_generation_error_after_a_stale_poll(
    interpreter, monkeypatch, error
):
    release = asyncio.Event()
    proposal = {"decisions": [{"action": "ignore"}]}
    stored = None
    reads = []

    async def generate(*args):
        nonlocal stored
        await release.wait()
        stored = proposal  # A validated Tool Calling proposal is now durable.
        raise error("final prose or transport failed after tool acceptance")

    async def api(*args):
        snapshot = stored
        reads.append(snapshot)
        release.set()
        await asyncio.sleep(0)  # Generation completes while the earlier GET is in flight.
        return snapshot

    monkeypatch.setattr(interpreter, "generate_decisions", generate)
    monkeypatch.setattr(interpreter, "api", api)
    async with interpreter.interpret_current({"lease": "job"}, 1, None) as result:
        assert result == proposal
    assert reads == [None, proposal]


@pytest.mark.parametrize("prompt_finishes", [False, True])
@pytest.mark.parametrize("body_fails", [False, True])
async def test_proposal_executes_before_cleanup_but_context_owns_cleanup(
    interpreter, monkeypatch, delayed_client, prompt_finishes, body_fails
):
    client = delayed_client
    client.prompt_finishes = prompt_finishes
    proposal = {"decisions": [{"action": "ignore"}]}
    stored = None
    executed = asyncio.Event()

    async def propose():
        nonlocal stored
        stored = proposal

    async def api(*args):
        return stored

    client.on_prompt = propose
    monkeypatch.setattr(interpreter, "api", api)

    async def consume():
        async with interpreter.interpret_current(
            {"lease": "job", "events": [], "output_schema": {}}, 1, None
        ) as result:
            assert result == proposal
            executed.set()
            if body_fails:
                raise ValueError("execution request failed")

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(executed.wait(), 2)
        await asyncio.wait_for(client.cleanup_started.wait(), 2)
        await asyncio.sleep(0)
        assert not task.done()  # No new model may start while this one is cleaning up.
        assert not client.stopped and not client.cleanup_cancelled
    finally:
        client.cleanup_release.set()
        if body_fails:
            with pytest.raises(ValueError, match="execution request failed"):
                await task
        else:
            await task
    assert client.stopped and not client.cleanup_cancelled


@pytest.mark.parametrize("prompt_finishes", [False, True])
@pytest.mark.parametrize("cancel_count", [1, 2])
async def test_consumer_cancellation_drains_cleanup_before_propagating(
    interpreter, monkeypatch, delayed_client, prompt_finishes, cancel_count
):
    client = delayed_client
    client.prompt_finishes = prompt_finishes
    stored = None
    body_finished = asyncio.Event()

    async def propose():
        nonlocal stored
        stored = {"decisions": [{"action": "ignore"}]}

    async def api(*args):
        return stored

    client.on_prompt = propose
    monkeypatch.setattr(interpreter, "api", api)

    async def consume():
        async with interpreter.interpret_current(
            {"lease": "job", "events": [], "output_schema": {}}, 1, None
        ):
            body_finished.set()

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(body_finished.wait(), 2)
        await asyncio.wait_for(client.cleanup_started.wait(), 2)
        for _ in range(cancel_count):
            task.cancel()  # Cancellation while the context is already awaiting MCP stop.
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert not task.done()
            assert not client.cleanup_cancelled
    finally:
        client.cleanup_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert client.stopped and not client.cleanup_cancelled


@pytest.mark.parametrize("invalidated", [None, "expired", "edited", "paused"])
async def test_timely_proposal_applies_before_cleanup_without_bypassing_execution_checks(
    interpreter, monkeypatch, delayed_client, invalidated
):
    from aiohttp import ClientSession
    from aiohttp.test_utils import TestClient, TestServer
    from comprehensive_channel import SnapshotChannel
    from test_signals import answer
    from test_trader import Exchange

    from trader.app import make_app, receive_message
    from trader.engine import Engine
    from trader.store import Store

    clock = [round(time.time(), 6)]
    source_time = round(clock[0] - 59, 6)
    monkeypatch.setattr(time, "time", lambda: clock[0])
    monkeypatch.setenv("TRADER_TOKEN", "local-interpreter-only")
    row = {"message_id": 1, "text": "#INJ 当前市价开空", "reply_to_message_id": None,
           "source_time": source_time, "edited_at": None, "media": None}
    channel = SnapshotChannel({1: row}, {})
    store = Store(":memory:")
    engine = Engine(store, Exchange(), enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    receive_message(engine, -100123, channel._message(1), source_time - 1)
    executed = asyncio.Event()
    client = delayed_client

    try:
        async with TestClient(TestServer(make_app(engine, channel))) as server:
            monkeypatch.setenv("TELEGRAM_TRADER_URL", str(server.make_url("")))
            async with ClientSession() as session:
                job = await interpreter.api(session, "POST", "/signals/claim")

                async def propose():
                    await interpreter.api(session, "POST", "/signals/proposals", json={
                        "lease": job["lease"],
                        "result": answer(job, symbol="INJ", side="short").model_dump(),
                    })

                client.on_prompt = propose

                async def consume():
                    async with interpreter.interpret_current(job, 1, session) as result:
                        if invalidated == "expired":
                            clock[0] = source_time + 61
                        elif invalidated == "edited":
                            channel._messages[1].update(text="取消，不开仓", edited_at=clock[0])
                        elif invalidated == "paused":
                            store.set("paused", True)
                        try:
                            await interpreter.api(session, "POST", "/signals/decisions", json={
                                "lease": job["lease"], "result": result,
                            })
                        except ClientResponseError as exc:
                            assert invalidated in {"edited", "paused"} and exc.status == 409
                        else:
                            assert invalidated != "edited"
                        executed.set()

                task = asyncio.create_task(consume())
                try:
                    await asyncio.wait_for(executed.wait(), 2)
                    await asyncio.wait_for(client.cleanup_started.wait(), 2)
                    assert len(engine.gate.orders) == (1 if invalidated is None else 0)
                    clock[0] = source_time + 61  # MCP cleanup extends past the entry deadline.
                    assert not task.done()
                finally:
                    client.cleanup_release.set()
                    await task
                assert client.stopped and not client.cleanup_cancelled
                assert len(engine.gate.orders) == (1 if invalidated is None else 0)
                assert any(call["op"] == "authority" for call in channel.calls)
    finally:
        store.close()


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
        async with asyncio.timeout(2):
            async with interpreter.interpret_current(job, 1, None) as result:
                assert result == proposal
    else:
        with pytest.raises(ValueError, match="without a validated decision tool call"):
            async with interpreter.interpret_current(job, 1, None):
                pytest.fail("text output must not yield a proposal")
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
        async with interpreter.interpret_current(job, 1, Session()) as result:
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
