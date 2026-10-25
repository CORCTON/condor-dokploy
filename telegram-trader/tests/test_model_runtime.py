"""Metadata diagnostics must not disclose payloads or change worker control flow."""

import asyncio
import importlib.util
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def runtime():
    path = Path(__file__).parents[1] / "condor-assets/telegram_agent/model_runtime.py"
    spec = importlib.util.spec_from_file_location("test_model_runtime_helper", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def event(name, **fields):
    return type(name, (), fields)()


async def test_stage_monotonic_and_return_control_unchanged(runtime, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    ticks = iter([10.0, 12.25])
    monkeypatch.setattr(runtime, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    reached = False
    async with runtime.stage("client.start"):
        reached = True
    assert reached
    assert [record.getMessage() for record in caplog.records] == [
        "Trader model phase=start stage=client.start tool=other",
        "Trader model phase=end stage=client.start tool=other duration=2.250000",
    ]


async def test_grouped_errors_are_types_only_and_original_exception_propagates(runtime, caplog):
    caplog.set_level(logging.INFO)
    nested = TimeoutError("SECRET URL?key=DO_NOT_LOG")
    nested.__cause__ = OSError("SECRET token")
    failure = ExceptionGroup("SECRET group prompt", [ValueError("SECRET argument"), nested])
    with pytest.raises(ExceptionGroup) as caught:
        async with runtime.stage("tool.call", tool="read_channel_history"):
            raise failure
    assert caught.value is failure
    assert set(runtime.error_types(failure)) == {
        "ExceptionGroup", "ValueError", "TimeoutError", "OSError",
    }
    assert "phase=error" in caplog.text
    assert "SECRET" not in caplog.text and "DO_NOT_LOG" not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


async def test_cancellation_is_not_swallowed_or_replaced(runtime, caplog):
    caplog.set_level(logging.INFO)
    entered = asyncio.Event()

    async def pending():
        async with runtime.stage("client.stop"):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(pending())
    await entered.wait()
    task.cancel("SECRET cancellation detail")
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()
    assert "types=CancelledError" in caplog.text
    assert "SECRET" not in caplog.text


def test_exception_cycles_and_large_groups_are_bounded(runtime):
    first, second = ValueError("secret"), OSError("secret")
    first.__cause__, second.__cause__ = second, first
    assert runtime.error_types(first) == ["ValueError", "OSError"]
    group = ExceptionGroup("secret", [ValueError("secret") for _ in range(10000)])
    assert runtime.error_types(group) == ["ExceptionGroup", "ValueError"]


async def test_unknown_stage_and_tool_cannot_be_log_injection(runtime, caplog):
    caplog.set_level(logging.INFO)
    async with runtime.stage("SECRET\nfake log", tool={"SECRET": "value"}):
        pass
    assert "stage=other tool=other" in caplog.text
    assert "SECRET" not in caplog.text


def test_event_payloads_redacted_and_chunks_not_logged_per_token(runtime, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    ticks = iter([100.0, 100.1, 100.2, 100.3, 100.4, 100.5, 100.6])
    monkeypatch.setattr(runtime, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    observer = runtime.EventObserver()
    for _ in range(1000):
        observer(event("TextChunk", text="SECRET response"))
        observer(event("ThoughtChunk", text="SECRET thought"))
    observer(event("Heartbeat", elapsed_seconds="SECRET"))
    observer(event("ToolCallEvent", title="read_channel_history", status="in_progress",
                   input={"SECRET": "argument"}, tool_call_id="SECRET call id"))
    observer(event("ToolCallUpdate", title="SECRET tool", status=["SECRET status"],
                   output="SECRET result"))
    observer(event("PromptDone", stop_reason="end_turn"))
    assert len(caplog.records) == 6
    assert "SECRET" not in caplog.text
    assert "event=ToolCallEvent status=in_progress tool=read_channel_history" in caplog.text
    assert "event=ToolCallUpdate status=other tool=other" in caplog.text
    assert "event=PromptDone status=end_turn tool=other elapsed=0.600000" in caplog.text


def test_unknown_event_names_are_not_logged_verbatim(runtime, caplog):
    caplog.set_level(logging.INFO)
    observer = runtime.EventObserver()
    for _ in range(3):
        observer(event("SECRET dynamic class name", text="SECRET"))
    assert len(caplog.records) == 1
    assert "event=other" in caplog.text and "SECRET" not in caplog.text
