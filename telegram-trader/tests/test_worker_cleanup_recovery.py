"""A stuck model cleanup exits its worker; no exchange command is replayed."""

import asyncio
import contextlib
import importlib.util
import sys
import time
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from test_trader import Exchange, plan, signal

from trader.app import make_app
from trader.engine import Engine
from trader.store import Store

AGENT = Path(__file__).parents[1] / "condor-assets/telegram_agent"


def load(path):
    spec = importlib.util.spec_from_file_location("cleanup_recovery_" + path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("natural_cleanup", [False, True])
@pytest.mark.parametrize("consumer_cancels", [0, 2])
async def test_stuck_drain_deadline_survives_repeated_consumer_cancellation(
    natural_cleanup, consumer_cancels
):
    runtime = load(AGENT / "model_runtime.py")
    started, release = asyncio.Event(), asyncio.Event()
    cleanup_cancelled = False

    async def model():
        nonlocal cleanup_cancelled
        try:
            if not natural_cleanup:
                await asyncio.Event().wait()
        finally:
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cleanup_cancelled = True
                raise

    task = asyncio.create_task(model())
    await asyncio.sleep(0)
    consumer = asyncio.create_task(runtime.drain(task, started, timeout=0.04))
    try:
        await asyncio.sleep(0)  # Begin the drain before cancelling its consumer.
        await asyncio.wait_for(started.wait(), 1)
        for _ in range(consumer_cancels):
            consumer.cancel()
            await asyncio.sleep(0)
        with pytest.raises(runtime.WorkerRestartRequired):
            await asyncio.wait_for(consumer, 1)
        assert not task.done() and not cleanup_cancelled
        assert task.cancelling() == (0 if natural_cleanup else 1)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.gather(consumer, return_exceptions=True)


async def test_completed_cleanup_preserves_consumer_cancellation_without_restart():
    runtime = load(AGENT / "model_runtime.py")
    started, release = asyncio.Event(), asyncio.Event()

    async def model():
        started.set()
        await release.wait()

    task = asyncio.create_task(model())
    await started.wait()
    consumer = asyncio.create_task(runtime.drain(task, started, timeout=1))
    try:
        await asyncio.sleep(0)
        consumer.cancel()
        await asyncio.sleep(0)
        consumer.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not task.cancelling()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await consumer
        assert task.done() and not task.cancelled()
    finally:
        release.set()
        await asyncio.gather(task, consumer, return_exceptions=True)


class SimulatedProcessExit(BaseException):
    pass


@pytest.mark.parametrize("kind", ["signal_interpreter", "exit_planner"])
@pytest.mark.parametrize("cancel_during_cleanup", [False, True])
async def test_worker_applies_accepted_proposal_once_then_exits_before_claiming_again(
    monkeypatch, kind, cancel_during_cleanup
):
    module = load(AGENT / "routines" / (kind + ".py"))
    store, exchange = Store(":memory:"), Exchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    monkeypatch.setenv("ADMIN_USER_ID", "123")
    monkeypatch.setenv("TRADER_LLM_READY", "true")
    monkeypatch.setenv("TRADER_TOKEN", "cleanup-recovery-only")
    claim = "/signals/claim" if kind == "signal_interpreter" else "/jobs/claim"
    apply = "/signals/decisions" if kind == "signal_interpreter" else "/plans"
    requests, generations = [], []
    release, second_claim = asyncio.Event(), asyncio.Event()
    draining = asyncio.Event()
    restarts = []
    if kind == "signal_interpreter":
        store.event(123, 1, "#INJ 当前价格开空", None, time.time(), True)
    else:
        await signal((store, exchange, engine))

    @web.middleware
    async def record(request, handler):
        requests.append(request.path)
        response = await handler(request)
        if requests.count(claim) == 2:
            second_claim.set()
        return response

    original_drain = module.runtime["drain"]

    async def short_drain(task, started):
        draining.set()
        await original_drain(task, started, timeout=0.03)

    def restart():
        restarts.append(True)
        raise SimulatedProcessExit

    async def generate(job, owner, *args):
        started = args[-1]
        generations.append(asyncio.current_task())
        headers = {"Authorization": "Bearer cleanup-recovery-only"}
        if kind == "signal_interpreter":
            payload = {"lease": job["lease"], "result": {"decisions": [{
                "event_id": job["events"][0]["event_id"], "action": "open",
                "symbol": "INJ", "side": "short", "reason": "explicit test signal",
                "evidence_message_ids": [1],
            }]}}
            endpoint = "/signals/proposals"
        else:
            payload = {key: job[key] for key in ("trade_id", "version", "lease")}
            payload["plan"] = plan().model_dump(mode="json")
            endpoint = "/plans/proposals"
        async with (
            aiohttp.ClientSession() as client,
            client.post(
                str(server.make_url(endpoint)), headers=headers, json=payload
            ) as response,
        ):
            response.raise_for_status()
        started.set()  # Natural model completion has entered a permanently stuck stop().
        await release.wait()

    monkeypatch.setitem(module.runtime, "drain", short_drain)
    monkeypatch.setitem(module.runtime, "restart_worker", restart)
    monkeypatch.setattr(module, "generate_decisions" if kind == "signal_interpreter"
                        else "request_plan", generate)
    app = make_app(engine)
    app.middlewares.append(record)
    running = restarted = None
    try:
        async with TestServer(app) as server:
            monkeypatch.setenv("TELEGRAM_TRADER_URL", str(server.make_url("/")).rstrip("/"))
            running = asyncio.create_task(module.run_worker())
            await asyncio.wait_for(draining.wait(), 2)
            if cancel_during_cleanup:
                for _ in range(2):
                    running.cancel()
                    await asyncio.sleep(0)
            with pytest.raises(SimulatedProcessExit):
                await asyncio.wait_for(running, 2)
            assert restarts == [True]
            assert requests.count(claim) == requests.count(apply) == 1
            assert not any(path.endswith("/fail") for path in requests)
            assert not engine.paused() and len(store.trades()) == 1
            trade = store.trades()[0]
            assert trade["filled"] == 14 and trade["status"] == "working"
            if kind == "exit_planner":
                assert not trade["plan_needed"]
                assert set(trade["exits"]) == {"sl", "tp1", "tp2"}
            writes = list(exchange.writes)
            # The real process exit ends this task. Release the local fake only
            # after proving no second job ran, then emulate a fresh worker.
            release.set()
            await asyncio.gather(*generations)
            restarted = asyncio.create_task(module.run_worker())
            await asyncio.wait_for(second_claim.wait(), 2)
            restarted.cancel()
            with pytest.raises(asyncio.CancelledError):
                await restarted
            assert len(generations) == 1 and exchange.writes == writes
            assert not store.unknowns() and not engine.paused()
    finally:
        release.set()
        if running is not None:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
        if restarted is not None:
            restarted.cancel()
            await asyncio.gather(restarted, return_exceptions=True)
        await asyncio.gather(*generations, return_exceptions=True)
        store.close()


async def test_restart_worker_exits_even_when_asyncio_shutdown_would_hang():
    code = (
        "import asyncio,runpy\n"
        f"runtime=runpy.run_path({str(AGENT / 'model_runtime.py')!r})\n"
        "async def stuck():\n"
        " try: await asyncio.Event().wait()\n"
        " finally: await asyncio.Event().wait()\n"
        "async def main():\n"
        " asyncio.create_task(stuck())\n"
        " await asyncio.sleep(0)\n"
        " runtime['restart_worker']()\n"
        " print('UNSAFE_CONTINUATION',flush=True)\n"
        "asyncio.run(main())\n"
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-c", code,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 3)
        assert process.returncode == 70
        assert b"UNSAFE_CONTINUATION" not in stdout
        assert b"supervised recovery" in stderr
    finally:
        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
        await process.wait()
