"""Metadata-only diagnostics and bounded cleanup for our isolated model workers."""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

import aiohttp

log = logging.getLogger(__name__)
STAGES = {"client.start", "client.stop", "model.turn", "tool.call",
          "proposal.wait", "proposal.apply"}
CLEANUP_TIMEOUT_SECONDS = 30
HEARTBEAT_INTERVAL_SECONDS = 10
HEARTBEAT_TIMEOUT_SECONDS = 5
TOOLS = {"read_channel_history", "read_channel_message", "view_channel_image",
         "read_execution_state", "submit_signal_decisions", "submit_exit_plan"}
EVENTS = {"Heartbeat", "ToolCallEvent", "ToolCallUpdate", "PromptDone",
          "TextChunk", "ThoughtChunk"}
STATUSES = {"pending", "in_progress", "completed", "failed", "blocked",
            "end_turn", "error", "timeout", "cancelled"}


def _known(value, allowed):
    return value if isinstance(value, str) and value in allowed else "other"


def error_types(error):
    """Bounded unique exception types, including grouped errors and causal chains."""
    pending, seen, names = [error], set(), []
    while pending and len(seen) < 32:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        name = type(current).__name__
        if name not in names:
            names.append(name)
        if isinstance(current, BaseExceptionGroup):
            pending.extend(reversed(current.exceptions[:32]))
        cause = current.__cause__ or current.__context__
        if cause is not None:
            pending.append(cause)
    return names


@asynccontextmanager
async def stage(name, *, tool=None):
    """Time a caller-owned await; exceptions and cancellation pass through unchanged."""
    name, tool = _known(name, STAGES), _known(tool, TOOLS)
    started = time.monotonic()
    log.info("Trader model phase=start stage=%s tool=%s", name, tool)
    try:
        yield
    except BaseException as exc:
        log.warning("Trader model phase=error stage=%s tool=%s duration=%.6f types=%s",
                    name, tool, time.monotonic() - started, ",".join(error_types(exc)))
        raise
    else:
        log.info("Trader model phase=end stage=%s tool=%s duration=%.6f",
                 name, tool, time.monotonic() - started)


class EventObserver:
    """Observe official events, not token text; create one observer per model turn.

    A completed ToolCallUpdate is an adapter event, NOT proof of tool execution.
    Actual tool completion is measured separately with stage('tool.call').
    """

    def __init__(self):
        self.started = time.monotonic()
        self.seen_chunks = set()

    def __call__(self, event):
        name = _known(type(event).__name__, EVENTS)
        if name in {"TextChunk", "ThoughtChunk", "other"}:
            if name in self.seen_chunks:
                return
            self.seen_chunks.add(name)
        status = _known(getattr(event, "stop_reason" if name == "PromptDone" else "status", None),
                        STATUSES)
        tool = _known(getattr(event, "title", None), TOOLS)
        log.info("Trader model phase=event event=%s status=%s tool=%s elapsed=%.6f",
                 name, status, tool, time.monotonic() - self.started)


class WorkerRestartRequired(RuntimeError):
    """The isolated worker cannot safely accept another job with a stuck client."""


async def send_heartbeats(session, name):
    """Prove the isolated worker event loop is alive, including during a long model turn."""
    previous = None
    while True:
        state = "waiting"
        if os.getenv("TRADER_LLM_READY") == "true":
            try:
                async with (
                    asyncio.timeout(HEARTBEAT_TIMEOUT_SECONDS),
                    session.post(
                        os.environ["TELEGRAM_TRADER_URL"].rstrip("/")
                        + f"/workers/{name}/heartbeat",
                        headers={"Authorization": "Bearer " + os.environ["TRADER_TOKEN"]},
                        timeout=aiohttp.ClientTimeout(total=HEARTBEAT_TIMEOUT_SECONDS),
                    ) as response,
                ):
                    response.raise_for_status()
                    await response.read()
                state = "ok"
            except (aiohttp.ClientError, TimeoutError):
                state = "unavailable"
        if state != previous:
            log.log(logging.WARNING if state == "unavailable" else logging.INFO,
                    "Trader worker heartbeat worker=%s state=%s", name, state)
            previous = state
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)


@asynccontextmanager
async def worker_heartbeat(session, name):
    if name not in {"interpreter", "planner"}:
        raise ValueError("unknown worker")
    task = asyncio.create_task(send_heartbeats(session, name), name=f"trader-{name}-heartbeat")
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def drain(task, cleanup_started, *, timeout=CLEANUP_TIMEOUT_SECONDS):
    """Drain once, preserving cancellation; a stuck SDK requires process teardown."""
    if not task.done() and not task.cancelling() and not cleanup_started.is_set():
        task.cancel()
    cleanup = asyncio.gather(task, return_exceptions=True)
    deadline = asyncio.get_running_loop().time() + timeout
    cancelled = False
    while not cleanup.done():
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise WorkerRestartRequired("model cleanup exceeded its process recovery budget")
        try:
            await asyncio.wait({cleanup}, timeout=remaining)
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError


def restart_worker():
    """Called only at the dedicated worker boundary, never from a shared chat client."""
    log.critical("Trader model cleanup stalled; exiting isolated worker for supervised recovery")
    # asyncio.run waits for all pending tasks during shutdown, including the
    # stuck cleanup. The worker supervisor restarts us; MCP children have a
    # Linux parent-death guard, and proposals cannot execute exchange orders.
    os._exit(70)
