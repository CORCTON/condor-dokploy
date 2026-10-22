"""Real MCP transport against a fixture channel, with no model or exchange writes."""

import json
import os
import sys
import time
from pathlib import Path

from aiohttp.test_utils import TestServer
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from test_channel import TelegramFixture
from test_trader import Exchange, plan, signal

from trader.app import make_app
from trader.channel import ChannelReader
from trader.engine import Engine
from trader.signals import SignalInbox
from trader.store import Store


async def test_stdio_tools_read_only_complete_history_and_image(monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "mcp-fixture-only")
    store = Store(":memory:")
    channel = ChannelReader()
    channel.client, channel.peer = TelegramFixture(), "allowlisted-channel"
    try:
        async with TestServer(make_app(Engine(store, None), channel)) as http:
            params = StdioServerParameters(
                command=sys.executable,
                args=[
                    str(Path(__file__).parents[1] / "condor-assets/telegram_agent/channel_tools.py")
                ],
                env={**os.environ, "TELEGRAM_TRADER_URL": str(http.make_url("/")).rstrip("/")},
            )
            async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                assert {t.name for t in tools.tools} == {
                    "read_channel_history",
                    "read_channel_message",
                    "view_channel_image",
                    "read_execution_state",
                }
                history = await session.call_tool("read_channel_history", {"limit": 2})
                assert not history.isError
                assert "A" * 5000 in history.content[0].text
                image = await session.call_tool("view_channel_image", {"message_id": 3})
                assert not image.isError
                assert any(c.type == "image" and c.mimeType == "image/png" for c in image.content)
                denied = await session.call_tool("read_channel_message", {"message_id": -1})
                assert denied.isError
                assert not store.trades() and not store.queued()
    finally:
        store.close()


async def test_typed_proposal_tool_rejects_bad_fields_without_exchange_writes(monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "mcp-fixture-only")
    store = Store(":memory:")
    engine = Engine(store, None)
    inbox = SignalInbox(engine)
    event_id = store.event(1, 1, "早上好", None, time.time(), True)
    job = await inbox.claim()
    decision = {
        "action": "ignore",
        "event_id": event_id,
        "reason": "greeting",
        "evidence_message_ids": [1],
    }
    try:
        async with TestServer(make_app(engine)) as http:
            params = StdioServerParameters(
                command=sys.executable,
                args=[
                    str(Path(__file__).parents[1] / "condor-assets/telegram_agent/channel_tools.py")
                ],
                env={
                    **os.environ,
                    "TELEGRAM_TRADER_URL": str(http.make_url("/")).rstrip("/"),
                    "SIGNAL_LEASE": job["lease"],
                    "SIGNAL_SCHEMA": json.dumps(job["output_schema"]),
                },
            )
            async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
                await session.initialize()
                assert "submit_signal_decisions" in {
                    t.name for t in (await session.list_tools()).tools
                }
                bad = await session.call_tool(
                    "submit_signal_decisions", {"decisions": [dict(decision, **{" levels": {}})]}
                )
                assert bad.isError and inbox.proposal(job["lease"]) is None
                good = await session.call_tool("submit_signal_decisions", {"decisions": [decision]})
                assert not good.isError and inbox.proposal(job["lease"]) is not None
                assert (
                    not store.trades()
                    and store.db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0
                )
                store.event(1, 2, "新消息", None, time.time(), True)
                stale = await session.call_tool(
                    "submit_signal_decisions", {"decisions": [decision]}
                )
                assert stale.isError
    finally:
        store.close()


async def test_exit_plan_tool_only_stages_valid_fresh_arguments(monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "mcp-fixture-only")
    store, exchange = Store(":memory:"), Exchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    await signal((store, exchange, engine))
    job = await engine.claim_plan()
    args = [job[key] for key in ("trade_id", "version", "lease")]
    writes = len(exchange.writes)
    try:
        async with TestServer(make_app(engine)) as http:
            params = StdioServerParameters(
                command=sys.executable,
                args=[
                    str(Path(__file__).parents[1] / "condor-assets/telegram_agent/channel_tools.py")
                ],
                env={
                    **os.environ,
                    "TELEGRAM_TRADER_URL": str(http.make_url("/")).rstrip("/"),
                    "EXIT_PLAN_JOB": json.dumps(
                        {k: job[k] for k in ("trade_id", "version", "lease")}
                    ),
                    "EXIT_PLAN_SCHEMA": json.dumps(job["output_schema"]),
                },
            )
            async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
                await session.initialize()
                assert {tool.name for tool in (await session.list_tools()).tools} == {
                    "submit_exit_plan"
                }
                values = plan().model_dump(mode="json")
                bad = await session.call_tool("submit_exit_plan", {**values, " sl": "11"})
                assert bad.isError and engine.plan_proposal(*args) is None
                bad = await session.call_tool("submit_exit_plan", {**values, "sl": "9"})
                assert bad.isError and engine.plan_proposal(*args) is None
                bad = await session.call_tool("submit_exit_plan", {**values, "sl": "11.001"})
                assert bad.isError and engine.plan_proposal(*args) is None
                good = await session.call_tool("submit_exit_plan", values)
                assert not good.isError and engine.plan_proposal(*args) == values
                assert len(exchange.writes) == writes
                await signal((store, exchange, engine), "SL 放在10.50", message=2, reply=1)
                writes = len(exchange.writes)
                stale = await session.call_tool("submit_exit_plan", values)
                assert stale.isError and len(exchange.writes) == writes
    finally:
        store.close()
