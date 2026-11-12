"""Policy-specific model/tool contracts; all exchange/model calls are offline fakes."""

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from test_exit_planner import PromptDone, install_client

from trader.gate import Gate

AGENT = Path(__file__).parents[1] / "condor-assets/telegram_agent"


def load_asset(relative):
    spec = importlib.util.spec_from_file_location(
        "single_exit_" + relative.replace("/", "_").replace(".", "_"), AGENT / relative
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("interval", [None, "5m", "4h"])
async def test_candle_timeframe_reuses_public_gate_get_with_same_history_size(interval):
    gate = Gate(None, "unused-offline", "unused-offline")
    candles = [{"t": 1, "o": "10", "h": "11", "l": "9", "c": "10"}]
    gate.request = AsyncMock(return_value=candles)
    result = await gate.candles("INJ_USDT", **({"interval": interval} if interval else {}))
    assert result is candles
    gate.request.assert_awaited_once_with(
        "GET", "/candlesticks",
        params={"contract": "INJ_USDT", "interval": interval or "5m", "limit": 120},
        private=False,
    )


async def test_single_target_tool_uses_supplied_schema_without_partial_target_fields(monkeypatch):
    tools = load_asset("channel_tools.py")
    schema = {
        "type": "object",
        "properties": {name: {"type": "string"} for name in ("sl", "tp", "rationale")},
        "required": ["sl", "tp", "rationale"],
        "additionalProperties": False,
    }
    job = {"trade_id": "trade", "version": 1, "lease": "fixture"}
    plan = {"sl": "11", "tp": "8", "rationale": "4h support, 5m invalidation"}
    monkeypatch.setenv("EXIT_PLAN_JOB", json.dumps(job))
    monkeypatch.setenv("EXIT_PLAN_SCHEMA", json.dumps(schema))
    tools.read = AsyncMock(return_value={"accepted": True})

    listed = await tools.list_tools()
    assert [tool.name for tool in listed] == ["submit_exit_plan"]
    assert listed[0].input_schema == schema
    assert "TP1" not in listed[0].description
    assert "single full-position TP" in listed[0].description
    assert await tools.execute_tool("submit_exit_plan", plan) == {"accepted": True}
    tools.read.assert_awaited_once_with("/plans/proposals", body={**job, "plan": plan})
    with pytest.raises(ValueError, match="only the assigned exit-plan tool"):
        await tools.execute_tool("read_execution_state", {})


async def test_planner_routes_both_timeframes_and_only_lease_bound_submission_tool(monkeypatch):
    planner = load_asset("routines/exit_planner.py")
    captured = {}

    class Client:
        async def start(self):
            pass

        async def stop(self):
            pass

        async def prompt_stream(self, prompt):
            captured["prompt"] = prompt
            yield PromptDone()

    client = Client()
    install_client(monkeypatch, client)

    def build(*args, **kwargs):
        captured.update(kwargs)
        return client

    monkeypatch.setattr(sys.modules["condor.runtime.llm_client"], "build_llm_client", build)
    job = {
        "trade_id": "trade", "version": 3, "lease": "fixture",
        "exit_policy": "single_tp_4h_v1", "output_schema": {"required": ["sl", "tp", "rationale"]},
        "candles_5m": [{"t": 100, "c": "10"}],
        "candles_4h": [{"t": 200, "c": "8"}],
    }
    await planner.request_plan(job, 1, asyncio.Event())
    prompt_job = json.loads(captured["prompt"].split("\n", 2)[-1])
    assert prompt_job == job
    mounted = captured["mcp_servers"]
    assert len(mounted) == 1
    assert mounted[0]["name"] == "telegram_exit_plan"
    env = {item["name"]: item["value"] for item in mounted[0]["env"]}
    assert json.loads(env["EXIT_PLAN_SCHEMA"]) == job["output_schema"]
    assert json.loads(env["EXIT_PLAN_JOB"]) == {
        "trade_id": "trade", "version": 3, "lease": "fixture"
    }
    policy = captured["system_prompt"]
    assert "submit only sl, tp and rationale" in policy
    assert "Use candles_5m for the unchanged SL" in policy
    assert "use candles_4h" in policy
    assert "resistance on the profit side" in policy
    assert "4-hour support on the profit side" in policy
    assert "Do not change the SL method to 4-hour structure" in policy
    assert "when 4-hour evidence is inadequate" in policy
    assert "tp1_70_tp2_full_v1" not in policy
    assert "tp1_fraction" not in policy


async def test_accepted_single_target_plan_recovery_never_calls_model(monkeypatch):
    planner = load_asset("routines/exit_planner.py")
    plan = {"sl": "11", "tp": "8", "rationale": "accepted 4h support"}
    planner.generate_plan = AsyncMock(side_effect=AssertionError("no new model plan"))
    async with planner.plan_for_job({"accepted_plan": plan}, 1) as accepted:
        assert accepted is plan
    planner.generate_plan.assert_not_called()


@pytest.mark.parametrize("fragment", [
    "The entry_exit_policy is single_tp_4h_v1: levels uses sl and one tp",
    "any explicit IMMEDIATE TP/TP1/TP2",
    "instruction means close: market full close",
    "TP1 目标设在 0.036 is targets with levels.tp",
    "Different simultaneous TP1/TP2 prices\nmean ignore",
    "Reports that TP was reached/completed remain ignore",
    "SL离场/止损离场 means close: market full close",
    "SL设置xxx instead means sl with that numeric",
    "当前价格可以TP1/TP2 is a current take-profit instruction",
    "entry_price from execution state",
    "not a guarantee of net breakeven after fees/funding",
    "There are no partial stages or separate TP1/TP2 actions",
    "An explicit partial quantity is unsupported; choose ignore",
])
def test_interpreter_exposes_single_target_intent_boundaries(fragment):
    assert fragment in load_asset("routines/signal_interpreter.py").POLICY


def test_agent_context_explains_single_schema_without_partial_target_policy():
    document = (AGENT / "AGENT.md").read_text()
    assert "The `single_tp_4h_v1` policy requires only" in document
    assert "`sl`, `tp`, `rationale`" in document
    assert "tp1_70_tp2_full_v1" not in document
    assert "tp1_fraction" not in document
    assert "The SL method is unchanged" in document
    assert "4-hour support below entry for a short" in document
    assert "An immediate \"SL离场\" also means market full close" in document
    assert "submit_exit_plan` with exactly these fields" not in document
