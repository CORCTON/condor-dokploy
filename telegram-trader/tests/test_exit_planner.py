"""Free-text model output must never become an exit plan."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer


async def test_valid_json_text_without_a_tool_call_is_rejected(monkeypatch):
    path = Path(__file__).parents[1] / "condor-assets/telegram_agent/routines/exit_planner.py"
    spec = importlib.util.spec_from_file_location("test_exit_planner_candidate", path)
    planner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(planner)

    class PromptDone:
        stop_reason = "end_turn"

    class Client:
        async def start(self):
            pass

        async def stop(self):
            pass

        async def prompt_stream(self, prompt):
            yield SimpleNamespace(
                text='{"sl":"11","tp1":"9","tp2":"8","tp1_fraction":"0.5","rationale":"valid text is still not a tool call"}'
            )
            yield PromptDone()

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
        "condor.runtime.llm_client": {"build_llm_client": lambda *args, **kwargs: Client()},
    }
    for name, members in modules.items():
        module = ModuleType(name)
        module.__dict__.update(members)
        monkeypatch.setitem(sys.modules, name, module)

    async def no_proposal(request):
        return web.json_response(None)

    app = web.Application()
    app.router.add_get("/plans/trade/proposal", no_proposal)
    async with TestServer(app) as server:
        monkeypatch.setenv("TELEGRAM_TRADER_URL", str(server.make_url("/")).rstrip("/"))
        monkeypatch.setenv("TRADER_TOKEN", "fixture-only")
        with pytest.raises(ValueError, match="without a validated exit-plan tool call"):
            await planner.generate_plan(
                {"trade_id": "trade", "version": 1, "lease": "lease", "output_schema": {}}, 1
            )
