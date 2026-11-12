"""Offline harness checks, NOT evidence of Gemini interpretation quality."""

import itertools
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from copy import deepcopy
from types import ModuleType, SimpleNamespace

import aiohttp
import pytest
from eval_single_exit_policy import (
    CASES,
    CandleExchange,
    assert_loopback,
    candidate_provider,
    check_decisions,
    check_plan,
    provider_config,
    replay_jsonl,
    run_case,
    wait_for_live_priority,
)

from trader.gate import Gate


class ScriptedInterpreter:
    """Submit an oracle via the actual loopback proposal endpoint, without a model."""

    def __init__(self, case, *, action_override=None, decision_patch=None):
        self.case, self.action_override = case, action_override
        self.decision_patch = decision_patch or {}

    @staticmethod
    async def api(session, method, path, **kwargs):
        url = os.environ["TELEGRAM_TRADER_URL"]
        assert_loopback(url)
        async with session.request(method, url + path, **kwargs) as response:
            response.raise_for_status()
            return await response.json()

    @asynccontextmanager
    async def interpret_current(self, job, _owner, session):
        actions = self.case.get("actions", [self.case.get("action")])
        result = {"decisions": []}
        for event, action in zip(job["events"], actions, strict=True):
            action = self.action_override or action
            item = {"event_id": event["event_id"], "action": action, "reason": "Offline harness fixture",
                        "evidence_message_ids": [event["message_id"]], "levels": self.case.get("levels", {})}
            if action == "open":
                item.update(symbol="INJ", side=self.case["side"])
            elif action not in {"ignore"}:
                item["target_trade_id"] = job["trades"][0]["id"]
            if "price" in self.case:
                item["price"] = self.case["price"]
            item.update(self.decision_patch)
            result["decisions"].append(item)
        await self.api(session, "POST", "/signals/proposals", json={"lease": job["lease"], "result": result})
        yield result


class ScriptedPlanner:
    @asynccontextmanager
    async def generate_plan(self, job, _owner):
        assert len(job["candles_5m"]) == len(job["candles_4h"]) == 120
        assert job["candles_5m"] != job["candles_4h"]
        plan = ({"sl": "9.6", "tp": "12"} if job["side"] == "long" else {"sl": "10.4", "tp": "8"})
        plan["rationale"] = "Offline 5m invalidation / 4h repeated structural level fixture"
        async with aiohttp.ClientSession() as session:
            await ScriptedInterpreter.api(session, "POST", "/plans/proposals", json={
                **{key: job[key] for key in ("trade_id", "version", "lease")}, "plan": plan})
        yield plan


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
async def test_constructed_fixture_semantics_and_real_engine_effects(case):
    report = await run_case(deepcopy(case), ScriptedInterpreter(case), ScriptedPlanner())
    assert report["passed"], report
    assert report["source_kind"] == "constructed"
    if case.get("planner"):
        assert report["planner_passed"] and set(report["plan"]) == {"sl", "tp", "rationale"}


async def test_incorrect_model_action_cannot_be_reported_as_pass():
    case = next(case for case in CASES if case["name"] == "tp1_now_full_close")
    report = await run_case(case, ScriptedInterpreter(case, action_override="ignore"), ScriptedPlanner())
    assert not report["passed"] and not report["semantic_passed"]


@pytest.mark.parametrize("name", ["breakeven_uses_own_entry", "numeric_sl_amends_only_stop"])
async def test_equivalent_targets_sl_changes_only_existing_stop(name):
    case = next(case for case in CASES if case["name"] == name)
    interpreter = ScriptedInterpreter(case, action_override="targets", decision_patch={
        "price": None, "levels": {"sl": case["price"]},
    })
    report = await run_case(case, interpreter, ScriptedPlanner())
    assert report["passed"], report
    assert report["result"]["decisions"][0]["action"] == "targets"
    assert len(report["writes"]) == 1
    stop_id = report["owned_before"]["exits"]["sl"]["id"]
    assert report["writes"][0][0:2] == ("PUT", f"/price_orders/amend/{stop_id}")
    assert report["positions_after"]["INJ_USDT"]["size"] == 14


@pytest.mark.parametrize("name", ["breakeven_uses_own_entry", "numeric_sl_amends_only_stop"])
@pytest.mark.parametrize("action", ["sl", "targets"])
async def test_sl_may_explicitly_identify_the_correct_owned_side(name, action):
    case = next(case for case in CASES if case["name"] == name)
    fields = {"side": case["owned"]}
    if action == "targets":
        fields.update(price=None, levels={"sl": case["price"]})
    report = await run_case(case, ScriptedInterpreter(case, action_override=action,
                                                     decision_patch=fields), ScriptedPlanner())
    assert report["passed"], report
    assert len(report["writes"]) == 1 and report["writes"][0][0] == "PUT"
    assert report["positions_after"]["INJ_USDT"]["size"] == 14


@pytest.mark.parametrize("patch", [
    {"levels": {"sl": "9.50", "tp": "11.50"}},
    {"levels": {"sl": "9.60"}},
    {"price": "9.50"},
    {"action": "sl", "price": "9.50"},
    {"levels": {"sl": "9.50", "tp1": "11.50"}},
    {"side": "short"},
])
def test_sl_equivalence_never_allows_extra_levels_or_unrelated_fields(patch):
    spec = next(case for case in CASES if case["name"] == "numeric_sl_amends_only_stop")
    decision = {"event_id": "event", "action": "targets", "target_trade_id": "owned",
                "reason": "offline", "evidence_message_ids": [101],
                "price": None, "levels": {"sl": "9.50"}} | patch
    with pytest.raises(AssertionError):
        check_decisions(spec, {"decisions": [decision]}, {"events": [{"event_id": "event"}]},
                        {"id": "owned"}, {101: {}})


@pytest.mark.parametrize("action", ["sl", "targets"])
async def test_wrong_side_sl_never_passes_even_if_engine_can_update_the_stop(action):
    case = next(case for case in CASES if case["name"] == "numeric_sl_amends_only_stop")
    fields = {"side": "short"}
    if action == "targets":
        fields.update(price=None, levels={"sl": case["price"]})
    report = await run_case(case, ScriptedInterpreter(case, action_override=action,
                                                     decision_patch=fields), ScriptedPlanner())
    assert not report["passed"] and not report["semantic_passed"]


async def test_extra_tp_in_sl_update_exposes_simulated_tp_side_effect_and_fails():
    case = next(case for case in CASES if case["name"] == "numeric_sl_amends_only_stop")
    interpreter = ScriptedInterpreter(case, action_override="targets", decision_patch={
        "price": None, "levels": {"sl": "9.50", "tp": "11.50"},
    })
    report = await run_case(case, interpreter, ScriptedPlanner())
    assert not report["passed"] and not report["semantic_passed"] and not report["execution_passed"]
    assert len(report["writes"]) == 2


async def test_incorrect_model_close_is_actually_simulated_and_exposes_side_effects():
    case = next(case for case in CASES if case["name"] == "completed_tp_report_noop")
    report = await run_case(case, ScriptedInterpreter(case, action_override="close"), ScriptedPlanner())
    assert not report["passed"] and not report["semantic_passed"] and not report["execution_passed"]
    assert report["positions_after"]["INJ_USDT"]["size"] == 0
    assert report["writes"], "A wrong model action must not be hidden by skipping simulation"


async def test_real_exchange_construction_is_disabled_inside_evaluation():
    class UnsafeInterpreter:
        @asynccontextmanager
        async def interpret_current(self, *_args):
            Gate(None, "never-a-real-key", "never-a-real-secret", testnet=False)
            yield None

    case = next(case for case in CASES if case["name"] == "unowned_exit_noop")
    report = await run_case(case, UnsafeInterpreter(), ScriptedPlanner())
    assert not report["passed"] and report["error_type"] == "AssertionError"
    assert report["writes"] == [] and report["positions_after"]["INJ_USDT"]["size"] == 0


@pytest.mark.parametrize("url", ["https://gate.io", "http://localhost:8000", "http://127.0.0.1"])
def test_tool_urls_must_be_the_explicit_loopback_server(url):
    with pytest.raises(AssertionError):
        assert_loopback(url)


def test_multi_timeframe_fixture_has_distinct_closed_candle_spacing():
    exchange = CandleExchange()
    for interval, seconds in (("5m", 300), ("4h", 14400)):
        rows = exchange.series[interval]
        assert all(right["t"] - left["t"] == seconds for left, right in itertools.pairwise(rows))
        assert rows[-1]["t"] + seconds <= time.time()
    assert exchange.series["5m"][0]["h"] != exchange.series["4h"][0]["h"]


def test_four_hour_target_cannot_silently_become_nearby_five_minute_target():
    with pytest.raises(AssertionError):
        check_plan({"side": "long", "entry": "10", "tick": "0.01"},
                   {"sl": "9.6", "tp": "10.3", "rationale": "near 5m target"}, None, None)


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("mutation", [None, "tp_side", "tp_contract", "sl_contract", "sl_rule", "sl_price_type"])
def test_plan_validator_checks_order_side_contract_and_mark_trigger(side, mutation):
    job = {"trade_id": "owned", "symbol": "INJ_USDT", "side": side, "entry": "10", "tick": "0.01"}
    plan = {"sl": "9.6" if side == "long" else "10.4", "tp": "12" if side == "long" else "8",
            "rationale": "Constructed validator fixture"}
    trade = {"exits": {"tp": {"id": "tp"}, "sl": {"id": "sl"}}, "plan_needed": False}
    engine = SimpleNamespace(store=SimpleNamespace(trade=lambda _trade_id: trade))
    target = {"contract": "INJ_USDT", "is_reduce_only": True, "size": -14 if side == "long" else 14,
              "left": 14, "price": plan["tp"], "status": "open"}
    stop = {"initial": {"contract": "INJ_USDT", "is_close": True, "is_reduce_only": True},
            "trigger": {"rule": 2 if side == "long" else 1, "price_type": 1, "price": plan["sl"]},
            "status": "open"}
    exchange = SimpleNamespace(orders={"entry": {}, "tp": target}, stops={"sl": stop})
    if mutation == "tp_side":
        target["size"] *= -1
    elif mutation == "tp_contract":
        target["contract"] = "SAFE_USDT"
    elif mutation == "sl_contract":
        stop["initial"]["contract"] = "SAFE_USDT"
    elif mutation == "sl_rule":
        stop["trigger"]["rule"] = 3 - stop["trigger"]["rule"]
    elif mutation == "sl_price_type":
        stop["trigger"]["price_type"] = 0
    if mutation is None:
        check_plan(job, plan, engine, exchange)
    else:
        with pytest.raises(AssertionError):
            check_plan(job, plan, engine, exchange)


async def test_saved_proposal_replay_preserves_source_and_uses_no_provider(tmp_path, monkeypatch):
    reports = []
    for case in CASES:
        fields = {"side": "long"} if case["name"] == "breakeven_uses_own_entry" else {}
        report = await run_case(case, ScriptedInterpreter(case, decision_patch=fields), ScriptedPlanner())
        assert report["passed"], report
        if case["name"] == "breakeven_uses_own_entry":
            report.update(passed=False, semantic_passed=False)  # Original oracle false negative.
        if case["name"] == "duplicate_opening_revision_noop":
            decision = report["result"]["decisions"][0]
            decision["event_id"] = decision["event_id"].rsplit(":", 1)[0] + ":" + "1" * 16
        reports.append(report)
    source, output = tmp_path / "original.jsonl", tmp_path / "re-audit.jsonl"
    source.write_text("\n".join(json.dumps(row) for row in [{"kind": "manifest"}, *reports]) + "\n")
    original = source.read_bytes()

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Offline replay must not load model/provider code or wait on live state")

    monkeypatch.setattr("eval_single_exit_policy.candidate_provider", forbidden)
    monkeypatch.setattr("eval_single_exit_policy.load", forbidden)
    monkeypatch.setenv("EVAL_PRIORITY_FILE", "/must-not-read-live-priority.json")
    assert await replay_jsonl(source, output) == 0
    assert source.read_bytes() == original
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert rows[-1] == {"kind": "summary", "total": len(CASES), "passed": len(CASES)}
    actual = {row["case"]: row for row in rows if "case" in row}
    sl = actual["breakeven_uses_own_entry"]
    assert sl["passed"] and sl["original_status"]["semantic_passed"] is False
    for old in reports:
        new = actual[old["case"]]
        expected = deepcopy(old["result"])
        for decision in expected["decisions"]:
            decision["event_id"] = new["event_id_mapping"][decision["event_id"]]
        assert new["result"] == expected  # No corrected action, price, target, side, or evidence.
        if old.get("plan"):
            assert new["plan"] == old["plan"]
    duplicate = actual["duplicate_opening_revision_noop"]
    assert any(before != after for before, after in duplicate["event_id_mapping"].items())
    assert "EVAL_PRIORITY_FILE" in os.environ
    with pytest.raises(FileExistsError):
        await replay_jsonl(source, output)
    with pytest.raises(AssertionError, match="overwrite"):
        await replay_jsonl(source, source)


@pytest.mark.parametrize("defect", ["missing_result", "wrong_target", "wrong_message", "missing_plan"])
async def test_saved_proposal_replay_never_invents_or_repairs_missing_decisions(tmp_path, defect):
    name = "market_long_and_4h_plan" if defect == "missing_plan" else "numeric_sl_amends_only_stop"
    case = next(case for case in CASES if case["name"] == name)
    report = await run_case(case, ScriptedInterpreter(case), ScriptedPlanner())
    assert report["passed"]
    if defect == "missing_result":
        del report["result"]
    elif defect == "missing_plan":
        del report["plan"]
    elif defect == "wrong_target":
        report["result"]["decisions"][0]["target_trade_id"] = "unowned-wrong-target"
    else:
        report["result"]["decisions"][0]["event_id"] = "-100123:999:" + "a" * 16
    source, output = tmp_path / "original.jsonl", tmp_path / "re-audit.jsonl"
    source.write_text(json.dumps(report) + "\n")
    assert await replay_jsonl(source, output) == 1
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert rows[-1] == {"kind": "summary", "total": 1, "passed": 0}
    if defect != "missing_plan":
        assert rows[1]["writes"] == []


def test_provider_config_rejects_non_provider_secrets_without_printing_them():
    path = SimpleNamespace(read_text=lambda: '{"model":"custom:gemini","base_url":"http://llm:80/v1",'
                                            '"api_key":"fixture","GATE_API_KEY":"must-not-load"}')
    with pytest.raises(AssertionError, match="LLM-only"):
        provider_config(path)


def test_provider_config_rejects_inherited_exchange_credentials(monkeypatch):
    monkeypatch.setenv("GATE_API_SECRET", "do-not-use")
    path = SimpleNamespace(read_text=lambda: '{"model":"custom:gemini","base_url":"http://llm:80/v1",'
                                            '"api_key":"fixture"}')
    with pytest.raises(AssertionError):
        provider_config(path)


def test_existing_isolated_provider_agent_key_is_supported(monkeypatch):
    for key in tuple(os.environ):
        if key.startswith(("GATE_", "TELEGRAM_API_", "TELEGRAM_SESSION", "TG_")) or key in {
            "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "BOT_TOKEN", "TRADER_TOKEN", "DATABASE_URL",
        }:
            monkeypatch.delenv(key)
    path = SimpleNamespace(read_text=lambda: '{"agent_key":"custom:gemini","base_url":"http://llm:80/v1",'
                                            '"api_key":"fixture"}')
    assert provider_config(path)["model"] == "custom:gemini"


def test_candidate_agent_uses_candidate_instructions_and_only_proposal_mcp(monkeypatch):
    from eval_single_exit_policy import ROOT

    class AgentStore:
        def get(self, slug):
            raise AssertionError("Must not read an existing AgentStore or production prompt")

    llm = ModuleType("condor.runtime.llm_client")
    llm.resolve_custom_endpoint = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("Must not read saved provider preferences"))
    llm.build_llm_client = lambda key, **kwargs: (key, llm.resolve_custom_endpoint(key), kwargs)
    memory = ModuleType("condor.memory")
    memory.domain_context = lambda *_args: ["old memory must not enter candidate"]
    agent_module = ModuleType("condor.agents.agent")
    agent_module.Agent = SimpleNamespace
    agent_module.AgentStore = AgentStore
    agent_module.parse_frontmatter = lambda text: ({"name": "TelegramAgent"}, text.split("---", 2)[2])
    runtime = ModuleType("condor.runtime")
    runtime.llm_client = llm
    condor = ModuleType("condor")
    condor.memory = memory
    for name, value in {
        "condor": condor, "condor.agents": ModuleType("condor.agents"),
        "condor.agents.agent": agent_module, "condor.memory": memory,
        "condor.runtime": runtime, "condor.runtime.llm_client": llm,
    }.items():
        monkeypatch.setitem(sys.modules, name, value)
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:10001")
    config = {"model": "custom:gemini", "base_url": "http://isolated-llm:80/v1", "api_key": "offline-only"}
    with candidate_provider(config):
        agent = AgentStore().get("telegram_agent")
        assert "single_tp_4h_v1" in agent.instructions and agent.agent_key == config["model"]
        assert memory.domain_context("telegram_agent", 1) == []
        servers = [{"name": "telegram_exit_plan", "args": [str(ROOT / "condor-assets/telegram_agent/channel_tools.py")]}]
        _, endpoint, _ = llm.build_llm_client(config["model"], mcp_servers=servers)
        assert endpoint == (config["base_url"], config["api_key"])
        with pytest.raises(AssertionError):
            llm.build_llm_client(config["model"], mcp_servers=[{"name": "hummingbot"}])


async def test_priority_gate_reuses_existing_waiter_only_when_configured(monkeypatch):
    calls = []

    async def waiter():
        calls.append("wait")

    module = ModuleType("eval_two_month_channel")
    module.wait_for_priority = waiter
    monkeypatch.setitem(sys.modules, "eval_two_month_channel", module)
    monkeypatch.delenv("EVAL_PRIORITY_FILE", raising=False)
    await wait_for_live_priority()
    assert calls == []
    monkeypatch.setenv("EVAL_PRIORITY_FILE", "/unused/offline-priority.json")
    await wait_for_live_priority()
    assert calls == ["wait"]
