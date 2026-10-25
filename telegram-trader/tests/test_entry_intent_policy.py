"""Offline policy delivery/oracle tests; semantic model validation remains opt-in."""

import asyncio
import sys

import eval_comprehensive_channel as evaluation
import pytest
from comprehensive_cases import CASES as COMPREHENSIVE_CASES
from entry_intent_cases import CASES
from replay_comprehensive_decisions import ScriptedInterpreter
from test_interpreter import delayed_client as delayed_client  # noqa: PLC0414 -- pytest fixture
from test_interpreter import interpreter as interpreter  # noqa: PLC0414 -- pytest fixture

from trader.models import Decision


async def test_entry_policy_uses_existing_model_call_and_restricted_tool_path(
    interpreter, delayed_client, monkeypatch,
):
    client = delayed_client
    client.prompt_finishes = True
    client.cleanup_release.set()
    calls = []

    def factory(*args, **kwargs):
        calls.append((args, kwargs))
        return client

    async def no_op():
        return None

    async def accepted_proposal(*args):
        return {"decisions": [{"action": "ignore"}]}

    client.on_prompt = no_op
    monkeypatch.setattr(sys.modules["condor.runtime.llm_client"], "build_llm_client", factory)
    monkeypatch.setattr(interpreter, "api", accepted_proposal)
    await interpreter.generate_decisions(
        {"lease": "isolated-policy-delivery", "events": [], "output_schema": {}},
        1, None, asyncio.Event())
    assert len(calls) == 1  # No extra classifier/verifier model is introduced.
    options = calls[0][1]
    assert options["system_prompt"] == interpreter.POLICY
    assert len(options["mcp_servers"]) == 1
    tools = options["mcp_servers"][0]
    assert tools["name"] == "telegram_channel_context"
    assert tools["args"][0].endswith("/telegram_agent/channel_tools.py")


def test_entry_goldens_use_real_ids_and_preserve_positive_counterexamples():
    assert len({item["name"] for item in CASES}) == len(CASES)
    assert all(item["name"].startswith("real_") and not item["trades"] for item in CASES)
    assert all(len(item["events"]) == 1 and isinstance(item["events"][0], int) for item in CASES)
    assert {item["events"][0] for item in CASES if item["expected"][0]["action"] == "open"} == {
        15309, 15342, 15333, 15368,
    }


def test_edited_entry_cases_distinguish_first_submission_from_real_duplicate():
    cases = {item["name"]: item for item in COMPREHENSIVE_CASES}
    first = cases["derived_first_edit_before_entry"]
    assert first["events"][0]["edited"] and first["expected"][0]["action"] == "open"
    assert first["trades"] == [] and not first.get("execution_review")
    repeated = cases["derived_edited_entry_already_owned"]
    assert repeated["events"][0]["edited"]
    assert repeated["trades"][0]["source"] == 20000  # run_case's first synthetic event ID
    assert set(repeated["expected"][0]["action"]) <= {"ignore", "review"}


@pytest.mark.parametrize("name,action", [
    ("derived_first_edit_before_entry", "open"),
    ("derived_edited_entry_already_owned", "ignore"),
])
async def test_edited_entry_fixtures_exercise_actual_http_and_duplicate_safety(
    name, action, interpreter, monkeypatch,
):
    monkeypatch.setenv("TRADER_TOKEN", "isolated-edit-policy-test")
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    spec = next(item for item in COMPREHENSIVE_CASES if item["name"] == name)
    fields = {"event_id": f"{evaluation.CHANNEL}:20000:old", "action": action,
              "reason": "offline scripted fixture, not model inference", "evidence_message_ids": [20000]}
    if action == "open":
        fields.update(symbol="INJ", side="short", price=None)
    saved = {"case": name, "result": {"decisions": [fields]}}
    report = await evaluation.run_case(
        spec, ScriptedInterpreter(saved, interpreter), None, {}, {}, 1)
    assert report["passed"], report
    assert len(report["owned_after"]) == 1
    if action == "ignore":
        assert report["exchange_writes"] == []


@pytest.mark.parametrize("spec", CASES, ids=lambda case: case["name"])
def test_entry_oracles_detect_spurious_open_and_wrong_order_type(spec):
    expected = spec["expected"][0]
    fields = {"event_id": "isolated", "action": "open", "reason": "offline oracle test",
              "evidence_message_ids": spec["events"], "symbol": expected.get("symbol", "CRCL"),
              "side": expected.get("side", "short"), "price": None}
    actual = Decision.model_validate(fields)
    messages = {spec["events"][0]: {}}
    if expected["action"] == "open":
        evaluation.assert_decision(actual, expected, {}, messages)
        with pytest.raises(AssertionError):
            evaluation.assert_decision(actual.model_copy(update={"price": "10"}), expected, {}, messages)
    else:
        with pytest.raises(AssertionError):
            evaluation.assert_decision(actual, expected, {}, messages)
