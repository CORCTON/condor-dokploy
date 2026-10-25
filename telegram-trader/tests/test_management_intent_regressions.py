"""Offline counterexamples from final-v2; not fresh model/vision measurements.

The predeclared EIGEN/EDGE and ZETA oracles remain unchanged. Correctly shaped
management tool calls must still fail semantic acceptance when they invent an
execution request or resolve conflicting trade identity without authorization.
"""

from hashlib import sha256

import eval_comprehensive_channel as evaluation
import pytest
from comprehensive_fixtures import FakeExchange
from eval_two_month_channel import clear_closed_app_cache
from replay_comprehensive_decisions import ScriptedInterpreter
from test_interpreter import interpreter as interpreter  # noqa: PLC0414
from test_two_month_context_cases import SOURCES as CONTEXT_SOURCES
from two_month_context_cases import CASES as CONTEXT_CASES
from two_month_context_cases import assert_context_report
from two_month_owned_cases import CASES as OWNED_CASES

from trader.gate import Gate
from trader.models import Decision

CONFLICT = next(case for case in OWNED_CASES if case["events"] == [15284])
COMPLETED_STATUS = next(case for case in CONTEXT_CASES if case["events"] == [15184])
SOURCES = CONTEXT_SOURCES | {
    15037: ("#EDGE\n\n突破這個壓力位就還要漲，可以做右側多頭交易📈", None),
    15281: ("#EIGEN\n\n當前價格入場一筆多單📈", None),
    15283: ("EDGE 浮盈中💰", 15281),
    15284: ("當前價格可以自行減倉提損，做TP1", 15283),
}


def proposal(spec, action, fixture=None):
    mid = spec["events"][0]
    fields = {
        "event_id": f"{evaluation.CHANNEL}:{mid}:offline", "action": action,
        "reason": "Scripted management-intent regression; no model or image inference",
        "evidence_message_ids": [mid],
    }
    if fixture:
        fields.update(symbol=fixture["symbol"], target_trade_id=sha256(
            f"{evaluation.CHANNEL}:{fixture['source']}".encode(),
        ).hexdigest()[:24])
    return fields


@pytest.mark.parametrize("spec,alias", [
    (CONFLICT, "A"), (CONFLICT, "B"), (COMPLETED_STATUS, "A"),
], ids=["conflict-cannot-choose-EIGEN", "conflict-cannot-choose-EDGE", "completed-is-not-tp1"])
def test_valid_target_does_not_make_reported_or_conflicted_tp1_semantically_valid(spec, alias):
    fixture = next(trade for trade in spec["trades"] if trade["alias"] == alias)
    fields = proposal(spec, "tp1", fixture)
    # All structural fields and the actual owned ID are valid; rejection must be
    # due to the unchanged semantic oracle, not a deliberately wrong target ID.
    actual = Decision.model_validate(fields)
    aliases = {alias: {"id": actual.target_trade_id}}
    with pytest.raises(AssertionError):
        evaluation.assert_decision(actual, spec["expected"][0], aliases,
                                   {spec["events"][0]: {}})


@pytest.mark.parametrize("spec,action", [
    (CONFLICT, "review"), (COMPLETED_STATUS, "ignore"), (COMPLETED_STATUS, "review"),
    (CONFLICT, "tp1"), (COMPLETED_STATUS, "tp1"),
] + [(spec, "review") for spec in CONTEXT_CASES[8:]],
    ids=["conflict-review", "completed-ignore", "completed-review",
         "wrong-conflict-tp1-fails-acceptance", "wrong-completed-tp1-fails-acceptance"]
    + [spec["name"] for spec in CONTEXT_CASES[8:]])
async def test_management_intent_regression_checks_state_effects(
    spec, action, interpreter, monkeypatch,
):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("Offline regression cannot call a model or real exchange")

    monkeypatch.setattr(Gate, "__init__", forbidden)
    monkeypatch.setattr(evaluation, "source_get", forbidden)
    monkeypatch.setattr(interpreter, "generate_decisions", forbidden)
    monkeypatch.setenv("TRADER_TOKEN", "isolated-management-intent-regression")
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    exchange = FakeExchange()
    monkeypatch.setattr(evaluation, "FakeExchange", lambda: exchange)
    history = {mid: evaluation.message(mid, text, reply) for mid, (text, reply) in SOURCES.items()}
    for mid, row in history.items():
        row["source_time"] = 1_790_000_000 + mid
    fixture = spec["trades"][0] if action == "tp1" else None
    original = {"case": spec["name"], "result": {"decisions": [proposal(spec, action, fixture)]}}
    try:
        report = await evaluation.run_case(
            spec, ScriptedInterpreter(original, interpreter), None, history, {}, 1,
        )
    finally:
        clear_closed_app_cache()
    if action == "tp1":
        # Reproduce why a schema-valid tool call is not proof of correct intent.
        # FakeExchange can execute it, but the oracle must reject the whole run.
        assert report["semantic_passed"] is report["execution_passed"] is False
        assert not report["passed"] and "unrelated position changed" in report["error"]
        before = report["owned_before"]["A"]
        assert 0 < abs(exchange.positions[before["symbol"]]["size"]) < before["filled"]
    else:
        assert report["passed"] and not exchange.writes, report
        assert_context_report(spec, report)
        after = {trade["id"]: trade for trade in report["owned_after"]}
        for before in report["owned_before"].values():
            current = after[before["id"]]
            assert abs(exchange.positions[before["symbol"]]["size"]) == before["filled"]
            assert current["exits"] == before["exits"]
            assert exchange.stops[before["exits"]["sl"]["id"]]["status"] == "open"
            assert all(exchange.orders[before["exits"][key]["id"]]["status"] == "open"
                       for key in ("tp1", "tp2"))
