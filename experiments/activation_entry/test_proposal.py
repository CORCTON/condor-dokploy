"""Offline guards for the isolated experiment, not production trading tests."""

import asyncio
from decimal import Decimal as D

import pytest
from evaluate import Session
from mcp import types
from proposal import Proposal, position_config, simulation_amount
from tools import on_call_tool


def arm(**kwargs):
    return Proposal(action="arm", trading_pair="INJ-USDT", side="long",
                    lower="10.50", upper="10.60", evidence_message_ids=[101],
                    structural_lower="10.40", structural_upper="10.49",
                    rationale="Based on the repeatedly tested resistance at 10.50", **kwargs)


@pytest.mark.parametrize("side,lower,upper", [("long", "10.50", "10.60"),
                                             ("short", "9.40", "9.50")])
def test_config_matches_absolute_bounds(side, lower, upper):
    plan = arm().model_copy(update={"side": side, "lower": D(lower), "upper": D(upper)})
    config = position_config(plan, mid=D(10), tick=D("0.01"), amount=D(1), leverage=10)
    centre, radius = D(config["entry_price"]), D(config["activation_bounds"][0])
    assert abs(centre * (1 - radius) - D(lower)) < D("1e-24")
    assert abs(centre * (1 + radius) - D(upper)) < D("1e-24")
    assert all(D(v) > 0 for v in config["activation_bounds"])
    assert config["triple_barrier_config"] == {"open_order_type": 1}
    assert "stop_loss" not in config and "time_limit" not in config


@pytest.mark.parametrize("mid", ["10.50", "10.55", "11"])
def test_no_chasing_on_creation(mid):
    with pytest.raises(ValueError, match="already reached"):
        position_config(arm(), mid=D(mid), tick=D("0.01"), amount=D(1), leverage=10)


@pytest.mark.parametrize("field,value", [("lower", "NaN"), ("upper", "Infinity"),
                                         ("lower", "0"), ("upper", "10.49"),
                                         ("target_plan_id", "foreign")])
def test_invalid_tool_arguments(field, value):
    raw = arm().model_dump(mode="json")
    raw[field] = value
    with pytest.raises(ValueError):
        Proposal.model_validate(raw)


@pytest.mark.parametrize("side,lower,upper,structural_lower,structural_upper", [
    ("long", "2.425", "2.438", "2.424", "2.435"),
    ("long", "2.435", "2.450", "2.424", "2.435"),
    ("short", "53.75", "54.10", "54.04", "54.15"),
    ("short", "53.75", "54.04", "54.04", "54.15"),
])
def test_entry_cannot_overlap_or_touch_its_own_structural_band(
    side, lower, upper, structural_lower, structural_upper,
):
    raw = arm().model_dump(mode="json") | dict(
        side=side, lower=lower, upper=upper,
        structural_lower=structural_lower, structural_upper=structural_upper,
    )
    with pytest.raises(ValueError, match="strictly"):
        Proposal.model_validate(raw)


def test_off_tick_range():
    plan = arm().model_copy(update={"lower": D("10.505")})
    with pytest.raises(ValueError, match="tick"):
        position_config(plan, mid=D(10), tick=D("0.01"), amount=D(1), leverage=10)


def test_evidence_required_before_acceptance():
    session = Session({"text": "breakout"})
    with pytest.raises(ValueError, match="context"):
        asyncio.run(session.tool("submit_activation_proposal", arm().model_dump(mode="json")))
    session.reads.add("read_source_context")
    with pytest.raises(ValueError, match="missing successful evidence"):
        asyncio.run(session.tool("submit_activation_proposal", arm().model_dump(mode="json")))


@pytest.mark.parametrize("state", [{"pending": True}, {"filled": True}])
def test_duplicate_and_foreign_position_block(state):
    session = Session({"text": "breakout", **state})
    session.reads.update({"read_source_context", "get_prices", "get_market_data",
                          "get_trading_rules", "get_order_book"})
    with pytest.raises(ValueError, match="existing plan/position"):
        asyncio.run(session.tool("submit_activation_proposal", arm().model_dump(mode="json")))


def test_cancellation_cannot_target_another_plan_or_filled_position():
    for pending, target in [(True, "foreign"), (False, "eval-plan-100")]:
        session = Session({"text": "cancel", "pending": pending})
        session.reads.add("read_source_context")
        proposal = Proposal(action="cancel", target_plan_id=target, evidence_message_ids=[101],
                            rationale="Cancel the matching pending source plan only")
        with pytest.raises(ValueError, match="no matching"):
            asyncio.run(session.tool("submit_activation_proposal", proposal.model_dump(mode="json")))


def test_accepted_proposal_idempotent_and_immutable():
    session = Session({"text": "ignore"})
    session.reads.add("read_source_context")
    raw = Proposal(action="ignore", evidence_message_ids=[101],
                   rationale="No sufficiently clear authorization in the message").model_dump(mode="json")
    assert asyncio.run(session.tool("submit_activation_proposal", raw))["accepted"]
    assert asyncio.run(session.tool("submit_activation_proposal", raw))["duplicate"]
    with pytest.raises(ValueError, match="immutable"):
        asyncio.run(session.tool("submit_activation_proposal", arm().model_dump(mode="json")))


def test_made_up_evidence_id_rejected():
    session = Session({"text": "ignore"})
    session.reads.add("read_source_context")
    raw = Proposal(action="ignore", evidence_message_ids=[999],
                   rationale="No clear authorization in the message").model_dump(mode="json")
    with pytest.raises(ValueError, match="unknown evidence"):
        asyncio.run(session.tool("submit_activation_proposal", raw))


def test_official_market_tools_receive_complete_fixture_rows():
    session = Session({"text": "research"})
    for interval in ("5m", "15m", "1h", "4h"):
        result = asyncio.run(session.tool("get_market_data", {"interval": interval}))
        assert result["count"] == 80
        assert result["candles"][0]["high"] == 10.5


@pytest.mark.parametrize("name,arguments", [
    ("unknown_tool", {}),
    ("get_market_data", {}),
    ("get_market_data", {"interval": "1s"}),
    ("get_prices", {"execute_order": True}),
])
def test_mcp_rejects_invalid_arguments_before_loopback_call(name, arguments, monkeypatch):
    monkeypatch.delenv("ACTIVATION_EVAL_URL", raising=False)
    result = asyncio.run(on_call_tool(None, types.CallToolRequestParams(
        name=name, arguments=arguments,
    )))
    assert result.is_error


def test_real_channel_case_never_receives_constructed_market_data():
    session = Session({"text": "archive", "source_kind": "real_channel_archive",
                       "trading_pair": "FOLKS-USDT"})
    for name in ("get_prices", "get_trading_rules"):
        with pytest.raises(ValueError):
            asyncio.run(session.tool(name, {}))
    with pytest.raises(ValueError, match="unavailable"):
        asyncio.run(session.tool("get_market_data", {"interval": "15m"}))
    assert asyncio.run(session.tool("get_order_book", {}))["available"] is False


def test_real_source_ids_and_pending_plan_identity_are_preserved():
    messages = [{"message_id": 15297, "text": "GRASS 交易設置取消",
                 "reply_to_message_id": 15295}]
    session = Session({"text": messages[0]["text"], "messages": messages,
                       "current_message_id": 15297, "trading_pair": "GRASS-USDT",
                       "pending_plans": [{"plan_id": "source-15295"}]})
    context = asyncio.run(session.tool("read_source_context", {}))
    assert context["messages"] == messages and context["current_message_id"] == 15297
    proposal = Proposal(action="cancel", target_plan_id="source-15295",
                        evidence_message_ids=[15297], rationale="Cancel the matching owned source plan")
    assert asyncio.run(session.tool("submit_activation_proposal", proposal.model_dump(mode="json")))["accepted"]


@pytest.mark.parametrize("lot,minimum,price,expected", [
    ("10", "10", "0.79", "10"),
    ("1", "1", "0.416", "3"),
    ("0.01", "0.01", "54", "0.02"),
])
def test_rehearsal_size_honors_real_venue_minima(lot, minimum, price, expected):
    rule = {"min_base_amount_increment": lot, "min_order_size": minimum,
            "min_notional_size": "1"}
    assert simulation_amount(rule, D(price)) == D(expected)
