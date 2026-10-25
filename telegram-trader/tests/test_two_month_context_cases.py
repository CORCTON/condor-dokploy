"""Offline fixture/oracle tests, not new Gemini or image-understanding samples."""

from copy import deepcopy

import eval_comprehensive_channel as evaluation
import pytest
from comprehensive_fixtures import FakeExchange
from eval_two_month_channel import clear_closed_app_cache
from replay_comprehensive_decisions import ScriptedInterpreter
from test_interpreter import interpreter as interpreter  # noqa: PLC0414
from two_month_context_cases import CASES, assert_context_report

from trader.gate import Gate
from trader.models import Decision

# Verbatim source text and reply IDs from the frozen snapshot; images are not
# substituted or fabricated. The real-model runner loads the full original files.
SOURCES = {
    14996: ("", 14979),
    14979: ("部分會員的反饋⬆️\n\n《來自烏克蘭會員的認可》\n《資金翻倉3倍》等等\n\n還是那句話，用實力説話，用數據證明，市場永遠在變，不變的是成熟的交易體系。想在亂局裏不踩坑、抓機會的，來找我，帶你度過各種波動期\n\n加入VIP群（入群/一鍵跟單）聯繫助理:⭐️ @cryptococo0", None),
    15179: ("#ZETA\n\n當前價格入場一筆多單📈", None),
    15181: ("這個幣波動有點大，剛剛一波插針都收益都翻倍了，還沒來得及提示，我的開倉價在0.06378\n\n第一目標先放在0.0665\n如果到 0.0619就補倉一次 \nSL 0.060", 15179),
    15184: ("補倉直接撤了，第一目標已經完成✔️", 15181),
    15163: ("#BR 市價入場一筆多單📈", None),
    15194: ("#BR\n\n看跌楔形通道內📉，短線看空", None),
    15196: ("BR 跌了8%就收針上去了，沒有到我預期位置，這個幣就是容易插針，先出了吧", 15194),
    15161: ("BTC/USDT\u00a0📈 周末掛單\n\n80000 限價多單\n\nTP1：80800\nTP2:   81300\nTP3：82000\n\nSL： 78950", None),
    15355: ("", None),
}


@pytest.mark.parametrize("spec", CASES, ids=lambda spec: spec["name"])
@pytest.mark.parametrize("action", ["ignore", "review"])
async def test_noop_preserves_owned_entry_position_and_native_protection(
    spec, action, interpreter, monkeypatch,
):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("Offline fixture cannot use a model, production or real exchange")

    monkeypatch.setattr(Gate, "__init__", forbidden)
    monkeypatch.setattr(evaluation, "source_get", forbidden)
    monkeypatch.setattr(interpreter, "generate_decisions", forbidden)
    monkeypatch.setenv("TRADER_TOKEN", "isolated-context-fixture")
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    exchange = FakeExchange()
    monkeypatch.setattr(evaluation, "FakeExchange", lambda: exchange)
    history = {mid: evaluation.message(mid, text, reply) for mid, (text, reply) in SOURCES.items()}
    for mid, row in history.items():
        row["source_time"] = 1_790_000_000 + mid
    mid = spec["events"][0]
    original = {"case": spec["name"], "result": {"decisions": [{
        "event_id": f"{evaluation.CHANNEL}:{mid}:offline", "action": action,
        "reason": "Scripted offline fixture, not model inference", "evidence_message_ids": [mid],
    }]}}
    try:
        report = await evaluation.run_case(
            spec, ScriptedInterpreter(original, interpreter), None, history, {}, 1)
    finally:
        clear_closed_app_cache()
    assert report["passed"], report
    # The offline script deliberately does not pretend to have interpreted an image.
    if spec.get("require_source_image"):
        with pytest.raises(AssertionError, match="actual frozen image"):
            assert_context_report(spec, report)
    else:
        assert_context_report(spec, report)
    owned = report["owned_before"]["A"]
    assert owned["filled"] > 0 and set(owned["exits"]) == {"sl", "tp1", "tp2"}
    assert exchange.stops[owned["exits"]["sl"]["id"]]["status"] == "open"
    assert all(exchange.orders[owned["exits"][key]["id"]]["status"] == "open"
               for key in ("tp1", "tp2"))
    entry = exchange.orders[owned["entry_id"]]
    if mid == 15355:
        assert owned["filled"] == 4 and entry["status"] == "open" and entry["left"] > 0
    else:
        assert entry["status"] == "finished" and entry["left"] == 0
    assert exchange.writes == []


@pytest.mark.parametrize("spec", CASES, ids=lambda spec: spec["name"])
@pytest.mark.parametrize("action", ["open", "cancel", "close", "tp1"])
def test_context_oracles_reject_unintended_actions(spec, action):
    mid = spec["events"][0]
    fields = {"event_id": "offline", "action": action, "reason": "Invalid test proposal",
              "evidence_message_ids": [mid]}
    if action == "open":
        fields.update(symbol=spec["trades"][0]["symbol"], side="short")
    else:
        fields["target_trade_id"] = "wrong-target"
    with pytest.raises(AssertionError):
        evaluation.assert_decision(Decision.model_validate(fields), spec["expected"][0], {}, {mid: {}})


def test_context_cases_do_not_mutate_other_frozen_golden_collections():
    from entry_intent_cases import CASES as entry
    from two_month_owned_cases import CASES as owned

    assert len(entry) == 10 and len(owned) == 17
    assert len(CASES) == 4
    assert {spec["events"][0] for spec in CASES} == {14996, 15184, 15196, 15355}
    assert not {spec["name"] for spec in CASES} & {spec["name"] for spec in entry + owned}
    # Reporting must not accept a provider failure or any hidden exchange mutation.
    for field in ("semantic_passed", "execution_passed"):
        report = {"semantic_passed": True, "execution_passed": True, field: False}
        with pytest.raises(AssertionError):
            assert_context_report(deepcopy(CASES[0]), report)
