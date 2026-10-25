"""Offline fixture/oracle tests, not new Gemini or image-understanding samples."""

from copy import deepcopy
from hashlib import sha256

import eval_comprehensive_channel as evaluation
import pytest
from comprehensive_fixtures import FakeExchange
from eval_two_month_channel import clear_closed_app_cache
from replay_comprehensive_decisions import ScriptedInterpreter
from test_interpreter import interpreter as interpreter  # noqa: PLC0414
from two_month_context_cases import CASES, assert_context_report
from two_month_owned_cases import CASES as OWNED_CASES

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
    15272: ("#JUP\n\n當前價格入場一筆空單📉\n（我的開倉價：0.3738）", None),
    15274: ("JUP 當前可以帶個成本損，無風險持倉✔️", 15272),
    15275: ("TP：0.3645-0.3600-0.3555", 15274),
    15276: ("JUP 即將完成TP3", 15275),
    15277: ("JUP 已完成所有TP目標，後續可以自行選擇全部止盈/格局💰", 15276),
    15102: ("#SUI\n\n當前價格入場一筆多單📈\n我的開倉價：0.7143", None),
    15104: ("SUI 繼續拿", 15102),
    15094: ("#RAY\n\n代幣化美股上線導致交易量大增，還能讓meme跟股票對交易，SOL近30日代幣化股票DEX量裡，Raydium 佔比很大一塊，基本盤穩定\n\n目前第三浪已經基本走完，我判斷會進行回測，走第四浪下跌，然後再進行新的一輪上漲", None),
    15099: ("符合第四浪下跌思路，當前可以止盈", 15094),
    15317: ("#INJ\n\n當前價格入場一筆空單📉", None),
    15319: ("當前價格也可以做TP1✔️", 15317),
    15065: ("#SUI\n\n當前價格入場一筆多單📈\n（我的開倉價：0.8192）", None),
    15067: ("SUI價格回調了，先繼續拿著", 15065),
    15069: ("SUI 目前繼續浮盈中，帶個成本損，倉位重的可以自行減倉", 15067),
    14956: ("#HYPE\n\n當前價格入場一筆多單📈\n（我的開倉價：81.788）", None),
    14960: ("HYPE 這裡倉位重的減倉一次，設置一個成本損\n因為上方83.5附近的壓力還是存在的", 14956),
}

SUI_COST = next(spec for spec in OWNED_CASES if spec["events"] == [15069])
HYPE_REVIEW = next(spec for spec in OWNED_CASES if spec["events"] == [14960])
PRIMARY_CASES = CASES[4:8] + [SUI_COST]


@pytest.mark.parametrize("spec", CASES[:4], ids=lambda spec: spec["name"])
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


@pytest.mark.parametrize("spec", CASES[:4], ids=lambda spec: spec["name"])
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


@pytest.mark.parametrize("spec,action", [
    (CASES[4], "close"), (CASES[5], "ignore"), (CASES[6], "close"), (CASES[7], "tp1"),
    (SUI_COST, "sl"), (SUI_COST, "targets"), (HYPE_REVIEW, "review"),
], ids=["JUP-close", "SUI-hold", "RAY-close", "INJ-tp1", "SUI-sl", "SUI-targets", "HYPE-review"])
async def test_user_authorized_primary_recommendation_has_exact_owned_effect(
    spec, action, interpreter, monkeypatch,
):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("Offline recommendation test cannot use a model or real exchange")

    monkeypatch.setattr(Gate, "__init__", forbidden)
    monkeypatch.setattr(evaluation, "source_get", forbidden)
    monkeypatch.setattr(interpreter, "generate_decisions", forbidden)
    monkeypatch.setenv("TRADER_TOKEN", "isolated-primary-recommendation")
    monkeypatch.setenv("ADMIN_USER_ID", "1")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    exchange = FakeExchange()
    monkeypatch.setattr(evaluation, "FakeExchange", lambda: exchange)
    history = {mid: evaluation.message(mid, text, reply) for mid, (text, reply) in SOURCES.items()}
    for mid, row in history.items():
        row["source_time"] = 1_790_000_000 + mid
    mid = spec["events"][0]
    fixture, = spec["trades"]
    fields = {"event_id": f"{evaluation.CHANNEL}:{mid}:offline", "action": action,
              "reason": "Scripted 2026-10-25 operator-policy regression; no model inference",
              "evidence_message_ids": [mid]}
    if action not in {"ignore", "review"}:
        fields["target_trade_id"] = sha256(
            f"{evaluation.CHANNEL}:{fixture['source']}".encode(),
        ).hexdigest()[:24]
    if action == "sl":
        fields["price"] = "0.8192"
    elif action == "targets":
        fields["levels"] = {"sl": "0.8192"}
    original = {"case": spec["name"], "result": {"decisions": [fields]}}
    try:
        report = await evaluation.run_case(
            spec, ScriptedInterpreter(original, interpreter), None, history, {}, 1,
        )
    finally:
        clear_closed_app_cache()
    assert report["passed"], report
    assert_context_report(spec, report)
    before = report["owned_before"]["A"]
    after, = report["owned_after"]
    position = report["positions_after"][before["symbol"]]
    if action == "close":
        assert position["size"] == 0 and after["status"] == "closed"
    elif action == "tp1":
        assert 0 < abs(position["size"]) < before["filled"]
        assert after["exits"]["tp1"]["done"] is True
        assert not after["exits"]["tp2"].get("done", False)
    elif action in {"sl", "targets"}:
        assert abs(position["size"]) == before["filled"]
        assert after["entry_price"] == after["channel_levels"]["sl"] == "0.8192"
        assert after["exits"]["tp1"] == before["exits"]["tp1"]
        assert after["exits"]["tp2"] == before["exits"]["tp2"]
        assert not any(method == "POST" and path == "/orders"
                       for method, path, _body, _params in exchange.writes)
    else:
        assert not exchange.writes and abs(position["size"]) == before["filled"]


@pytest.mark.parametrize("spec,action", [
    (spec, action) for spec in PRIMARY_CASES + [HYPE_REVIEW]
    for action in ("ignore", "review", "close", "tp1", "sl", "targets")
    if action not in (spec["expected"][0]["action"] if isinstance(
        spec["expected"][0]["action"], list) else [spec["expected"][0]["action"]])
], ids=lambda value: value["name"] if isinstance(value, dict) else value)
def test_primary_recommendation_oracle_rejects_opposite_action(spec, action):
    expected = spec["expected"][0]
    mid = spec["events"][0]
    fields = {"event_id": "offline", "action": action, "reason": "Opposite policy choice",
              "evidence_message_ids": [mid], "target_trade_id": "owned-A"}
    if action == "sl":
        fields["price"] = "0.8192"
    elif action == "targets":
        fields["levels"] = {"sl": "0.8192"}
    with pytest.raises(AssertionError):
        evaluation.assert_decision(
            Decision.model_validate(fields), expected, {"A": {"id": "owned-A"}}, {mid: {}},
        )


@pytest.mark.parametrize("fields", [
    {"action": "sl", "price": "0.8200"},
    {"action": "targets", "levels": {"sl": "0.8200"}},
    {"action": "sl", "price": "0.8192", "levels": {"tp1": "0.85"}},
    {"action": "targets", "levels": {"sl": "0.8192", "tp1": "0.85"}},
])
def test_sui_cost_oracle_rejects_changed_cost_or_extra_tp_update(fields):
    with pytest.raises(AssertionError):
        evaluation.assert_decision(Decision.model_validate({
            "event_id": "offline", "reason": "Invalid cost or extra update",
            "evidence_message_ids": [15069], "target_trade_id": "owned-A", **fields,
        }), SUI_COST["expected"][0], {"A": {"id": "owned-A"}}, {15069: {}})


def test_context_cases_do_not_mutate_other_frozen_golden_collections():
    from entry_intent_cases import CASES as entry
    from two_month_owned_cases import CASES as owned

    assert len(entry) == 10 and len(owned) == 17
    assert len(CASES) == 12
    assert [spec["events"][0] for spec in CASES[:4]] == [14996, 15184, 15196, 15355]
    assert {spec["events"][0] for spec in CASES[4:8]} == {15277, 15104, 15099, 15319}
    assert not {spec["name"] for spec in CASES} & {spec["name"] for spec in entry + owned}
    # Reporting must not accept a provider failure or any hidden exchange mutation.
    for field in ("semantic_passed", "execution_passed"):
        report = {"semantic_passed": True, "execution_passed": True, field: False}
        with pytest.raises(AssertionError):
            assert_context_report(deepcopy(CASES[0]), report)


def test_stress_repeats_change_only_name_not_evidence_owned_state_or_oracle():
    conflict = next(spec for spec in OWNED_CASES if spec["events"] == [15284])
    originals = [CASES[1], CASES[1], conflict, conflict]
    assert [spec["events"][0] for spec in CASES[8:]] == [15184, 15184, 15284, 15284]
    for repeated, original in zip(CASES[8:], originals, strict=True):
        assert repeated["name"] != original["name"]
        assert dict(repeated, name=original["name"]) == original
        for key in ("events", "expected", "trades"):
            assert repeated[key] is not original[key]


@pytest.mark.parametrize("mutation", ["missing_trade", "extra_trade", "changed_protection", "changed_version"])
def test_noop_acceptance_checks_every_owned_trade_completely(mutation):
    spec = CASES[-1]
    before = {"A": {"id": "a", "version": 2, "exits": {"sl": "a-sl"}},
              "B": {"id": "b", "version": 2, "exits": {"sl": "b-sl"}}}
    after = deepcopy(list(before.values()))
    if mutation == "missing_trade":
        after.pop()
    elif mutation == "extra_trade":
        after.append({"id": "c"})
    elif mutation == "changed_protection":
        after[1]["exits"]["sl"] = "different-stop"
    else:
        after[1]["version"] += 1
    with pytest.raises(AssertionError, match="mutated owned trades"):
        assert_context_report(spec, {"semantic_passed": True, "execution_passed": True,
                                    "exchange_writes": [], "owned_before": before,
                                    "owned_after": after})
