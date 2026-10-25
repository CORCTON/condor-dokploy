"""Offline checks of historical-evaluation boundaries; no model or live exchange."""

import json
from copy import deepcopy
from pathlib import Path

import eval_comprehensive_channel as comprehensive
import eval_two_month_channel as evaluation
import pytest
from replay_comprehensive_decisions import ScriptedInterpreter


def source(mid, timestamp=10, media=None):
    return {"message_id": mid, "text": f"source {mid}", "reply_to_message_id": None,
            "source_time": timestamp, "edited_at": timestamp + 1000, "media": media,
            "context_only": True}


def frozen(tmp_path, messages, images=None):
    (tmp_path / "images").mkdir()
    evaluation.save(tmp_path / "history.json", messages)
    manifest = {}
    for mid, data in (images or {}).items():
        (tmp_path / "images" / f"{mid}.bin").write_bytes(data)
        manifest[str(mid)] = {"mime": "image/png", "sha256": evaluation.digest(data)}
    evaluation.save(tmp_path / "images/manifest.json", manifest)
    return tmp_path


def test_prefix_never_exposes_later_publications_or_impossible_future_edits():
    history = {1: source(1), 2: source(2, 20), 3: source(3, 20), 4: source(4, 30)}
    history[1]["edited_at"] = 15
    before = deepcopy(history)
    visible = evaluation.first_arrival_prefix(history, 2, 100)
    assert set(visible) == {1, 2}
    assert [row["source_time"] for row in visible.values()] == [90, 100]
    assert visible[1]["edited_at"] == 95 and visible[2]["edited_at"] is None
    assert history == before


async def test_earlier_message_future_revision_is_unavailable_to_all_evidence_tools(tmp_path):
    media = {"id": "1", "mime": "image/png", "size": 3, "grouped_id": None}
    history = {1: source(1, media=media), 2: source(2, 20)}
    history[1]["text"] = "future edit: CLOSE ALL, SL 999, not known at message 2"
    snapshot = frozen(tmp_path, history, {1: b"png"})
    visible = evaluation.first_arrival_prefix(history, 2, 100)
    assert set(visible) == {2}
    assert evaluation.excluded_future_edits(history, 2) == [1]
    channel = evaluation.DiskSnapshotChannel(visible, snapshot)
    assert [row["message_id"] for row in (await channel.history())["messages"]] == [2]
    assert (await channel.history(query="CLOSE"))["messages"] == []
    with pytest.raises(ValueError, match="unavailable"):
        await channel.read(1)
    with pytest.raises(ValueError, match="unavailable"):
        await channel.image(1)
    assert not any(call["op"] == "download" for call in channel.calls)


async def test_disk_images_are_lazy_verified_and_prefix_bound(tmp_path):
    media = {"id": "1", "mime": "image/png", "size": 3, "grouped_id": None}
    history = {1: source(1, media=media), 2: source(2, 20, media=media)}
    snapshot = frozen(tmp_path, history, {1: b"png", 2: b"later"})
    channel = evaluation.DiskSnapshotChannel({1: history[1]}, snapshot)
    assert await channel.image(1) == (b"png", "image/png")
    with pytest.raises(ValueError, match="unavailable"):
        await channel.image(2)
    (snapshot / "images/1.bin").write_bytes(b"changed")
    with pytest.raises(ValueError, match="hash or MIME"):
        await channel.image(1)


async def test_missing_images_do_not_fall_back_to_live_channel(tmp_path):
    media = {"id": "1", "mime": "image/png", "size": 3, "grouped_id": None}
    history = {1: source(1, media=media)}
    channel = evaluation.DiskSnapshotChannel(history, frozen(tmp_path, history))
    with pytest.raises(ValueError, match="unavailable or unverified"):
        await channel.image(1)


@pytest.mark.parametrize("action,fields", [
    ("ignore", {}), ("review", {}),
    ("open", {"symbol": "INJ", "side": "short", "price": None}),
    ("open", {"symbol": "BTC", "side": "long", "price": "100000"}),
    ("open", {"symbol": "PEPE", "side": "short", "price": "0.00000123"}),
])
async def test_scripted_protocol_scan_isolated_and_never_claims_semantic_accuracy(
    tmp_path, monkeypatch, action, fields,
):
    monkeypatch.setenv("TRADER_TOKEN", "isolated-offline-test")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    history = {1: source(1)}
    snapshot = frozen(tmp_path, history)
    routine = Path(__file__).resolve().parents[1] / "condor-assets/telegram_agent/routines/signal_interpreter.py"
    actual = comprehensive.load("two_month_test_interpreter", routine)
    saved = {"case": "offline-harness-only", "result": {"decisions": [{
        "event_id": f"{comprehensive.CHANNEL}:1:original", "action": action,
        "reason": "offline scripted fixture, not a real model sample", "evidence_message_ids": [1],
        **fields,
    }]}}
    report = await evaluation.scan_message(
        1, ScriptedInterpreter(saved, actual), history, snapshot, 1)
    assert report["protocol_passed"], report
    assert report["semantic_passed"] is None
    assert report["semantic_status"].startswith("unlabelled")
    if action != "open":
        assert report["exchange_writes"] == [] and report["owned_after"] == []
    else:
        assert len(report["owned_after"]) == 1
    assert report["original_edited_at"] == 1010


def test_config_rejects_credentials_outside_model_scope(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"agent_key": "custom:gemini-example", "base_url": "http://cpa:8317/v1",
                                "api_key": "fake-model-key", "gate_secret": "not-permitted"}))
    with pytest.raises(ValueError, match="only agent_key"):
        evaluation.configuration(path)


@pytest.mark.parametrize("kinds,stopped", [
    ([], False),
    (["model_infrastructure"] * 2, False),
    (["model_infrastructure"] * 3, True),
    (["model_infrastructure", "case_invariant", "model_infrastructure"], False),
    (["model_infrastructure"] * 3 + [None], False),
    (["case_invariant"] * 3, False),
])
def test_failure_circuit_only_counts_consecutive_infrastructure_failures(kinds, stopped):
    assert evaluation.model_failure_circuit([{"failure_kind": kind} for kind in kinds]) is stopped


def test_protocol_pass_does_not_hide_age_guard_execution_rejection():
    reports = [{"protocol_passed": True, "result": {"decisions": [{"action": "open"}]},
                "event_result": {"status": status, "detail": detail}}
               for status, detail in (("done", ""),
                                      ("review", "entry signal older than 60 seconds; no new entry"))]
    counts = evaluation.outcome_counts(reports)
    assert counts["open_decisions"] == 2
    assert counts["open_executed"] == 1
    assert counts["open_review"] == 1
    assert counts["open_age_guard_blocked"] == 1


def test_additional_golden_selection_preserves_all_17_including_six_empty_states():
    cases = [{"name": f"case-{index}", "trades": [] if index < 6 else [{"symbol": "INJ"}]}
             for index in range(17)]
    assert evaluation.select_golden_cases("all", cases) == cases
    assert evaluation.select_golden_cases("case-0,case-6", cases) == [cases[0], cases[6]]
    with pytest.raises(ValueError, match="Unknown/empty"):
        evaluation.select_golden_cases("case-0,missing", cases)
