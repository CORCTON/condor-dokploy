"""Disposable replay apps must not retain earlier snapshots; no model or RSS checks."""

import gc
import weakref
from pathlib import Path
from types import SimpleNamespace

import eval_comprehensive_channel as comprehensive
import eval_two_month_channel as evaluation
import pytest
from aiohttp import web_app
from replay_comprehensive_decisions import ScriptedInterpreter
from test_two_month_evaluation import frozen, source


@pytest.fixture
def tracked_apps(monkeypatch):
    references = []
    original = evaluation.make_app

    def make_app(engine, channel, **kwargs):
        app = original(engine, channel, **kwargs)
        references.append((weakref.ref(app), weakref.ref(channel)))
        return app

    monkeypatch.setattr(evaluation, "make_app", make_app)
    monkeypatch.setenv("TRADER_TOKEN", "offline-lifecycle-test")
    monkeypatch.setenv("TELEGRAM_TRADER_URL", "http://127.0.0.1:1")
    return references


@pytest.fixture
def actual():
    path = (Path(__file__).resolve().parents[1]
            / "condor-assets/telegram_agent/routines/signal_interpreter.py")
    return comprehensive.load("offline_memory_interpreter_api", path)


def scripted(mid, actual, *, malformed=False):
    result = None if malformed else {"decisions": [{
        "event_id": f"{comprehensive.CHANNEL}:{mid}:original",
        "action": "ignore", "reason": "offline lifecycle fixture, not model inference",
        "evidence_message_ids": [mid],
    }]}
    return ScriptedInterpreter({"case": "lifecycle-only", "result": result}, actual)


async def test_426_completed_cases_release_apps_and_growing_snapshots(
    tmp_path, tracked_apps, actual,
):
    history = {mid: {**source(mid, mid), "edited_at": None} for mid in range(1, 427)}
    snapshot = frozen(tmp_path, history)
    reports = []
    for mid in history:
        report = await evaluation.scan_message(mid, scripted(mid, actual), history, snapshot, 1)
        assert report["protocol_passed"], report
        reports.append(report)  # Reports remain retained just like the real evaluator.
        if mid % 25 == 0 or mid == 426:
            gc.collect()
            assert all(app() is None and channel() is None for app, channel in tracked_apps)
    assert len(reports) == len(tracked_apps) == 426


async def test_failed_case_also_releases_app_and_snapshot(tmp_path, tracked_apps, actual):
    history = {1: source(1)}
    report = await evaluation.scan_message(
        1, scripted(1, actual, malformed=True), history, frozen(tmp_path, history), 1)
    assert not report["protocol_passed"]
    assert report["failure_phase"] == "model_proposal"
    gc.collect()
    assert all(app() is None and channel() is None for app, channel in tracked_apps)


def test_aiohttp_without_middleware_cache_needs_no_compatibility_hook(monkeypatch):
    monkeypatch.delattr(web_app, "_cached_build_middleware")
    evaluation.clear_closed_app_cache()


def test_changed_private_cache_contract_fails_visibly(monkeypatch):
    monkeypatch.setattr(web_app, "_cached_build_middleware", SimpleNamespace())
    with pytest.raises(AttributeError, match="cache_clear"):
        evaluation.clear_closed_app_cache()
