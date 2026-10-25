"""Opt-in real-model semantic scan of frozen, current-revision channel evidence.

Each message is a hypothetical first arrival with EMPTY owned state and only
earlier channel context. This is not a continuous lifecycle or point-in-time
backtest: edits, historical fills, latency, profitability and fees are unknown.
Unlabelled model decisions are observations, NEVER automatic semantic passes.

Run inside a disposable, resource-limited Condor image, without live volumes or
exchange/Telegram credentials. Only the LLM endpoint needs network access::

    EVAL_LLM_CONFIG=/private/llm.json PYTHONPATH=/candidate/telegram-trader:/app \
      python /candidate/telegram-trader/tests/eval_two_month_channel.py \
      /candidate /evidence /results --since 1787587200 --ids 15370,15376

llm.json contains exactly agent_key, base_url, api_key, optionally user_id.
Images are images/{message_id}.bin, with images/manifest.json mapping IDs to
{mime, sha256}. Missing media fail explicitly; there is no live-channel fallback.
Omit --ids/--max-cases for all messages. Resume skips durable existing reports.
--owned-cases runs separately labelled additional golden scenarios, including
empty-owned-state cases (the flag name is retained for compatibility).
"""

import argparse
import asyncio
import hashlib
import json
import os
import time
import traceback
from contextlib import ExitStack
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlparse

import aiohttp
import eval_comprehensive_channel as comprehensive
from aiohttp import web
from comprehensive_cases import CASES
from comprehensive_channel import SnapshotChannel
from comprehensive_fixtures import FakeExchange

from trader.app import make_app
from trader.engine import Engine
from trader.gate import Gate
from trader.models import Decisions
from trader.store import Store

SEMANTICS = ("current-revision hypothetical first arrival; empty owned state; "
             "prefix context excluding revisions edited after evaluation cutoff")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def save(path, value):
    """Publish complete reports so interruption cannot produce a false resume."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2))
    temporary.replace(path)


def read_history(snapshot):
    rows = json.loads((snapshot / "history.json").read_text())
    history = {int(key): row for key, row in rows.items()}
    if any(key <= 0 or row["message_id"] != key for key, row in history.items()):
        raise ValueError("snapshot keys must match positive message IDs")
    return history


def excluded_future_edits(history, message_id):
    """Original text before a later edit is unknown: exclude the entire revision."""
    current = history[message_id]
    cutoff = (current["source_time"], message_id)
    return sorted(key for key, row in history.items()
                  if key != message_id and (row["source_time"], key) <= cutoff
                  and row.get("edited_at") is not None
                  and row["edited_at"] > current["source_time"])


def first_arrival_prefix(history, message_id, now):
    """Never expose later publications or earlier messages' future-edited content."""
    current = history[message_id]
    cutoff = (current["source_time"], message_id)
    excluded = set(excluded_future_edits(history, message_id))
    messages = deepcopy({key: row for key, row in history.items()
                         if key not in excluded and (row["source_time"], key) <= cutoff})
    shift = now - current["source_time"]
    for row in messages.values():
        row["source_time"] = round(row["source_time"] + shift, 6)
        if row["message_id"] == message_id:
            # Only the evaluated revision is the counterfactual first arrival.
            row["edited_at"] = None
        elif row.get("edited_at") is not None:
            row["edited_at"] = round(row["edited_at"] + shift, 6)
    return messages


class DiskSnapshotChannel(SnapshotChannel):
    """Read only visible, hash-verified image files, lazily and without network."""

    def __init__(self, messages, snapshot, images=None):
        super().__init__(messages, images or {})
        self.snapshot = snapshot
        self.manifest = json.loads((snapshot / "images/manifest.json").read_text())
        self.manifest = self.manifest.get("images", self.manifest)

    async def download_media(self, message, file):
        if message.id in self._images:
            return await super().download_media(message, file)
        self.calls.append({"op": "download", "message_id": message.id})
        if file is not bytes or message.id not in self._messages:
            raise ValueError("image outside visible frozen snapshot")
        entry = self.manifest.get(str(message.id))
        if not entry or not entry.get("sha256"):
            raise ValueError("image unavailable or unverified in frozen snapshot")
        path = self.snapshot / "images" / f"{message.id}.bin"
        if not path.is_file() or path.stat().st_size > 8 * 1024 * 1024:
            raise ValueError("image missing or too large in frozen snapshot")
        data = path.read_bytes()
        if digest(data) != entry["sha256"] or entry["mime"] != message.file.mime_type:
            raise ValueError("frozen image hash or MIME mismatch")
        return data


def add_synthetic_contract(exchange, decision):
    """Give unlabelled entries a fake market, not historical exchange evidence."""
    price = Decimal(decision.price or "10")
    tick = Decimal(1).scaleb(min(price.as_tuple().exponent, -8))
    # 1,000 contracts of synthetic notional keep the fixture's integer limits
    # from accidentally rejecting either high- or low-priced channel symbols.
    multiplier = Decimal("0.2") / price
    exchange.add_contract(decision.symbol + "_USDT", price=str(price),
                          tick=str(tick), multiplier=str(multiplier))


def assert_observation(decision, event_id, messages, row, exchange, store):
    assert decision.event_id == event_id
    assert decision.evidence_message_ids and set(decision.evidence_message_ids) <= messages.keys()
    if decision.action == "open":
        assert decision.symbol and decision.side in {"long", "short"}
        assert not decision.target_trade_id
        assert row["status"] in {"done", "review"}, row
        if row["status"] == "done":
            trades = store.trades()
            assert len(trades) == 1 and trades[0]["symbol"] == decision.symbol + "_USDT"
            assert trades[0]["side"] == decision.side
            entry = exchange.orders[trades[0]["entry_id"]]
            assert (entry["size"] > 0) == (decision.side == "long")
            assert Decimal(entry["price"]) == Decimal(decision.price or "0")
    else:
        assert not exchange.writes, "Empty-owned management/no-op must never write to exchange"
        assert not store.trades(), "No-op/management invented an owned trade"
        assert row["status"] in {"ignore", "review"}, row
    assert not store.unknowns()


async def scan_message(mid, interpreter, history, snapshot, owner):
    store, exchange = Store(":memory:"), FakeExchange()
    engine = Engine(store, exchange, enabled=True)
    engine.listener_ready = True
    store.set("paused", False)
    messages = first_arrival_prefix(history, mid, round(time.time(), 6))
    channel = DiskSnapshotChannel(messages, snapshot)
    row = messages[mid]
    event_id = store.event(comprehensive.CHANNEL, mid, row["text"], row["reply_to_message_id"],
                           row["source_time"], True, media=row["media"], edited_at=None)
    report = {"message_id": mid, "scope": SEMANTICS, "semantic_passed": None,
              "semantic_status": "unlabelled observation; independent review required",
              "protocol_passed": False, "original_source_time": history[mid]["source_time"],
              "original_edited_at": history[mid].get("edited_at"),
              "excluded_future_edited_context_ids": excluded_future_edits(history, mid),
              "excluded_future_edited_context_count": len(excluded_future_edits(history, mid)),
              "source_sha256": digest(json.dumps(history[mid], sort_keys=True).encode()),
              "market": "synthetic; no historical quotes, fees or margin accounting"}
    runner = web.AppRunner(make_app(engine, channel, credentials=exchange))
    started = time.monotonic()
    phase = "infrastructure_setup"
    try:
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
        assert urlparse(url).hostname == "127.0.0.1"
        os.environ["TELEGRAM_TRADER_URL"] = url
        async with aiohttp.ClientSession() as session:
            job = await interpreter.api(session, "POST", "/signals/claim")
            assert job and len(job["events"]) == 1
            phase = "model_proposal"
            async with interpreter.interpret_current(job, owner, session) as result:
                report["proposal_ready_seconds"] = round(time.monotonic() - started, 6)
                assert result is not None, "Analysis became stale without accepted proposal"
                report["result"] = result
                phase = "decision_validation"
                decisions = Decisions.model_validate(result).decisions
                assert len(decisions) == 1 and decisions[0].event_id == event_id
                decision = decisions[0]
                assert set(decision.evidence_message_ids) <= messages.keys()
                if decision.action == "open":
                    add_synthetic_contract(exchange, decision)
                body = {"lease": job["lease"], "result": result}
                phase = "execution"
                await interpreter.api(session, "POST", "/signals/decisions", json=body)
                report["execution_applied_seconds"] = round(time.monotonic() - started, 6)
                phase = "model_cleanup"
            phase = "execution_checks"
            for _ in range(3):
                await engine.poll()
            row = dict(store.db.execute("SELECT status,detail FROM events WHERE id=?",
                                        (event_id,)).fetchone())
            report["event_result"] = row
            writes = deepcopy(exchange.writes)
            await interpreter.api(session, "POST", "/signals/decisions", json=body)
            assert exchange.writes == writes, "Duplicate lease caused additional exchange writes"
            assert_observation(decision, event_id, messages, row, exchange, store)
            report["protocol_passed"] = True
    except Exception as exc:  # noqa: BLE001 -- retain independent case failures as evidence
        infrastructure = not isinstance(exc, AssertionError) and phase in {
            "infrastructure_setup", "model_proposal", "model_cleanup"}
        report.update(error=type(exc).__name__ + ": " + str(exc), traceback=traceback.format_exc(),
                      failure_phase=phase,
                      failure_kind="model_infrastructure" if infrastructure else "case_invariant")
        if isinstance(exc, comprehensive.restart_errors(interpreter, interpreter)):
            report["fatal_cleanup_failure"] = True
    finally:
        report.update(seconds=round(time.monotonic() - started, 6), tools=channel.calls,
                      exchange_writes=exchange.writes, owned_after=store.trades())
        await runner.cleanup()
        store.close()
    return report


def model_failure_circuit(reports):
    """Three consecutive model/transport failures stop a wasteful full sweep."""
    return len(reports) >= 3 and all(
        report.get("failure_kind") == "model_infrastructure" for report in reports[-3:])


def select_golden_cases(selection, available):
    chosen = selection.split(",")
    cases = [case for case in available if chosen == ["all"] or case["name"] in chosen]
    if not cases or (chosen != ["all"] and set(chosen) != {case["name"] for case in cases}):
        raise ValueError("Unknown/empty additional golden case selection")
    return cases


def outcome_counts(reports):
    opens = [report for report in reports if any(
        decision["action"] == "open" for decision in report.get("result", {}).get("decisions", []))]
    return {"open_decisions": len(opens),
            "open_executed": sum(row.get("event_result", {}).get("status") == "done" for row in opens),
            "open_review": sum(row.get("event_result", {}).get("status") == "review" for row in opens),
            "open_age_guard_blocked": sum("entry signal older than 60 seconds" in
                                           (row.get("event_result", {}).get("detail") or "") for row in opens),
            "note": "Protocol pass is not execution success or semantic accuracy; the 60-second entry guard remains active"}


def configuration(path):
    config = json.loads(path.read_text())
    allowed = {"agent_key", "base_url", "api_key", "user_id"}
    if set(config) - allowed or not all(config.get(k) for k in allowed - {"user_id"}):
        raise ValueError("LLM config must contain only agent_key/base_url/api_key/optional user_id")
    if not config["agent_key"].startswith("custom:") or "gemini" not in config["agent_key"].lower():
        raise ValueError("This evaluation requires the configured custom Gemini model")
    parsed = urlparse(config["base_url"])
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username
            or parsed.query or parsed.fragment):
        raise ValueError("Invalid LLM endpoint")
    return config


async def wait_for_priority():
    """Optional parent-written live-priority signal; stale state is not permission."""
    filename = os.environ.get("EVAL_PRIORITY_FILE")
    if not filename:
        return
    path = Path(filename)
    while True:
        try:
            status = json.loads(path.read_text())
            if status["busy"] is False and 0 <= time.time() - status["checked_at"] <= 30:
                return
        except (OSError, ValueError, KeyError, TypeError):
            pass
        await asyncio.sleep(2)


async def main(args, config):
    history = read_history(args.snapshot)
    selected = sorted((mid for mid, row in history.items() if row["source_time"] >= args.since),
                      key=lambda mid: (history[mid]["source_time"], mid))
    if args.ids:
        requested = {int(value) for value in args.ids.split(",")}
        if not requested <= set(selected):
            raise ValueError("Selected IDs absent from date-bounded frozen snapshot")
        selected = [mid for mid in selected if mid in requested]
    if args.max_cases is not None:
        selected = selected[:args.max_cases]
    if not selected:
        raise ValueError("Empty historical selection")
    args.output.mkdir(parents=True, exist_ok=True)
    identity = {"scope": SEMANTICS, "since": args.since, "message_ids": selected,
                "agent_key": config["agent_key"], "base_url": config["base_url"],
                "history_sha256": digest((args.snapshot / "history.json").read_bytes()),
                "images_sha256": digest((args.snapshot / "images/manifest.json").read_bytes()),
                "owned_cases": args.owned_cases}
    source_root = args.root / "telegram-trader"
    identity["source_hashes"] = {str(path.relative_to(source_root)): digest(path.read_bytes())
                                  for folder in ("trader", "condor-assets", "tests")
                                  for path in (source_root / folder).rglob("*")
                                  if path.is_file() and path.suffix in {".py", ".md"}}
    manifest = args.output / "manifest.json"
    if manifest.exists() and json.loads(manifest.read_text()) != identity:
        raise ValueError("Resume identity mismatch; use a new output directory")
    if manifest.exists() and not args.resume:
        raise ValueError("Output already started; pass --resume or use a new output directory")
    save(manifest, identity)
    os.environ["TRADER_TOKEN"] = "isolated-two-month-memory-only"
    os.environ["ADMIN_USER_ID"] = str(config.get("user_id", 1))
    routines = source_root / "condor-assets/telegram_agent/routines"
    interpreter = comprehensive.load("two_month_interpreter", routines / "signal_interpreter.py")
    planner = comprehensive.load("two_month_planner", routines / "exit_planner.py")
    reports = []
    for mid in selected:
        path = args.output / f"message-{mid}.json"
        if not path.exists():
            await wait_for_priority()
        report = json.loads(path.read_text()) if path.exists() else await scan_message(
            mid, interpreter, history, args.snapshot, config.get("user_id", 1))
        save(path, report)
        reports.append(report)
        print(json.dumps({key: report.get(key) for key in
                          ("message_id", "protocol_passed", "seconds", "error")}), flush=True)
        if report.get("fatal_cleanup_failure"):
            save(args.output / "failure.json", report)
            os._exit(70)  # Never start another model while abandoned SDK cleanup is alive.
        if model_failure_circuit(reports):
            save(args.output / "failure.json", {
                "reason": "Three consecutive model/infrastructure failures; sweep stopped",
                "completed": len(reports), "selected": len(selected),
                "message_ids": [row["message_id"] for row in reports[-3:]],
            })
            return 75
    owned_reports = []
    if args.owned_cases:
        cases = select_golden_cases(args.owned_cases, CASES)
        images = {name: ((source_root / "tests/assets" / f"{name}.png").read_bytes(), "image/png")
                  for name in ("explicit-short", "commentary")}
        with patch.object(comprehensive, "SnapshotChannel", lambda messages, image_cache:
                          DiskSnapshotChannel(messages, args.snapshot, image_cache)):
            for case in cases:
                path = args.output / (case["name"] + ".json")
                if not path.exists():
                    await wait_for_priority()
                try:
                    report = json.loads(path.read_text()) if path.exists() else await comprehensive.run_case(
                        case, interpreter, planner, history, images, 1)
                except comprehensive.restart_errors(interpreter, planner) as exc:
                    save(path, exc.evaluation_report)
                    os._exit(70)
                report["scope"] = ("separate additional golden scenario; current-revision "
                                   "context, not an as-of historical replay")
                report["future_edited_context_filtered"] = False
                save(path, report)
                owned_reports.append(report)
    summary = {"total": len(reports), "protocol_passed": sum(r["protocol_passed"] for r in reports),
               "semantic_passed": None, "scope": SEMANTICS,
               "execution_outcomes": outcome_counts(reports),
               "owned_golden_total": len(owned_reports),
               "owned_golden_passed": sum(r["passed"] for r in owned_reports)}
    save(args.output / "done.json", summary)
    return int(summary["protocol_passed"] != len(reports) or
               summary["owned_golden_passed"] != len(owned_reports))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--since", type=float, default=1787587200)
    parser.add_argument("--ids")
    parser.add_argument("--max-cases", type=int)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--owned-cases", help="Comma-separated additional golden case names, or all (includes empty state)")
    args = parser.parse_args()
    os.umask(0o077)
    config = configuration(Path(os.environ["EVAL_LLM_CONFIG"]))
    from condor.agents.agent import Agent, AgentStore, parse_frontmatter
    from condor.runtime import llm_client

    meta, instructions = parse_frontmatter(
        (args.root / "telegram-trader/condor-assets/telegram_agent/AGENT.md").read_text())
    agent = Agent(slug="telegram_agent", name=meta["name"], instructions=instructions,
                  agent_key=config["agent_key"], server_required=False)
    with ExitStack() as stack:
        stack.enter_context(patch.object(Gate, "__init__", side_effect=AssertionError("Real Gate forbidden")))
        stack.enter_context(patch.object(AgentStore, "get", lambda _store, slug:
                                        deepcopy(agent) if slug == "telegram_agent" else None))
        stack.enter_context(patch.object(llm_client, "resolve_custom_endpoint",
                                        return_value=(config["base_url"], config["api_key"])))
        raise SystemExit(asyncio.run(main(args, config)))
