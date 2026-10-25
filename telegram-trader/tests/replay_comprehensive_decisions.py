"""Offline execution-only replay of saved real-model comprehensive decisions.

Usage: uv run --offline python tests/replay_comprehensive_decisions.py EVIDENCE_DIR
Reads results.json/history.json and writes replay-results.json in EVIDENCE_DIR.
No model is called, no channel is fetched, and no live/testnet exchange is used.
Semantic assertions recheck existing decisions; they are NOT new model samples.
"""

import argparse
import asyncio
import hashlib
import json
import os
import sys
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

import aiohttp

# Allow this standalone tests/ CLI to import the sibling trader package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import eval_comprehensive_channel as evaluation


def digest(data):
    return hashlib.sha256(data).hexdigest()


def local_url(path):
    base = os.environ["TELEGRAM_TRADER_URL"].rstrip("/")
    parsed = urlsplit(base)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port
            or parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment
            or not path.startswith("/") or path.startswith("//") or "?" in path or "#" in path):
        raise ValueError("Replay requests must target the isolated loopback HTTP server")
    return base + path


def source_disabled(*_args, **_kwargs):
    raise RuntimeError("Offline replay cannot retrieve production evidence")


class ScriptedInterpreter:
    """Stage saved decisions through the actual interpreter's HTTP API helper."""

    def __init__(self, original, actual):
        self.original, self.actual = original, actual
        self.remapped_event_ids = {}

    async def api(self, session, method, path, **kwargs):
        local_url(path)
        return await self.actual.api(session, method, path, **kwargs)

    @asynccontextmanager
    async def interpret_current(self, job, _owner, session):
        saved = self.original.get("result")
        if not isinstance(saved, dict) or not isinstance(saved.get("decisions"), list):
            raise TypeError("Original sample has no recorded decision proposal; cannot replay")
        result = deepcopy(saved)
        current = {event["message_id"]: event["event_id"] for event in job["events"]}
        seen = set()
        for decision in result["decisions"]:
            original_id = decision["event_id"]
            channel, message, _revision = original_id.split(":")
            message_id = int(message)
            if int(channel) != evaluation.CHANNEL or message_id not in current or message_id in seen:
                raise ValueError("Saved decision IDs do not match this case's claimed messages")
            seen.add(message_id)
            decision["event_id"] = current[message_id]
            self.remapped_event_ids[original_id] = current[message_id]
        if seen != set(current):
            raise ValueError("Saved proposal does not cover every claimed message")

        if self.original["case"] == "derived_fifth_image_explicit_entry":
            if any({"op": op, "message_id": 20004} not in self.original.get("tools", [])
                   for op in ("image", "download")):
                raise ValueError("Original sample did not record inspecting the fifth source image")
            # Reenact this evidenced read against local assets only, so run_case's
            # image assertion remains meaningful. This is not new vision inference.
            async with session.get(
                local_url("/channel/messages/20004/image"),
                headers={"Authorization": "Bearer " + os.environ["TRADER_TOKEN"]},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as response:
                response.raise_for_status()
                if not (await response.read()):
                    raise ValueError("Local fifth-image fixture is empty")

        response = await self.api(session, "POST", "/signals/proposals", json={
            "lease": job["lease"], "result": result,
        })
        if response != {"accepted": True, "executed": False}:
            raise ValueError("Decision proposal was not staged without execution")
        yield result


class ScriptedPlanner:
    def __init__(self, original, interpreter):
        self.original, self.interpreter = original, interpreter

    @asynccontextmanager
    async def generate_plan(self, job, _owner):
        saved = self.original.get("real_model_exit_plan")
        if not isinstance(saved, dict):
            raise TypeError("Original sample has no recorded exit plan; cannot regenerate one offline")
        plan = deepcopy(saved)
        async with aiohttp.ClientSession() as session:
            response = await self.interpreter.api(session, "POST", "/plans/proposals", json={
                "trade_id": job["trade_id"], "version": job["version"],
                "lease": job["lease"], "plan": plan,
            })
        if response != {"accepted": True, "executed": False}:
            raise ValueError("Exit proposal was not staged without execution")
        yield plan


def load_evidence(directory):
    result_bytes = (directory / "results.json").read_bytes()
    history_bytes = (directory / "history.json").read_bytes()
    originals = json.loads(result_bytes)
    if not isinstance(originals, list) or not originals:
        raise ValueError("results.json must contain at least one recorded sample")
    history = {int(key): value for key, value in json.loads(history_bytes).items()}
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    for key, entry in manifest.items():
        if digest(json.dumps(history[int(key)], sort_keys=True).encode()) != entry["text_sha256"]:
            raise ValueError(f"Frozen history differs from the source manifest: {key}")
    images = {}
    for path in directory.glob("source-image-*"):
        mid = int(path.name.removeprefix("source-image-"))
        data = path.read_bytes()
        recorded_hash = manifest.get(str(mid), {}).get("image_sha256")
        if recorded_hash and digest(data) != recorded_hash:
            raise ValueError(f"Source image differs from the source manifest: {mid}")
        images[mid] = data, history[mid]["media"]["mime"]
    for name in ("explicit-short", "commentary"):
        images[name] = (Path(__file__).parent / "assets" / f"{name}.png").read_bytes(), "image/png"
    return originals, history, images, {
        "results_sha256": digest(result_bytes), "history_sha256": digest(history_bytes),
        "available_source_images": sorted(key for key in images if isinstance(key, int)),
        "local_asset_sha256": {key: digest(data) for key, (data, _mime) in images.items()
                               if isinstance(key, str)},
    }


async def replay(directory):
    originals, history, images, evidence = load_evidence(directory)
    specs = {spec["name"]: spec for spec in evaluation.CASES}
    routine = Path(__file__).resolve().parents[1] / "condor-assets/telegram_agent/routines/signal_interpreter.py"
    actual = evaluation.load("offline_replay_interpreter_api", routine)
    reports, seen, errors = [], set(), []
    output = {
        "kind": "execution-only replay", "new_model_samples": 0,
        "description": "Saved model decisions and exit plans, current local HTTP/execution assertions; no new inference",
        "snapshot_semantics": "Current-revision evidence retimed as first arrivals; not a point-in-time backtest",
        "evidence": evidence, "reports": reports, "errors": errors,
        "candidate_hashes": {
            str(path.relative_to(routine.parents[3])): digest(path.read_bytes())
            for folder in (routine.parents[3] / "trader", Path(__file__).parent)
            for path in sorted(folder.rglob("*.py"))
        },
        "interpreter_sha256": digest(routine.read_bytes()),
    }
    for original in originals:
        name, run = original.get("case"), original.get("run", 1)
        identity = name, run
        if name not in specs or identity in seen:
            errors.append(f"Unknown or duplicate recorded sample: {identity}")
            continue
        seen.add(identity)
        interpreter = ScriptedInterpreter(original, actual)
        report = await evaluation.run_case(
            specs[name], interpreter, ScriptedPlanner(original, interpreter), history, images, run,
        )
        # run_case's timer normally encloses inference. Here it measures replay only.
        report["replay_processing_seconds"] = report.pop("model_seconds", None)
        report.update(
            evaluation_kind="execution-only replay", new_model_sample=False,
            original_passed=original.get("passed"), original_model_seconds=original.get("model_seconds"),
            original_report_sha256=digest(json.dumps(original, sort_keys=True).encode()),
            remapped_event_ids=interpreter.remapped_event_ids,
            oracle_changed=original.get("expected") != specs[name]["expected"],
        )
        reports.append(report)
        print(json.dumps({key: report.get(key) for key in
                          ("case", "passed", "semantic_passed", "execution_passed", "error")}), flush=True)
    output["missing_cases"] = sorted(set(specs) - {name for name, _run in seen})
    output["collection_complete"] = not output["missing_cases"] and not errors
    output["total"] = len(reports)
    output["passed_count"] = sum(report["passed"] for report in reports)
    output["passed"] = bool(reports) and output["collection_complete"] and all(r["passed"] for r in reports)
    (directory / "replay-results.json").write_text(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if output["passed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence_dir", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    with patch.dict(os.environ, {
        "TELEGRAM_TRADER_URL": "http://127.0.0.1:1", "TRADER_TOKEN": "isolated-offline-replay",
        "ADMIN_USER_ID": "1",
    }), patch.object(evaluation, "source_get", source_disabled):
        return asyncio.run(replay(args.evidence_dir.resolve()))


if __name__ == "__main__":
    raise SystemExit(main())
