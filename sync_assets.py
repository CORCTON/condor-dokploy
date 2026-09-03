"""Synchronize versioned Condor assets into the persistent runtime volume."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path

import yaml

MUTABLE_PARTS = frozenset({"store", "sessions", "dry_runs"})
MUTABLE_FILES = frozenset({"config.yml", "learnings.md", "audit.log"})


def _managed_files(root: Path) -> dict[str, Path]:
    files: dict[str, Path] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if any(part in MUTABLE_PARTS for part in relative.parts):
            continue
        if relative.name in MUTABLE_FILES:
            continue
        files[relative.as_posix()] = path
    return files


def _atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as handle:
        temporary = Path(handle.name)
    try:
        shutil.copyfile(source, temporary)
        shutil.copymode(source, temporary)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _load_manifest(path: Path) -> set[str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return set()
    entries = payload.get("files", []) if isinstance(payload, dict) else []
    return {entry for entry in entries if isinstance(entry, str)}


def _write_manifest(path: Path, files: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({"schema": 1, "files": sorted(files)}, indent=2) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        handle.write(payload)
        temporary = Path(handle.name)
    os.replace(temporary, path)


def sync_tree(source: Path, destination: Path, manifest: Path) -> None:
    current = _managed_files(source)
    previous = _load_manifest(manifest)
    destination.mkdir(parents=True, exist_ok=True)

    for relative, source_file in current.items():
        _atomic_copy(source_file, destination / relative)

    for relative in sorted(previous - set(current)):
        stale = destination / relative
        if stale.is_file() or stale.is_symlink():
            stale.unlink()

    _write_manifest(manifest, set(current))


def ensure_config(path: Path) -> None:
    try:
        config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        config = {}
    if not isinstance(config, dict):
        raise ValueError(f"invalid Condor config at {path}")

    config["servers"] = {
        "main": {
            "host": os.environ.get("HUMMINGBOT_API_HOST", "hummingbot-api"),
            "port": int(os.environ.get("HUMMINGBOT_API_PORT", "8000")),
            "username": os.environ["HUMMINGBOT_API_USERNAME"],
            "password": os.environ["HUMMINGBOT_API_PASSWORD"],
        }
    }
    config["default_server"] = "main"
    config["admin_id"] = int(os.environ["ADMIN_USER_ID"])
    config.setdefault("users", {})
    config.setdefault("server_access", {})
    config.setdefault("chat_defaults", {})
    config.setdefault("audit_log", [])
    config.setdefault("telemetry", {})["consent"] = "denied"
    config.setdefault("sharing", {})["enabled"] = False

    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = yaml.safe_dump(config, sort_keys=False)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        handle.write(rendered)
        temporary = Path(handle.name)
    os.replace(temporary, path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--assets", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()

    sync_tree(
        args.assets / "agents",
        args.state / "agents",
        args.state / ".managed-agents.json",
    )
    sync_tree(
        args.assets / "routines",
        args.state / "routines",
        args.state / ".managed-routines.json",
    )
    ensure_config(args.state / "config.yml")


if __name__ == "__main__":
    main()
