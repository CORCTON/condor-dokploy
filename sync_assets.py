"""Prepare persistent Condor state and remove the legacy asset mirror."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

import yaml

def _load_manifest(path: Path) -> set[str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return set()
    entries = payload.get("files", []) if isinstance(payload, dict) else []
    return {entry for entry in entries if isinstance(entry, str)}


def remove_legacy_managed_files(root: Path, manifest: Path) -> None:
    """Remove files copied by the pre-layering deployment wrapper.

    The manifests contain only paths that the old wrapper managed. Runtime
    state (stores, sessions, strategy config and learnings) was never listed,
    so it remains untouched while the official stock tree moves back to
    ``/app/agents``.
    """
    root.mkdir(parents=True, exist_ok=True)
    for relative in sorted(_load_manifest(manifest)):
        candidate = Path(relative)
        if candidate.is_absolute() or ".." in candidate.parts:
            continue
        target = root / candidate
        if target.is_file() or target.is_symlink():
            target.unlink()

        parent = target.parent
        while parent != root:
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent

    manifest.unlink(missing_ok=True)


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
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()

    remove_legacy_managed_files(
        args.state / "agents",
        args.state / ".managed-agents.json",
    )
    remove_legacy_managed_files(
        args.state / "routines",
        args.state / ".managed-routines.json",
    )
    ensure_config(args.state / "config.yml")


if __name__ == "__main__":
    main()
