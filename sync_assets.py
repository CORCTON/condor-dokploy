"""Prepare persistent Condor state and remove the legacy asset mirror."""

from __future__ import annotations

import argparse
import io
import json
import os
import pickle
import shutil
import tempfile
from pathlib import Path

import yaml

LEGACY_TELEGRAM_MODELS = {"custom:glm-5.2", "custom:glm-5-2"}


def migrate_telegram_admin_model(state: Path) -> bool:
    """Move only the admin's old Telegram GLM choice to the deployed default."""
    path = state / "data" / "condor_bot_data.pickle"
    if not path.exists() or not os.environ.get("TELEGRAM_TOKEN"):
        return False

    from telegram.ext import ExtBot
    from telegram.ext._picklepersistence import _BotPickler, _BotUnpickler

    bot = ExtBot(token=os.environ["TELEGRAM_TOKEN"])
    with path.open("rb") as handle:
        data = _BotUnpickler(bot, handle).load()
    admin_id = int(os.environ["ADMIN_USER_ID"])
    admin = data.get("user_data", {}).get(admin_id)
    if (
        not isinstance(admin, dict)
        or admin.get("agent_llm") not in LEGACY_TELEGRAM_MODELS
    ):
        return False

    target = os.environ["CONDOR_DEFAULT_AGENT"]
    admin["agent_llm"] = target
    agent_prefs = admin.setdefault("user_preferences", {}).setdefault("agent", {})
    agent_prefs["active_agent_key"] = target
    if agent_prefs.get("default_agent") in LEGACY_TELEGRAM_MODELS:
        agent_prefs["default_agent"] = target

    buffer = io.BytesIO()
    _BotPickler(bot, buffer, protocol=pickle.HIGHEST_PROTOCOL).dump(data)
    backup = path.with_suffix(".pickle.pre-swe2.bak")
    if not backup.exists():
        shutil.copy2(path, backup)
    from condor.fsutil import atomic_write_bytes

    atomic_write_bytes(path, buffer.getvalue())
    return True


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

    admin_prefs = config.get("user_preferences", {}).get(config["admin_id"], {})
    agent_prefs = admin_prefs.get("agent", {})
    if agent_prefs.get("active_agent_key") in LEGACY_TELEGRAM_MODELS:
        agent_prefs["active_agent_key"] = os.environ["CONDOR_DEFAULT_AGENT"]
    if agent_prefs.get("default_agent") in LEGACY_TELEGRAM_MODELS:
        agent_prefs["default_agent"] = os.environ["CONDOR_DEFAULT_AGENT"]

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
    if migrate_telegram_admin_model(args.state):
        print("Migrated Telegram admin model to CONDOR_DEFAULT_AGENT", flush=True)


if __name__ == "__main__":
    main()
