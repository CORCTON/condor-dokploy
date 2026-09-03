"""Seed official Hummingbot assets for a Gate.io spot paper account."""

from __future__ import annotations

import shutil
from pathlib import Path

import yaml

SOURCE = Path("/opt/hummingbot-bots")
TARGET = Path("/seed-target")
MARKER = TARGET / ".condor-seeded"


def main() -> None:
    if MARKER.exists():
        return

    TARGET.mkdir(parents=True, exist_ok=True)
    shutil.copytree(SOURCE, TARGET, dirs_exist_ok=True)

    config_path = TARGET / "credentials/master_account/conf_client.yml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["paper_trade"] = {
        "paper_trade_exchanges": ["gate_io"],
        "paper_trade_account_balance": {"USDT": 100.0},
    }
    config["send_error_logs"] = False
    config_path.write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )
    MARKER.touch()


if __name__ == "__main__":
    main()
