"""Populate only this extension's owned files; never edit upstream /app source."""

import os
import shutil
from pathlib import Path


def install():
    source = Path("/bundle")
    deploy = Path("/deploy")
    agent = Path("/condor-state/agents/telegram_agent")
    deploy.mkdir(parents=True, exist_ok=True)
    agent.mkdir(parents=True, exist_ok=True)
    for name in ("entrypoint.sh", "sync_assets.py", "telegram_bootstrap.py"):
        shutil.copy2(source / name, deploy / name)
    (deploy / "entrypoint.sh").chmod(0o755)
    shutil.copytree(source / "condor-assets/telegram_agent", agent, dirs_exist_ok=True)
    path = agent / "AGENT.md"
    path.write_text(
        path.read_text().replace(
            "__MODEL__", os.getenv("TRADER_AGENT_MODEL", "custom:pending-provider")
        )
    )


if __name__ == "__main__":
    install()
