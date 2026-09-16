import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from sync_assets import ensure_config, remove_legacy_managed_files


class LegacyMigrationTests(unittest.TestCase):
    def test_removes_only_manifested_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            agents = state / "agents"
            managed = agents / "example" / "AGENT.md"
            runtime = agents / "example" / "store" / "memory.md"
            managed.parent.mkdir(parents=True)
            runtime.parent.mkdir(parents=True)
            managed.write_text("stock copy", encoding="utf-8")
            runtime.write_text("keep me", encoding="utf-8")

            manifest = state / ".managed-agents.json"
            manifest.write_text(
                json.dumps({"schema": 1, "files": ["example/AGENT.md"]}),
                encoding="utf-8",
            )

            remove_legacy_managed_files(agents, manifest)

            self.assertFalse(managed.exists())
            self.assertTrue(runtime.exists())
            self.assertFalse(manifest.exists())

    def test_ignores_paths_outside_the_managed_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            agents = state / "agents"
            outside = state / "outside.txt"
            outside.write_text("keep me", encoding="utf-8")
            manifest = state / ".managed-agents.json"
            manifest.write_text(
                json.dumps({"schema": 1, "files": ["../outside.txt"]}),
                encoding="utf-8",
            )

            remove_legacy_managed_files(agents, manifest)

            self.assertTrue(outside.exists())


class ConfigTests(unittest.TestCase):
    def test_preserves_existing_preferences_and_sets_server(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yml"
            path.write_text(
                yaml.safe_dump({"user_preferences": {"1": {"theme": "dark"}}}),
                encoding="utf-8",
            )
            environment = {
                "HUMMINGBOT_API_USERNAME": "user",
                "HUMMINGBOT_API_PASSWORD": "password",
                "ADMIN_USER_ID": "1",
            }

            with patch.dict(os.environ, environment, clear=False):
                ensure_config(path)

            config = yaml.safe_load(path.read_text(encoding="utf-8"))
            self.assertEqual(config["default_server"], "main")
            self.assertEqual(config["servers"]["main"]["host"], "hummingbot-api")
            self.assertEqual(config["user_preferences"]["1"]["theme"], "dark")


if __name__ == "__main__":
    unittest.main()
