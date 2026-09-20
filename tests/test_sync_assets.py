import json
import os
import pickle
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from sync_assets import (
    ensure_config,
    migrate_telegram_admin_model,
    remove_legacy_managed_files,
)


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

    def test_migrates_only_legacy_admin_model(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "user_preferences": {
                            1: {"agent": {"active_agent_key": "custom:glm-5.2"}},
                            2: {"agent": {"active_agent_key": "custom:glm-5.2"}},
                        }
                    }
                ),
                encoding="utf-8",
            )
            environment = {
                "HUMMINGBOT_API_USERNAME": "user",
                "HUMMINGBOT_API_PASSWORD": "password",
                "ADMIN_USER_ID": "1",
                "CONDOR_DEFAULT_AGENT": "custom:swe-2-high",
            }

            with patch.dict(os.environ, environment):
                ensure_config(path)

            config = yaml.safe_load(path.read_text(encoding="utf-8"))
            self.assertEqual(
                config["user_preferences"][1]["agent"]["active_agent_key"],
                "custom:swe-2-high",
            )
            self.assertEqual(
                config["user_preferences"][2]["agent"]["active_agent_key"],
                "custom:glm-5.2",
            )


class TelegramModelMigrationTests(unittest.TestCase):
    def test_preserves_other_persisted_data_and_is_idempotent(self) -> None:
        from telegram.ext import ExtBot
        from telegram.ext._picklepersistence import _BotPickler, _BotUnpickler

        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            path = state / "data" / "condor_bot_data.pickle"
            path.parent.mkdir()
            data = {
                "user_data": {
                    1: {
                        "agent_llm": "custom:glm-5.2",
                        "conversation_id": "keep-this",
                        "user_preferences": {
                            "agent": {"active_agent_key": "custom:glm-5.2"}
                        },
                    },
                    2: {"agent_llm": "custom:glm-5.2"},
                },
                "chat_data": {10: {"keep": True}},
                "conversations": {},
                "bot_data": {},
                "callback_data": None,
            }
            token = "123456:TESTTOKEN"
            bot = ExtBot(token=token)
            with path.open("wb") as handle:
                _BotPickler(bot, handle, protocol=pickle.HIGHEST_PROTOCOL).dump(data)

            with patch.dict(
                os.environ,
                {
                    "TELEGRAM_TOKEN": token,
                    "ADMIN_USER_ID": "1",
                    "CONDOR_DEFAULT_AGENT": "custom:swe-2-high",
                },
            ):
                self.assertTrue(migrate_telegram_admin_model(state))
                self.assertFalse(migrate_telegram_admin_model(state))

            with path.open("rb") as handle:
                updated = _BotUnpickler(bot, handle).load()
            with path.with_suffix(".pickle.pre-swe2.bak").open("rb") as handle:
                original = _BotUnpickler(bot, handle).load()
            self.assertEqual(original, data)
            self.assertEqual(updated["user_data"][1]["agent_llm"], "custom:swe-2-high")
            self.assertEqual(updated["user_data"][1]["conversation_id"], "keep-this")
            self.assertEqual(updated["user_data"][2]["agent_llm"], "custom:glm-5.2")
            self.assertEqual(updated["chat_data"], data["chat_data"])


if __name__ == "__main__":
    unittest.main()
