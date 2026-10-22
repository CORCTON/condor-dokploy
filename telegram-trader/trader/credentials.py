"""Read the selected credential source once at startup; never export plaintext."""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

CONNECTOR = "gate_io_perpetual"
SAVED_PATH = Path("/hummingbot-credentials") / f"{CONNECTOR}.yml"


@dataclass(frozen=True)
class GateCredentials:
    source: str
    environment: str
    error: str = ""
    key: str = field(default="", repr=False)
    secret: str = field(default="", repr=False)

    @property
    def ready(self):
        return bool(self.key and self.secret and not self.error)

    def status(self):
        # Explicit allowlist: this object must never be serialized with asdict().
        return {
            "source": self.source,
            "environment": self.environment,
            "ready": self.ready,
            "error": self.error,
        }


def load_credentials(*, environment=None, path=SAVED_PATH):
    environment = environment or os.getenv("GATE_ENVIRONMENT", "testnet")
    if environment not in {"testnet", "live"}:
        raise ValueError("GATE_ENVIRONMENT must be testnet or live")
    source = os.getenv("GATE_CREDENTIAL_SOURCE", "env")

    def unavailable(reason):
        return GateCredentials(source=source, environment=environment, error=reason)

    key, secret = os.getenv("GATE_API_KEY", ""), os.getenv("GATE_API_SECRET", "")
    if source == "env":
        if not key or not secret:
            return unavailable("GATE_API_KEY / GATE_API_SECRET missing")
    elif source == "hummingbot":
        if key or secret:
            return unavailable("Remove separate Gate environment keys when using Hummingbot")
        # This official connector has a live endpoint, not a testnet domain.
        if environment != "live":
            return unavailable("Saved gate_io_perpetual is live; GATE_ENVIRONMENT is testnet")
        password = os.getenv("HUMMINGBOT_CONFIG_PASSWORD", "")
        if not password:
            return unavailable("HUMMINGBOT_CONFIG_PASSWORD missing")
        try:
            from eth_keyfile import decode_keyfile_json

            config = yaml.safe_load(path.read_text())
            if config.get("connector") != CONNECTOR:
                return unavailable("Saved Hummingbot connector does not match gate_io_perpetual")

            def decrypt(name):
                # Same V3 keyfile codec used by Hummingbot's ETHKeyFileSecretManger.
                keyfile = json.loads(bytes.fromhex(config[name]))
                return decode_keyfile_json(keyfile, password.encode()).decode()

            key = decrypt("gate_io_perpetual_api_key")
            secret = decrypt("gate_io_perpetual_secret_key")
        except Exception:  # noqa: BLE001 -- YAML/crypto errors can contain secret input
            return unavailable(
                "Saved Hummingbot Gate credentials unavailable or cannot be decrypted"
            )
        if not key or not secret:
            return unavailable("Saved Hummingbot Gate credentials are empty")
    else:
        return unavailable("GATE_CREDENTIAL_SOURCE must be hummingbot or env")
    return GateCredentials(source=source, environment=environment, key=key, secret=secret)
