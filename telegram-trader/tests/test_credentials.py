import json
import os

import pytest
import yaml
from aiohttp.test_utils import TestClient, TestServer
from eth_keyfile import create_keyfile_json

from trader.app import make_app
from trader.credentials import CONNECTOR, load_credentials
from trader.engine import Engine
from trader.gate import Gate, GateError
from trader.store import Store

KEY, SECRET, PASSWORD = "k" * 32, "s" * 32, "test-only-password"


@pytest.fixture
def saved(tmp_path, monkeypatch):
    monkeypatch.setenv("GATE_CREDENTIAL_SOURCE", "hummingbot")
    monkeypatch.setenv("GATE_ENVIRONMENT", "live")
    monkeypatch.setenv("HUMMINGBOT_CONFIG_PASSWORD", PASSWORD)
    monkeypatch.delenv("GATE_API_KEY", raising=False)
    monkeypatch.delenv("GATE_API_SECRET", raising=False)

    def encrypt(value):
        # Hummingbot stores hex-encoded JSON V3 keyfiles without address/id.
        keyfile = create_keyfile_json(value.encode(), PASSWORD.encode(), iterations=10)
        keyfile.pop("address", None)
        keyfile.pop("id", None)
        return json.dumps(keyfile).encode().hex()

    path = tmp_path / f"{CONNECTOR}.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "connector": CONNECTOR,
                "gate_io_perpetual_api_key": encrypt(KEY),
                "gate_io_perpetual_secret_key": encrypt(SECRET),
            }
        )
    )
    return path


def test_saved_keyfile_is_reused_without_export_or_mutation(saved):
    before = saved.read_bytes()
    credentials = load_credentials(path=saved)
    assert credentials.ready and (credentials.key, credentials.secret) == (KEY, SECRET)
    assert saved.read_bytes() == before
    assert not os.getenv("GATE_API_KEY")
    assert not os.getenv("GATE_API_SECRET")
    assert credentials.status() == {
        "source": "hummingbot",
        "environment": "live",
        "ready": True,
        "error": "",
    }
    for value in (KEY, SECRET, PASSWORD):
        assert value not in repr(credentials) + json.dumps(credentials.status())


@pytest.mark.parametrize(
    "failure", ["missing", "malformed", "wrong_password", "wrong_connector", "empty"]
)
def test_saved_credential_failures_are_sanitized_and_never_fallback(saved, monkeypatch, failure):
    if failure == "missing":
        saved.unlink()
    elif failure == "malformed":
        saved.write_text(f"connector: [{SECRET}")
    elif failure == "wrong_password":
        monkeypatch.setenv("HUMMINGBOT_CONFIG_PASSWORD", "wrong")
    elif failure == "wrong_connector":
        saved.write_text(yaml.safe_dump({"connector": "gate_io"}))
    else:
        saved.write_text("")
    credentials = load_credentials(path=saved)
    assert not credentials.ready and not credentials.key and not credentials.secret
    assert credentials.error
    for value in (KEY, SECRET, PASSWORD):
        assert value not in repr(credentials) + json.dumps(credentials.status())


def test_live_saved_credentials_never_reach_testnet(saved):
    credentials = load_credentials(environment="testnet", path=saved)
    assert not credentials.ready and not credentials.key and not credentials.secret
    assert "is live" in credentials.error and "testnet" in credentials.error


@pytest.mark.parametrize("name", ["GATE_API_KEY", "GATE_API_SECRET"])
def test_mixed_sources_rejected(saved, monkeypatch, name):
    monkeypatch.setenv(name, "other-account")
    credentials = load_credentials(path=saved)
    assert not credentials.ready and "separate Gate" in credentials.error


def test_missing_password_blocks_saved_source(saved, monkeypatch):
    monkeypatch.delenv("HUMMINGBOT_CONFIG_PASSWORD")
    assert load_credentials(path=saved).error == "HUMMINGBOT_CONFIG_PASSWORD missing"


def test_explicit_testnet_source_does_not_read_saved_file(saved, monkeypatch):
    monkeypatch.setenv("GATE_CREDENTIAL_SOURCE", "env")
    monkeypatch.setenv("GATE_API_KEY", "testnet-only-key")
    monkeypatch.setenv("GATE_API_SECRET", "testnet-only-secret")
    saved.unlink()
    credentials = load_credentials(environment="testnet", path=saved)
    assert credentials.ready and credentials.key == "testnet-only-key"
    monkeypatch.delenv("GATE_API_SECRET")
    assert not load_credentials(path=saved).ready


def test_unknown_source_or_environment_rejected(saved, monkeypatch):
    monkeypatch.setenv("GATE_CREDENTIAL_SOURCE", "typo")
    assert not load_credentials(path=saved).ready
    with pytest.raises(ValueError, match="testnet or live"):
        load_credentials(environment="typo", path=saved)


def test_next_start_observes_credential_removal(saved):
    assert load_credentials(path=saved).ready
    saved.unlink()
    assert not load_credentials(path=saved).ready


async def test_missing_credentials_block_private_network_requests():
    gate = Gate(None, "", "")  # Any network access would fail with AttributeError.
    with pytest.raises(GateError, match="CREDENTIALS_UNAVAILABLE"):
        await gate.get("/accounts")
    with pytest.raises(GateError, match="CREDENTIALS_UNAVAILABLE"):
        await gate.request("POST", "/orders", {"contract": "INJ_USDT"})


async def test_status_redacts_credentials_and_loading_never_enables_trading(saved, monkeypatch):
    monkeypatch.setenv("TRADER_TOKEN", "test-only-token")
    credentials = load_credentials(path=saved)
    store = Store(":memory:")
    store.set("paused", True)
    engine = Engine(store, Gate(None, credentials.key, credentials.secret))
    try:
        async with TestClient(TestServer(make_app(engine, credentials=credentials))) as client:
            response = await client.get(
                "/status", headers={"Authorization": "Bearer test-only-token"}
            )
            raw = await response.text()
            status = json.loads(raw)
            assert status["credentials"]["ready"]
            assert not status["enabled"] and status["paused"] and not status["trades"]
            assert not (await (await client.get("/health")).json())["ready"]
            assert all(value not in raw for value in (KEY, SECRET, PASSWORD))
    finally:
        store.close()


async def test_health_not_ready_on_environment_mismatch(saved, monkeypatch, tmp_path):
    for name in ("TG_API_ID", "TG_API_HASH", "TG_CHANNEL_ID", "TRADER_TOKEN"):
        monkeypatch.setenv(name, "configured")
    session = tmp_path / "session"
    session.touch()
    monkeypatch.setenv("TG_SESSION_PATH", str(session))
    credentials = load_credentials(environment="testnet", path=saved)
    store = Store(":memory:")
    engine = Engine(store, Gate(None, "", ""), enabled=True)
    store.set("paused", False)
    engine.listener_ready = True
    try:
        async with TestClient(TestServer(make_app(engine, credentials=credentials))) as client:
            status = await (await client.get("/health")).json()
            assert not status["ready"] and status["missing"] == [credentials.error]
    finally:
        store.close()
