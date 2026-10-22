from pathlib import Path

import yaml


def test_trader_reuses_encrypted_account_and_password_without_other_host_access():
    compose = yaml.safe_load((Path(__file__).parents[1] / "docker-compose.yml").read_text())
    services = compose["services"]
    trader = services["telegram-trader"]
    assert trader["environment"]["HUMMINGBOT_CONFIG_PASSWORD"] == (
        services["hummingbot-api"]["environment"]["CONFIG_PASSWORD"]
    )
    assert trader["environment"]["GATE_CREDENTIAL_SOURCE"] == "${GATE_CREDENTIAL_SOURCE:-hummingbot}"
    assert trader["volumes"] == [
        "telegram-trader-state:/state",
        "/var/lib/condor-spot/bots/credentials/${HUMMINGBOT_ACCOUNT_NAME:-master_account}"
        "/connectors:/hummingbot-credentials:ro",
    ]
    assert trader["environment"]["TRADER_ENABLED"] == "${TRADER_ENABLED:-false}"
    assert trader["environment"]["GATE_ENVIRONMENT"] == "${GATE_ENVIRONMENT:-testnet}"
    assert "GATE_API_KEY" not in services["condor"]["environment"]
    assert "HUMMINGBOT_CONFIG_PASSWORD" not in services["condor"]["environment"]
    assert trader["environment"]["TELEGRAM_TOKEN"] == services["condor"]["environment"]["TELEGRAM_TOKEN"]
    assert trader["environment"]["TELEGRAM_ADMIN_USER_ID"] == services["condor"]["environment"]["ADMIN_USER_ID"]
