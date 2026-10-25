"""Deployment probes and bounded logs must survive future Compose edits."""

from pathlib import Path

import yaml


def services():
    return yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "docker-compose.yml").read_text()
    )["services"]


def test_all_container_stdout_logs_have_finite_retention():
    for name, service in services().items():
        assert service["logging"] == {
            "driver": "json-file", "options": {"max-size": "10m", "max-file": "3"}
        }, name


def test_business_health_is_authenticated_read_only_and_bounds_network_wait():
    for name in ("condor", "telegram-trader"):
        health = services()[name]["healthcheck"]
        assert health["test"][:3] == ["CMD", "python", "-c"]
        code = health["test"][3]
        compile(code, "<healthcheck>", "exec")
        assert "/ready" in code and "TRADER_TOKEN" in code and "Authorization" in code
        assert "timeout=3" in code
        assert "/start" not in code and "/jobs/claim" not in code
        assert health["start_period"] == "120s"
        assert not services()[name].get("labels", {}).get("autoheal")


def test_readiness_does_not_introduce_a_startup_dependency_cycle():
    config = services()
    assert "telegram-trader" not in config["condor"].get("depends_on", {})
    assert "condor" not in config["telegram-trader"].get("depends_on", {})
