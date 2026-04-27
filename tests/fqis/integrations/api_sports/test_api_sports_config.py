from pathlib import Path

import pytest

from app.fqis.integrations.api_sports.config import ApiSportsConfig, ApiSportsConfigError


def test_config_from_env_requires_key(monkeypatch):
    monkeypatch.delenv("APISPORTS_KEY", raising=False)

    with pytest.raises(ApiSportsConfigError):
        ApiSportsConfig.from_env(require_key=True)


def test_config_from_env_without_key_for_tests(monkeypatch):
    monkeypatch.delenv("APISPORTS_KEY", raising=False)

    config = ApiSportsConfig.from_env(require_key=False)

    assert config.api_key == ""
    assert config.base_url == "https://v3.football.api-sports.io"


def test_config_redacts_key():
    config = ApiSportsConfig(api_key="SECRET", cache_dir=Path("data/cache/api_sports"))

    redacted = config.redacted()

    assert redacted["api_key"] == "***REDACTED***"
    assert "SECRET" not in str(redacted)


def test_config_rejects_invalid_timeout(monkeypatch):
    monkeypatch.setenv("APISPORTS_KEY", "SECRET")
    monkeypatch.setenv("APISPORTS_TIMEOUT_SECONDS", "not-a-number")

    with pytest.raises(ApiSportsConfigError):
        ApiSportsConfig.from_env(require_key=True)
