"""Tests for background runner configuration."""

from tinvest_trader.app.config import BackgroundConfig


def test_background_config_defaults(config):
    assert isinstance(config.background, BackgroundConfig)
    assert config.background.enabled is False
    assert config.background.sentiment_ingest_interval_seconds == 300
    assert config.background.observation_interval_seconds == 600
    assert config.background.run_sentiment is True
    assert config.background.run_observation is True


def test_background_config_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_BACKGROUND_ENABLED", "true")
    monkeypatch.setenv("TINVEST_BACKGROUND_SENTIMENT_INTERVAL_SECONDS", "45")
    monkeypatch.setenv("TINVEST_BACKGROUND_OBSERVATION_INTERVAL_SECONDS", "90")
    monkeypatch.setenv("TINVEST_BACKGROUND_RUN_SENTIMENT", "false")
    monkeypatch.setenv("TINVEST_BACKGROUND_RUN_OBSERVATION", "true")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.background.enabled is True
    assert cfg.background.sentiment_ingest_interval_seconds == 45
    assert cfg.background.observation_interval_seconds == 90
    assert cfg.background.run_sentiment is False
    assert cfg.background.run_observation is True
