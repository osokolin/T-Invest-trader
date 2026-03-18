"""Tests for observation config loading."""

from tinvest_trader.app.config import ObservationConfig


def test_observation_config_defaults(config):
    assert isinstance(config.observation, ObservationConfig)
    assert config.observation.enabled is False
    assert config.observation.windows == ("5m", "15m", "1h")
    assert config.observation.persist_derived_metrics is True
    assert config.observation.tracked_tickers == ()


def test_observation_enabled_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_OBSERVATION_ENABLED", "true")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.observation.enabled is True


def test_observation_windows_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_OBSERVATION_WINDOWS", "1m,10m,2h")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.observation.windows == ("1m", "10m", "2h")


def test_observation_tracked_tickers_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_OBSERVATION_TRACKED_TICKERS", "SBER,GAZP")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.observation.tracked_tickers == ("SBER", "GAZP")


def test_observation_persist_false_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_OBSERVATION_PERSIST_DERIVED", "false")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.observation.persist_derived_metrics is False
