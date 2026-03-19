"""Tests for broker event ingestion configuration."""

from tinvest_trader.app.config import BrokerEventsConfig


def test_broker_events_config_defaults(config):
    assert isinstance(config.broker_events, BrokerEventsConfig)
    assert config.broker_events.enabled is False
    assert config.broker_events.event_types == ("dividends", "reports", "insider_deals")
    assert config.broker_events.poll_interval_seconds == 1800
    assert config.broker_events.lookback_days == 30
    assert config.broker_events.tracked_figis_override == ()


def test_broker_events_config_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_ENABLED", "true")
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_TYPES", "reports,insider_deals")
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_POLL_INTERVAL_SECONDS", "120")
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_LOOKBACK_DAYS", "14")
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_TRACKED_FIGIS", "FIGI1,FIGI2")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.broker_events.enabled is True
    assert cfg.broker_events.event_types == ("reports", "insider_deals")
    assert cfg.broker_events.poll_interval_seconds == 120
    assert cfg.broker_events.lookback_days == 14
    assert cfg.broker_events.tracked_figis_override == ("FIGI1", "FIGI2")
