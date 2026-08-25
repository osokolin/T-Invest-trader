from tinvest_trader.app.config import MarketActivityConfig, load_config


def test_market_activity_defaults_are_observational(monkeypatch) -> None:
    monkeypatch.delenv("TINVEST_MARKET_ACTIVITY_ENABLED", raising=False)

    cfg = load_config().market_activity

    assert cfg == MarketActivityConfig()
    assert cfg.enabled is False
    assert cfg.candle_interval == "CANDLE_INTERVAL_1_MIN"


def test_market_activity_config_parses_environment(monkeypatch) -> None:
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_ENABLED", "true")
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_POLL_INTERVAL_SECONDS", "90")
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_LOOKBACK_MINUTES", "120")
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_BASELINE_CANDLES", "30")
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_VOLUME_SPIKE_MULTIPLIER", "4.5")
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_PRICE_CHANGE_SPIKE_PCT", "0.02")
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_SESSION_FILTER_ENABLED", "false")
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_SESSION_START_HOUR_MOSCOW", "10")
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_SESSION_START_MINUTE_MOSCOW", "5")
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_SESSION_END_HOUR_MOSCOW", "18")
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_SESSION_END_MINUTE_MOSCOW", "40")

    cfg = load_config().market_activity

    assert cfg.enabled is True
    assert cfg.poll_interval_seconds == 90
    assert cfg.lookback_minutes == 120
    assert cfg.baseline_candles == 30
    assert cfg.volume_spike_multiplier == 4.5
    assert cfg.price_change_spike_pct == 0.02
    assert cfg.session_filter_enabled is False
    assert cfg.session_start_hour_moscow == 10
    assert cfg.session_start_minute_moscow == 5
    assert cfg.session_end_hour_moscow == 18
    assert cfg.session_end_minute_moscow == 40
