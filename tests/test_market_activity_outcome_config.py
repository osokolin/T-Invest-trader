from tinvest_trader.app.config import MarketActivityOutcomeConfig, load_config


def test_market_activity_outcome_defaults_are_shadow_only(monkeypatch) -> None:
    monkeypatch.delenv("TINVEST_MARKET_ACTIVITY_OUTCOMES_ENABLED", raising=False)

    cfg = load_config().market_activity_outcomes

    assert cfg == MarketActivityOutcomeConfig()
    assert cfg.enabled is False
    assert cfg.horizons_minutes == (5, 15, 60)
    assert cfg.eod_enabled is True


def test_market_activity_outcome_config_parses_environment(monkeypatch) -> None:
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_OUTCOMES_ENABLED", "true")
    monkeypatch.setenv(
        "TINVEST_MARKET_ACTIVITY_OUTCOMES_HORIZONS_MINUTES", "3,10,30",
    )
    monkeypatch.setenv(
        "TINVEST_MARKET_ACTIVITY_OUTCOMES_NEUTRAL_THRESHOLD_PCT", "0.002",
    )
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_OUTCOMES_EOD_ENABLED", "false")
    monkeypatch.setenv("TINVEST_MARKET_ACTIVITY_OUTCOMES_LOOKBACK_DAYS", "14")
    monkeypatch.setenv(
        "TINVEST_MARKET_ACTIVITY_OUTCOMES_MAX_PRICE_DELAY_MINUTES", "45",
    )

    cfg = load_config().market_activity_outcomes

    assert cfg.enabled is True
    assert cfg.horizons_minutes == (3, 10, 30)
    assert cfg.neutral_threshold_pct == 0.002
    assert cfg.eod_enabled is False
    assert cfg.lookback_days == 14
    assert cfg.max_price_delay_minutes == 45
