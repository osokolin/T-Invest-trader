"""Tests for MOEX config -- defaults and env var loading."""

from tinvest_trader.app.config import MoexConfig, load_config


def test_moex_disabled_by_default():
    cfg = load_config()
    assert cfg.moex.enabled is False


def test_moex_metadata_enabled_by_default():
    cfg = MoexConfig()
    assert cfg.metadata_enabled is True


def test_moex_history_enabled_by_default():
    cfg = MoexConfig()
    assert cfg.history_enabled is True


def test_moex_default_engine():
    cfg = MoexConfig()
    assert cfg.engine == "stock"


def test_moex_default_market():
    cfg = MoexConfig()
    assert cfg.market == "shares"


def test_moex_default_board():
    cfg = MoexConfig()
    assert cfg.board == "TQBR"


def test_moex_default_lookback():
    cfg = MoexConfig()
    assert cfg.history_lookback_days == 90


def test_moex_default_poll_interval():
    cfg = MoexConfig()
    assert cfg.poll_interval_seconds == 3600


def test_moex_env_vars(monkeypatch):
    monkeypatch.setenv("TINVEST_MOEX_ENABLED", "true")
    monkeypatch.setenv("TINVEST_MOEX_METADATA_ENABLED", "false")
    monkeypatch.setenv("TINVEST_MOEX_HISTORY_ENABLED", "false")
    monkeypatch.setenv("TINVEST_MOEX_POLL_INTERVAL_SECONDS", "1800")
    monkeypatch.setenv("TINVEST_MOEX_HISTORY_LOOKBACK_DAYS", "30")
    monkeypatch.setenv("TINVEST_MOEX_TRACKED_TICKERS", "SBER,GAZP")
    monkeypatch.setenv("TINVEST_MOEX_ENGINE", "currency")
    monkeypatch.setenv("TINVEST_MOEX_MARKET", "selt")
    monkeypatch.setenv("TINVEST_MOEX_BOARD", "CETS")
    cfg = load_config()
    assert cfg.moex.enabled is True
    assert cfg.moex.metadata_enabled is False
    assert cfg.moex.history_enabled is False
    assert cfg.moex.poll_interval_seconds == 1800
    assert cfg.moex.history_lookback_days == 30
    assert cfg.moex.tracked_tickers_override == ("SBER", "GAZP")
    assert cfg.moex.engine == "currency"
    assert cfg.moex.market == "selt"
    assert cfg.moex.board == "CETS"


def test_background_run_moex_default():
    cfg = load_config()
    assert cfg.background.run_moex is True


def test_background_run_moex_disabled(monkeypatch):
    monkeypatch.setenv("TINVEST_BACKGROUND_RUN_MOEX", "false")
    cfg = load_config()
    assert cfg.background.run_moex is False
