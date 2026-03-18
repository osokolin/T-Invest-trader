from tinvest_trader.app.config import (
    AppConfig,
    DatabaseConfig,
    MarketDataConfig,
    _parse_csv,
)


def test_load_config_returns_app_config(config):
    assert isinstance(config, AppConfig)


def test_default_config_is_sandbox(config):
    assert config.environment == "sandbox"
    assert config.broker.sandbox is True


def test_config_fields_populated(config):
    assert config.trading.max_position_size >= 1
    assert config.logging.level == "INFO"


def test_database_config_defaults(config):
    assert isinstance(config.database, DatabaseConfig)
    assert config.database.postgres_dsn == ""
    assert config.database.pool_min_size == 2
    assert config.database.pool_max_size == 5


def test_market_data_config_defaults(config):
    assert isinstance(config.market_data, MarketDataConfig)
    assert config.market_data.tracked_instruments == ()


def test_enabled_instruments_defaults(config):
    assert config.trading.enabled_instruments == ()


def test_parse_csv_basic():
    assert _parse_csv("AAA,BBB,CCC") == ("AAA", "BBB", "CCC")


def test_parse_csv_with_spaces():
    assert _parse_csv("  AAA , BBB , CCC  ") == ("AAA", "BBB", "CCC")


def test_parse_csv_empty():
    assert _parse_csv("") == ()
    assert _parse_csv("   ") == ()


def test_parse_csv_single():
    assert _parse_csv("FIGI1") == ("FIGI1",)


def test_tracked_instruments_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_TRACKED_INSTRUMENTS", "FIGI1,FIGI2,FIGI3")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.market_data.tracked_instruments == ("FIGI1", "FIGI2", "FIGI3")


def test_enabled_instruments_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_ENABLED_INSTRUMENTS", "FIGI1,FIGI2")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.trading.enabled_instruments == ("FIGI1", "FIGI2")


def test_account_id_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_ACCOUNT_ID", "acc-123")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.broker.account_id == "acc-123"
