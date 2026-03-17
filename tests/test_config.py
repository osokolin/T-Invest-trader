from tinvest_trader.app.config import AppConfig


def test_load_config_returns_app_config(config):
    assert isinstance(config, AppConfig)


def test_default_config_is_sandbox(config):
    assert config.environment == "sandbox"
    assert config.broker.sandbox is True


def test_config_fields_populated(config):
    assert config.trading.max_position_size >= 1
    assert config.logging.level == "INFO"
