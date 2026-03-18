"""Tests for sentiment configuration."""

from tinvest_trader.app.config import SentimentConfig


def test_sentiment_config_defaults(config):
    assert isinstance(config.sentiment, SentimentConfig)
    assert config.sentiment.enabled is False
    assert config.sentiment.channels == ()
    assert config.sentiment.tracked_tickers == ()
    assert config.sentiment.model_name == "stub"
    assert config.sentiment.source_backend == "stub"


def test_sentiment_enabled_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_SENTIMENT_ENABLED", "true")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.sentiment.enabled is True


def test_sentiment_channels_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_SENTIMENT_CHANNELS", "MarketTwits,InvestChannel")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.sentiment.channels == ("MarketTwits", "InvestChannel")


def test_sentiment_tracked_tickers_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_SENTIMENT_TRACKED_TICKERS", "SBER,GAZP,LKOH")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.sentiment.tracked_tickers == ("SBER", "GAZP", "LKOH")


def test_sentiment_model_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_SENTIMENT_MODEL_NAME", "custom-model")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.sentiment.model_name == "custom-model"


def test_sentiment_telethon_defaults(config):
    assert config.sentiment.telethon_api_id is None
    assert config.sentiment.telethon_api_hash == ""
    assert config.sentiment.telethon_session_path == ""
    assert config.sentiment.telethon_poll_limit == 50
    assert config.sentiment.telethon_request_timeout_sec is None


def test_sentiment_telethon_fields_from_env(monkeypatch):
    monkeypatch.setenv("TINVEST_SENTIMENT_TELETHON_API_ID", "12345")
    monkeypatch.setenv("TINVEST_SENTIMENT_TELETHON_API_HASH", "hash-value")
    monkeypatch.setenv("TINVEST_SENTIMENT_TELETHON_SESSION_PATH", "/tmp/telethon.session")
    monkeypatch.setenv("TINVEST_SENTIMENT_TELETHON_POLL_LIMIT", "25")
    monkeypatch.setenv("TINVEST_SENTIMENT_TELETHON_TIMEOUT_SEC", "7.5")
    from tinvest_trader.app.config import load_config

    cfg = load_config()
    assert cfg.sentiment.telethon_api_id == 12345
    assert cfg.sentiment.telethon_api_hash == "hash-value"
    assert cfg.sentiment.telethon_session_path == "/tmp/telethon.session"
    assert cfg.sentiment.telethon_poll_limit == 25
    assert cfg.sentiment.telethon_request_timeout_sec == 7.5
