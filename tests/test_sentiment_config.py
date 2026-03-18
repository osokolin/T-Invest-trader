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
