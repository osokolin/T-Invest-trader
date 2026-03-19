"""Tests for CBR config -- defaults and env var loading."""

from tinvest_trader.app.config import CbrConfig, load_config


def test_cbr_disabled_by_default():
    cfg = load_config()
    assert cfg.cbr.enabled is False


def test_cbr_rss_enabled_by_default():
    cfg = CbrConfig()
    assert cfg.rss_enabled is True


def test_cbr_default_urls():
    cfg = CbrConfig()
    assert len(cfg.rss_urls) == 2
    assert "eventrss" in cfg.rss_urls[0]
    assert "RssPress" in cfg.rss_urls[1]


def test_cbr_default_poll_interval():
    cfg = CbrConfig()
    assert cfg.poll_interval_seconds == 3600


def test_cbr_default_store_raw():
    cfg = CbrConfig()
    assert cfg.store_raw_payloads is True


def test_cbr_env_vars(monkeypatch):
    monkeypatch.setenv("TINVEST_CBR_ENABLED", "true")
    monkeypatch.setenv("TINVEST_CBR_RSS_ENABLED", "false")
    monkeypatch.setenv("TINVEST_CBR_RSS_URLS", "http://example.com/rss1,http://example.com/rss2")
    monkeypatch.setenv("TINVEST_CBR_POLL_INTERVAL_SECONDS", "1800")
    monkeypatch.setenv("TINVEST_CBR_STORE_RAW_PAYLOADS", "false")
    cfg = load_config()
    assert cfg.cbr.enabled is True
    assert cfg.cbr.rss_enabled is False
    assert cfg.cbr.rss_urls == ("http://example.com/rss1", "http://example.com/rss2")
    assert cfg.cbr.poll_interval_seconds == 1800
    assert cfg.cbr.store_raw_payloads is False


def test_background_run_cbr_default():
    cfg = load_config()
    assert cfg.background.run_cbr is True


def test_background_run_cbr_disabled(monkeypatch):
    monkeypatch.setenv("TINVEST_BACKGROUND_RUN_CBR", "false")
    cfg = load_config()
    assert cfg.background.run_cbr is False
