import logging

from tinvest_trader.app.container import Container
from tinvest_trader.execution.engine import ExecutionEngine
from tinvest_trader.infra.tbank.client import TBankClient
from tinvest_trader.instruments.registry import InstrumentRegistry
from tinvest_trader.market_data.service import MarketDataService
from tinvest_trader.portfolio.state import PortfolioState
from tinvest_trader.sentiment.source import StubMessageSource
from tinvest_trader.sentiment.telethon_source import (
    TelethonConfigError,
    TelethonMessageSource,
)
from tinvest_trader.services.background_runner import BackgroundRunner
from tinvest_trader.services.broker_event_ingestion_service import (
    BrokerEventIngestionService,
)
from tinvest_trader.services.cbr_ingestion_service import CbrIngestionService
from tinvest_trader.services.fusion_service import FusionService
from tinvest_trader.services.moex_ingestion_service import MoexIngestionService
from tinvest_trader.services.trading_service import TradingService


def test_build_container(container):
    assert isinstance(container, Container)


def test_container_has_logger(container):
    assert isinstance(container.logger, logging.Logger)


def test_container_has_tbank_client(container):
    assert isinstance(container.tbank_client, TBankClient)


def test_container_has_market_data(container):
    assert isinstance(container.market_data, MarketDataService)


def test_container_has_execution_engine(container):
    assert isinstance(container.execution_engine, ExecutionEngine)


def test_container_has_portfolio(container):
    assert isinstance(container.portfolio, PortfolioState)


def test_container_has_trading_service(container):
    assert isinstance(container.trading_service, TradingService)


def test_container_has_instrument_registry(container):
    assert isinstance(container.instrument_registry, InstrumentRegistry)


def test_container_storage_none_without_dsn(container):
    assert container.storage_pool is None
    assert container.repository is None


def test_container_sentiment_none_when_disabled(container):
    assert container.telegram_sentiment_service is None


def test_container_sentiment_wired_when_enabled(monkeypatch):
    monkeypatch.setenv("TINVEST_SENTIMENT_ENABLED", "true")
    monkeypatch.setenv("TINVEST_SENTIMENT_CHANNELS", "TestChannel")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container
    from tinvest_trader.services.telegram_sentiment_service import TelegramSentimentService

    cfg = load_config()
    c = build_container(cfg)
    assert isinstance(c.telegram_sentiment_service, TelegramSentimentService)


def test_container_sentiment_uses_stub_backend(monkeypatch):
    monkeypatch.setenv("TINVEST_SENTIMENT_ENABLED", "true")
    monkeypatch.setenv("TINVEST_SENTIMENT_CHANNELS", "TestChannel")
    monkeypatch.setenv("TINVEST_SENTIMENT_SOURCE_BACKEND", "stub")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    c = build_container(load_config())
    assert isinstance(c.telegram_sentiment_service._source, StubMessageSource)


def test_container_sentiment_uses_telethon_backend(monkeypatch):
    monkeypatch.setenv("TINVEST_SENTIMENT_ENABLED", "true")
    monkeypatch.setenv("TINVEST_SENTIMENT_CHANNELS", "TestChannel")
    monkeypatch.setenv("TINVEST_SENTIMENT_SOURCE_BACKEND", "telethon")
    monkeypatch.setenv("TINVEST_SENTIMENT_TELETHON_API_ID", "12345")
    monkeypatch.setenv("TINVEST_SENTIMENT_TELETHON_API_HASH", "hash-value")
    monkeypatch.setenv("TINVEST_SENTIMENT_TELETHON_SESSION_PATH", "/tmp/test.session")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    c = build_container(load_config())
    assert isinstance(c.telegram_sentiment_service._source, TelethonMessageSource)


def test_container_sentiment_telethon_requires_config(monkeypatch):
    monkeypatch.setenv("TINVEST_SENTIMENT_ENABLED", "true")
    monkeypatch.setenv("TINVEST_SENTIMENT_CHANNELS", "TestChannel")
    monkeypatch.setenv("TINVEST_SENTIMENT_SOURCE_BACKEND", "telethon")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    try:
        build_container(load_config())
    except TelethonConfigError:
        pass
    else:
        raise AssertionError("expected TelethonConfigError")


def test_container_sentiment_unknown_backend_raises(monkeypatch):
    monkeypatch.setenv("TINVEST_SENTIMENT_ENABLED", "true")
    monkeypatch.setenv("TINVEST_SENTIMENT_CHANNELS", "TestChannel")
    monkeypatch.setenv("TINVEST_SENTIMENT_SOURCE_BACKEND", "unknown")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    try:
        build_container(load_config())
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")


def test_container_observation_none_when_disabled(container):
    assert container.observation_service is None


def test_container_observation_wired_when_enabled(monkeypatch):
    monkeypatch.setenv("TINVEST_OBSERVATION_ENABLED", "true")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container
    from tinvest_trader.services.observation_service import ObservationService

    cfg = load_config()
    c = build_container(cfg)
    assert isinstance(c.observation_service, ObservationService)


def test_container_background_runner_none_when_disabled(container):
    assert container.background_runner is None


def test_container_background_runner_wired_when_enabled(monkeypatch):
    monkeypatch.setenv("TINVEST_BACKGROUND_ENABLED", "true")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    c = build_container(load_config())
    assert isinstance(c.background_runner, BackgroundRunner)


def test_container_background_runner_can_exist_without_optional_services(monkeypatch):
    monkeypatch.setenv("TINVEST_BACKGROUND_ENABLED", "true")
    monkeypatch.delenv("TINVEST_SENTIMENT_ENABLED", raising=False)
    monkeypatch.delenv("TINVEST_OBSERVATION_ENABLED", raising=False)
    monkeypatch.delenv("TINVEST_BROKER_EVENTS_ENABLED", raising=False)
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    c = build_container(load_config())
    assert isinstance(c.background_runner, BackgroundRunner)
    assert c.telegram_sentiment_service is None
    assert c.observation_service is None


def test_container_broker_event_service_none_when_disabled(container):
    assert container.broker_event_ingestion_service is None


def test_container_broker_event_service_wired_when_enabled(monkeypatch):
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_ENABLED", "true")
    monkeypatch.setenv("TINVEST_TRACKED_INSTRUMENTS", "FIGI1,FIGI2")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    c = build_container(load_config())
    assert isinstance(c.broker_event_ingestion_service, BrokerEventIngestionService)


def test_container_broker_event_service_uses_override_scope(monkeypatch):
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_ENABLED", "true")
    monkeypatch.setenv("TINVEST_TRACKED_INSTRUMENTS", "FIGI1,FIGI2")
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_TRACKED_FIGIS", "FIGI3")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    c = build_container(load_config())
    assert c.broker_event_ingestion_service._tracked_figis == ("FIGI3",)


def test_container_broker_event_service_uses_source_specific_lookbacks(monkeypatch):
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_ENABLED", "true")
    monkeypatch.setenv("TINVEST_TRACKED_INSTRUMENTS", "FIGI1")
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_DIVIDENDS_LOOKBACK_DAYS", "365")
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_REPORTS_LOOKBACK_DAYS", "90")
    monkeypatch.setenv("TINVEST_BROKER_EVENTS_INSIDER_DEALS_LOOKBACK_DAYS", "3650")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    c = build_container(load_config())
    assert c.broker_event_ingestion_service._lookback_days_by_event_type == {
        "dividends": 365,
        "reports": 90,
        "insider_deals": 3650,
    }


def test_container_fusion_none_when_disabled(container):
    assert container.fusion_service is None


def test_container_fusion_wired_when_enabled(monkeypatch):
    monkeypatch.setenv("TINVEST_FUSION_ENABLED", "true")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    c = build_container(load_config())
    assert isinstance(c.fusion_service, FusionService)


def test_container_cbr_none_when_disabled(container):
    assert container.cbr_ingestion_service is None


def test_container_cbr_wired_when_enabled(monkeypatch):
    monkeypatch.setenv("TINVEST_CBR_ENABLED", "true")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    c = build_container(load_config())
    assert isinstance(c.cbr_ingestion_service, CbrIngestionService)


def test_container_moex_none_when_disabled(container):
    assert container.moex_ingestion_service is None


def test_container_moex_wired_when_enabled(monkeypatch):
    monkeypatch.setenv("TINVEST_MOEX_ENABLED", "true")
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    c = build_container(load_config())
    assert isinstance(c.moex_ingestion_service, MoexIngestionService)
