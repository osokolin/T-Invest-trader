import logging

from tinvest_trader.app.container import Container
from tinvest_trader.execution.engine import ExecutionEngine
from tinvest_trader.infra.tbank.client import TBankClient
from tinvest_trader.instruments.registry import InstrumentRegistry
from tinvest_trader.market_data.service import MarketDataService
from tinvest_trader.portfolio.state import PortfolioState
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
