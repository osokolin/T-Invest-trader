import logging

from tinvest_trader.app.config import load_config
from tinvest_trader.app.container import Container, build_container
from tinvest_trader.infra.tbank.client import TBankClient
from tinvest_trader.market_data.service import MarketDataService
from tinvest_trader.services.trading_service import TradingService


def test_build_container():
    config = load_config()
    container = build_container(config)
    assert isinstance(container, Container)


def test_container_has_logger():
    config = load_config()
    container = build_container(config)
    assert isinstance(container.logger, logging.Logger)


def test_container_has_tbank_client():
    config = load_config()
    container = build_container(config)
    assert isinstance(container.tbank_client, TBankClient)


def test_container_has_market_data():
    config = load_config()
    container = build_container(config)
    assert isinstance(container.market_data, MarketDataService)


def test_container_has_trading_service():
    config = load_config()
    container = build_container(config)
    assert isinstance(container.trading_service, TradingService)
