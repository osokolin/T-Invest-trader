import logging

from tinvest_trader.app.container import Container
from tinvest_trader.infra.tbank.client import TBankClient
from tinvest_trader.services.trading_service import TradingService


def test_build_container(container):
    assert isinstance(container, Container)


def test_container_has_logger(container):
    assert isinstance(container.logger, logging.Logger)


def test_container_has_tbank_client(container):
    assert isinstance(container.tbank_client, TBankClient)


def test_container_has_trading_service(container):
    assert isinstance(container.trading_service, TradingService)
