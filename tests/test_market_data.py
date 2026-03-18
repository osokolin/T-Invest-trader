"""Tests for market_data/service.py -- normalized market data access."""

import logging

from tinvest_trader.app.config import BrokerConfig
from tinvest_trader.domain.enums import CandleInterval, TradingStatus
from tinvest_trader.domain.models import Candle, Instrument, MarketSnapshot
from tinvest_trader.infra.tbank.client import TBankClient
from tinvest_trader.market_data.service import MarketDataService


def _make_service() -> MarketDataService:
    """Create a MarketDataService with a stub client for testing."""
    client = TBankClient(
        config=BrokerConfig(),
        logger=logging.getLogger("test"),
    )
    return MarketDataService(
        client=client,
        logger=logging.getLogger("test"),
    )


def test_get_instrument():
    service = _make_service()
    result = service.get_instrument("BBG000B9XRY4")
    assert isinstance(result, Instrument)
    assert result.figi == "BBG000B9XRY4"


def test_get_snapshot():
    service = _make_service()
    result = service.get_snapshot("BBG000B9XRY4")
    assert isinstance(result, MarketSnapshot)
    assert result.instrument.figi == "BBG000B9XRY4"
    assert result.trading_status == TradingStatus.OPEN
    assert result.last_price.as_float > 0


def test_get_recent_candles():
    service = _make_service()
    result = service.get_recent_candles("BBG000B9XRY4", CandleInterval.MIN_5)
    assert len(result) >= 1
    assert isinstance(result[0], Candle)
    assert result[0].interval == CandleInterval.MIN_5
