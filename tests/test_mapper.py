"""Tests for infra/tbank/mapper.py -- broker DTO to domain model mapping."""

from tinvest_trader.domain.enums import CandleInterval, TradingStatus
from tinvest_trader.domain.models import Candle, Instrument, MarketSnapshot, MoneyValue
from tinvest_trader.infra.tbank.mapper import (
    map_candle,
    map_instrument,
    map_market_snapshot,
    map_money_value,
    map_trading_status,
)


def test_map_money_value():
    raw = {"currency": "RUB", "units": 100, "nano": 500_000_000}
    result = map_money_value(raw)
    assert isinstance(result, MoneyValue)
    assert result.currency == "RUB"
    assert result.units == 100
    assert result.nano == 500_000_000
    assert result.as_float == 100.5


def test_map_money_value_defaults():
    result = map_money_value({})
    assert result.currency == "RUB"
    assert result.units == 0
    assert result.nano == 0
    assert result.as_float == 0.0


def test_map_instrument():
    raw = {"figi": "BBG000B9XRY4", "ticker": "AAPL", "name": "Apple Inc"}
    result = map_instrument(raw)
    assert isinstance(result, Instrument)
    assert result.figi == "BBG000B9XRY4"
    assert result.ticker == "AAPL"
    assert result.name == "Apple Inc"


def test_map_trading_status_known():
    assert map_trading_status("SECURITY_TRADING_STATUS_NORMAL_TRADING") == TradingStatus.OPEN
    assert (
        map_trading_status("SECURITY_TRADING_STATUS_NOT_AVAILABLE_FOR_TRADING")
        == TradingStatus.CLOSED
    )
    assert map_trading_status("SECURITY_TRADING_STATUS_OPENING_PERIOD") == TradingStatus.PREMARKET


def test_map_trading_status_unknown():
    assert map_trading_status("SOMETHING_UNEXPECTED") == TradingStatus.UNKNOWN


def test_map_candle():
    price = {"currency": "RUB", "units": 100, "nano": 0}
    raw = {
        "open": price,
        "high": {**price, "units": 101},
        "low": {**price, "units": 99},
        "close": price,
        "volume": 500,
        "time": "2026-03-18T10:00:00+00:00",
    }
    result = map_candle(raw, CandleInterval.MIN_5)
    assert isinstance(result, Candle)
    assert result.open.units == 100
    assert result.high.units == 101
    assert result.low.units == 99
    assert result.volume == 500
    assert result.interval == CandleInterval.MIN_5


def test_map_market_snapshot():
    instrument = Instrument(figi="BBG000B9XRY4", ticker="AAPL", name="Apple Inc")
    last_price_raw = {"currency": "USD", "units": 170, "nano": 250_000_000}
    result = map_market_snapshot(
        instrument,
        last_price_raw,
        "SECURITY_TRADING_STATUS_NORMAL_TRADING",
    )
    assert isinstance(result, MarketSnapshot)
    assert result.instrument.ticker == "AAPL"
    assert result.last_price.as_float == 170.25
    assert result.trading_status == TradingStatus.OPEN
