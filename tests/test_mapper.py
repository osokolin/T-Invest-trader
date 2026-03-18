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
    mv = map_money_value(raw)
    assert isinstance(mv, MoneyValue)
    assert mv.currency == "RUB"
    assert mv.units == 100
    assert mv.nano == 500_000_000
    assert mv.as_float == 100.5


def test_map_money_value_defaults():
    mv = map_money_value({})
    assert mv.currency == "RUB"
    assert mv.units == 0
    assert mv.nano == 0


def test_map_instrument():
    raw = {"figi": "BBG000B9XRY4", "ticker": "AAPL", "name": "Apple Inc"}
    inst = map_instrument(raw)
    assert isinstance(inst, Instrument)
    assert inst.figi == "BBG000B9XRY4"
    assert inst.ticker == "AAPL"
    assert inst.name == "Apple Inc"


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
    candle = map_candle(raw, CandleInterval.MIN_5)
    assert isinstance(candle, Candle)
    assert candle.open.units == 100
    assert candle.high.units == 101
    assert candle.low.units == 99
    assert candle.volume == 500
    assert candle.interval == CandleInterval.MIN_5


def test_map_market_snapshot():
    instrument = Instrument(figi="BBG000B9XRY4", ticker="AAPL", name="Apple Inc")
    last_price_raw = {"currency": "USD", "units": 170, "nano": 250_000_000}
    snapshot = map_market_snapshot(
        instrument,
        last_price_raw,
        "SECURITY_TRADING_STATUS_NORMAL_TRADING",
    )
    assert isinstance(snapshot, MarketSnapshot)
    assert snapshot.instrument.ticker == "AAPL"
    assert snapshot.last_price.as_float == 170.25
    assert snapshot.trading_status == TradingStatus.OPEN
