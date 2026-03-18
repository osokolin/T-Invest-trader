"""Maps T-Bank API response dicts into internal domain models.

All broker-specific structure knowledge is isolated here.
Nothing outside infra/ should depend on broker payload shapes.
"""

from __future__ import annotations

from datetime import UTC, datetime

from tinvest_trader.domain.enums import CandleInterval, TradingStatus
from tinvest_trader.domain.models import Candle, Instrument, MarketSnapshot, MoneyValue


def map_money_value(raw: dict) -> MoneyValue:
    return MoneyValue(
        currency=raw.get("currency", "RUB"),
        units=int(raw.get("units", 0)),
        nano=int(raw.get("nano", 0)),
    )


def map_instrument(raw: dict) -> Instrument:
    return Instrument(
        figi=raw["figi"],
        ticker=raw["ticker"],
        name=raw.get("name", ""),
    )


def map_trading_status(raw: str) -> TradingStatus:
    mapping = {
        "SECURITY_TRADING_STATUS_NORMAL_TRADING": TradingStatus.OPEN,
        "SECURITY_TRADING_STATUS_NOT_AVAILABLE_FOR_TRADING": TradingStatus.CLOSED,
        "SECURITY_TRADING_STATUS_OPENING_PERIOD": TradingStatus.PREMARKET,
    }
    return mapping.get(raw, TradingStatus.UNKNOWN)


def map_candle(raw: dict, interval: CandleInterval) -> Candle:
    return Candle(
        open=map_money_value(raw["open"]),
        high=map_money_value(raw["high"]),
        low=map_money_value(raw["low"]),
        close=map_money_value(raw["close"]),
        volume=int(raw.get("volume", 0)),
        time=_parse_timestamp(raw.get("time")),
        interval=interval,
    )


def map_market_snapshot(
    instrument: Instrument,
    last_price_raw: dict,
    trading_status_raw: str,
) -> MarketSnapshot:
    return MarketSnapshot(
        instrument=instrument,
        last_price=map_money_value(last_price_raw),
        trading_status=map_trading_status(trading_status_raw),
        time=datetime.now(UTC),
    )


def _parse_timestamp(value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return datetime.now(UTC)
