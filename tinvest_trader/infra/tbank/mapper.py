"""Maps T-Bank API response dicts into internal domain models.

All broker-specific structure knowledge is isolated here.
Nothing outside infra/ should depend on broker payload shapes.
"""

from __future__ import annotations

from datetime import UTC, datetime

from tinvest_trader.domain.enums import CandleInterval, OrderSide, OrderStatus, TradingStatus
from tinvest_trader.domain.models import (
    BrokerOrder,
    Candle,
    Instrument,
    MarketSnapshot,
    MoneyValue,
)


def map_money_value(raw: dict) -> MoneyValue:
    """Map a broker money-value dict to MoneyValue."""
    return MoneyValue(
        currency=raw.get("currency", "RUB"),
        units=int(raw.get("units", 0)),
        nano=int(raw.get("nano", 0)),
    )


def map_instrument(raw: dict) -> Instrument:
    """Map a broker instrument dict to Instrument."""
    return Instrument(
        figi=raw["figi"],
        ticker=raw["ticker"],
        name=raw.get("name", ""),
    )


def map_trading_status(raw: str) -> TradingStatus:
    """Map a broker trading-status string to TradingStatus enum."""
    mapping = {
        "SECURITY_TRADING_STATUS_NORMAL_TRADING": TradingStatus.OPEN,
        "SECURITY_TRADING_STATUS_NOT_AVAILABLE_FOR_TRADING": TradingStatus.CLOSED,
        "SECURITY_TRADING_STATUS_OPENING_PERIOD": TradingStatus.PREMARKET,
    }
    return mapping.get(raw, TradingStatus.UNKNOWN)


def map_candle(raw: dict, interval: CandleInterval) -> Candle:
    """Map a broker candle dict to Candle."""
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
    """Combine instrument, price, and status into a MarketSnapshot."""
    return MarketSnapshot(
        instrument=instrument,
        last_price=map_money_value(last_price_raw),
        trading_status=map_trading_status(trading_status_raw),
        time=datetime.now(UTC),
    )


def _parse_timestamp(value: str | None) -> datetime:
    """Parse an ISO timestamp string, falling back to now(UTC)."""
    if value is None:
        return datetime.now(UTC)
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return datetime.now(UTC)


# -- Order mapping --


def map_order_status(raw: str) -> OrderStatus:
    """Map a broker execution-report status string to OrderStatus."""
    mapping = {
        "EXECUTION_REPORT_STATUS_NEW": OrderStatus.NEW,
        "EXECUTION_REPORT_STATUS_PARTIALLYFILL": OrderStatus.PARTIALLY_FILLED,
        "EXECUTION_REPORT_STATUS_FILL": OrderStatus.FILLED,
        "EXECUTION_REPORT_STATUS_CANCELLED": OrderStatus.CANCELLED,
        "EXECUTION_REPORT_STATUS_REJECTED": OrderStatus.REJECTED,
    }
    return mapping.get(raw, OrderStatus.REJECTED)


def map_order_direction(raw: str) -> OrderSide:
    """Map a broker order-direction string to OrderSide."""
    mapping = {
        "ORDER_DIRECTION_BUY": OrderSide.BUY,
        "ORDER_DIRECTION_SELL": OrderSide.SELL,
    }
    return mapping.get(raw, OrderSide.BUY)


def map_broker_order(raw: dict) -> BrokerOrder:
    """Map a broker order response dict to BrokerOrder."""
    return BrokerOrder(
        order_id=raw["order_id"],
        figi=raw.get("figi", ""),
        direction=map_order_direction(raw.get("direction", "")),
        quantity=int(raw.get("requested_quantity", 0)),
        filled_quantity=int(raw.get("filled_quantity", 0)),
        status=map_order_status(raw.get("status", "")),
        message=raw.get("message", ""),
    )
