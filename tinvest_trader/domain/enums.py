"""Domain enums -- shared across the trading system."""

from __future__ import annotations

from enum import Enum


class Signal(Enum):
    """Strategy signal output."""

    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class OrderSide(Enum):
    """Direction of an order."""

    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    """Type of order to place."""

    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderStatus(Enum):
    """Lifecycle status of an order."""

    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class Environment(Enum):
    """Runtime environment."""

    PAPER = "paper"
    SANDBOX = "sandbox"
    PRODUCTION = "production"


class TradingStatus(Enum):
    """Trading status of an instrument on the exchange."""

    OPEN = "OPEN"
    CLOSED = "CLOSED"
    PREMARKET = "PREMARKET"
    UNKNOWN = "UNKNOWN"


class CandleInterval(Enum):
    """Supported candle time intervals."""

    MIN_1 = "1min"
    MIN_5 = "5min"
    MIN_15 = "15min"
    HOUR = "hour"
    DAY = "day"
