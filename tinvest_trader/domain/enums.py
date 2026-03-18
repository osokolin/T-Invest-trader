from __future__ import annotations

from enum import Enum


class Signal(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class Environment(Enum):
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
