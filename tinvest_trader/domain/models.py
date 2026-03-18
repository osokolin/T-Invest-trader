"""Domain models for the trading system.

Pure data structures with no infrastructure dependencies.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime

from tinvest_trader.domain.enums import (
    CandleInterval,
    OrderSide,
    OrderStatus,
    OrderType,
    TradingStatus,
)

# -- Market data models --


@dataclass(frozen=True)
class Instrument:
    """Tradeable instrument identified by FIGI."""

    figi: str
    ticker: str
    name: str


@dataclass(frozen=True)
class MoneyValue:
    """Monetary amount in broker format (units + nano)."""

    currency: str
    units: int
    nano: int

    @property
    def as_float(self) -> float:
        """Convert to float for display and comparison."""
        return self.units + self.nano / 1_000_000_000


@dataclass(frozen=True)
class Candle:
    """OHLCV candle with time and interval."""

    open: MoneyValue
    high: MoneyValue
    low: MoneyValue
    close: MoneyValue
    volume: int
    time: datetime
    interval: CandleInterval


@dataclass(frozen=True)
class MarketSnapshot:
    """Point-in-time market state for a single instrument."""

    instrument: Instrument
    last_price: MoneyValue
    trading_status: TradingStatus
    time: datetime


# -- Position model --


@dataclass
class Position:
    """Local representation of a held position."""

    figi: str
    quantity: int = 0


# -- Order / execution models --


def _generate_idempotency_key() -> str:
    """Generate a unique idempotency key for order submission."""
    return uuid.uuid4().hex


@dataclass(frozen=True)
class OrderIntent:
    """What we want to send to the broker.

    The idempotency_key ensures safe retries -- the broker will deduplicate
    requests with the same key.
    """

    figi: str
    direction: OrderSide
    quantity: int
    order_type: OrderType = OrderType.MARKET
    limit_price: MoneyValue | None = None
    idempotency_key: str = field(default_factory=_generate_idempotency_key)


@dataclass(frozen=True)
class BrokerOrder:
    """Normalized representation of a broker order response."""

    order_id: str
    figi: str
    direction: OrderSide
    quantity: int
    filled_quantity: int
    status: OrderStatus
    message: str = ""


@dataclass(frozen=True)
class ExecutionResult:
    """Result of submitting an OrderIntent to the broker."""

    success: bool
    broker_order: BrokerOrder | None = None
    error: str = ""
