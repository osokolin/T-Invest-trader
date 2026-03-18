"""Domain models for the trading system.

Pure data structures with no infrastructure dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from tinvest_trader.domain.enums import CandleInterval, OrderSide, TradingStatus


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


@dataclass(frozen=True)
class Position:
    """Local representation of a held position."""

    figi: str
    quantity: int


@dataclass(frozen=True)
class Order:
    """An order in the system."""

    order_id: str
    figi: str
    direction: OrderSide
    quantity: int
