from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from tinvest_trader.domain.enums import CandleInterval, OrderSide, TradingStatus


@dataclass(frozen=True)
class Instrument:
    figi: str
    ticker: str
    name: str


@dataclass(frozen=True)
class MoneyValue:
    currency: str
    units: int
    nano: int

    @property
    def as_float(self) -> float:
        return self.units + self.nano / 1_000_000_000


@dataclass(frozen=True)
class Candle:
    open: MoneyValue
    high: MoneyValue
    low: MoneyValue
    close: MoneyValue
    volume: int
    time: datetime
    interval: CandleInterval


@dataclass(frozen=True)
class MarketSnapshot:
    instrument: Instrument
    last_price: MoneyValue
    trading_status: TradingStatus
    time: datetime


@dataclass(frozen=True)
class Position:
    figi: str
    quantity: int


@dataclass(frozen=True)
class Order:
    order_id: str
    figi: str
    direction: OrderSide
    quantity: int
