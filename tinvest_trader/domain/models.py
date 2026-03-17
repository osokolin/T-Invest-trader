from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Instrument:
    figi: str
    ticker: str
    name: str


@dataclass(frozen=True)
class Position:
    figi: str
    quantity: int


@dataclass(frozen=True)
class Order:
    order_id: str
    figi: str
    direction: str
    quantity: int
