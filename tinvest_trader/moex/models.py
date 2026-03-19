"""MOEX ISS domain models -- pure dataclasses, no DB code."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass(frozen=True)
class MoexSecurityInfo:
    """Parsed security metadata from ISS /securities/{secid}.json."""

    secid: str
    name: str
    short_name: str
    isin: str
    reg_number: str
    list_level: int | None
    issuer: str
    issue_size: int | None
    group: str
    primary_boardid: str
    raw_description: dict = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class MoexHistoryRow:
    """Single daily OHLCV row from ISS history endpoint."""

    secid: str
    boardid: str
    trade_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    legal_close: float | None
    waprice: float | None
    volume: int | None
    value: float | None
    num_trades: int | None


@dataclass(frozen=True)
class MoexMarketHistoryNormalized:
    """Normalized daily market history row ready for persistence."""

    secid: str
    boardid: str
    trade_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    waprice: float | None
    volume: int | None
    value: float | None
    num_trades: int | None
    recorded_at: datetime | None = None
