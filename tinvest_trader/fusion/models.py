"""Fusion domain models -- unified per-ticker per-window feature rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class FusedSignalFeature:
    """Combined sentiment + broker event feature row for a ticker and window."""

    ticker: str
    figi: str | None
    window: str
    observation_time: datetime

    # Sentiment features
    sentiment_message_count: int | None
    sentiment_positive_count: int | None
    sentiment_negative_count: int | None
    sentiment_neutral_count: int | None
    sentiment_positive_avg: float | None
    sentiment_negative_avg: float | None
    sentiment_neutral_avg: float | None
    sentiment_balance: float | None

    # Broker event counts
    broker_dividends_count: int
    broker_reports_count: int
    broker_insider_deals_count: int
    broker_total_event_count: int

    # Latest broker event values (within window)
    broker_latest_dividend_value: float | None = None
    broker_latest_dividend_currency: str | None = None
    broker_latest_report_time: datetime | None = None
    broker_latest_insider_deal_time: datetime | None = None

    # Broker event recency (global latest per ticker, independent of window)
    last_dividend_at: datetime | None = None
    last_report_at: datetime | None = None
    last_insider_deal_at: datetime | None = None
    days_since_last_dividend: float | None = None
    days_since_last_report: float | None = None
    days_since_last_insider_deal: float | None = None
