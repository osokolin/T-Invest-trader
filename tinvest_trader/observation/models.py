"""Observation domain models -- pure data structures for aggregated metrics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ObservationWindow:
    """A named time window for aggregation (e.g. '5m' = 5 minutes)."""

    label: str  # e.g. "5m", "15m", "1h"
    seconds: int  # duration in seconds


@dataclass(frozen=True)
class SignalObservation:
    """Aggregated sentiment observation for a ticker over a time window."""

    ticker: str
    figi: str | None
    window: str
    observation_time: datetime
    message_count: int
    positive_count: int
    negative_count: int
    neutral_count: int
    positive_score_avg: float | None
    negative_score_avg: float | None
    neutral_score_avg: float | None
    sentiment_balance: float | None  # positive_score_avg - negative_score_avg
