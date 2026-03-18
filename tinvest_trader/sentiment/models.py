"""Sentiment domain models -- pure data structures, no DB code."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class TelegramMessage:
    """Raw message from a Telegram channel."""

    channel_name: str
    message_id: str
    message_text: str
    published_at: datetime | None = None
    source_payload: dict | None = field(default=None, repr=False)


@dataclass(frozen=True)
class TickerMention:
    """A ticker symbol extracted from message text."""

    ticker: str
    mention_type: str  # "hashtag", "cashtag", "plain"
    figi: str | None = None
    confidence: float | None = None


@dataclass(frozen=True)
class SentimentResult:
    """Sentiment scoring output for a piece of text."""

    label: str  # "positive", "negative", "neutral"
    score_positive: float
    score_negative: float
    score_neutral: float
    model_name: str
    scored_at: datetime
