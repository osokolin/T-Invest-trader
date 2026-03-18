"""Sentiment scoring interface and stub implementation."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from tinvest_trader.sentiment.models import SentimentResult


class SentimentScorer(Protocol):
    """Protocol for sentiment scoring."""

    def score(self, text: str) -> SentimentResult: ...


# Keywords for stub scorer (Russian and English)
_POSITIVE_KEYWORDS = frozenset({
    "рост", "растет", "растёт", "рекорд", "прибыль", "дивиденд",
    "buy", "growth", "bullish", "profit", "upgrade",
})
_NEGATIVE_KEYWORDS = frozenset({
    "падение", "падает", "падени", "обвал", "убыток", "убытк",
    "отмен", "санкци", "снижен",
    "sell", "drop", "bearish", "loss", "downgrade", "crash",
})


class StubSentimentScorer:
    """Deterministic keyword-based scorer for testing. No model downloads."""

    def __init__(self, model_name: str = "stub") -> None:
        self._model_name = model_name

    def score(self, text: str) -> SentimentResult:
        text_lower = text.lower()
        pos_hits = sum(1 for kw in _POSITIVE_KEYWORDS if kw in text_lower)
        neg_hits = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in text_lower)

        if pos_hits > neg_hits:
            label = "positive"
            scores = (0.75, 0.10, 0.15)
        elif neg_hits > pos_hits:
            label = "negative"
            scores = (0.10, 0.75, 0.15)
        else:
            label = "neutral"
            scores = (0.20, 0.20, 0.60)

        return SentimentResult(
            label=label,
            score_positive=scores[0],
            score_negative=scores[1],
            score_neutral=scores[2],
            model_name=self._model_name,
            scored_at=datetime.now(tz=UTC),
        )
