"""Sentiment aggregator -- computes derived metrics from raw sentiment rows."""

from __future__ import annotations

from datetime import datetime

from tinvest_trader.observation.models import SignalObservation


def aggregate_sentiment_rows(
    ticker: str,
    figi: str | None,
    window_label: str,
    observation_time: datetime,
    rows: list[dict],
) -> SignalObservation:
    """Aggregate a list of sentiment event rows into a single SignalObservation.

    Each row is expected to have keys: label, score_positive, score_negative, score_neutral.
    """
    total = len(rows)
    if total == 0:
        return SignalObservation(
            ticker=ticker,
            figi=figi,
            window=window_label,
            observation_time=observation_time,
            message_count=0,
            positive_count=0,
            negative_count=0,
            neutral_count=0,
            positive_score_avg=None,
            negative_score_avg=None,
            neutral_score_avg=None,
            sentiment_balance=None,
        )

    positive_count = sum(1 for r in rows if r["label"] == "positive")
    negative_count = sum(1 for r in rows if r["label"] == "negative")
    neutral_count = sum(1 for r in rows if r["label"] == "neutral")

    pos_scores = [float(r["score_positive"]) for r in rows if r["score_positive"] is not None]
    neg_scores = [float(r["score_negative"]) for r in rows if r["score_negative"] is not None]
    neu_scores = [float(r["score_neutral"]) for r in rows if r["score_neutral"] is not None]

    pos_avg = sum(pos_scores) / len(pos_scores) if pos_scores else None
    neg_avg = sum(neg_scores) / len(neg_scores) if neg_scores else None
    neu_avg = sum(neu_scores) / len(neu_scores) if neu_scores else None

    balance = None
    if pos_avg is not None and neg_avg is not None:
        balance = pos_avg - neg_avg

    return SignalObservation(
        ticker=ticker,
        figi=figi,
        window=window_label,
        observation_time=observation_time,
        message_count=total,
        positive_count=positive_count,
        negative_count=negative_count,
        neutral_count=neutral_count,
        positive_score_avg=pos_avg,
        negative_score_avg=neg_avg,
        neutral_score_avg=neu_avg,
        sentiment_balance=balance,
    )
