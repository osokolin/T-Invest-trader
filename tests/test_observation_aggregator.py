"""Tests for observation/aggregator.py -- sentiment aggregation math."""

from datetime import UTC, datetime

import pytest

from tinvest_trader.observation.aggregator import aggregate_sentiment_rows


def _now():
    return datetime(2026, 3, 18, 12, 0, 0, tzinfo=UTC)


def _row(label, pos, neg, neu):
    return {
        "label": label,
        "score_positive": pos,
        "score_negative": neg,
        "score_neutral": neu,
    }


def test_empty_rows():
    obs = aggregate_sentiment_rows("SBER", None, "5m", _now(), [])
    assert obs.message_count == 0
    assert obs.positive_count == 0
    assert obs.negative_count == 0
    assert obs.neutral_count == 0
    assert obs.positive_score_avg is None
    assert obs.sentiment_balance is None


def test_all_positive():
    rows = [_row("positive", 0.9, 0.05, 0.05), _row("positive", 0.8, 0.1, 0.1)]
    obs = aggregate_sentiment_rows("SBER", "FIGI1", "5m", _now(), rows)
    assert obs.message_count == 2
    assert obs.positive_count == 2
    assert obs.negative_count == 0
    assert obs.neutral_count == 0
    assert obs.positive_score_avg == pytest.approx(0.85, abs=0.001)
    assert obs.negative_score_avg == pytest.approx(0.075, abs=0.001)
    assert obs.sentiment_balance == pytest.approx(0.775, abs=0.001)


def test_all_negative():
    rows = [_row("negative", 0.1, 0.8, 0.1)]
    obs = aggregate_sentiment_rows("GAZP", None, "15m", _now(), rows)
    assert obs.positive_count == 0
    assert obs.negative_count == 1
    assert obs.sentiment_balance == pytest.approx(-0.7, abs=0.001)


def test_mixed_sentiments():
    rows = [
        _row("positive", 0.8, 0.1, 0.1),
        _row("negative", 0.1, 0.8, 0.1),
        _row("neutral", 0.3, 0.3, 0.4),
    ]
    obs = aggregate_sentiment_rows("LKOH", "FIGI2", "1h", _now(), rows)
    assert obs.message_count == 3
    assert obs.positive_count == 1
    assert obs.negative_count == 1
    assert obs.neutral_count == 1
    assert obs.positive_score_avg == pytest.approx(0.4, abs=0.001)
    assert obs.negative_score_avg == pytest.approx(0.4, abs=0.001)
    assert obs.sentiment_balance == pytest.approx(0.0, abs=0.001)


def test_ticker_and_window_preserved():
    rows = [_row("neutral", 0.33, 0.33, 0.34)]
    obs = aggregate_sentiment_rows("YNDX", "FIGI3", "2d", _now(), rows)
    assert obs.ticker == "YNDX"
    assert obs.figi == "FIGI3"
    assert obs.window == "2d"
    assert obs.observation_time == _now()


def test_none_scores_handled():
    rows = [_row("positive", None, None, None)]
    obs = aggregate_sentiment_rows("SBER", None, "5m", _now(), rows)
    assert obs.message_count == 1
    assert obs.positive_count == 1
    assert obs.positive_score_avg is None
    assert obs.sentiment_balance is None
