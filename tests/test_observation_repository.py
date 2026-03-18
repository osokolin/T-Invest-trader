"""Tests for observation repository methods -- query and insert."""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

from tinvest_trader.infra.storage.repository import TradingRepository
from tinvest_trader.observation.models import SignalObservation


def _make_repo():
    pool = MagicMock()
    conn = MagicMock()
    pool.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
    pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)
    logger = logging.getLogger("test")
    repo = TradingRepository(pool=pool, logger=logger)
    return repo, conn


def test_insert_signal_observation():
    repo, conn = _make_repo()
    obs = SignalObservation(
        ticker="SBER",
        figi="FIGI1",
        window="5m",
        observation_time=datetime(2026, 3, 18, 12, 0, 0, tzinfo=UTC),
        message_count=10,
        positive_count=5,
        negative_count=3,
        neutral_count=2,
        positive_score_avg=0.75,
        negative_score_avg=0.6,
        neutral_score_avg=0.35,
        sentiment_balance=0.15,
    )
    repo.insert_signal_observation(obs)
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()
    sql = conn.execute.call_args[0][0]
    assert "signal_observations" in sql
    params = conn.execute.call_args[0][1]
    assert params[0] == "SBER"
    assert params[1] == "FIGI1"
    assert params[2] == "5m"


def test_fetch_sentiment_events_by_ticker():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchall.return_value = [
        ("positive", 0.8, 0.1, 0.1),
        ("negative", 0.2, 0.7, 0.1),
    ]
    start = datetime(2026, 3, 18, 11, 0, 0, tzinfo=UTC)
    end = datetime(2026, 3, 18, 12, 0, 0, tzinfo=UTC)
    rows = repo.fetch_sentiment_events_for_window("SBER", start, end)
    assert len(rows) == 2
    assert rows[0]["label"] == "positive"
    assert rows[1]["score_negative"] == 0.7


def test_fetch_sentiment_events_by_figi():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchall.return_value = []
    start = datetime(2026, 3, 18, 11, 0, 0, tzinfo=UTC)
    end = datetime(2026, 3, 18, 12, 0, 0, tzinfo=UTC)
    rows = repo.fetch_sentiment_events_for_window("SBER", start, end, figi="FIGI1")
    assert rows == []
    sql = conn.execute.call_args[0][0]
    assert "figi" in sql


def test_fetch_distinct_tickers():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchall.return_value = [
        ("SBER", "FIGI1"),
        ("GAZP", None),
    ]
    start = datetime(2026, 3, 18, 11, 0, 0, tzinfo=UTC)
    end = datetime(2026, 3, 18, 12, 0, 0, tzinfo=UTC)
    result = repo.fetch_distinct_tickers_with_sentiment(start, end)
    assert len(result) == 2
    assert result[0] == {"ticker": "SBER", "figi": "FIGI1"}
    assert result[1] == {"ticker": "GAZP", "figi": None}
