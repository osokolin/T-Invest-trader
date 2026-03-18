"""Tests for services/observation_service.py -- orchestration."""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

from tinvest_trader.observation.models import ObservationWindow
from tinvest_trader.services.observation_service import ObservationService


def _make_service(
    tracked_tickers=frozenset(),
    persist=True,
    windows=None,
    repo=None,
):
    if windows is None:
        windows = [
            ObservationWindow(label="5m", seconds=300),
            ObservationWindow(label="1h", seconds=3600),
        ]
    if repo is None:
        repo = MagicMock()
    return ObservationService(
        repository=repo,
        windows=windows,
        tracked_tickers=tracked_tickers,
        persist=persist,
        logger=logging.getLogger("test"),
    ), repo


def _now():
    return datetime(2026, 3, 18, 12, 0, 0, tzinfo=UTC)


def test_observe_ticker_basic():
    svc, repo = _make_service()
    repo.fetch_sentiment_events_for_window.return_value = [
        {"label": "positive", "score_positive": 0.8, "score_negative": 0.1, "score_neutral": 0.1},
    ]
    results = svc.observe_ticker("SBER", as_of=_now())
    assert len(results) == 2  # two windows
    assert results[0].ticker == "SBER"
    assert results[0].message_count == 1
    assert results[0].positive_count == 1


def test_observe_ticker_persists_when_enabled():
    svc, repo = _make_service(persist=True)
    repo.fetch_sentiment_events_for_window.return_value = [
        {"label": "positive", "score_positive": 0.8, "score_negative": 0.1, "score_neutral": 0.1},
    ]
    svc.observe_ticker("SBER", as_of=_now())
    assert repo.insert_signal_observation.call_count == 2  # once per window


def test_observe_ticker_skips_persist_when_disabled():
    svc, repo = _make_service(persist=False)
    repo.fetch_sentiment_events_for_window.return_value = [
        {"label": "positive", "score_positive": 0.8, "score_negative": 0.1, "score_neutral": 0.1},
    ]
    svc.observe_ticker("SBER", as_of=_now())
    repo.insert_signal_observation.assert_not_called()


def test_observe_ticker_skips_persist_for_empty_window():
    svc, repo = _make_service(persist=True)
    repo.fetch_sentiment_events_for_window.return_value = []
    svc.observe_ticker("SBER", as_of=_now())
    repo.insert_signal_observation.assert_not_called()


def test_observe_ticker_no_repo():
    svc = ObservationService(
        repository=None,
        windows=[ObservationWindow(label="5m", seconds=300)],
        tracked_tickers=frozenset(),
        persist=True,
        logger=logging.getLogger("test"),
    )
    results = svc.observe_ticker("SBER", as_of=_now())
    assert results == []


def test_observe_all_with_tracked_tickers():
    svc, repo = _make_service(tracked_tickers=frozenset({"SBER", "GAZP"}))
    repo.fetch_sentiment_events_for_window.return_value = [
        {"label": "neutral", "score_positive": 0.33,
         "score_negative": 0.33, "score_neutral": 0.34},
    ]
    results = svc.observe_all(as_of=_now())
    # 2 tickers x 2 windows = 4 observations
    assert len(results) == 4


def test_observe_all_discovers_tickers():
    svc, repo = _make_service(tracked_tickers=frozenset())
    repo.fetch_distinct_tickers_with_sentiment.return_value = [
        {"ticker": "SBER", "figi": "FIGI1"},
    ]
    repo.fetch_sentiment_events_for_window.return_value = [
        {"label": "positive", "score_positive": 0.9,
         "score_negative": 0.05, "score_neutral": 0.05},
    ]
    results = svc.observe_all(as_of=_now())
    assert len(results) == 2  # 1 ticker x 2 windows
    repo.fetch_distinct_tickers_with_sentiment.assert_called_once()


def test_observe_all_no_repo():
    svc = ObservationService(
        repository=None,
        windows=[ObservationWindow(label="5m", seconds=300)],
        tracked_tickers=frozenset({"SBER"}),
        persist=True,
        logger=logging.getLogger("test"),
    )
    results = svc.observe_all(as_of=_now())
    assert results == []


def test_observe_ticker_survives_fetch_failure():
    svc, repo = _make_service()
    repo.fetch_sentiment_events_for_window.side_effect = RuntimeError("db down")
    results = svc.observe_ticker("SBER", as_of=_now())
    assert results == []


def test_observe_ticker_survives_persist_failure():
    svc, repo = _make_service(persist=True)
    repo.fetch_sentiment_events_for_window.return_value = [
        {"label": "positive", "score_positive": 0.8, "score_negative": 0.1, "score_neutral": 0.1},
    ]
    repo.insert_signal_observation.side_effect = RuntimeError("db down")
    results = svc.observe_ticker("SBER", as_of=_now())
    # Should still return results even if persist fails
    assert len(results) == 2
