import logging
from datetime import UTC, date, datetime
from unittest.mock import MagicMock

from tinvest_trader.observation.models import ObservationWindow, SignalObservation
from tinvest_trader.services.fusion_service import FusionService

NOW = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)


def _make_service(persist=False, tracked=frozenset(), windows=None):
    repo = MagicMock()
    logger = logging.getLogger("test_fusion")
    if windows is None:
        windows = [
            ObservationWindow(label="5m", seconds=300),
            ObservationWindow(label="1h", seconds=3600),
        ]
    svc = FusionService(
        repository=repo,
        windows=windows,
        tracked_tickers=tracked,
        persist=persist,
        logger=logger,
    )
    # Default: no recency data
    repo.fetch_broker_event_recency.return_value = {
        "last_dividend_at": None,
        "last_report_at": None,
        "last_insider_deal_at": None,
    }
    # Default: no MOEX data
    repo.fetch_moex_market_context.return_value = None
    return svc, repo


def test_fuse_ticker_combines_observation_and_events():
    svc, repo = _make_service()
    obs = SignalObservation(
        ticker="SBER", figi=None, window="5m", observation_time=NOW,
        message_count=3, positive_count=2, negative_count=1, neutral_count=0,
        positive_score_avg=0.8, negative_score_avg=0.3, neutral_score_avg=None,
        sentiment_balance=0.5,
    )
    repo.fetch_latest_signal_observation.return_value = obs
    repo.fetch_broker_event_features_for_window.return_value = [
        {
            "source_method": "dividends", "event_type": "dividends",
            "event_direction": None, "event_value": 10.0,
            "currency": "RUB", "event_time": NOW,
        },
    ]

    results = svc.fuse_ticker("SBER", as_of=NOW)
    assert len(results) == 2  # one per window
    assert results[0].sentiment_message_count == 3
    assert results[0].broker_dividends_count == 1


def test_fuse_ticker_no_repository():
    logger = logging.getLogger("test_fusion")
    svc = FusionService(
        repository=None,
        windows=[ObservationWindow(label="5m", seconds=300)],
        tracked_tickers=frozenset(),
        persist=False,
        logger=logger,
    )
    assert svc.fuse_ticker("SBER", as_of=NOW) == []


def test_fuse_ticker_persists_when_enabled():
    svc, repo = _make_service(persist=True)
    repo.fetch_latest_signal_observation.return_value = None
    repo.fetch_broker_event_features_for_window.return_value = []

    svc.fuse_ticker("SBER", as_of=NOW)
    assert repo.insert_fused_signal_feature.call_count == 2  # one per window


def test_fuse_all_uses_tracked_tickers():
    svc, repo = _make_service(tracked=frozenset({"SBER", "GAZP"}))
    repo.fetch_latest_signal_observation.return_value = None
    repo.fetch_broker_event_features_for_window.return_value = []

    results = svc.fuse_all(as_of=NOW)
    # 2 tickers x 2 windows
    assert len(results) == 4


def test_fuse_all_discovers_tickers_when_not_tracked():
    svc, repo = _make_service(tracked=frozenset())
    repo.fetch_distinct_tickers_with_sentiment.return_value = [
        {"ticker": "YNDX", "figi": None},
    ]
    repo.fetch_latest_signal_observation.return_value = None
    repo.fetch_broker_event_features_for_window.return_value = []

    results = svc.fuse_all(as_of=NOW)
    assert len(results) == 2  # 1 ticker x 2 windows
    repo.fetch_distinct_tickers_with_sentiment.assert_called_once()


def test_fuse_all_no_repository():
    logger = logging.getLogger("test_fusion")
    svc = FusionService(
        repository=None,
        windows=[ObservationWindow(label="5m", seconds=300)],
        tracked_tickers=frozenset(),
        persist=False,
        logger=logger,
    )
    assert svc.fuse_all(as_of=NOW) == []


# --- Long windows ---


def test_fuse_ticker_with_long_windows():
    """Service should produce results for long windows like 1d, 7d, 30d."""
    windows = [
        ObservationWindow(label="5m", seconds=300),
        ObservationWindow(label="1d", seconds=86400),
        ObservationWindow(label="7d", seconds=604800),
        ObservationWindow(label="30d", seconds=2592000),
    ]
    svc, repo = _make_service(windows=windows)
    repo.fetch_latest_signal_observation.return_value = None
    repo.fetch_broker_event_features_for_window.return_value = []

    results = svc.fuse_ticker("SBER", as_of=NOW)
    assert len(results) == 4
    assert [r.window for r in results] == ["5m", "1d", "7d", "30d"]


# --- Recency ---


def test_fuse_ticker_passes_recency():
    """Service should fetch recency once per ticker and pass to aggregator."""
    div_time = datetime(2025, 5, 25, 12, 0, 0, tzinfo=UTC)
    svc, repo = _make_service()
    repo.fetch_latest_signal_observation.return_value = None
    repo.fetch_broker_event_features_for_window.return_value = []
    repo.fetch_broker_event_recency.return_value = {
        "last_dividend_at": div_time,
        "last_report_at": None,
        "last_insider_deal_at": None,
    }

    results = svc.fuse_ticker("SBER", as_of=NOW)
    # Recency fetched once (not once per window)
    repo.fetch_broker_event_recency.assert_called_once_with(ticker="SBER", figi=None)
    # Both windows should have the same recency data
    for r in results:
        assert r.last_dividend_at == div_time
        assert r.days_since_last_dividend == 7.0


def test_fuse_ticker_recency_error_graceful():
    """Service should not fail when recency fetch raises."""
    svc, repo = _make_service()
    repo.fetch_latest_signal_observation.return_value = None
    repo.fetch_broker_event_features_for_window.return_value = []
    repo.fetch_broker_event_recency.side_effect = RuntimeError("db error")

    results = svc.fuse_ticker("SBER", as_of=NOW)
    assert len(results) == 2
    # Recency fields should be None (graceful degradation)
    for r in results:
        assert r.last_dividend_at is None
        assert r.days_since_last_dividend is None


# --- MOEX market context ---


def test_fuse_ticker_passes_moex_context():
    """Service should fetch MOEX context once per ticker and pass to aggregator."""
    svc, repo = _make_service()
    repo.fetch_latest_signal_observation.return_value = None
    repo.fetch_broker_event_features_for_window.return_value = []
    repo.fetch_moex_market_context.return_value = {
        "latest": {
            "close": 300.5,
            "volume": 1000000,
            "num_trades": 5000,
            "trade_date": date(2025, 5, 30),
            "high": 305.0,
            "low": 295.0,
        },
        "previous_close": 298.0,
    }

    results = svc.fuse_ticker("SBER", as_of=NOW)
    # MOEX context fetched once (not once per window)
    repo.fetch_moex_market_context.assert_called_once_with(ticker="SBER")
    # Both windows should have the same MOEX data
    for r in results:
        assert r.moex_latest_close == 300.5
        assert r.moex_latest_volume == 1000000
        assert r.moex_last_trade_date == date(2025, 5, 30)


def test_fuse_ticker_moex_error_graceful():
    """Service should not fail when MOEX context fetch raises."""
    svc, repo = _make_service()
    repo.fetch_latest_signal_observation.return_value = None
    repo.fetch_broker_event_features_for_window.return_value = []
    repo.fetch_moex_market_context.side_effect = RuntimeError("db error")

    results = svc.fuse_ticker("SBER", as_of=NOW)
    assert len(results) == 2
    for r in results:
        assert r.moex_latest_close is None
        assert r.moex_latest_volume is None


def test_fuse_ticker_no_moex_data():
    """When MOEX returns None, MOEX fields should be None (no crash)."""
    svc, repo = _make_service()
    repo.fetch_latest_signal_observation.return_value = None
    repo.fetch_broker_event_features_for_window.return_value = []
    repo.fetch_moex_market_context.return_value = None

    results = svc.fuse_ticker("SBER", as_of=NOW)
    assert len(results) == 2
    for r in results:
        assert r.moex_latest_close is None
        assert r.moex_days_since_last_trade is None
