"""Tests for broker event ticker alignment fix.

Validates:
- Stub mode returns ticker=None (not "STUB")
- DB fallback resolves ticker when API returns no ticker
- Instrument caching via upsert_instrument
- Self-healing ON CONFLICT for previously-STUB rows
- Fusion produces non-zero broker counts when tickers match
- No regression for sentiment-only rows
"""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

from tinvest_trader.observation.models import ObservationWindow, SignalObservation
from tinvest_trader.services.broker_event_ingestion_service import (
    BrokerEventIngestionService,
)
from tinvest_trader.services.fusion_service import FusionService

NOW = datetime(2026, 3, 19, 12, 0, 0, tzinfo=UTC)


def _make_ingestion_service(
    *,
    tracked_figis: tuple[str, ...] = ("FIGI1",),
    event_types: tuple[str, ...] = ("dividends",),
    client_instrument: dict | None = None,
    repo: MagicMock | None = None,
) -> tuple[BrokerEventIngestionService, MagicMock, MagicMock]:
    client = MagicMock()
    client.get_instrument.return_value = client_instrument or {
        "figi": "FIGI1",
        "ticker": "SBER",
        "uid": "uid-1",
    }
    client.get_dividends.return_value = [
        {
            "record_date": "2026-03-15T00:00:00+00:00",
            "payment_date": "2026-03-20T00:00:00+00:00",
            "dividend_type": "Regular Cash",
            "dividend_net": {"currency": "RUB", "units": 10, "nano": 0},
        },
    ]
    if repo is None:
        repo = MagicMock()
    repo.insert_broker_event_raw.return_value = True
    repo.insert_broker_event_feature.return_value = True
    service = BrokerEventIngestionService(
        client=client,
        repository=repo,
        logger=logging.getLogger("test_alignment"),
        account_id="acc-1",
        tracked_figis=tracked_figis,
        event_types=event_types,
        lookback_days_by_event_type={"dividends": 30},
    )
    return service, client, repo


# -- Stub mode: ticker=None instead of "STUB" --

def test_stub_instrument_returns_none_ticker():
    from tinvest_trader.app.config import BrokerConfig
    from tinvest_trader.infra.tbank.client import TBankClient

    client = TBankClient(
        config=BrokerConfig(token="", account_id="acc-1"),
        logger=logging.getLogger("test"),
    )
    result = client.get_instrument("FIGI1")
    assert result["ticker"] is None
    assert result["figi"] == "FIGI1"


# -- DB fallback for ticker resolution --

def test_db_fallback_resolves_ticker_when_api_returns_none():
    repo = MagicMock()
    repo.fetch_ticker_by_figi.return_value = "SBER"
    service, client, _ = _make_ingestion_service(
        client_instrument={"figi": "FIGI1", "ticker": None, "uid": "uid-1"},
        repo=repo,
    )

    service.ingest_all(as_of=NOW)

    repo.fetch_ticker_by_figi.assert_called_once_with("FIGI1")
    # Ticker from DB fallback is used in the event feature
    feature_call = repo.insert_broker_event_feature.call_args
    feature = feature_call[0][0]
    assert feature.ticker == "SBER"


def test_db_fallback_resolves_ticker_when_api_returns_empty():
    repo = MagicMock()
    repo.fetch_ticker_by_figi.return_value = "GAZP"
    service, client, _ = _make_ingestion_service(
        client_instrument={"figi": "FIGI1", "ticker": "", "uid": "uid-1"},
        repo=repo,
    )

    service.ingest_all(as_of=NOW)

    repo.fetch_ticker_by_figi.assert_called_once_with("FIGI1")


def test_no_db_fallback_when_api_returns_valid_ticker():
    repo = MagicMock()
    service, client, _ = _make_ingestion_service(
        client_instrument={"figi": "FIGI1", "ticker": "SBER", "uid": "uid-1"},
        repo=repo,
    )

    service.ingest_all(as_of=NOW)

    repo.fetch_ticker_by_figi.assert_not_called()


def test_db_fallback_not_called_when_no_repository():
    service, client, _ = _make_ingestion_service(
        client_instrument={"figi": "FIGI1", "ticker": None, "uid": "uid-1"},
    )
    service._repository = None

    processed = service.ingest_all(as_of=NOW)

    # Should still process (repositoryless mode), just can't resolve ticker
    assert processed == 1


# -- Instrument caching via upsert_instrument --

def test_resolved_instrument_cached_via_upsert():
    repo = MagicMock()
    service, client, _ = _make_ingestion_service(
        client_instrument={"figi": "FIGI1", "ticker": "SBER", "uid": "uid-1"},
        repo=repo,
    )

    service.ingest_all(as_of=NOW)

    repo.upsert_instrument.assert_called_once()
    upsert_call = repo.upsert_instrument.call_args
    inst = upsert_call.kwargs["inst"]
    assert inst.figi == "FIGI1"
    assert inst.ticker == "SBER"
    assert upsert_call.kwargs["tracked"] is True
    assert upsert_call.kwargs["enabled"] is False
    assert upsert_call.kwargs["instrument_uid"] == "uid-1"


def test_instrument_not_cached_when_ticker_unresolved():
    repo = MagicMock()
    repo.fetch_ticker_by_figi.return_value = None
    service, _, _ = _make_ingestion_service(
        client_instrument={"figi": "FIGI1", "ticker": None, "uid": "uid-1"},
        repo=repo,
    )

    service.ingest_all(as_of=NOW)

    repo.upsert_instrument.assert_not_called()


def test_instrument_cache_failure_does_not_break_ingestion():
    repo = MagicMock()
    repo.upsert_instrument.side_effect = RuntimeError("DB write failed")
    service, _, _ = _make_ingestion_service(
        client_instrument={"figi": "FIGI1", "ticker": "SBER", "uid": "uid-1"},
        repo=repo,
    )

    processed = service.ingest_all(as_of=NOW)

    assert processed == 1  # ingestion continues despite cache failure


def test_api_resolution_failure_falls_back_to_db():
    repo = MagicMock()
    repo.fetch_ticker_by_figi.return_value = "SBER"
    service, client, _ = _make_ingestion_service(repo=repo)
    client.get_instrument.side_effect = RuntimeError("API unavailable")

    processed = service.ingest_all(as_of=NOW)

    repo.fetch_ticker_by_figi.assert_called_once_with("FIGI1")
    assert processed == 1


# -- Fusion produces non-zero broker counts when tickers match --

def test_fusion_produces_nonzero_broker_counts_with_matching_ticker():
    repo = MagicMock()
    obs = SignalObservation(
        ticker="SBER", figi=None, window="5m", observation_time=NOW,
        message_count=5, positive_count=3, negative_count=1, neutral_count=1,
        positive_score_avg=0.8, negative_score_avg=0.3, neutral_score_avg=0.1,
        sentiment_balance=0.5,
    )
    repo.fetch_latest_signal_observation.return_value = obs
    repo.fetch_broker_event_features_for_window.return_value = [
        {
            "source_method": "dividends", "event_type": "dividend",
            "event_direction": None, "event_value": 10.0,
            "currency": "RUB", "event_time": NOW,
            "ticker": "SBER",
        },
        {
            "source_method": "reports", "event_type": "report",
            "event_direction": None, "event_value": None,
            "currency": None, "event_time": NOW,
            "ticker": "SBER",
        },
    ]

    svc = FusionService(
        repository=repo,
        windows=[ObservationWindow(label="5m", seconds=300)],
        tracked_tickers=frozenset({"SBER"}),
        persist=False,
        logger=logging.getLogger("test_fusion_alignment"),
    )

    results = svc.fuse_ticker("SBER", as_of=NOW)

    assert len(results) == 1
    fused = results[0]
    assert fused.sentiment_message_count == 5
    assert fused.broker_dividends_count == 1
    assert fused.broker_reports_count == 1
    assert fused.broker_total_event_count == 2


# -- No regression: sentiment-only rows still work --

def test_fusion_sentiment_only_rows_have_zero_broker_counts():
    repo = MagicMock()
    obs = SignalObservation(
        ticker="GAZP", figi=None, window="1h", observation_time=NOW,
        message_count=10, positive_count=7, negative_count=2, neutral_count=1,
        positive_score_avg=0.9, negative_score_avg=0.2, neutral_score_avg=0.05,
        sentiment_balance=0.7,
    )
    repo.fetch_latest_signal_observation.return_value = obs
    repo.fetch_broker_event_features_for_window.return_value = []

    svc = FusionService(
        repository=repo,
        windows=[ObservationWindow(label="1h", seconds=3600)],
        tracked_tickers=frozenset({"GAZP"}),
        persist=False,
        logger=logging.getLogger("test_fusion_alignment"),
    )

    results = svc.fuse_ticker("GAZP", as_of=NOW)

    assert len(results) == 1
    fused = results[0]
    assert fused.sentiment_message_count == 10
    assert fused.broker_dividends_count == 0
    assert fused.broker_reports_count == 0
    assert fused.broker_insider_deals_count == 0
    assert fused.broker_total_event_count == 0


# -- No duplicate explosion --

def test_ingestion_idempotent_with_self_healing_conflict():
    """When the same event is re-ingested with a better ticker,
    insert_broker_event_feature is called but no duplicate is created."""
    repo = MagicMock()
    # First call: inserted (new row). Second call: updated (self-heal).
    repo.insert_broker_event_feature.side_effect = [True, True]
    repo.insert_broker_event_raw.side_effect = [True, False]  # raw is idempotent

    service, client, _ = _make_ingestion_service(
        client_instrument={"figi": "FIGI1", "ticker": "SBER", "uid": "uid-1"},
        repo=repo,
    )

    # First ingestion
    first = service.ingest_all(as_of=NOW)
    # Second ingestion (same events, ticker now correct)
    second = service.ingest_all(as_of=NOW)

    assert first == 1
    assert second == 1
    # Feature insert called twice (insert + self-heal update), not creating duplicates
    assert repo.insert_broker_event_feature.call_count == 2
