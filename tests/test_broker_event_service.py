"""Tests for broker structured-event ingestion orchestration."""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

from tinvest_trader.services.broker_event_ingestion_service import (
    BrokerEventIngestionService,
)


def _make_service(
    *,
    tracked_figis: tuple[str, ...] = ("FIGI1",),
    event_types: tuple[str, ...] = ("dividends", "reports", "insider_deals"),
    repository: MagicMock | None = None,
) -> tuple[BrokerEventIngestionService, MagicMock, MagicMock]:
    client = MagicMock()
    client.get_instrument.return_value = {
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
    client.get_asset_reports.return_value = [
        {
            "instrument_id": "uid-1",
            "report_date": "2026-03-18T00:00:00+00:00",
            "period_year": 2025,
            "period_num": 4,
            "period_type": "ASSET_REPORT_PERIOD_TYPE_Q4",
        },
    ]
    client.get_insider_deals.return_value = [
        {
            "trade_id": 10,
            "direction": "TRADE_DIRECTION_BUY",
            "currency": "RUB",
            "date": "2026-03-18T00:00:00+00:00",
            "price": {"units": 120, "nano": 0},
        },
    ]
    repo = repository if repository is not None else MagicMock()
    repo.insert_broker_event_raw.return_value = True
    repo.insert_broker_event_feature.return_value = True
    service = BrokerEventIngestionService(
        client=client,
        repository=repo,
        logger=logging.getLogger("test"),
        account_id="acc-1",
        tracked_figis=tracked_figis,
        event_types=event_types,
        lookback_days=30,
    )
    return service, client, repo


def test_ingest_all_processes_supported_event_types():
    service, client, repo = _make_service()

    processed = service.ingest_all(as_of=datetime(2026, 3, 19, tzinfo=UTC))

    assert processed == 3
    client.get_dividends.assert_called_once()
    client.get_asset_reports.assert_called_once_with(
        instrument_uid="uid-1",
        from_time=datetime(2026, 2, 17, 0, 0, tzinfo=UTC),
        to_time=datetime(2026, 3, 19, 0, 0, tzinfo=UTC),
    )
    client.get_insider_deals.assert_called_once_with(instrument_uid="uid-1")
    assert repo.insert_broker_event_raw.call_count == 3
    assert repo.insert_broker_event_feature.call_count == 3


def test_ingest_all_uses_repositoryless_mode():
    service, client, _repo = _make_service(repository=None)
    service._repository = None

    processed = service.ingest_all(as_of=datetime(2026, 3, 19, tzinfo=UTC))

    assert processed == 3
    client.get_dividends.assert_called_once()


def test_ingest_all_skips_unknown_event_type():
    service, client, repo = _make_service(event_types=("dividends", "unknown"))

    processed = service.ingest_all(as_of=datetime(2026, 3, 19, tzinfo=UTC))

    assert processed == 1
    client.get_dividends.assert_called_once()
    client.get_asset_reports.assert_not_called()
    client.get_insider_deals.assert_not_called()
    repo.insert_broker_event_feature.assert_called_once()


def test_ingest_all_filters_old_insider_deals():
    service, _client, repo = _make_service(event_types=("insider_deals",))
    service._client.get_insider_deals.return_value = [
        {
            "trade_id": 11,
            "direction": "TRADE_DIRECTION_BUY",
            "currency": "RUB",
            "date": "2025-01-01T00:00:00+00:00",
            "price": {"units": 120, "nano": 0},
        },
    ]

    processed = service.ingest_all(as_of=datetime(2026, 3, 19, tzinfo=UTC))

    assert processed == 0
    repo.insert_broker_event_raw.assert_not_called()
    repo.insert_broker_event_feature.assert_not_called()


def test_ingest_all_uses_tracked_figi_override_only():
    service, client, _repo = _make_service(tracked_figis=("FIGI1", "FIGI2"))
    client.get_instrument.side_effect = [
        {"figi": "FIGI1", "ticker": "SBER", "uid": "uid-1"},
        {"figi": "FIGI2", "ticker": "GAZP", "uid": "uid-2"},
    ]

    service.ingest_all(as_of=datetime(2026, 3, 19, tzinfo=UTC))

    assert client.get_instrument.call_count == 2
    assert client.get_instrument.call_args_list[0].args == ("FIGI1",)
    assert client.get_instrument.call_args_list[1].args == ("FIGI2",)
