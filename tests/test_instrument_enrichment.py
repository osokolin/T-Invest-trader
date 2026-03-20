"""Tests for instrument enrichment service."""

import logging
from unittest.mock import MagicMock

from tinvest_trader.services.instrument_enrichment import (
    _needs_enrichment,
    enrich_instruments,
)


def _make_repo():
    """Create a mock repository."""
    repo = MagicMock()
    repo.list_tracked_instruments.return_value = []
    return repo


def _make_client(api_data=None):
    """Create a mock T-Bank client."""
    client = MagicMock()
    client.get_instrument_by_ticker.return_value = api_data
    return client


# -- _needs_enrichment --


def test_needs_enrichment_placeholder_figi():
    assert _needs_enrichment({"figi": "TICKER:SBER"}) is True


def test_needs_enrichment_empty_figi():
    assert _needs_enrichment({"figi": ""}) is True


def test_needs_enrichment_null_figi():
    assert _needs_enrichment({"figi": None}) is True


def test_needs_enrichment_missing_uid():
    assert _needs_enrichment({
        "figi": "BBG004730N88",
        "instrument_uid": None,
        "name": "Sberbank",
    }) is True


def test_needs_enrichment_missing_name():
    assert _needs_enrichment({
        "figi": "BBG004730N88",
        "instrument_uid": "uid-123",
        "name": "",
    }) is True


def test_needs_enrichment_complete():
    assert _needs_enrichment({
        "figi": "BBG004730N88",
        "instrument_uid": "uid-123",
        "name": "Sberbank",
    }) is False


# -- enrich_instruments: placeholder upgraded --


def test_enrichment_upgrades_placeholder_figi():
    repo = _make_repo()
    repo.list_tracked_instruments.return_value = [
        {"ticker": "SBER", "figi": "TICKER:SBER", "instrument_uid": None, "name": ""},
    ]
    client = _make_client({
        "figi": "BBG004730N88",
        "ticker": "SBER",
        "name": "Sberbank",
        "uid": "e6123145-9665-43e0-8413-cd61b8aa9b13",
        "isin": "RU0009029540",
    })

    result = enrich_instruments(repo, client, logging.getLogger("test"))

    assert result.updated == 1
    assert result.skipped == 0
    assert result.failed == 0

    repo.ensure_instrument.assert_called_once_with(
        ticker="SBER",
        figi="BBG004730N88",
        name="Sberbank",
        isin="RU0009029540",
        moex_secid="SBER",
        tracked=True,
    )
    repo.update_instrument_uid.assert_called_once_with(
        ticker="SBER",
        instrument_uid="e6123145-9665-43e0-8413-cd61b8aa9b13",
    )


# -- enrich_instruments: fills missing fields --


def test_enrichment_fills_missing_fields():
    repo = _make_repo()
    repo.list_tracked_instruments.return_value = [
        {
            "ticker": "GAZP",
            "figi": "BBG004730RP0",
            "instrument_uid": None,
            "name": "",
        },
    ]
    client = _make_client({
        "figi": "BBG004730RP0",
        "ticker": "GAZP",
        "name": "Gazprom",
        "uid": "uid-gazp-123",
        "isin": "RU0007661625",
    })

    result = enrich_instruments(repo, client, logging.getLogger("test"))

    assert result.updated == 1
    repo.ensure_instrument.assert_called_once()
    repo.update_instrument_uid.assert_called_once_with(
        ticker="GAZP", instrument_uid="uid-gazp-123",
    )


# -- enrich_instruments: complete instruments skipped --


def test_enrichment_skips_complete_instruments():
    repo = _make_repo()
    repo.list_tracked_instruments.return_value = [
        {
            "ticker": "SBER",
            "figi": "BBG004730N88",
            "instrument_uid": "uid-123",
            "name": "Sberbank",
        },
    ]
    client = _make_client()

    result = enrich_instruments(repo, client, logging.getLogger("test"))

    assert result.processed == 0
    assert result.skipped == 1
    assert result.updated == 0
    client.get_instrument_by_ticker.assert_not_called()


# -- enrich_instruments: API failure does not abort --


def test_enrichment_survives_api_failure():
    repo = _make_repo()
    repo.list_tracked_instruments.return_value = [
        {"ticker": "SBER", "figi": "TICKER:SBER", "instrument_uid": None, "name": ""},
        {"ticker": "GAZP", "figi": "TICKER:GAZP", "instrument_uid": None, "name": ""},
    ]
    client = MagicMock()
    client.get_instrument_by_ticker.side_effect = [
        RuntimeError("API timeout"),
        {"figi": "BBG004730RP0", "ticker": "GAZP", "name": "Gazprom", "uid": "", "isin": ""},
    ]

    result = enrich_instruments(repo, client, logging.getLogger("test"))

    assert result.failed == 1
    assert result.updated == 1
    assert result.processed == 2
    assert len(result.errors) == 1
    assert "SBER" in result.errors[0]


# -- enrich_instruments: API returns None --


def test_enrichment_skips_when_api_returns_none():
    repo = _make_repo()
    repo.list_tracked_instruments.return_value = [
        {"ticker": "YNDX", "figi": "TICKER:YNDX", "instrument_uid": None, "name": ""},
    ]
    client = _make_client(None)

    result = enrich_instruments(repo, client, logging.getLogger("test"))

    assert result.skipped == 1
    assert result.updated == 0
    repo.ensure_instrument.assert_not_called()


# -- enrich_instruments: limit --


def test_enrichment_respects_limit():
    repo = _make_repo()
    repo.list_tracked_instruments.return_value = [
        {"ticker": "SBER", "figi": "TICKER:SBER", "instrument_uid": None, "name": ""},
        {"ticker": "GAZP", "figi": "TICKER:GAZP", "instrument_uid": None, "name": ""},
        {"ticker": "LKOH", "figi": "TICKER:LKOH", "instrument_uid": None, "name": ""},
    ]
    client = _make_client({
        "figi": "BBG123", "ticker": "X", "name": "Test", "uid": "", "isin": "",
    })

    result = enrich_instruments(
        repo, client, logging.getLogger("test"), limit=2,
    )

    assert result.processed == 2
    assert client.get_instrument_by_ticker.call_count == 2


# -- enrich_instruments: no uid in response --


def test_enrichment_skips_uid_update_when_empty():
    repo = _make_repo()
    repo.list_tracked_instruments.return_value = [
        {"ticker": "SBER", "figi": "TICKER:SBER", "instrument_uid": None, "name": ""},
    ]
    client = _make_client({
        "figi": "BBG004730N88", "ticker": "SBER", "name": "Sberbank",
        "uid": "", "isin": "",
    })

    enrich_instruments(repo, client, logging.getLogger("test"))

    repo.ensure_instrument.assert_called_once()
    repo.update_instrument_uid.assert_not_called()
