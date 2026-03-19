"""Tests for MOEX ingestion service -- offline, mocked."""

import logging
from unittest.mock import MagicMock, patch

from tinvest_trader.services.moex_ingestion_service import MoexIngestionService

SAMPLE_SECURITY_JSON = {
    "description": {
        "columns": ["name", "title", "value"],
        "data": [
            ["SECID", "Ticker", "SBER"],
            ["NAME", "Name", "Sberbank"],
            ["SHORTNAME", "Short name", "Sberbank AO"],
            ["ISIN", "ISIN", "RU0009029540"],
            ["REGNUMBER", "Reg number", "10301481B"],
            ["ISSUESIZE", "Issue size", "21586948000"],
            ["LISTLEVEL", "List level", "1"],
            ["ISQUALIFIEDINVESTORS", "Qualified", "0"],
            ["GROUP", "Group", "stock_shares"],
        ],
    },
    "boards": {
        "columns": ["secid", "boardid", "title", "is_primary", "is_traded"],
        "data": [["SBER", "TQBR", "T+ Shares", 1, 1]],
    },
}

SAMPLE_HISTORY_JSON = {
    "history": {
        "columns": [
            "BOARDID", "TRADEDATE", "SHORTNAME", "SECID",
            "NUMTRADES", "VALUE", "OPEN", "LOW", "HIGH",
            "LEGALCLOSEPRICE", "WAPRICE", "CLOSE", "VOLUME",
        ],
        "data": [
            [
                "TQBR", "2025-03-14", "Sberbank", "SBER",
                50000, 1500000000.0, 280.5, 278.0, 285.0,
                283.0, 282.5, 284.0, 5000000,
            ],
            [
                "TQBR", "2025-03-17", "Sberbank", "SBER",
                45000, 1400000000.0, 284.0, 282.0, 288.0,
                286.0, 285.5, 287.0, 4800000,
            ],
        ],
    },
    "history.cursor": {
        "columns": ["INDEX", "TOTAL", "PAGESIZE"],
        "data": [[0, 2, 100]],
    },
}


def _make_service(**kwargs):
    repo = MagicMock()
    logger = logging.getLogger("test_moex")
    defaults = {
        "repository": repo,
        "logger": logger,
        "tracked_tickers": ("SBER",),
        "engine": "stock",
        "market": "shares",
        "board": "TQBR",
        "history_lookback_days": 90,
        "metadata_enabled": True,
        "history_enabled": True,
    }
    defaults.update(kwargs)
    svc = MoexIngestionService(**defaults)
    return svc, repo


@patch("tinvest_trader.services.moex_ingestion_service.fetch_security_info")
@patch("tinvest_trader.services.moex_ingestion_service.fetch_market_history")
def test_ingest_all_metadata_and_history(mock_history, mock_security):
    mock_security.return_value = SAMPLE_SECURITY_JSON
    mock_history.return_value = SAMPLE_HISTORY_JSON
    svc, repo = _make_service()
    repo.upsert_moex_security_reference.return_value = True
    repo.insert_moex_market_history_raw.return_value = True
    repo.insert_moex_market_history.return_value = True

    count = svc.ingest_all()
    # 1 metadata upsert + 2 history rows
    assert count == 3
    assert repo.upsert_moex_security_reference.call_count == 1
    assert repo.insert_moex_market_history_raw.call_count == 2
    assert repo.insert_moex_market_history.call_count == 2


@patch("tinvest_trader.services.moex_ingestion_service.fetch_security_info")
@patch("tinvest_trader.services.moex_ingestion_service.fetch_market_history")
def test_ingest_metadata_only(mock_history, mock_security):
    mock_security.return_value = SAMPLE_SECURITY_JSON
    svc, repo = _make_service(history_enabled=False)
    repo.upsert_moex_security_reference.return_value = True

    count = svc.ingest_all()
    assert count == 1
    mock_history.assert_not_called()


@patch("tinvest_trader.services.moex_ingestion_service.fetch_security_info")
@patch("tinvest_trader.services.moex_ingestion_service.fetch_market_history")
def test_ingest_history_only(mock_history, mock_security):
    mock_history.return_value = SAMPLE_HISTORY_JSON
    svc, repo = _make_service(metadata_enabled=False)
    repo.insert_moex_market_history_raw.return_value = True
    repo.insert_moex_market_history.return_value = True

    count = svc.ingest_all()
    assert count == 2
    mock_security.assert_not_called()


def test_ingest_all_no_repository():
    logger = logging.getLogger("test_moex")
    svc = MoexIngestionService(
        repository=None,
        logger=logger,
        tracked_tickers=("SBER",),
    )
    count = svc.ingest_all()
    assert count == 0


def test_ingest_all_no_tickers():
    svc, _ = _make_service(tracked_tickers=())
    count = svc.ingest_all()
    assert count == 0


@patch("tinvest_trader.services.moex_ingestion_service.fetch_security_info")
@patch("tinvest_trader.services.moex_ingestion_service.fetch_market_history")
def test_ingest_history_deduplicates(mock_history, mock_security):
    mock_security.return_value = SAMPLE_SECURITY_JSON
    mock_history.return_value = SAMPLE_HISTORY_JSON
    svc, repo = _make_service()
    repo.upsert_moex_security_reference.return_value = True
    # First row is duplicate, second is new
    repo.insert_moex_market_history_raw.side_effect = [False, True]
    repo.insert_moex_market_history.return_value = True

    count = svc.ingest_all()
    # 1 metadata + 1 new history row
    assert count == 2
    # Normalized insert only called for the new row
    assert repo.insert_moex_market_history.call_count == 1


@patch("tinvest_trader.services.moex_ingestion_service.fetch_security_info")
@patch("tinvest_trader.services.moex_ingestion_service.fetch_market_history")
def test_ingest_filters_by_board(mock_history, mock_security):
    """Rows from non-configured boards should be skipped."""
    history_data = {
        "history": {
            "columns": [
                "BOARDID", "TRADEDATE", "SHORTNAME", "SECID",
                "NUMTRADES", "VALUE", "OPEN", "LOW", "HIGH",
                "LEGALCLOSEPRICE", "WAPRICE", "CLOSE", "VOLUME",
            ],
            "data": [
                [
                    "EQBR", "2025-03-14", "Sberbank", "SBER",
                    50000, 1500000000.0, 280.5, 278.0, 285.0,
                    283.0, 282.5, 284.0, 5000000,
                ],
            ],
        },
        "history.cursor": {
            "columns": ["INDEX", "TOTAL", "PAGESIZE"],
            "data": [[0, 1, 100]],
        },
    }
    mock_security.return_value = SAMPLE_SECURITY_JSON
    mock_history.return_value = history_data
    svc, repo = _make_service()
    repo.upsert_moex_security_reference.return_value = True

    count = svc.ingest_all()
    # Only metadata, no history rows passed board filter
    assert count == 1
    repo.insert_moex_market_history_raw.assert_not_called()


@patch("tinvest_trader.services.moex_ingestion_service.fetch_security_info")
@patch("tinvest_trader.services.moex_ingestion_service.fetch_market_history")
def test_ingest_metadata_fetch_failure(mock_history, mock_security):
    mock_security.return_value = None
    mock_history.return_value = SAMPLE_HISTORY_JSON
    svc, repo = _make_service()
    repo.insert_moex_market_history_raw.return_value = True
    repo.insert_moex_market_history.return_value = True

    count = svc.ingest_all()
    # 0 metadata + 2 history
    assert count == 2


@patch("tinvest_trader.services.moex_ingestion_service.fetch_security_info")
@patch("tinvest_trader.services.moex_ingestion_service.fetch_market_history")
def test_ingest_history_fetch_failure(mock_history, mock_security):
    mock_security.return_value = SAMPLE_SECURITY_JSON
    mock_history.return_value = None
    svc, repo = _make_service()
    repo.upsert_moex_security_reference.return_value = True

    count = svc.ingest_all()
    # Only metadata
    assert count == 1


@patch("tinvest_trader.services.moex_ingestion_service.fetch_security_info")
@patch("tinvest_trader.services.moex_ingestion_service.fetch_market_history")
def test_ingest_history_persist_error_continues(mock_history, mock_security):
    """One history persist failure should not block other rows."""
    mock_security.return_value = SAMPLE_SECURITY_JSON
    mock_history.return_value = SAMPLE_HISTORY_JSON
    svc, repo = _make_service()
    repo.upsert_moex_security_reference.return_value = True
    repo.insert_moex_market_history_raw.side_effect = [RuntimeError("db error"), True]
    repo.insert_moex_market_history.return_value = True

    count = svc.ingest_all()
    # 1 metadata + 1 successful history row
    assert count == 2


@patch("tinvest_trader.services.moex_ingestion_service.fetch_security_info")
@patch("tinvest_trader.services.moex_ingestion_service.fetch_market_history")
def test_ingest_pagination(mock_history, mock_security):
    """Service should paginate through history when more data is available."""
    page1 = {
        "history": {
            "columns": [
                "BOARDID", "TRADEDATE", "SHORTNAME", "SECID",
                "NUMTRADES", "VALUE", "OPEN", "LOW", "HIGH",
                "LEGALCLOSEPRICE", "WAPRICE", "CLOSE", "VOLUME",
            ],
            "data": [
                [
                    "TQBR", "2025-03-14", "Sberbank", "SBER",
                    50000, 1500000000.0, 280.5, 278.0, 285.0,
                    283.0, 282.5, 284.0, 5000000,
                ],
            ],
        },
        "history.cursor": {
            "columns": ["INDEX", "TOTAL", "PAGESIZE"],
            "data": [[0, 2, 1]],
        },
    }
    page2 = {
        "history": {
            "columns": [
                "BOARDID", "TRADEDATE", "SHORTNAME", "SECID",
                "NUMTRADES", "VALUE", "OPEN", "LOW", "HIGH",
                "LEGALCLOSEPRICE", "WAPRICE", "CLOSE", "VOLUME",
            ],
            "data": [
                [
                    "TQBR", "2025-03-17", "Sberbank", "SBER",
                    45000, 1400000000.0, 284.0, 282.0, 288.0,
                    286.0, 285.5, 287.0, 4800000,
                ],
            ],
        },
        "history.cursor": {
            "columns": ["INDEX", "TOTAL", "PAGESIZE"],
            "data": [[1, 2, 1]],
        },
    }
    mock_security.return_value = SAMPLE_SECURITY_JSON
    mock_history.side_effect = [page1, page2]
    svc, repo = _make_service()
    repo.upsert_moex_security_reference.return_value = True
    repo.insert_moex_market_history_raw.return_value = True
    repo.insert_moex_market_history.return_value = True

    count = svc.ingest_all()
    # 1 metadata + 2 history rows across 2 pages
    assert count == 3
    assert mock_history.call_count == 2
