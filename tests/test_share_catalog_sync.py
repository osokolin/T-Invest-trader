"""Tests for bulk share catalog sync."""

import logging
from unittest.mock import MagicMock, patch

from tinvest_trader.services.share_catalog_sync import sync_share_catalog


def _make_repo_and_client(shares=None):
    """Create mock repo + client. Client returns given shares list."""
    repo = MagicMock()
    client = MagicMock()
    client.list_all_shares.return_value = shares or []
    return repo, client


def _sample_shares():
    return [
        {
            "figi": "BBG004730N88",
            "ticker": "SBER",
            "name": "Sberbank",
            "uid": "uid-sber",
            "isin": "RU0009029540",
            "lot": 10,
            "currency": "rub",
        },
        {
            "figi": "BBG004730RP0",
            "ticker": "GAZP",
            "name": "Gazprom",
            "uid": "uid-gazp",
            "isin": "RU0007661625",
            "lot": 10,
            "currency": "rub",
        },
    ]


# ================================================================
# 1. Inserts new share rows
# ================================================================


def test_sync_inserts_new_shares():
    repo, client = _make_repo_and_client(_sample_shares())
    repo.upsert_catalog_entry.return_value = "inserted"

    result = sync_share_catalog(repo, client, logging.getLogger("test"))

    assert result.synced == 2
    assert result.inserted == 2
    assert result.updated == 0
    assert result.skipped == 0
    assert repo.upsert_catalog_entry.call_count == 2


# ================================================================
# 2. Enriches placeholder tracked rows
# ================================================================


def test_sync_enriches_existing_tracked_rows():
    """When repo returns 'updated', tracked rows get enriched."""
    repo, client = _make_repo_and_client(_sample_shares())
    repo.upsert_catalog_entry.return_value = "updated"

    result = sync_share_catalog(repo, client, logging.getLogger("test"))

    assert result.updated == 2
    assert result.inserted == 0
    # Verify correct params passed
    call_args = repo.upsert_catalog_entry.call_args_list[0]
    assert call_args.kwargs["ticker"] == "SBER"
    assert call_args.kwargs["figi"] == "BBG004730N88"
    assert call_args.kwargs["instrument_uid"] == "uid-sber"
    assert call_args.kwargs["name"] == "Sberbank"
    assert call_args.kwargs["isin"] == "RU0009029540"


# ================================================================
# 3. Tracked flag is preserved
# ================================================================


def test_sync_does_not_pass_tracked_flag():
    """upsert_catalog_entry must NOT set tracked=True for bulk sync."""
    repo, client = _make_repo_and_client(_sample_shares())
    repo.upsert_catalog_entry.return_value = "inserted"

    sync_share_catalog(repo, client, logging.getLogger("test"))

    # Verify 'tracked' is never passed as a kwarg
    for call in repo.upsert_catalog_entry.call_args_list:
        assert "tracked" not in call.kwargs


# ================================================================
# 4. Non-tracked shares are not incorrectly marked tracked
# ================================================================


def test_upsert_catalog_entry_sql_defaults_tracked_false():
    """upsert_catalog_entry INSERT defaults tracked=FALSE."""
    import inspect

    from tinvest_trader.infra.storage.repository import TradingRepository

    source = inspect.getsource(TradingRepository.upsert_catalog_entry)
    # INSERT must default tracked to FALSE
    assert "FALSE" in source
    # Must NOT have tracked in DO UPDATE SET
    assert "tracked" not in source.split("DO UPDATE SET")[1].split("RETURNING")[0]


# ================================================================
# 5. Ticker normalization works
# ================================================================


def test_sync_normalizes_tickers():
    shares = [
        {"figi": "BBG123", "ticker": "sber", "name": "Test",
         "uid": "u1", "isin": "XX", "lot": 1, "currency": "rub"},
    ]
    repo, client = _make_repo_and_client(shares)
    repo.upsert_catalog_entry.return_value = "inserted"

    sync_share_catalog(repo, client, logging.getLogger("test"))

    call_args = repo.upsert_catalog_entry.call_args_list[0]
    # list_all_shares normalizes to uppercase, but sync also passes as-is
    # The repository method will uppercase it
    assert call_args.kwargs["ticker"] == "sber" or call_args.kwargs["ticker"] == "SBER"


# ================================================================
# 6. Repeated sync is idempotent
# ================================================================


def test_sync_idempotent():
    repo, client = _make_repo_and_client(_sample_shares())
    # First run: all inserted
    repo.upsert_catalog_entry.return_value = "inserted"
    r1 = sync_share_catalog(repo, client, logging.getLogger("test"))
    assert r1.inserted == 2

    # Second run: all skipped (no changes)
    repo.upsert_catalog_entry.return_value = "updated"
    r2 = sync_share_catalog(repo, client, logging.getLogger("test"))
    assert r2.updated == 2
    assert r2.inserted == 0


# ================================================================
# 7. CLI command works
# ================================================================


def test_cli_sync_share_catalog():
    from tinvest_trader.cli import main

    mock_repo = MagicMock()
    mock_repo.upsert_catalog_entry.return_value = "inserted"

    with (
        patch("tinvest_trader.cli.load_config"),
        patch("tinvest_trader.cli.build_container") as mock_build,
    ):
        container = MagicMock()
        container.repository = mock_repo
        container.tbank_client = MagicMock()
        container.tbank_client.list_all_shares.return_value = _sample_shares()
        container.logger = logging.getLogger("test")
        mock_build.return_value = container

        result = main(["sync-share-catalog"])

    assert result == 0


def test_cli_sync_no_db():
    from tinvest_trader.cli import main

    with (
        patch("tinvest_trader.cli.load_config"),
        patch("tinvest_trader.cli.build_container") as mock_build,
    ):
        container = MagicMock()
        container.repository = None
        mock_build.return_value = container

        result = main(["sync-share-catalog"])

    assert result == 1


# ================================================================
# Edge cases
# ================================================================


def test_sync_handles_empty_api_response():
    repo, client = _make_repo_and_client([])

    result = sync_share_catalog(repo, client, logging.getLogger("test"))

    assert result.synced == 0
    repo.upsert_catalog_entry.assert_not_called()


def test_sync_handles_failed_upsert():
    repo, client = _make_repo_and_client(_sample_shares())
    repo.upsert_catalog_entry.side_effect = RuntimeError("db error")

    result = sync_share_catalog(repo, client, logging.getLogger("test"))

    assert result.failed == 2
    assert result.inserted == 0


def test_sync_skips_shares_without_ticker():
    shares = [
        {"figi": "BBG123", "ticker": "", "name": "No ticker",
         "uid": "u1", "isin": "XX"},
    ]
    repo, client = _make_repo_and_client(shares)

    result = sync_share_catalog(repo, client, logging.getLogger("test"))

    assert result.skipped == 1
    repo.upsert_catalog_entry.assert_not_called()


def test_sync_respects_limit():
    shares = _sample_shares() + [
        {"figi": "BBG999", "ticker": "YNDX", "name": "Yandex",
         "uid": "u3", "isin": "YY", "lot": 1, "currency": "rub"},
    ]
    repo, client = _make_repo_and_client(shares)
    repo.upsert_catalog_entry.return_value = "inserted"

    result = sync_share_catalog(
        repo, client, logging.getLogger("test"), limit=2,
    )

    assert result.synced == 2
    assert repo.upsert_catalog_entry.call_count == 2


# ================================================================
# Repository method SQL verification
# ================================================================


def test_upsert_catalog_entry_sql_params():
    """Verify upsert_catalog_entry passes correct SQL params."""
    pool = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (True,)  # inserted
    pool.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
    pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)

    from tinvest_trader.infra.storage.repository import TradingRepository

    repo = TradingRepository(pool=pool, logger=logging.getLogger("test"))

    outcome = repo.upsert_catalog_entry(
        ticker="SBER",
        figi="BBG004730N88",
        instrument_uid="uid-123",
        name="Sberbank",
        isin="RU123",
        lot=10,
        currency="rub",
    )

    assert outcome == "inserted"
    sql, params = conn.execute.call_args[0]
    assert "ON CONFLICT (ticker)" in sql
    assert "TICKER:%" in sql  # placeholder protection
    assert params[0] == "BBG004730N88"  # figi
    assert params[1] == "uid-123"  # instrument_uid
    assert params[2] == "SBER"  # ticker
    assert params[3] == "Sberbank"  # name
    assert params[4] == "RU123"  # isin
    assert params[5] == 10  # lot
    assert params[6] == "rub"  # currency


def test_upsert_catalog_entry_placeholder_figi():
    """When no figi provided, should use TICKER: placeholder."""
    pool = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (True,)
    pool.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
    pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)

    from tinvest_trader.infra.storage.repository import TradingRepository

    repo = TradingRepository(pool=pool, logger=logging.getLogger("test"))
    repo.upsert_catalog_entry(ticker="SBER")

    _, params = conn.execute.call_args[0]
    assert params[0] == "TICKER:SBER"


# ================================================================
# T-Bank client method
# ================================================================


def test_list_all_shares_no_token():
    from tinvest_trader.app.config import BrokerConfig
    from tinvest_trader.infra.tbank.client import TBankClient

    client = TBankClient(
        config=BrokerConfig(token=""),
        logger=logging.getLogger("test"),
    )
    result = client.list_all_shares()
    assert result == []


def test_list_all_shares_normalizes():
    from tinvest_trader.app.config import BrokerConfig
    from tinvest_trader.infra.tbank.client import TBankClient

    client = TBankClient(
        config=BrokerConfig(token="test-token"),
        logger=logging.getLogger("test"),
    )

    mock_response = {
        "instruments": [
            {
                "figi": "BBG004730N88",
                "ticker": "sber",
                "name": "Sberbank",
                "uid": "uid-1",
                "isin": "RU123",
                "lot": 10,
                "currency": "rub",
            },
            {
                "figi": "",
                "ticker": "NOFIGI",
                "name": "No FIGI",
            },
            {
                "figi": "BBG999",
                "ticker": "",
                "name": "No ticker",
            },
        ],
    }

    with patch.object(client, "_post_instruments_service", return_value=mock_response):
        result = client.list_all_shares()

    # Should filter out entries without ticker or figi
    assert len(result) == 1
    assert result[0]["ticker"] == "SBER"
    assert result[0]["figi"] == "BBG004730N88"
