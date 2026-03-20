"""Integration tests for instrument registry -- real method calls through repository.

These tests call actual TradingRepository methods with a mock pool that captures
SQL execution. They verify the full flow from bootstrap through enrichment,
including parameter correctness, placeholder protection, and idempotency.
No real Postgres required.
"""

import logging
from unittest.mock import MagicMock, call

from tinvest_trader.infra.storage.repository import TradingRepository
from tinvest_trader.services.instrument_enrichment import enrich_instruments


def _make_repo_with_spy():
    """Create repo with a spy pool that captures all SQL calls."""
    pool = MagicMock()
    conn = MagicMock()
    pool.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
    pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)

    logger = logging.getLogger("test-integration")
    repo = TradingRepository(pool=pool, logger=logger)
    return repo, conn


def _extract_sql_calls(conn):
    """Extract all (sql, params) tuples from mock connection."""
    return [
        (c.args[0], c.args[1] if len(c.args) > 1 else None)
        for c in conn.execute.call_args_list
    ]


# ================================================================
# Full flow: bootstrap -> enrich -> verify
# ================================================================


def test_full_flow_bootstrap_then_enrich():
    """Empty DB -> bootstrap 2 tickers -> enrich one -> verify SQL flow."""
    repo, conn = _make_repo_with_spy()

    # Step 1: count_tracked returns 0 (empty DB)
    conn.execute.return_value.fetchone.return_value = (0,)

    repo.bootstrap_tracked_instruments(("SBER", "GAZP"))

    # Should have: 1 count query + 2 ensure_instrument calls
    calls = _extract_sql_calls(conn)
    assert len(calls) == 3

    # First call: count query
    assert "SELECT count(*)" in calls[0][0]

    # Second call: ensure SBER with placeholder FIGI
    assert "ON CONFLICT (ticker)" in calls[1][0]
    assert calls[1][1][0] == "TICKER:SBER"  # placeholder figi
    assert calls[1][1][1] == "SBER"  # ticker

    # Third call: ensure GAZP with placeholder FIGI
    assert calls[2][1][0] == "TICKER:GAZP"
    assert calls[2][1][1] == "GAZP"

    conn.reset_mock()

    # Step 2: enrich SBER with real data
    repo.ensure_instrument(
        ticker="SBER", figi="BBG004730N88",
        name="Sberbank", isin="RU0009029540",
    )

    calls = _extract_sql_calls(conn)
    assert len(calls) == 1
    sql, params = calls[0]
    assert "ON CONFLICT (ticker)" in sql
    assert params[0] == "BBG004730N88"  # real figi replaces placeholder
    assert params[1] == "SBER"
    assert params[2] == "Sberbank"
    assert params[3] == "RU0009029540"

    conn.reset_mock()

    # Step 3: update_instrument_uid
    repo.update_instrument_uid(ticker="SBER", instrument_uid="uid-123")

    calls = _extract_sql_calls(conn)
    assert len(calls) == 1
    sql, params = calls[0]
    assert "UPDATE instrument_catalog" in sql
    assert "instrument_uid" in sql
    assert params[0] == "uid-123"
    assert params[1] == "SBER"


# ================================================================
# Placeholder FIGI protection
# ================================================================


def test_placeholder_figi_not_overwritten_by_another_placeholder():
    """Calling ensure_instrument twice without figi must not change placeholder."""
    repo, conn = _make_repo_with_spy()

    # First: bootstrap creates TICKER:SBER
    repo.ensure_instrument(ticker="SBER", tracked=True)
    sql1, params1 = _extract_sql_calls(conn)[0]
    assert params1[0] == "TICKER:SBER"

    conn.reset_mock()

    # Second: another ensure without figi
    repo.ensure_instrument(ticker="SBER", tracked=True)
    sql2, params2 = _extract_sql_calls(conn)[0]
    # SQL CASE ensures: placeholder EXCLUDED does not overwrite existing
    assert "NOT EXCLUDED.figi LIKE 'TICKER:%%'" in sql2
    # The EXCLUDED value is still a placeholder, but CASE keeps existing
    assert params2[0] == "TICKER:SBER"


def test_real_figi_replaces_placeholder():
    """ensure_instrument with real figi must produce SQL that replaces placeholder."""
    repo, conn = _make_repo_with_spy()

    # Bootstrap
    repo.ensure_instrument(ticker="GAZP", tracked=True)
    conn.reset_mock()

    # Enrich with real figi
    repo.ensure_instrument(
        ticker="GAZP", figi="BBG004730RP0", name="Gazprom",
    )
    sql, params = _extract_sql_calls(conn)[0]

    # Real figi in EXCLUDED
    assert params[0] == "BBG004730RP0"
    # SQL CASE: real figi passes the check, replaces placeholder
    assert "WHEN EXCLUDED.figi != ''" in sql
    assert "NOT EXCLUDED.figi LIKE 'TICKER:%%'" in sql


def test_real_figi_not_overwritten_by_placeholder():
    """Once a real FIGI is set, calling ensure without figi must not downgrade it."""
    repo, conn = _make_repo_with_spy()

    # Already enriched (real figi). Now another ensure without figi.
    repo.ensure_instrument(ticker="SBER", tracked=True)
    sql, params = _extract_sql_calls(conn)[0]

    # The EXCLUDED figi is TICKER:SBER (placeholder), but SQL CASE keeps existing
    assert params[0] == "TICKER:SBER"
    assert "ELSE instrument_catalog.figi" in sql


# ================================================================
# Enrichment idempotency
# ================================================================


def test_enrichment_idempotent_double_run():
    """Running enrichment twice with same data: second run skips already-complete."""
    repo = MagicMock()

    # First run: instrument needs enrichment
    repo.list_tracked_instruments.return_value = [
        {"ticker": "SBER", "figi": "TICKER:SBER",
         "instrument_uid": None, "name": ""},
    ]

    client = MagicMock()
    client.get_instrument_by_ticker.return_value = {
        "figi": "BBG004730N88", "ticker": "SBER",
        "name": "Sberbank", "uid": "uid-1", "isin": "RU123",
    }

    result1 = enrich_instruments(
        repo, client, logging.getLogger("test"),
    )
    assert result1.updated == 1
    assert result1.processed == 1

    # Second run: instrument is now complete
    repo.list_tracked_instruments.return_value = [
        {"ticker": "SBER", "figi": "BBG004730N88",
         "instrument_uid": "uid-1", "name": "Sberbank"},
    ]
    client.reset_mock()

    result2 = enrich_instruments(
        repo, client, logging.getLogger("test"),
    )
    assert result2.updated == 0
    assert result2.processed == 0
    assert result2.skipped == 1
    client.get_instrument_by_ticker.assert_not_called()


def test_enrichment_partial_fill_still_needs_enrichment():
    """Instrument with real figi but missing uid still needs enrichment."""
    repo = MagicMock()
    repo.list_tracked_instruments.return_value = [
        {"ticker": "SBER", "figi": "BBG004730N88",
         "instrument_uid": None, "name": "Sberbank"},
    ]

    client = MagicMock()
    client.get_instrument_by_ticker.return_value = {
        "figi": "BBG004730N88", "ticker": "SBER",
        "name": "Sberbank", "uid": "uid-1", "isin": "RU123",
    }

    result = enrich_instruments(
        repo, client, logging.getLogger("test"),
    )
    assert result.updated == 1
    assert result.processed == 1


# ================================================================
# Tracked flag never downgrades
# ================================================================


def test_tracked_flag_or_logic():
    """ensure_instrument SQL uses OR to never downgrade tracked from True to False."""
    repo, conn = _make_repo_with_spy()

    # Call ensure with tracked=False
    repo.ensure_instrument(ticker="SBER", tracked=False)
    sql, params = _extract_sql_calls(conn)[0]

    # SQL uses OR: tracked = catalog.tracked OR EXCLUDED.tracked
    # So even if EXCLUDED.tracked = False, existing True is preserved
    assert "instrument_catalog.tracked OR EXCLUDED.tracked" in sql


# ================================================================
# update_instrument_uid guards
# ================================================================


def test_update_uid_only_when_empty():
    """update_instrument_uid SQL only sets uid when existing is NULL or empty."""
    repo, conn = _make_repo_with_spy()

    repo.update_instrument_uid(ticker="SBER", instrument_uid="uid-new")
    sql, params = _extract_sql_calls(conn)[0]

    assert "instrument_uid IS NULL OR instrument_uid = ''" in sql
    assert params[0] == "uid-new"
    assert params[1] == "SBER"


def test_update_uid_normalizes_ticker():
    """update_instrument_uid normalizes ticker to uppercase."""
    repo, conn = _make_repo_with_spy()

    repo.update_instrument_uid(ticker="sber", instrument_uid="uid-x")
    _, params = _extract_sql_calls(conn)[0]
    assert params[1] == "SBER"


# ================================================================
# ensure_instrument metadata fill logic
# ================================================================


def test_ensure_fills_name_only_when_nonempty():
    """ensure_instrument SQL uses CASE to keep existing name when new is empty."""
    repo, conn = _make_repo_with_spy()

    repo.ensure_instrument(ticker="SBER", name="")
    sql, _ = _extract_sql_calls(conn)[0]

    assert "WHEN EXCLUDED.name != ''" in sql
    assert "ELSE instrument_catalog.name" in sql


def test_ensure_fills_moex_secid_defaults_to_ticker():
    """ensure_instrument defaults moex_secid to ticker when not provided."""
    repo, conn = _make_repo_with_spy()

    repo.ensure_instrument(ticker="SBER")
    _, params = _extract_sql_calls(conn)[0]

    # moex_secid parameter (index 4) should be ticker
    assert params[4] == "SBER"


def test_ensure_fills_moex_secid_custom():
    """ensure_instrument uses provided moex_secid when given."""
    repo, conn = _make_repo_with_spy()

    repo.ensure_instrument(ticker="SBER", moex_secid="SBER-MOEX")
    _, params = _extract_sql_calls(conn)[0]

    assert params[4] == "SBER-MOEX"


# ================================================================
# Full enrichment flow through repo methods
# ================================================================


def test_enrichment_calls_repo_methods_in_order():
    """enrich_instruments calls ensure_instrument then update_instrument_uid."""
    repo = MagicMock()
    repo.list_tracked_instruments.return_value = [
        {"ticker": "SBER", "figi": "TICKER:SBER",
         "instrument_uid": None, "name": ""},
    ]
    client = MagicMock()
    client.get_instrument_by_ticker.return_value = {
        "figi": "BBG004730N88", "ticker": "SBER",
        "name": "Sberbank", "uid": "uid-abc", "isin": "RU123",
    }

    enrich_instruments(repo, client, logging.getLogger("test"))

    # Verify call order: ensure_instrument before update_instrument_uid
    ensure_call = call.ensure_instrument(
        ticker="SBER", figi="BBG004730N88", name="Sberbank",
        isin="RU123", moex_secid="SBER", tracked=True,
    )
    uid_call = call.update_instrument_uid(
        ticker="SBER", instrument_uid="uid-abc",
    )
    repo.assert_has_calls([ensure_call, uid_call], any_order=False)


def test_enrichment_mixed_batch():
    """Batch with complete, placeholder, and missing-uid instruments."""
    repo = MagicMock()
    repo.list_tracked_instruments.return_value = [
        # Complete: skipped
        {"ticker": "SBER", "figi": "BBG004730N88",
         "instrument_uid": "uid-1", "name": "Sberbank"},
        # Placeholder: needs enrichment
        {"ticker": "GAZP", "figi": "TICKER:GAZP",
         "instrument_uid": None, "name": ""},
        # Missing name: needs enrichment
        {"ticker": "LKOH", "figi": "BBG123",
         "instrument_uid": "uid-2", "name": ""},
    ]
    client = MagicMock()
    client.get_instrument_by_ticker.return_value = {
        "figi": "BBG999", "ticker": "X", "name": "Test",
        "uid": "uid-new", "isin": "XX",
    }

    result = enrich_instruments(repo, client, logging.getLogger("test"))

    # SBER skipped (complete), GAZP + LKOH processed
    assert result.processed == 2
    assert result.updated == 2
    assert result.skipped == 1  # SBER
    assert client.get_instrument_by_ticker.call_count == 2


# ================================================================
# set_tracked_status
# ================================================================


def test_set_tracked_status_sql():
    """set_tracked_status sends correct SQL and params."""
    repo, conn = _make_repo_with_spy()
    conn.execute.return_value.fetchone.return_value = (1,)

    updated = repo.set_tracked_status(ticker="sber", tracked=False)

    assert updated is True
    sql, params = _extract_sql_calls(conn)[0]
    assert "UPDATE instrument_catalog" in sql
    assert "SET tracked = %s" in sql
    assert params[0] is False
    assert params[1] == "SBER"


def test_set_tracked_status_not_found():
    """set_tracked_status returns False when no row matches."""
    repo, conn = _make_repo_with_spy()
    conn.execute.return_value.fetchone.return_value = None

    updated = repo.set_tracked_status(ticker="UNKNOWN", tracked=True)

    assert updated is False
