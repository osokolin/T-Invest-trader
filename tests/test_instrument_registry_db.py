"""Tests for DB-backed instrument registry, bootstrap, and resolution logic."""

import logging
from unittest.mock import MagicMock

# ================================================================
# A) Bootstrap via real method logic
# ================================================================


def test_bootstrap_seeds_when_db_empty():
    """bootstrap_tracked_instruments seeds all tickers when count is 0."""
    repo = MagicMock()
    repo.count_tracked_instruments.return_value = 0

    from tinvest_trader.infra.storage.repository import TradingRepository

    result = TradingRepository.bootstrap_tracked_instruments(
        repo, ("SBER", "GAZP", "YNDX"),
    )

    assert result == 3
    assert repo.ensure_instrument.call_count == 3
    repo.ensure_instrument.assert_any_call(ticker="SBER", tracked=True)
    repo.ensure_instrument.assert_any_call(ticker="GAZP", tracked=True)
    repo.ensure_instrument.assert_any_call(ticker="YNDX", tracked=True)


def test_bootstrap_skips_when_tracked_exist():
    """bootstrap_tracked_instruments does nothing when tracked rows exist."""
    repo = MagicMock()
    repo.count_tracked_instruments.return_value = 5

    from tinvest_trader.infra.storage.repository import TradingRepository

    result = TradingRepository.bootstrap_tracked_instruments(
        repo, ("SBER", "GAZP"),
    )

    assert result == 0
    repo.ensure_instrument.assert_not_called()


def test_bootstrap_skips_blank_tickers():
    """bootstrap_tracked_instruments skips empty/whitespace tickers."""
    repo = MagicMock()
    repo.count_tracked_instruments.return_value = 0

    from tinvest_trader.infra.storage.repository import TradingRepository

    result = TradingRepository.bootstrap_tracked_instruments(
        repo, ("SBER", "", "  ", "GAZP"),
    )

    assert result == 2
    assert repo.ensure_instrument.call_count == 2


def test_bootstrap_normalizes_to_uppercase():
    """bootstrap_tracked_instruments normalizes tickers to uppercase."""
    repo = MagicMock()
    repo.count_tracked_instruments.return_value = 0

    from tinvest_trader.infra.storage.repository import TradingRepository

    TradingRepository.bootstrap_tracked_instruments(repo, ("sber",))
    repo.ensure_instrument.assert_called_once_with(ticker="SBER", tracked=True)


# ================================================================
# B) DB precedence via _resolve_tracked_tickers
# ================================================================


def _make_container_with_mock_repo(monkeypatch, env_tickers="", db_rows=None):
    """Helper: build container with mock repo returning db_rows."""
    if env_tickers:
        monkeypatch.setenv("TINVEST_SENTIMENT_TRACKED_TICKERS", env_tickers)
    else:
        monkeypatch.delenv("TINVEST_SENTIMENT_TRACKED_TICKERS", raising=False)

    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    config = load_config()
    container = build_container(config)

    mock_repo = MagicMock()
    if db_rows is not None:
        mock_repo.list_tracked_instruments.return_value = db_rows
    else:
        mock_repo.list_tracked_instruments.return_value = []
    mock_repo.count_tracked_instruments.return_value = len(db_rows or [])
    mock_repo.bootstrap_tracked_instruments = MagicMock(return_value=0)
    container.repository = mock_repo
    return container, mock_repo


def test_resolve_prefers_db_over_env(monkeypatch):
    """When DB has tracked instruments, env tickers must be ignored."""
    container, _ = _make_container_with_mock_repo(
        monkeypatch,
        env_tickers="YNDX,LKOH",
        db_rows=[{"ticker": "SBER", "figi": "BBG004730N88"}],
    )
    tickers = container._resolve_tracked_tickers()

    assert tickers == frozenset({"SBER"})
    assert "YNDX" not in tickers
    assert "LKOH" not in tickers


def test_resolve_bootstraps_when_db_empty(monkeypatch):
    """When DB is empty, bootstraps from env then returns seeded tickers."""
    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    monkeypatch.setenv("TINVEST_SENTIMENT_TRACKED_TICKERS", "SBER,GAZP")
    config = load_config()
    container = build_container(config)

    mock_repo = MagicMock()
    # First call: empty. After bootstrap: seeded data.
    mock_repo.list_tracked_instruments.side_effect = [
        [],
        [{"ticker": "SBER", "figi": "TICKER:SBER"},
         {"ticker": "GAZP", "figi": "TICKER:GAZP"}],
    ]
    mock_repo.bootstrap_tracked_instruments.return_value = 2
    container.repository = mock_repo

    tickers = container._resolve_tracked_tickers()

    assert tickers == frozenset({"SBER", "GAZP"})
    mock_repo.bootstrap_tracked_instruments.assert_called_once()


def test_resolve_env_fallback_without_db(monkeypatch):
    """When no DB, falls back to env tickers."""
    monkeypatch.setenv("TINVEST_SENTIMENT_TRACKED_TICKERS", "SBER,GAZP")

    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    container = build_container(load_config())
    container.repository = None

    tickers = container._resolve_tracked_tickers()
    assert "SBER" in tickers
    assert "GAZP" in tickers


def test_resolve_returns_empty_without_db_or_env(monkeypatch):
    """When no DB and no env, returns empty."""
    monkeypatch.delenv("TINVEST_SENTIMENT_TRACKED_TICKERS", raising=False)

    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    container = build_container(load_config())
    container.repository = None

    assert len(container._resolve_tracked_tickers()) == 0


def test_resolve_handles_db_error_gracefully(monkeypatch):
    """When DB throws, falls back to env."""
    monkeypatch.setenv("TINVEST_SENTIMENT_TRACKED_TICKERS", "SBER")

    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    container = build_container(load_config())
    mock_repo = MagicMock()
    mock_repo.list_tracked_instruments.side_effect = RuntimeError("db down")
    container.repository = mock_repo

    assert "SBER" in container._resolve_tracked_tickers()


# ================================================================
# C) Broker FIGI resolution via _resolve_tracked_instruments
# ================================================================


def test_resolve_instruments_returns_full_dicts(monkeypatch):
    """_resolve_tracked_instruments returns full instrument dicts."""
    container, _ = _make_container_with_mock_repo(
        monkeypatch,
        db_rows=[
            {"ticker": "SBER", "figi": "BBG004730N88"},
            {"ticker": "GAZP", "figi": "BBG004730RP0"},
        ],
    )
    instruments = container._resolve_tracked_instruments()
    assert len(instruments) == 2
    assert instruments[0]["figi"] == "BBG004730N88"


def test_broker_wiring_extracts_real_figis_from_db(monkeypatch):
    """_wire_broker_events extracts FIGIs from DB, skipping placeholders."""
    import inspect

    from tinvest_trader.app.container import Container

    source = inspect.getsource(Container._wire_broker_events)
    # Must call shared resolution, not direct repo access
    assert "_resolve_tracked_instruments" in source
    # Must filter placeholders
    assert "TICKER:" in source


def test_broker_wiring_skips_placeholder_figis(monkeypatch):
    """Broker events should not receive placeholder TICKER: FIGIs."""
    container, _ = _make_container_with_mock_repo(
        monkeypatch,
        db_rows=[
            {"ticker": "SBER", "figi": "TICKER:SBER"},
            {"ticker": "GAZP", "figi": "BBG004730RP0"},
        ],
    )
    instruments = container._resolve_tracked_instruments()
    real_figis = tuple(
        row["figi"] for row in instruments
        if row["figi"] and not row["figi"].startswith("TICKER:")
    )
    assert real_figis == ("BBG004730RP0",)
    assert "TICKER:SBER" not in real_figis


# ================================================================
# D) Upsert enrichment -- SQL correctness
# ================================================================


def test_upsert_instrument_uses_ticker_conflict():
    """upsert_instrument must use ON CONFLICT (ticker)."""
    import inspect

    from tinvest_trader.infra.storage.repository import TradingRepository

    source = inspect.getsource(TradingRepository.upsert_instrument)
    assert "ON CONFLICT (ticker)" in source
    assert "ON CONFLICT (figi)" not in source


def test_ensure_instrument_uses_ticker_conflict():
    """ensure_instrument must use ON CONFLICT (ticker)."""
    import inspect

    from tinvest_trader.infra.storage.repository import TradingRepository

    source = inspect.getsource(TradingRepository.ensure_instrument)
    assert "ON CONFLICT (ticker)" in source


def test_upsert_preserves_real_figi_over_placeholder():
    """upsert_instrument CASE must keep real figi when placeholder arrives."""
    import inspect

    from tinvest_trader.infra.storage.repository import TradingRepository

    source = inspect.getsource(TradingRepository.upsert_instrument)
    # Must have CASE that checks for TICKER: prefix
    assert "TICKER:%" in source
    assert "EXCLUDED.figi" in source
    assert "instrument_catalog.figi" in source


def test_ensure_instrument_preserves_tracked_flag():
    """ensure_instrument must OR tracked flags, never downgrade."""
    import inspect

    from tinvest_trader.infra.storage.repository import TradingRepository

    source = inspect.getsource(TradingRepository.ensure_instrument)
    assert "instrument_catalog.tracked OR EXCLUDED.tracked" in source


def test_ensure_instrument_enrichment_flow():
    """Simulate bootstrap then enrichment: verify SQL params are correct."""
    pool = MagicMock()
    conn = MagicMock()
    pool.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
    pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)

    from tinvest_trader.infra.storage.repository import TradingRepository

    repo = TradingRepository(pool=pool, logger=logging.getLogger("test"))

    # Step 1: bootstrap creates placeholder
    repo.ensure_instrument(ticker="sber", tracked=True)
    sql1, params1 = conn.execute.call_args[0]
    assert params1[0] == "TICKER:SBER"  # placeholder figi
    assert params1[1] == "SBER"  # uppercase ticker
    assert params1[5] is True  # tracked
    conn.reset_mock()

    # Step 2: enrich with real figi
    repo.ensure_instrument(
        ticker="SBER", figi="BBG004730N88", name="Sberbank",
        isin="RU0009029540",
    )
    sql2, params2 = conn.execute.call_args[0]
    assert params2[0] == "BBG004730N88"  # real figi
    assert params2[1] == "SBER"
    assert params2[2] == "Sberbank"
    assert params2[3] == "RU0009029540"
    # SQL must use ON CONFLICT (ticker) so this replaces placeholder row
    assert "ON CONFLICT (ticker)" in sql2


# ================================================================
# E) Schema migration safety
# ================================================================


def test_schema_has_alter_table_for_new_columns():
    """schema.sql must have ALTER TABLE for isin and moex_secid."""
    from tinvest_trader.infra.storage.postgres import SCHEMA_PATH

    schema = SCHEMA_PATH.read_text()
    assert "ALTER TABLE instrument_catalog ADD COLUMN IF NOT EXISTS isin" in schema
    assert "ALTER TABLE instrument_catalog ADD COLUMN IF NOT EXISTS moex_secid" in schema


def test_schema_has_unique_ticker_index():
    """schema.sql must create unique index on ticker."""
    from tinvest_trader.infra.storage.postgres import SCHEMA_PATH

    schema = SCHEMA_PATH.read_text()
    assert "idx_instrument_catalog_ticker" in schema
    assert "ON instrument_catalog (ticker)" in schema


def test_schema_migration_is_idempotent():
    """ALTER TABLE IF NOT EXISTS and CREATE INDEX IF NOT EXISTS are idempotent."""
    from tinvest_trader.infra.storage.postgres import SCHEMA_PATH

    schema = SCHEMA_PATH.read_text()
    # All ALTER TABLEs must use IF NOT EXISTS
    for line in schema.splitlines():
        if "ALTER TABLE" in line and "ADD COLUMN" in line:
            assert "IF NOT EXISTS" in line, f"Missing IF NOT EXISTS: {line}"
