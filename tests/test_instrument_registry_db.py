"""Tests for DB-backed instrument registry, bootstrap, and resolution logic."""

from unittest.mock import MagicMock

# -- Bootstrap logic (repository.bootstrap_tracked_instruments) --


def test_bootstrap_seeds_when_db_empty():
    """bootstrap_tracked_instruments should seed all tickers when count is 0."""
    repo = MagicMock()
    repo.count_tracked_instruments.return_value = 0

    # Import real function to test its logic directly
    from tinvest_trader.infra.storage.repository import TradingRepository

    # Call the real method with mocked self
    result = TradingRepository.bootstrap_tracked_instruments(repo, ("SBER", "GAZP", "YNDX"))

    assert result == 3
    assert repo.ensure_instrument.call_count == 3
    repo.ensure_instrument.assert_any_call(ticker="SBER", tracked=True)
    repo.ensure_instrument.assert_any_call(ticker="GAZP", tracked=True)
    repo.ensure_instrument.assert_any_call(ticker="YNDX", tracked=True)


def test_bootstrap_skips_when_tracked_exist():
    """bootstrap_tracked_instruments should do nothing when tracked rows exist."""
    repo = MagicMock()
    repo.count_tracked_instruments.return_value = 5

    from tinvest_trader.infra.storage.repository import TradingRepository

    result = TradingRepository.bootstrap_tracked_instruments(repo, ("SBER", "GAZP"))

    assert result == 0
    repo.ensure_instrument.assert_not_called()


def test_bootstrap_skips_empty_tickers():
    """bootstrap_tracked_instruments should skip blank ticker strings."""
    repo = MagicMock()
    repo.count_tracked_instruments.return_value = 0

    from tinvest_trader.infra.storage.repository import TradingRepository

    result = TradingRepository.bootstrap_tracked_instruments(repo, ("SBER", "", "  ", "GAZP"))

    assert result == 2
    assert repo.ensure_instrument.call_count == 2


# -- Container._resolve_tracked_tickers --


def test_resolve_prefers_db_over_env(monkeypatch):
    """When DB has tracked instruments, env tickers must be ignored."""
    monkeypatch.setenv("TINVEST_SENTIMENT_TRACKED_TICKERS", "YNDX,LKOH")

    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    config = load_config()
    container = build_container(config)

    # Inject a mock repo that returns DB tickers
    mock_repo = MagicMock()
    mock_repo.list_tracked_instruments.return_value = [
        {"ticker": "SBER"},
        {"ticker": "GAZP"},
    ]
    container.repository = mock_repo

    tickers = container._resolve_tracked_tickers()

    assert tickers == frozenset({"SBER", "GAZP"})
    # Env tickers should NOT be in the result
    assert "YNDX" not in tickers
    assert "LKOH" not in tickers


def test_resolve_bootstraps_when_db_empty(monkeypatch):
    """When DB is empty, should bootstrap from env and return seeded tickers."""
    monkeypatch.setenv("TINVEST_SENTIMENT_TRACKED_TICKERS", "SBER,GAZP")

    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    config = load_config()
    container = build_container(config)

    # First call returns empty, second returns seeded data
    mock_repo = MagicMock()
    mock_repo.list_tracked_instruments.side_effect = [
        [],  # first call: empty
        [{"ticker": "SBER"}, {"ticker": "GAZP"}],  # after bootstrap
    ]
    mock_repo.bootstrap_tracked_instruments.return_value = 2
    container.repository = mock_repo

    tickers = container._resolve_tracked_tickers()

    assert tickers == frozenset({"SBER", "GAZP"})
    mock_repo.bootstrap_tracked_instruments.assert_called_once()


def test_resolve_falls_back_to_env_without_db(monkeypatch):
    """When no DB is available, should fall back to env tickers."""
    monkeypatch.setenv("TINVEST_SENTIMENT_TRACKED_TICKERS", "SBER,GAZP")

    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    config = load_config()
    container = build_container(config)
    container.repository = None  # No DB

    tickers = container._resolve_tracked_tickers()
    assert "SBER" in tickers
    assert "GAZP" in tickers


def test_resolve_returns_empty_without_db_or_env(monkeypatch):
    """When no DB and no env tickers, should return empty set."""
    monkeypatch.delenv("TINVEST_SENTIMENT_TRACKED_TICKERS", raising=False)

    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    config = load_config()
    container = build_container(config)
    container.repository = None

    tickers = container._resolve_tracked_tickers()
    assert len(tickers) == 0


def test_resolve_handles_db_exception_gracefully(monkeypatch):
    """When DB raises an exception, should fall back to env."""
    monkeypatch.setenv("TINVEST_SENTIMENT_TRACKED_TICKERS", "SBER")

    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    config = load_config()
    container = build_container(config)

    mock_repo = MagicMock()
    mock_repo.list_tracked_instruments.side_effect = RuntimeError("db down")
    container.repository = mock_repo

    tickers = container._resolve_tracked_tickers()
    assert "SBER" in tickers


# -- Upsert enrichment (placeholder figi -> real figi) --


def test_upsert_instrument_sql_uses_ticker_conflict():
    """upsert_instrument should use ON CONFLICT (ticker), not ON CONFLICT (figi)."""
    # Read the source to verify SQL
    import inspect

    from tinvest_trader.infra.storage.repository import TradingRepository
    source = inspect.getsource(TradingRepository.upsert_instrument)
    assert "ON CONFLICT (ticker)" in source
    assert "ON CONFLICT (figi)" not in source


def test_ensure_instrument_sql_uses_ticker_conflict():
    """ensure_instrument should use ON CONFLICT (ticker)."""
    import inspect

    from tinvest_trader.infra.storage.repository import TradingRepository
    source = inspect.getsource(TradingRepository.ensure_instrument)
    assert "ON CONFLICT (ticker)" in source


def test_ensure_instrument_preserves_existing_figi():
    """ensure_instrument should not overwrite real figi with placeholder."""
    import inspect

    from tinvest_trader.infra.storage.repository import TradingRepository
    source = inspect.getsource(TradingRepository.ensure_instrument)
    # Should have CASE logic to preserve real figi
    assert "TICKER:%" in source


# -- Schema migration safety --


def test_schema_has_alter_table_for_new_columns():
    """schema.sql must have ALTER TABLE for isin and moex_secid columns."""
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


# -- Broker events wiring uses DB when available --


def test_wire_broker_events_tries_db_figis(monkeypatch):
    """Broker events pipeline should try to resolve FIGIs from DB."""
    import inspect

    from tinvest_trader.app.container import Container
    source = inspect.getsource(Container._wire_broker_events)
    assert "list_tracked_instruments" in source
    assert "TICKER:" in source  # filters out placeholder figis
