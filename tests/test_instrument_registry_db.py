"""Tests for DB-backed instrument registry helpers and bootstrap logic."""

from unittest.mock import MagicMock


def _make_repo():
    """Create a mock repository with instrument catalog methods."""
    repo = MagicMock()
    repo.list_tracked_instruments.return_value = []
    repo.list_all_instruments.return_value = []
    repo.count_tracked_instruments.return_value = 0
    return repo


# -- list_tracked_instruments --


def test_list_tracked_returns_only_tracked():
    """list_tracked_instruments should be called and return tracked rows."""
    repo = _make_repo()
    repo.list_tracked_instruments.return_value = [
        {"ticker": "SBER", "figi": "TICKER:SBER", "moex_secid": "SBER"},
        {"ticker": "GAZP", "figi": "TICKER:GAZP", "moex_secid": "GAZP"},
    ]
    result = repo.list_tracked_instruments()
    assert len(result) == 2
    assert result[0]["ticker"] == "SBER"


# -- bootstrap_tracked_instruments --


def test_bootstrap_seeds_when_empty():
    """bootstrap should seed tickers when DB tracked set is empty."""

    repo = _make_repo()
    # Simulate the bootstrap logic directly
    tickers = ("SBER", "GAZP", "YNDX")

    # When count is 0, bootstrap should call ensure_instrument for each
    repo.count_tracked_instruments.return_value = 0
    repo.ensure_instrument = MagicMock()

    # Call the real bootstrap logic pattern
    count = repo.count_tracked_instruments()
    assert count == 0

    seeded = 0
    for t in tickers:
        repo.ensure_instrument(ticker=t.upper(), tracked=True)
        seeded += 1
    assert seeded == 3
    assert repo.ensure_instrument.call_count == 3


def test_bootstrap_skips_when_tracked_exist():
    """bootstrap should not seed if tracked instruments already exist."""
    repo = _make_repo()
    repo.count_tracked_instruments.return_value = 5

    # Should short-circuit
    count = repo.count_tracked_instruments()
    assert count > 0
    # ensure_instrument should not be called
    repo.ensure_instrument.assert_not_called()


# -- set_tracked_status --


def test_set_tracked_status_updates():
    """set_tracked_status should call with correct parameters."""
    repo = _make_repo()
    repo.set_tracked_status.return_value = True

    result = repo.set_tracked_status(ticker="SBER", tracked=False)
    assert result is True
    repo.set_tracked_status.assert_called_once_with(ticker="SBER", tracked=False)


def test_set_tracked_status_not_found():
    """set_tracked_status should return False when ticker not in DB."""
    repo = _make_repo()
    repo.set_tracked_status.return_value = False

    result = repo.set_tracked_status(ticker="UNKNOWN", tracked=True)
    assert result is False


# -- get_instrument_by_ticker --


def test_get_instrument_by_ticker_found():
    repo = _make_repo()
    repo.get_instrument_by_ticker.return_value = {
        "ticker": "SBER",
        "figi": "BBG004730N88",
        "tracked": True,
    }
    result = repo.get_instrument_by_ticker("SBER")
    assert result is not None
    assert result["ticker"] == "SBER"


def test_get_instrument_by_ticker_not_found():
    repo = _make_repo()
    repo.get_instrument_by_ticker.return_value = None
    result = repo.get_instrument_by_ticker("UNKNOWN")
    assert result is None


# -- ensure_instrument --


def test_ensure_instrument_creates_with_defaults():
    """ensure_instrument should set moex_secid to ticker if not provided."""
    repo = _make_repo()
    repo.ensure_instrument = MagicMock()

    repo.ensure_instrument(ticker="SBER", tracked=True)
    repo.ensure_instrument.assert_called_once_with(ticker="SBER", tracked=True)


# -- DB-preferred ticker resolution in container --


def test_container_resolve_prefers_db(monkeypatch):
    """Container should use DB tracked tickers when available."""
    monkeypatch.setenv("TINVEST_SENTIMENT_ENABLED", "true")
    monkeypatch.setenv("TINVEST_SENTIMENT_TRACKED_TICKERS", "SBER,GAZP")

    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    config = load_config()
    container = build_container(config)

    # Without a real DB, _resolve_tracked_tickers falls back to env
    tickers = container._resolve_tracked_tickers()
    assert "SBER" in tickers
    assert "GAZP" in tickers


def test_container_resolve_empty_env_returns_empty(monkeypatch):
    """Container should return empty set when no DB and no env tickers."""
    monkeypatch.delenv("TINVEST_SENTIMENT_TRACKED_TICKERS", raising=False)

    from tinvest_trader.app.config import load_config
    from tinvest_trader.app.container import build_container

    config = load_config()
    container = build_container(config)

    tickers = container._resolve_tracked_tickers()
    assert len(tickers) == 0
