"""Tests for instrument health monitoring."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from tinvest_trader.cli import main
from tinvest_trader.services.instrument_health import evaluate_instrument_health


def _make_repo(instruments):
    repo = MagicMock()
    repo.list_tracked_instruments.return_value = instruments
    return repo


def _now():
    return datetime.now(tz=UTC)


# -- Health evaluation --


def test_detects_placeholder_figi():
    repo = _make_repo([
        {"ticker": "SBER", "figi": "TICKER:SBER",
         "instrument_uid": "uid", "name": "Sber", "isin": "RU123",
         "moex_secid": "SBER", "updated_at": _now()},
    ])
    report = evaluate_instrument_health(repo)
    assert report.placeholder_figi_count == 1
    assert report.complete == 0
    assert report.instruments_with_issues[0].ticker == "SBER"
    assert "placeholder_figi" in report.instruments_with_issues[0].issues


def test_detects_missing_metadata():
    repo = _make_repo([
        {"ticker": "GAZP", "figi": "BBG004730RP0",
         "instrument_uid": None, "name": "", "isin": None,
         "moex_secid": "GAZP", "updated_at": _now()},
    ])
    report = evaluate_instrument_health(repo)
    assert report.missing_metadata_count == 1
    issues = report.instruments_with_issues[0].issues
    assert "missing_uid" in issues
    assert "missing_name" in issues
    assert "missing_isin" in issues


def test_detects_stale_instrument():
    old = _now() - timedelta(days=10)
    repo = _make_repo([
        {"ticker": "LKOH", "figi": "BBG000TEST",
         "instrument_uid": "uid", "name": "Lukoil", "isin": "RU456",
         "moex_secid": "LKOH", "updated_at": old},
    ])
    report = evaluate_instrument_health(repo)
    assert report.stale_count == 1
    assert "stale" in report.instruments_with_issues[0].issues


def test_clean_report_no_issues():
    repo = _make_repo([
        {"ticker": "SBER", "figi": "BBG004730N88",
         "instrument_uid": "uid-123", "name": "Sberbank", "isin": "RU0009029540",
         "moex_secid": "SBER", "updated_at": _now()},
        {"ticker": "GAZP", "figi": "BBG004730RP0",
         "instrument_uid": "uid-456", "name": "Gazprom", "isin": "RU0007661625",
         "moex_secid": "GAZP", "updated_at": _now()},
    ])
    report = evaluate_instrument_health(repo)
    assert report.total_tracked == 2
    assert report.complete == 2
    assert report.placeholder_figi_count == 0
    assert report.missing_metadata_count == 0
    assert report.stale_count == 0
    assert not report.has_issues


def test_multiple_issues_per_instrument():
    old = _now() - timedelta(days=10)
    repo = _make_repo([
        {"ticker": "X", "figi": "TICKER:X",
         "instrument_uid": None, "name": "", "isin": None,
         "moex_secid": None, "updated_at": old},
    ])
    report = evaluate_instrument_health(repo)
    assert report.placeholder_figi_count == 1
    assert report.missing_metadata_count == 1
    assert report.stale_count == 1
    issues = report.instruments_with_issues[0].issues
    assert len(issues) >= 4  # placeholder + missing_uid + missing_name + stale


def test_custom_stale_days():
    two_days_old = _now() - timedelta(days=2)
    repo = _make_repo([
        {"ticker": "SBER", "figi": "BBG123",
         "instrument_uid": "uid", "name": "Sber", "isin": "RU",
         "moex_secid": "SBER", "updated_at": two_days_old},
    ])
    # Default 7 days: not stale
    report = evaluate_instrument_health(repo, stale_days=7)
    assert report.stale_count == 0

    # Custom 1 day: stale
    report = evaluate_instrument_health(repo, stale_days=1)
    assert report.stale_count == 1


def test_empty_tracked_set():
    repo = _make_repo([])
    report = evaluate_instrument_health(repo)
    assert report.total_tracked == 0
    assert report.complete == 0
    assert not report.has_issues


# -- CLI --


def _mock_container(repo):
    container = MagicMock()
    container.repository = repo
    container.storage_pool = MagicMock()
    return container


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_cli_instrument_health_clean(mock_config, mock_build, capsys):
    repo = _make_repo([
        {"ticker": "SBER", "figi": "BBG123",
         "instrument_uid": "uid", "name": "Sber", "isin": "RU",
         "moex_secid": "SBER", "updated_at": _now()},
    ])
    mock_build.return_value = _mock_container(repo)
    result = main(["instrument-health"])
    assert result == 0
    out = capsys.readouterr().out
    assert "tracked_total: 1" in out
    assert "complete: 1" in out


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_cli_fail_on_issues_returns_1(mock_config, mock_build, capsys):
    repo = _make_repo([
        {"ticker": "SBER", "figi": "TICKER:SBER",
         "instrument_uid": None, "name": "", "isin": None,
         "moex_secid": None, "updated_at": _now()},
    ])
    mock_build.return_value = _mock_container(repo)
    result = main(["instrument-health", "--fail-on-issues"])
    assert result == 1
    out = capsys.readouterr().out
    assert "SBER" in out


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_cli_fail_on_issues_returns_0_when_clean(mock_config, mock_build, capsys):
    repo = _make_repo([
        {"ticker": "SBER", "figi": "BBG123",
         "instrument_uid": "uid", "name": "Sber", "isin": "RU",
         "moex_secid": "SBER", "updated_at": _now()},
    ])
    mock_build.return_value = _mock_container(repo)
    result = main(["instrument-health", "--fail-on-issues"])
    assert result == 0


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_cli_no_db(mock_config, mock_build, capsys):
    container = MagicMock()
    container.repository = None
    container.storage_pool = None
    mock_build.return_value = container
    result = main(["instrument-health"])
    assert result == 1
    assert "not configured" in capsys.readouterr().out
