"""Tests for CLI instrument management commands."""

from unittest.mock import MagicMock, patch

from tinvest_trader.cli import main


def _mock_container(repo=None):
    """Create a mock container with optional repo."""
    container = MagicMock()
    container.repository = repo
    container.storage_pool = MagicMock() if repo else None
    container.telegram_sentiment_service = None
    container.observation_service = None
    container.broker_event_ingestion_service = None
    container.fusion_service = None
    container.cbr_ingestion_service = None
    container.moex_ingestion_service = None
    container.background_runner = None
    return container


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_list_tracked_no_db(mock_config, mock_build, capsys):
    container = _mock_container(repo=None)
    mock_build.return_value = container
    result = main(["list-tracked"])
    assert result == 0
    assert "not configured" in capsys.readouterr().out


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_list_tracked_empty(mock_config, mock_build, capsys):
    repo = MagicMock()
    repo.list_tracked_instruments.return_value = []
    container = _mock_container(repo=repo)
    mock_build.return_value = container
    result = main(["list-tracked"])
    assert result == 0
    assert "no tracked" in capsys.readouterr().out


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_list_tracked_with_data(mock_config, mock_build, capsys):
    repo = MagicMock()
    repo.list_tracked_instruments.return_value = [
        {
            "ticker": "SBER", "figi": "TICKER:SBER",
            "instrument_uid": None, "name": "Sberbank",
            "isin": "", "moex_secid": "SBER",
            "lot": None, "currency": None,
            "enabled": False, "updated_at": None,
        },
    ]
    container = _mock_container(repo=repo)
    mock_build.return_value = container
    result = main(["list-tracked"])
    assert result == 0
    out = capsys.readouterr().out
    assert "SBER" in out
    assert "tracked: 1" in out


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_list_instruments_with_data(mock_config, mock_build, capsys):
    repo = MagicMock()
    repo.list_all_instruments.return_value = [
        {
            "ticker": "SBER", "figi": "BBG004730N88",
            "instrument_uid": None, "name": "Sberbank",
            "isin": "RU0009029540", "moex_secid": "SBER",
            "lot": 1, "currency": "RUB",
            "tracked": True, "enabled": False, "updated_at": None,
        },
        {
            "ticker": "GAZP", "figi": "BBG004730RP0",
            "instrument_uid": None, "name": "Gazprom",
            "isin": "", "moex_secid": "GAZP",
            "lot": 10, "currency": "RUB",
            "tracked": False, "enabled": False, "updated_at": None,
        },
    ]
    container = _mock_container(repo=repo)
    mock_build.return_value = container
    result = main(["list-instruments"])
    assert result == 0
    out = capsys.readouterr().out
    assert "SBER" in out
    assert "GAZP" in out
    assert "total: 2" in out


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_track_command(mock_config, mock_build, capsys):
    repo = MagicMock()
    container = _mock_container(repo=repo)
    mock_build.return_value = container
    result = main(["track", "sber"])
    assert result == 0
    repo.ensure_instrument.assert_called_once_with(ticker="SBER", tracked=True)
    assert "tracked: SBER" in capsys.readouterr().out


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_untrack_command(mock_config, mock_build, capsys):
    repo = MagicMock()
    repo.set_tracked_status.return_value = True
    container = _mock_container(repo=repo)
    mock_build.return_value = container
    result = main(["untrack", "sber"])
    assert result == 0
    repo.set_tracked_status.assert_called_once_with(ticker="SBER", tracked=False)
    assert "untracked: SBER" in capsys.readouterr().out


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_untrack_not_found(mock_config, mock_build, capsys):
    repo = MagicMock()
    repo.set_tracked_status.return_value = False
    container = _mock_container(repo=repo)
    mock_build.return_value = container
    result = main(["untrack", "UNKNOWN"])
    assert result == 0
    assert "not found: UNKNOWN" in capsys.readouterr().out


@patch("tinvest_trader.cli.build_container")
@patch("tinvest_trader.cli.load_config")
def test_track_no_db(mock_config, mock_build, capsys):
    container = _mock_container(repo=None)
    mock_build.return_value = container
    result = main(["track", "SBER"])
    assert result == 1
    assert "not configured" in capsys.readouterr().out
