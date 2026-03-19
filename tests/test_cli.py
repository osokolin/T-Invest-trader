from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from tinvest_trader.cli import main


def _make_config() -> SimpleNamespace:
    return SimpleNamespace(
        database=SimpleNamespace(postgres_dsn="postgresql://example"),
        sentiment=SimpleNamespace(
            enabled=True,
            channels=("MarketTwits",),
            source_backend="stub",
        ),
        observation=SimpleNamespace(enabled=True),
        background=SimpleNamespace(enabled=True),
    )


def _make_container() -> SimpleNamespace:
    return SimpleNamespace(
        telegram_sentiment_service=MagicMock(),
        observation_service=MagicMock(),
        background_runner=MagicMock(),
        repository=MagicMock(),
        storage_pool=None,
    )


def test_cli_status_prints_operational_flags(monkeypatch, capsys):
    config = _make_config()
    container = _make_container()
    monkeypatch.setattr("tinvest_trader.cli.load_config", lambda: config)
    monkeypatch.setattr("tinvest_trader.cli.build_container", lambda cfg: container)

    exit_code = main(["status"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "db_configured: True" in output
    assert "sentiment_enabled: True" in output
    assert "observation_enabled: True" in output
    assert "background_enabled: True" in output
    assert "sentiment_backend: stub" in output


def test_cli_ingest_sentiment_runs_service_once(monkeypatch, capsys):
    config = _make_config()
    container = _make_container()
    container.telegram_sentiment_service.ingest_all_channels.return_value = 5
    monkeypatch.setattr("tinvest_trader.cli.load_config", lambda: config)
    monkeypatch.setattr("tinvest_trader.cli.build_container", lambda cfg: container)

    exit_code = main(["ingest-sentiment"])

    assert exit_code == 0
    container.telegram_sentiment_service.ingest_all_channels.assert_called_once_with(
        ("MarketTwits",),
    )
    assert "sentiment_processed: 5" in capsys.readouterr().out


def test_cli_observe_runs_service_once(monkeypatch, capsys):
    config = _make_config()
    container = _make_container()
    container.observation_service.observe_all.return_value = [object(), object(), object()]
    monkeypatch.setattr("tinvest_trader.cli.load_config", lambda: config)
    monkeypatch.setattr("tinvest_trader.cli.build_container", lambda cfg: container)

    exit_code = main(["observe"])

    assert exit_code == 0
    container.observation_service.observe_all.assert_called_once_with()
    assert "observations_generated: 3" in capsys.readouterr().out


def test_cli_db_summary_prints_counts(monkeypatch, capsys):
    config = _make_config()
    container = _make_container()
    container.repository.fetch_operational_summary.return_value = {
        "telegram_messages_raw": 10,
        "telegram_message_mentions": 12,
        "telegram_sentiment_events": 14,
        "signal_observations": 16,
        "market_snapshots": 18,
    }
    monkeypatch.setattr("tinvest_trader.cli.load_config", lambda: config)
    monkeypatch.setattr("tinvest_trader.cli.build_container", lambda cfg: container)

    exit_code = main(["db-summary"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "telegram_messages_raw: 10" in output
    assert "telegram_message_mentions: 12" in output
    assert "telegram_sentiment_events: 14" in output
    assert "signal_observations: 16" in output
    assert "market_snapshots: 18" in output


def test_cli_db_summary_handles_missing_database(monkeypatch, capsys):
    config = _make_config()
    container = _make_container()
    container.repository = None
    monkeypatch.setattr("tinvest_trader.cli.load_config", lambda: config)
    monkeypatch.setattr("tinvest_trader.cli.build_container", lambda cfg: container)

    exit_code = main(["db-summary"])

    assert exit_code == 0
    assert "database is not configured" in capsys.readouterr().out


def test_cli_closes_storage_pool(monkeypatch):
    config = _make_config()
    container = _make_container()
    container.storage_pool = MagicMock()
    monkeypatch.setattr("tinvest_trader.cli.load_config", lambda: config)
    monkeypatch.setattr("tinvest_trader.cli.build_container", lambda cfg: container)

    exit_code = main(["status"])

    assert exit_code == 0
    container.storage_pool.close.assert_called_once_with()
