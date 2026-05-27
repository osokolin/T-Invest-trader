from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from tinvest_trader.cli import main
from tinvest_trader.services.ai_research.orchestrator import (
    AIResearchOrchestrator,
    UnknownTickerError,
)
from tinvest_trader.services.ai_research.prompts import build_research_prompt
from tinvest_trader.services.ai_research.providers import StubAIResearchProvider
from tinvest_trader.services.ai_research.schemas import ResearchSnapshot


def _snapshot() -> ResearchSnapshot:
    return ResearchSnapshot(
        ticker="SBER",
        generated_at=datetime(2026, 5, 27, 12, 0, tzinfo=UTC),
        latest_quote={"price": 320.5},
        warnings=["sentiment summary missing"],
    )


def _repo() -> MagicMock:
    repo = MagicMock()
    repo.get_instrument_by_ticker.return_value = {
        "ticker": "SBER",
        "figi": "FIGI1",
        "tracked": True,
    }
    repo.get_latest_quote_by_ticker.return_value = {"price": 320.5}
    repo.fetch_moex_market_context.return_value = None
    repo.get_research_broker_event_summary.return_value = None
    repo.get_research_sentiment_summary.return_value = None
    repo.get_research_macro_summary.return_value = None
    repo.get_research_signal_summary.return_value = {"total": 3}
    repo.insert_ai_research_report.return_value = 1
    return repo


def test_prompt_requires_json_and_forbids_invented_data() -> None:
    prompt = build_research_prompt(_snapshot())

    assert "Return JSON only" in prompt
    assert "Do not invent prices, news, indicators, events, volumes" in prompt
    assert "SNAPSHOT_JSON" in prompt
    assert '"ticker": "SBER"' in prompt


def test_orchestrator_builds_snapshot_with_missing_optional_warnings() -> None:
    repo = _repo()
    orchestrator = AIResearchOrchestrator(
        repository=repo,
        provider=StubAIResearchProvider(),
        logger=logging.getLogger("test"),
        model="stub-research-v1",
    )

    snapshot = orchestrator.build_snapshot("sber")

    assert snapshot.ticker == "SBER"
    assert snapshot.latest_quote == {"price": 320.5}
    assert snapshot.signal_summary == {"total": 3}
    assert "MOEX context missing" in snapshot.warnings
    assert "sentiment summary missing" in snapshot.warnings


def test_unknown_ticker_fails_clearly() -> None:
    repo = _repo()
    repo.get_instrument_by_ticker.return_value = None
    orchestrator = AIResearchOrchestrator(
        repository=repo,
        provider=StubAIResearchProvider(),
        logger=logging.getLogger("test"),
        model="stub-research-v1",
    )

    try:
        orchestrator.build_snapshot("NOPE")
    except UnknownTickerError as exc:
        assert "unknown ticker: NOPE" in str(exc)
    else:
        raise AssertionError("UnknownTickerError was not raised")


def test_stub_provider_returns_persisted_report() -> None:
    repo = _repo()
    orchestrator = AIResearchOrchestrator(
        repository=repo,
        provider=StubAIResearchProvider(),
        logger=logging.getLogger("test"),
        model="stub-research-v1",
    )

    report, snapshot = orchestrator.generate_report("SBER")

    assert report.ticker == "SBER"
    assert report.model == "stub-research-v1"
    assert report.bull_case
    repo.insert_ai_research_report.assert_called_once_with(report, snapshot)


def test_cli_analyze_ticker_prints_report(monkeypatch, capsys) -> None:
    config = SimpleNamespace(
        database=SimpleNamespace(postgres_dsn="postgresql://example"),
        sentiment=SimpleNamespace(
            enabled=True,
            channels=("MarketTwits",),
            source_backend="stub",
        ),
        observation=SimpleNamespace(enabled=True),
        background=SimpleNamespace(enabled=True),
        ai_research=SimpleNamespace(provider="", model="stub-research-v1"),
    )
    container = SimpleNamespace(
        telegram_sentiment_service=MagicMock(),
        observation_service=MagicMock(),
        background_runner=MagicMock(),
        repository=_repo(),
        storage_pool=None,
        logger=logging.getLogger("test"),
    )
    monkeypatch.setattr("tinvest_trader.cli.load_config", lambda: config)
    monkeypatch.setattr("tinvest_trader.cli.build_container", lambda cfg: container)

    exit_code = main(["analyze-ticker", "SBER", "--provider", "stub"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Ticker: SBER" in output
    assert "Confidence: 0.35" in output
    assert "Bull case:" in output
    assert "Warnings:" in output


def test_cli_analyze_ticker_fails_closed_without_provider(monkeypatch, capsys) -> None:
    config = SimpleNamespace(
        database=SimpleNamespace(postgres_dsn="postgresql://example"),
        sentiment=SimpleNamespace(
            enabled=True,
            channels=("MarketTwits",),
            source_backend="stub",
        ),
        observation=SimpleNamespace(enabled=True),
        background=SimpleNamespace(enabled=True),
        ai_research=SimpleNamespace(provider="", model="stub-research-v1"),
    )
    repo = _repo()
    container = SimpleNamespace(
        telegram_sentiment_service=MagicMock(),
        observation_service=MagicMock(),
        background_runner=MagicMock(),
        repository=repo,
        storage_pool=None,
        logger=logging.getLogger("test"),
    )
    monkeypatch.setattr("tinvest_trader.cli.load_config", lambda: config)
    monkeypatch.setattr("tinvest_trader.cli.build_container", lambda cfg: container)

    exit_code = main(["analyze-ticker", "SBER"])

    assert exit_code == 1
    assert "AI research provider is not configured" in capsys.readouterr().out
    repo.insert_ai_research_report.assert_not_called()


def test_ai_research_schema_is_idempotent() -> None:
    schema = Path("tinvest_trader/infra/storage/schema.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS ai_research_reports" in schema
    assert "CREATE INDEX IF NOT EXISTS idx_ai_research_reports_ticker_created" in schema
