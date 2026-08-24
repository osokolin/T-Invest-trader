"""AI research orchestration for existing ticker context."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from tinvest_trader.services.ai_research.prompts import build_research_prompt
from tinvest_trader.services.ai_research.providers import AIResearchProvider
from tinvest_trader.services.ai_research.schemas import (
    ResearchReport,
    ResearchSnapshot,
)

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


class AIResearchConfigError(RuntimeError):
    """Raised when no AI research provider is configured."""


class UnknownTickerError(ValueError):
    """Raised when the requested ticker is not known to the repository."""


class AIResearchOrchestrator:
    """Build deterministic snapshots and persist AI research reports."""

    def __init__(
        self,
        repository: TradingRepository,
        provider: AIResearchProvider,
        logger: logging.Logger,
        *,
        model: str,
    ) -> None:
        self._repository = repository
        self._provider = provider
        self._logger = logger
        self._model = model

    def build_snapshot(self, ticker: str) -> ResearchSnapshot:
        """Build a curated research snapshot for a known ticker."""
        normalized = ticker.upper()
        instrument = self._repository.get_instrument_by_ticker(normalized)
        if instrument is None:
            raise UnknownTickerError(f"unknown ticker: {normalized}")

        warnings: list[str] = []

        latest_quote = self._optional(
            warnings, "latest quote",
            lambda: self._repository.get_latest_quote_by_ticker(normalized),
        )
        moex_context = self._optional(
            warnings, "MOEX context",
            lambda: self._repository.fetch_moex_market_context(normalized),
        )
        broker_event_summary = self._optional(
            warnings, "broker event summary",
            lambda: self._repository.get_research_broker_event_summary(normalized),
        )
        sentiment_summary = self._optional(
            warnings, "sentiment summary",
            lambda: self._repository.get_research_sentiment_summary(normalized),
        )
        macro_summary = self._optional(
            warnings, "macro summary",
            lambda: self._repository.get_research_macro_summary(normalized),
        )
        signal_summary = self._optional(
            warnings, "signal summary",
            lambda: self._repository.get_research_signal_summary(normalized),
        )

        return ResearchSnapshot(
            ticker=normalized,
            generated_at=datetime.now(UTC),
            latest_quote=latest_quote,
            moex_context=moex_context,
            broker_event_summary=broker_event_summary,
            sentiment_summary=sentiment_summary,
            macro_summary=macro_summary,
            signal_summary=signal_summary,
            warnings=warnings,
        )

    def generate_report(self, ticker: str) -> tuple[ResearchReport, ResearchSnapshot]:
        """Build a snapshot, call provider, persist, and return the report."""
        snapshot = self.build_snapshot(ticker)
        prompt = build_research_prompt(snapshot)
        report = self._provider.generate_report(snapshot, prompt, model=self._model)
        self._repository.insert_ai_research_report(report, snapshot)
        self._logger.info(
            "ai research report generated",
            extra={
                "component": "ai_research",
                "ticker": snapshot.ticker,
                "model": report.model,
            },
        )
        return report, snapshot

    def _optional(self, warnings: list[str], label: str, fn) -> dict | None:
        try:
            value = fn()
        except Exception as exc:
            self._logger.warning(
                "ai research snapshot source unavailable",
                extra={
                    "component": "ai_research",
                    "source": label,
                    "error": str(exc),
                },
            )
            warnings.append(f"{label} unavailable: {exc}")
            return None
        if not value:
            warnings.append(f"{label} missing")
            return None
        return value
