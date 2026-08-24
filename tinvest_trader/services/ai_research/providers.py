"""AI research provider abstractions."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from tinvest_trader.services.ai_research.schemas import (
    ResearchReport,
    ResearchSnapshot,
)


class AIResearchProvider(Protocol):
    """Provider interface for AI research generation."""

    def generate_report(
        self,
        snapshot: ResearchSnapshot,
        prompt: str,
        *,
        model: str,
    ) -> ResearchReport:
        """Generate a report from a curated snapshot and prompt."""


class StubAIResearchProvider:
    """Deterministic provider for tests and local dry research flows."""

    def generate_report(
        self,
        snapshot: ResearchSnapshot,
        prompt: str,
        *,
        model: str,
    ) -> ResearchReport:
        data_points = [
            name for name, value in (
                ("latest quote", snapshot.latest_quote),
                ("MOEX context", snapshot.moex_context),
                ("broker events", snapshot.broker_event_summary),
                ("sentiment", snapshot.sentiment_summary),
                ("macro", snapshot.macro_summary),
                ("signals", snapshot.signal_summary),
            )
            if value
        ]
        coverage = ", ".join(data_points) if data_points else "no optional data"
        confidence = 0.35 if data_points else None
        return ResearchReport(
            ticker=snapshot.ticker,
            created_at=datetime.now(UTC),
            model=model,
            bull_case=f"Stub bull case based on available {coverage}.",
            bear_case="Stub bear case: missing or weak data should limit conviction.",
            skeptic_notes=(
                "Stub skeptic view: this report is explanatory only and does "
                "not create a trade recommendation."
            ),
            risk_notes=(
                "Stub risk notes: verify liquidity, event timing, and source "
                "coverage before acting."
            ),
            final_summary=(
                f"Stub research summary for {snapshot.ticker}: use as an "
                "auditable explanation layer only."
            ),
            confidence=confidence,
            raw_response_json={
                "provider": "stub",
                "prompt_length": len(prompt),
                "warnings": snapshot.warnings,
            },
        )
