"""Typed schemas for AI research reports."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


def _json_safe(value: Any) -> Any:
    """Convert common DB/Python values into JSON-serializable values."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


@dataclass(frozen=True)
class ResearchSnapshot:
    """Curated deterministic snapshot passed to AI research providers."""

    ticker: str
    generated_at: datetime
    latest_quote: dict | None = None
    moex_context: dict | None = None
    broker_event_summary: dict | None = None
    sentiment_summary: dict | None = None
    macro_summary: dict | None = None
    signal_summary: dict | None = None
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(frozen=True)
class ResearchReport:
    """AI research report generated from a curated snapshot."""

    ticker: str
    created_at: datetime
    model: str
    bull_case: str
    bear_case: str
    skeptic_notes: str
    risk_notes: str
    final_summary: str
    confidence: float | None = None
    raw_response_json: dict | None = None

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))
