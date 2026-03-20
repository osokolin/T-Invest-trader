"""Instrument health monitoring -- detects data quality issues."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository

_STALE_THRESHOLD_DAYS = 7


@dataclass
class InstrumentIssue:
    """One instrument with its detected issues."""

    ticker: str
    issues: list[str] = field(default_factory=list)


@dataclass
class InstrumentHealthReport:
    """Summary of instrument catalog health."""

    total_tracked: int = 0
    complete: int = 0
    placeholder_figi_count: int = 0
    missing_metadata_count: int = 0
    stale_count: int = 0
    instruments_with_issues: list[InstrumentIssue] = field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        return bool(self.instruments_with_issues)


def evaluate_instrument_health(
    repository: TradingRepository,
    *,
    stale_days: int = _STALE_THRESHOLD_DAYS,
) -> InstrumentHealthReport:
    """Evaluate data quality of tracked instruments."""
    tracked = repository.list_tracked_instruments()
    now = datetime.now(tz=UTC)
    stale_cutoff = now - timedelta(days=stale_days)

    report = InstrumentHealthReport(total_tracked=len(tracked))

    for inst in tracked:
        issues: list[str] = []

        figi = inst.get("figi") or ""
        if not figi or figi.startswith("TICKER:"):
            issues.append("placeholder_figi")

        if not inst.get("instrument_uid"):
            issues.append("missing_uid")
        if not inst.get("name"):
            issues.append("missing_name")
        if not inst.get("isin"):
            issues.append("missing_isin")
        if not inst.get("moex_secid"):
            issues.append("missing_moex_secid")

        updated_at = inst.get("updated_at")
        if updated_at is not None and updated_at < stale_cutoff:
            issues.append("stale")

        if issues:
            # Categorize for summary counts
            if "placeholder_figi" in issues:
                report.placeholder_figi_count += 1
            metadata_issues = {
                "missing_uid", "missing_name", "missing_isin", "missing_moex_secid",
            }
            if set(issues) & metadata_issues:
                report.missing_metadata_count += 1
            if "stale" in issues:
                report.stale_count += 1

            report.instruments_with_issues.append(
                InstrumentIssue(ticker=inst["ticker"], issues=issues),
            )
        else:
            report.complete += 1

    return report
