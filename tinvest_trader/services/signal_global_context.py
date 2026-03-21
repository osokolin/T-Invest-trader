"""Global context -> signal enrichment service -- shadow mode only.

For each signal, attaches a recent global context snapshot, classifies
alignment (aligned/against/neutral/unknown), and computes a small
confidence adjustment. Everything is stored as shadow fields -- NO
impact on actual signal delivery, calibration, or execution.
"""

from __future__ import annotations

import json as _json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


# -- Constants --

ALIGNED = "aligned"
AGAINST = "against"
NEUTRAL = "neutral"
UNKNOWN = "unknown"

# Adjustment values (shadow only)
_ADJUSTMENT = {
    ALIGNED: 0.05,
    AGAINST: -0.10,
    NEUTRAL: 0.0,
    UNKNOWN: 0.0,
}

# Event types we consider for alignment
_ALIGNMENT_TYPES = ("risk_sentiment", "oil", "crypto")


# -- Context snapshot --

def get_recent_global_context(
    repository: TradingRepository,
    *,
    lookback_seconds: int = 900,
) -> dict[str, str]:
    """Aggregate recent global context into a direction-per-type snapshot.

    Returns e.g. {"risk_sentiment": "positive", "oil": "negative", "crypto": "unknown"}.
    Uses simple majority vote among recent events per type.
    Unknown events are ignored.
    """
    rows = repository.get_global_context_for_enrichment(
        lookback_seconds=lookback_seconds,
    )

    # Group directions by event_type
    type_directions: dict[str, list[str]] = {}
    for row in rows:
        event_type = row["event_type"]
        direction = row["direction"]
        if event_type == "unknown" or direction == "unknown":
            continue
        type_directions.setdefault(event_type, []).append(direction)

    result: dict[str, str] = {}
    for et in _ALIGNMENT_TYPES:
        dirs = type_directions.get(et, [])
        if not dirs:
            result[et] = "unknown"
            continue
        # Simple majority
        pos = dirs.count("positive")
        neg = dirs.count("negative")
        if pos > neg:
            result[et] = "positive"
        elif neg > pos:
            result[et] = "negative"
        elif pos == neg and pos > 0:
            result[et] = "neutral"
        else:
            result[et] = "unknown"

    return result


# -- Alignment classification --

def classify_global_alignment(
    signal_direction: str | None,
    global_context: dict[str, str],
) -> str:
    """Classify how global context aligns with signal direction.

    signal_direction: "up" or "down" (from signal_type)
    global_context: {"risk_sentiment": "positive", ...}

    Returns: aligned | against | neutral | unknown.

    Rules (v1, generic, no ticker-specific logic):
    - Count how many context types support vs oppose signal direction.
    - UP signal: positive context = supporting, negative = opposing
    - DOWN signal: negative context = supporting, positive = opposing
    - If more supporting than opposing -> aligned
    - If more opposing than supporting -> against
    - If equal non-zero -> neutral
    - If no usable context -> unknown
    """
    if not signal_direction or signal_direction.lower() not in ("up", "down"):
        return UNKNOWN

    if not global_context:
        return UNKNOWN

    is_up = signal_direction.lower() == "up"

    supporting = 0
    opposing = 0

    for et in _ALIGNMENT_TYPES:
        ctx_dir = global_context.get(et, "unknown")
        if ctx_dir == "unknown" or ctx_dir == "neutral":
            continue
        if is_up:
            if ctx_dir == "positive":
                supporting += 1
            elif ctx_dir == "negative":
                opposing += 1
        else:  # down
            if ctx_dir == "negative":
                supporting += 1
            elif ctx_dir == "positive":
                opposing += 1

    if supporting == 0 and opposing == 0:
        return UNKNOWN
    if supporting > opposing:
        return ALIGNED
    if opposing > supporting:
        return AGAINST
    return NEUTRAL


# -- Adjustment --

def compute_global_adjustment(alignment: str) -> float:
    """Compute shadow confidence adjustment based on alignment.

    aligned  -> +0.05
    against  -> -0.10
    neutral  ->  0.00
    unknown  ->  0.00
    """
    return _ADJUSTMENT.get(alignment, 0.0)


def compute_global_adjusted_confidence(
    original_confidence: float | None,
    adjustment: float,
) -> float | None:
    """Apply adjustment to confidence, clamped to [0, 1]."""
    if original_confidence is None:
        return None
    return max(0.0, min(1.0, round(original_confidence + adjustment, 6)))


# -- Batch enrichment --

def apply_global_context_enrichment(
    repository: TradingRepository,
    logger: logging.Logger,
    *,
    lookback_seconds: int = 900,
    limit: int = 500,
) -> int:
    """Enrich signals with global context alignment (shadow mode).

    Finds signals where global_alignment IS NULL, fetches current global
    context snapshot, classifies alignment, and stores shadow fields.

    Returns count of signals enriched.
    """
    # Get current global context snapshot
    context = get_recent_global_context(
        repository, lookback_seconds=lookback_seconds,
    )

    # Fetch unenriched signals
    signals = repository.get_unenriched_global_context_signals(limit=limit)
    if not signals:
        return 0

    context_json = _json.dumps(context, default=str)
    enriched = 0

    for signal in signals:
        signal_id = signal["id"]
        signal_type = signal.get("signal_type")
        original_confidence = signal.get("confidence")

        alignment = classify_global_alignment(signal_type, context)
        adjustment = compute_global_adjustment(alignment)
        adjusted_conf = compute_global_adjusted_confidence(
            original_confidence, adjustment,
        )

        ok = repository.update_global_context_enrichment(
            signal_id,
            global_alignment=alignment,
            global_adjustment=adjustment,
            global_adjusted_confidence=adjusted_conf,
            global_context_json=context_json,
        )
        if ok:
            enriched += 1
            logger.info(
                "global_context_enrichment",
                extra={
                    "component": "global_context",
                    "signal_id": signal_id,
                    "alignment": alignment,
                    "adjustment": adjustment,
                },
            )

    return enriched


# -- Report building --

@dataclass
class AlignmentPerformance:
    """Performance metrics for a single alignment bucket."""

    alignment: str
    count: int = 0
    resolved: int = 0
    wins: int = 0
    losses: int = 0
    avg_return: float | None = None

    @property
    def win_rate(self) -> float | None:
        if self.resolved == 0:
            return None
        return self.wins / self.resolved

    @property
    def ev(self) -> float | None:
        wr = self.win_rate
        if wr is None or self.avg_return is None:
            return None
        return wr * self.avg_return


@dataclass
class GlobalContextImpactReport:
    """Full global context impact analysis report."""

    baseline: AlignmentPerformance
    by_alignment: list[AlignmentPerformance] = field(default_factory=list)
    insights: list[str] = field(default_factory=list)


def build_global_context_impact_report(
    repository: TradingRepository,
    *,
    min_resolved: int = 0,
) -> GlobalContextImpactReport:
    """Build a report comparing performance across alignment categories."""
    # Baseline: all resolved signals
    baseline_stats = repository.get_source_weighting_baseline()
    baseline = AlignmentPerformance(
        alignment="baseline",
        count=baseline_stats.get("total", 0),
        resolved=baseline_stats.get("resolved", 0),
        wins=baseline_stats.get("wins", 0),
        losses=baseline_stats.get("losses", 0),
        avg_return=baseline_stats.get("avg_return"),
    )

    # Performance by alignment
    alignment_rows = repository.get_global_alignment_performance()
    by_alignment: list[AlignmentPerformance] = []
    for row in alignment_rows:
        resolved = row.get("resolved", 0)
        if min_resolved > 0 and resolved < min_resolved:
            continue
        by_alignment.append(AlignmentPerformance(
            alignment=row["alignment"],
            count=row.get("total", 0),
            resolved=resolved,
            wins=row.get("wins", 0),
            losses=row.get("losses", 0),
            avg_return=row.get("avg_return"),
        ))

    insights = _generate_insights(baseline, by_alignment)

    return GlobalContextImpactReport(
        baseline=baseline,
        by_alignment=by_alignment,
        insights=insights,
    )


def _generate_insights(
    baseline: AlignmentPerformance,
    by_alignment: list[AlignmentPerformance],
) -> list[str]:
    """Generate simple deterministic insights."""
    insights: list[str] = []

    b_wr = baseline.win_rate
    aligned = next((a for a in by_alignment if a.alignment == ALIGNED), None)
    against = next((a for a in by_alignment if a.alignment == AGAINST), None)

    if aligned and aligned.win_rate is not None and b_wr is not None:
        diff = (aligned.win_rate - b_wr) * 100
        if diff > 0:
            insights.append(
                f"aligned signals outperform baseline by {diff:.1f}pp win rate",
            )
        elif diff < 0:
            insights.append(
                f"aligned signals underperform baseline by {abs(diff):.1f}pp win rate",
            )

    if against and against.win_rate is not None and b_wr is not None:
        diff = (against.win_rate - b_wr) * 100
        if diff < 0:
            insights.append(
                "against signals underperform baseline by "
                f"{abs(diff):.1f}pp -- potential filter candidate",
            )
        elif diff > 0:
            insights.append(
                f"against signals surprisingly outperform baseline by {diff:.1f}pp",
            )

    if aligned and against and aligned.win_rate is not None and against.win_rate is not None:
        spread = (aligned.win_rate - against.win_rate) * 100
        if spread > 5:
            insights.append(
                f"alignment spread is {spread:.1f}pp -- global context has predictive value",
            )
        elif spread < -5:
            insights.append(
                "negative alignment spread "
                f"({spread:.1f}pp) -- global context may be counter-signal",
            )
        else:
            insights.append("alignment spread is small -- limited predictive value so far")

    enriched_count = sum(a.count for a in by_alignment)
    if enriched_count == 0:
        insights.append("no enriched signals yet -- run apply-global-context first")

    return insights


def format_global_context_impact_report(
    report: GlobalContextImpactReport,
) -> str:
    """Format the global context impact report for CLI output."""
    lines: list[str] = ["global context impact report (shadow mode)", ""]

    # Baseline
    lines.append("baseline (all resolved signals):")
    _append_perf_block(lines, report.baseline)

    # By alignment
    order = [ALIGNED, AGAINST, NEUTRAL, UNKNOWN]
    by_key = {a.alignment: a for a in report.by_alignment}
    for alignment in order:
        perf = by_key.get(alignment)
        if perf is None:
            continue
        lines.append("")
        lines.append(f"{alignment} signals:")
        _append_perf_block(lines, perf)

    # Insights
    if report.insights:
        lines.append("")
        lines.append("insights:")
        for insight in report.insights:
            lines.append(f"  - {insight}")

    return "\n".join(lines)


def _append_perf_block(
    lines: list[str], perf: AlignmentPerformance,
) -> None:
    """Append formatted performance metrics."""
    wr_str = f"{perf.win_rate * 100:.1f}%" if perf.win_rate is not None else "n/a"
    ar_str = f"{perf.avg_return:+.4f}" if perf.avg_return is not None else "n/a"
    ev_str = f"{perf.ev:+.6f}" if perf.ev is not None else "n/a"
    lines.append(f"  count: {perf.count}")
    lines.append(f"  resolved: {perf.resolved}")
    lines.append(f"  win_rate: {wr_str}")
    lines.append(f"  avg_return: {ar_str}")
    lines.append(f"  EV: {ev_str}")
