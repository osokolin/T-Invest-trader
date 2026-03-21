"""Source-aware weighting service -- shadow mode only.

Computes a deterministic weight for each signal source based on
historical performance (win rate, EV). The weight adjusts signal
confidence in shadow mode for retrospective analysis.

NO impact on actual pipeline execution or delivery.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


# -- Configuration --

@dataclass(frozen=True)
class SourceWeightingConfig:
    """Thresholds for source weight calculation."""

    min_resolved: int = 5          # min signals to trust source stats
    good_ev_threshold: float = 0.0   # EV above this = positive source
    good_wr_threshold: float = 0.55  # win rate above this = good source
    weak_wr_threshold: float = 0.45  # win rate below this = weak source
    weight_min: float = 0.5        # clamp floor
    weight_max: float = 1.5        # clamp ceiling


# -- Weight computation (pure, deterministic) --

@dataclass(frozen=True)
class SourceWeightResult:
    """Result of source weight calculation."""

    source_channel: str
    weight: float
    reason: str
    resolved: int = 0
    win_rate: float | None = None
    ev: float | None = None


def _compute_ev(win_rate: float, avg_return: float) -> float:
    return win_rate * avg_return


def compute_source_weight(
    source_stats: dict | None,
    config: SourceWeightingConfig | None = None,
) -> SourceWeightResult:
    """Compute a weight for a signal source based on historical performance.

    Returns weight in [config.weight_min, config.weight_max].
    Deterministic, no side effects.

    source_stats should have keys:
        source_channel, resolved, wins, avg_return
    """
    cfg = config or SourceWeightingConfig()

    if source_stats is None:
        return SourceWeightResult(
            source_channel="unknown",
            weight=1.0,
            reason="no_source_data",
        )

    source_channel = source_stats.get("source_channel", "unknown")
    resolved = source_stats.get("resolved", 0)

    if resolved < cfg.min_resolved:
        return SourceWeightResult(
            source_channel=source_channel,
            weight=1.0,
            reason="insufficient_data",
            resolved=resolved,
        )

    wins = source_stats.get("wins", 0)
    avg_return = source_stats.get("avg_return") or 0.0
    wr = wins / resolved if resolved > 0 else 0.0
    ev = _compute_ev(wr, avg_return)

    # Determine weight based on performance tiers
    weight = 1.0
    reason = "neutral"

    if ev > cfg.good_ev_threshold and wr >= cfg.good_wr_threshold:
        # Strong source: scale weight by how good the EV is
        # Base 1.1, up to 1.3 for very strong sources
        weight = 1.1 + min(ev * 10, 0.2)  # ev=0.02 -> 1.3
        reason = "positive_ev"
    elif ev > cfg.good_ev_threshold and wr >= cfg.weak_wr_threshold:
        # Mildly positive: slight boost
        weight = 1.05
        reason = "moderate_positive"
    elif ev < 0 or wr < cfg.weak_wr_threshold:
        # Weak source: penalize
        # Base 0.9, down to 0.7 for very weak sources
        weight = 0.9 + max(ev * 10, -0.2)  # ev=-0.02 -> 0.7
        reason = "negative_ev"
    else:
        weight = 1.0
        reason = "neutral"

    # Clamp
    weight = max(cfg.weight_min, min(cfg.weight_max, weight))

    return SourceWeightResult(
        source_channel=source_channel,
        weight=round(weight, 4),
        reason=reason,
        resolved=resolved,
        win_rate=round(wr, 4),
        ev=round(ev, 6),
    )


def compute_weighted_confidence(
    original_confidence: float | None,
    source_weight: float,
) -> float | None:
    """Compute shadow weighted confidence.

    Simply: original_confidence * source_weight, clamped to [0, 1].
    Returns None if original_confidence is None.
    """
    if original_confidence is None:
        return None
    return max(0.0, min(1.0, round(original_confidence * source_weight, 6)))


# -- Batch processing --

def apply_source_weights(
    repository: TradingRepository,
    logger: logging.Logger,
    *,
    config: SourceWeightingConfig | None = None,
    limit: int = 500,
) -> int:
    """Compute and store source weights for unweighted signals.

    Finds signals where source_weight IS NULL and source_channel IS NOT NULL,
    computes weights, and persists them. Shadow mode only.

    Returns count of signals updated.
    """
    cfg = config or SourceWeightingConfig()

    # Build source stats lookup
    source_stats_list = repository.get_signal_stats_by_source()
    stats_by_channel: dict[str, dict] = {
        s["source_channel"]: s for s in source_stats_list
    }

    # Fetch unweighted signals
    unweighted = repository.get_unweighted_signals(limit=limit)
    if not unweighted:
        return 0

    updated = 0
    for signal in unweighted:
        signal_id = signal["id"]
        source_channel = signal.get("source_channel")
        original_confidence = signal.get("confidence")

        # Look up source stats
        source_stats = stats_by_channel.get(source_channel) if source_channel else None

        # Compute weight
        result = compute_source_weight(source_stats, config=cfg)
        weighted_conf = compute_weighted_confidence(
            original_confidence, result.weight,
        )

        # Derive weighted severity (simple thresholds, same as signal_severity)
        weighted_sev = _derive_weighted_severity(weighted_conf)

        # Persist
        ok = repository.update_source_weight(
            signal_id,
            source_weight=result.weight,
            weighted_confidence=weighted_conf,
            weighted_severity=weighted_sev,
        )
        if ok:
            updated += 1
            logger.info(
                "source_weighting",
                extra={
                    "component": "source_weighting",
                    "signal_id": signal_id,
                    "source": source_channel or "none",
                    "weight": result.weight,
                    "reason": result.reason,
                },
            )

    return updated


def _derive_weighted_severity(weighted_confidence: float | None) -> str | None:
    """Derive a shadow severity from weighted confidence.

    Simple threshold-based: matches signal_severity scoring logic
    where confidence >= 0.6 adds +2 points (HIGH territory).
    """
    if weighted_confidence is None:
        return None
    if weighted_confidence >= 0.6:
        return "HIGH"
    if weighted_confidence >= 0.42:
        return "MEDIUM"
    return "LOW"


# -- Report building --

@dataclass
class WeightedPerformance:
    """Performance metrics for a subset of signals."""

    label: str
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
class SourceWeightSnapshot:
    """Snapshot of a source's current weight."""

    source_channel: str
    weight: float
    reason: str
    resolved: int
    win_rate: float | None
    ev: float | None


@dataclass
class SourceWeightingReport:
    """Full source weighting analysis report."""

    baseline: WeightedPerformance
    weighted: WeightedPerformance
    sources: list[SourceWeightSnapshot] = field(default_factory=list)
    insights: list[str] = field(default_factory=list)


def build_source_weighting_report(
    repository: TradingRepository,
    *,
    threshold: float = 0.6,
    min_resolved: int = 0,
    config: SourceWeightingConfig | None = None,
) -> SourceWeightingReport:
    """Build a comparative report: baseline vs weighted performance."""
    cfg = config or SourceWeightingConfig()

    # Baseline: all resolved delivered signals
    baseline_stats = repository.get_source_weighting_baseline()
    baseline = WeightedPerformance(
        label="baseline",
        count=baseline_stats.get("total", 0),
        resolved=baseline_stats.get("resolved", 0),
        wins=baseline_stats.get("wins", 0),
        losses=baseline_stats.get("losses", 0),
        avg_return=baseline_stats.get("avg_return"),
    )

    # Weighted: signals where weighted_confidence >= threshold
    weighted_stats = repository.get_weighted_performance(threshold=threshold)
    weighted = WeightedPerformance(
        label=f"weighted (threshold={threshold})",
        count=weighted_stats.get("total", 0),
        resolved=weighted_stats.get("resolved", 0),
        wins=weighted_stats.get("wins", 0),
        losses=weighted_stats.get("losses", 0),
        avg_return=weighted_stats.get("avg_return"),
    )

    # Source weight snapshots
    source_stats_list = repository.get_signal_stats_by_source()
    sources: list[SourceWeightSnapshot] = []
    for s in source_stats_list:
        resolved = s.get("resolved", 0)
        if min_resolved > 0 and resolved < min_resolved:
            continue
        result = compute_source_weight(s, config=cfg)
        sources.append(SourceWeightSnapshot(
            source_channel=result.source_channel,
            weight=result.weight,
            reason=result.reason,
            resolved=result.resolved,
            win_rate=result.win_rate,
            ev=result.ev,
        ))

    # Sort by weight descending
    sources.sort(key=lambda s: s.weight, reverse=True)

    # Generate insights
    insights = _generate_insights(baseline, weighted, sources)

    return SourceWeightingReport(
        baseline=baseline,
        weighted=weighted,
        sources=sources,
        insights=insights,
    )


def _generate_insights(
    baseline: WeightedPerformance,
    weighted: WeightedPerformance,
    sources: list[SourceWeightSnapshot],
) -> list[str]:
    """Generate simple deterministic insights."""
    insights: list[str] = []

    b_wr = baseline.win_rate
    w_wr = weighted.win_rate

    if b_wr is not None and w_wr is not None:
        if w_wr > b_wr:
            diff = (w_wr - b_wr) * 100
            insights.append(
                f"weighting improves win rate by {diff:.1f}pp",
            )
        elif w_wr < b_wr:
            diff = (b_wr - w_wr) * 100
            insights.append(
                f"weighting reduces win rate by {diff:.1f}pp",
            )
        else:
            insights.append("weighting has no effect on win rate")

    b_ev = baseline.ev
    w_ev = weighted.ev
    if b_ev is not None and w_ev is not None:
        if w_ev > b_ev:
            insights.append("weighted EV is higher than baseline")
        elif w_ev < b_ev:
            insights.append("weighted EV is lower than baseline")

    weak = [s for s in sources if s.ev is not None and s.ev < 0]
    if weak:
        names = ", ".join(s.source_channel for s in weak[:3])
        insights.append(f"weak sources with negative EV: {names}")

    strong = [s for s in sources if s.weight > 1.05]
    if strong:
        names = ", ".join(s.source_channel for s in strong[:3])
        insights.append(f"strong sources boosted: {names}")

    if weighted.count > 0 and baseline.count > 0:
        filtered_pct = (1 - weighted.count / baseline.count) * 100
        if filtered_pct > 0:
            insights.append(
                f"threshold filters out {filtered_pct:.0f}% of signals",
            )

    return insights


def format_source_weighting_report(report: SourceWeightingReport) -> str:
    """Format the source weighting report for CLI output."""
    lines: list[str] = ["source weighting report (shadow mode)", ""]

    # Baseline
    lines.append("baseline (all resolved signals):")
    _append_perf_block(lines, report.baseline)

    # Weighted
    lines.append("")
    lines.append(f"{report.weighted.label}:")
    _append_perf_block(lines, report.weighted)

    # Sources
    if report.sources:
        strong = [s for s in report.sources if s.weight > 1.0]
        weak = [s for s in report.sources if s.weight < 1.0]
        neutral = [s for s in report.sources if s.weight == 1.0]

        if strong:
            lines.append("")
            lines.append("strong sources:")
            for s in strong:
                wr_str = f"{s.win_rate * 100:.1f}%" if s.win_rate is not None else "n/a"
                ev_str = f"{s.ev:+.4f}" if s.ev is not None else "n/a"
                lines.append(
                    f"  {s.source_channel} "
                    f"weight={s.weight:.2f} "
                    f"EV={ev_str} win_rate={wr_str} "
                    f"({s.resolved} resolved)",
                )

        if weak:
            lines.append("")
            lines.append("weak sources:")
            for s in weak:
                wr_str = f"{s.win_rate * 100:.1f}%" if s.win_rate is not None else "n/a"
                ev_str = f"{s.ev:+.4f}" if s.ev is not None else "n/a"
                lines.append(
                    f"  {s.source_channel} "
                    f"weight={s.weight:.2f} "
                    f"EV={ev_str} win_rate={wr_str} "
                    f"({s.resolved} resolved)",
                )

        if neutral:
            lines.append("")
            lines.append("neutral sources:")
            for s in neutral:
                lines.append(
                    f"  {s.source_channel} "
                    f"weight={s.weight:.2f} "
                    f"({s.reason}, {s.resolved} resolved)",
                )

    # Insights
    if report.insights:
        lines.append("")
        lines.append("insights:")
        for insight in report.insights:
            lines.append(f"  - {insight}")

    return "\n".join(lines)


def _append_perf_block(lines: list[str], perf: WeightedPerformance) -> None:
    """Append formatted performance metrics."""
    wr_str = f"{perf.win_rate * 100:.1f}%" if perf.win_rate is not None else "n/a"
    ar_str = f"{perf.avg_return:+.4f}" if perf.avg_return is not None else "n/a"
    ev_str = f"{perf.ev:+.6f}" if perf.ev is not None else "n/a"
    lines.append(f"  count: {perf.count}")
    lines.append(f"  resolved: {perf.resolved}")
    lines.append(f"  win_rate: {wr_str}")
    lines.append(f"  avg_return: {ar_str}")
    lines.append(f"  EV: {ev_str}")
