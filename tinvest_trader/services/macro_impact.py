"""Macro impact analysis — measures tag/signal performance correlation.

Links macro-tagged messages to nearby signals and computes performance
metrics by tag, tag+ticker, and tag+direction. Analytics only — no
impact on signal generation, calibration, or execution.

SHADOW / READ-ONLY — measurement only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass(frozen=True)
class TagPerformance:
    """Performance metrics for a group of signals near a macro tag."""

    label: str
    total_signals: int = 0
    resolved: int = 0
    wins: int = 0
    losses: int = 0
    neutrals: int = 0
    win_rate: float | None = None
    avg_return: float | None = None

    @property
    def ev(self) -> float | None:
        return self.avg_return


@dataclass
class MacroImpactReport:
    """Full macro impact analysis report."""

    window_minutes: int = 60
    min_resolved: int = 5
    baseline: TagPerformance | None = None
    by_tag: list[TagPerformance] = field(default_factory=list)
    by_tag_ticker: list[TagPerformance] = field(default_factory=list)
    by_tag_direction: list[TagPerformance] = field(default_factory=list)
    insights: list[str] = field(default_factory=list)


def build_macro_impact_report(
    repository: TradingRepository,
    *,
    window_minutes: int = 60,
    min_resolved: int = 5,
    limit: int = 20,
) -> MacroImpactReport:
    """Build a macro impact analysis report.

    Links signals to macro messages within [window_minutes] before the signal,
    then aggregates performance by tag, tag+ticker, tag+direction.
    Only uses past macro messages (no future leakage).
    """
    report = MacroImpactReport(
        window_minutes=window_minutes,
        min_resolved=min_resolved,
    )

    # Baseline: all resolved signals
    baseline_data = repository.get_macro_impact_baseline()
    if baseline_data:
        report.baseline = _to_perf("all_signals", baseline_data)

    # By tag
    by_tag_data = repository.get_macro_impact_by_tag(
        window_minutes=window_minutes,
        min_resolved=min_resolved,
    )
    report.by_tag = [
        _to_perf(row["tag"], row) for row in by_tag_data
    ]

    # By tag + ticker
    by_tt_data = repository.get_macro_impact_by_tag_and_ticker(
        window_minutes=window_minutes,
        min_resolved=min_resolved,
        limit=limit,
    )
    report.by_tag_ticker = [
        _to_perf(f"{row['tag']} / {row['ticker']}", row)
        for row in by_tt_data
    ]

    # By tag + direction
    by_td_data = repository.get_macro_impact_by_tag_and_direction(
        window_minutes=window_minutes,
        min_resolved=min_resolved,
    )
    report.by_tag_direction = [
        _to_perf(f"{row['tag']} / {row['direction']}", row)
        for row in by_td_data
    ]

    # Generate insights
    report.insights = _generate_insights(report)

    return report


def _to_perf(label: str, row: dict) -> TagPerformance:
    """Convert a raw dict to TagPerformance."""
    resolved = row.get("resolved", 0)
    wins = row.get("wins", 0)
    wr = wins / resolved if resolved > 0 else None
    avg_ret = row.get("avg_return")
    if avg_ret is not None:
        avg_ret = float(avg_ret)

    return TagPerformance(
        label=label,
        total_signals=row.get("total_signals", 0),
        resolved=resolved,
        wins=wins,
        losses=row.get("losses", 0),
        neutrals=row.get("neutrals", 0),
        win_rate=round(wr, 4) if wr is not None else None,
        avg_return=round(avg_ret, 6) if avg_ret is not None else None,
    )


def _generate_insights(report: MacroImpactReport) -> list[str]:
    """Generate conservative, deterministic insights."""
    insights: list[str] = []
    baseline_wr = report.baseline.win_rate if report.baseline else None

    if baseline_wr is None:
        return insights

    # Tag-level insights
    for perf in report.by_tag:
        if perf.win_rate is None or perf.resolved < report.min_resolved:
            continue
        diff = perf.win_rate - baseline_wr
        if diff > 0.10:
            insights.append(
                f"{perf.label}: supportive (+{diff:.0%} vs baseline)",
            )
        elif diff < -0.10:
            insights.append(
                f"{perf.label}: headwind ({diff:+.0%} vs baseline)",
            )

    # Direction bias insights
    tag_dirs: dict[str, list[TagPerformance]] = {}
    for perf in report.by_tag_direction:
        parts = perf.label.split(" / ")
        if len(parts) == 2:
            tag_dirs.setdefault(parts[0], []).append(perf)

    for tag, perfs in tag_dirs.items():
        if len(perfs) < 2:
            continue
        by_dir = {p.label.split(" / ")[1]: p for p in perfs}
        up = by_dir.get("up")
        down = by_dir.get("down")
        if (
            up and down
            and up.win_rate is not None
            and down.win_rate is not None
            and up.resolved >= report.min_resolved
            and down.resolved >= report.min_resolved
        ):
            gap = up.win_rate - down.win_rate
            if abs(gap) > 0.15:
                bias = "bullish" if gap > 0 else "bearish"
                insights.append(f"{tag}: directional bias ({bias})")

    return insights


def format_macro_impact_report(report: MacroImpactReport) -> str:
    """Format the macro impact report for CLI output."""
    lines: list[str] = [
        f"macro impact report (window: {report.window_minutes}m, "
        f"min_resolved: {report.min_resolved})",
        "",
    ]

    # Baseline
    if report.baseline:
        lines.append("baseline (all resolved signals):")
        _append_perf(lines, report.baseline)
        lines.append("")

    # By tag
    if report.by_tag:
        lines.append("by tag:")
        for perf in report.by_tag:
            _append_perf_line(lines, perf)
        lines.append("")

    # By tag + ticker
    if report.by_tag_ticker:
        lines.append("by tag + ticker:")
        for perf in report.by_tag_ticker:
            _append_perf_line(lines, perf)
        lines.append("")

    # By tag + direction
    if report.by_tag_direction:
        lines.append("by tag + direction:")
        for perf in report.by_tag_direction:
            _append_perf_line(lines, perf)
        lines.append("")

    # Insights
    if report.insights:
        lines.append("insights:")
        for insight in report.insights:
            lines.append(f"  - {insight}")
    else:
        lines.append("insights: (not enough data for conclusions)")

    return "\n".join(lines)


def _append_perf(lines: list[str], perf: TagPerformance) -> None:
    """Append full performance block."""
    wr = f"{perf.win_rate:.1%}" if perf.win_rate is not None else "n/a"
    ar = f"{perf.avg_return:+.4f}" if perf.avg_return is not None else "n/a"
    lines.append(f"  signals: {perf.total_signals}")
    lines.append(f"  resolved: {perf.resolved}")
    lines.append(f"  wins: {perf.wins}  losses: {perf.losses}  neutrals: {perf.neutrals}")
    lines.append(f"  win_rate: {wr}  avg_return: {ar}")


def _append_perf_line(lines: list[str], perf: TagPerformance) -> None:
    """Append compact one-line performance."""
    wr = f"{perf.win_rate:.0%}" if perf.win_rate is not None else "n/a"
    ar = f"{perf.avg_return:+.4f}" if perf.avg_return is not None else "n/a"
    lines.append(
        f"  {perf.label:<25} "
        f"signals={perf.total_signals:<4} "
        f"resolved={perf.resolved:<4} "
        f"win_rate={wr:<5} "
        f"avg_return={ar}",
    )
