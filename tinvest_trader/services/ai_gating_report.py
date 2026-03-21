"""AI gating shadow-mode report -- compares baseline vs AI-filtered outcomes.

Answers: "If AI gating was active, would results improve?"
Measurement only. No execution impact.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass(frozen=True)
class GatingGroupStats:
    """Outcome stats for a group of signals (baseline, filtered, blocked)."""

    label: str
    total: int = 0
    resolved: int = 0
    wins: int = 0
    losses: int = 0
    neutrals: int = 0
    avg_return: float | None = None

    @property
    def win_rate(self) -> float | None:
        if self.resolved == 0:
            return None
        return self.wins / self.resolved

    @property
    def ev(self) -> float | None:
        if self.win_rate is None or self.avg_return is None:
            return None
        return self.win_rate * self.avg_return


@dataclass(frozen=True)
class AIGatingReport:
    """Shadow-mode AI gating comparison report."""

    total_signals: int = 0
    total_with_gate: int = 0
    blocked_count: int = 0
    caution_count: int = 0
    allow_count: int = 0
    baseline: GatingGroupStats = field(
        default_factory=lambda: GatingGroupStats("baseline"),
    )
    ai_filtered: GatingGroupStats = field(
        default_factory=lambda: GatingGroupStats("ai_filtered"),
    )
    blocked: GatingGroupStats = field(
        default_factory=lambda: GatingGroupStats("blocked"),
    )

    @property
    def blocked_pct(self) -> float | None:
        if self.total_with_gate == 0:
            return None
        return self.blocked_count / self.total_with_gate


def build_ai_gating_report(
    repository: TradingRepository,
    *,
    min_resolved: int = 0,
) -> AIGatingReport:
    """Build gating report from repository data."""
    stats = repository.get_ai_gating_stats()
    if not stats:
        return AIGatingReport()

    perf = repository.get_ai_gating_performance()
    if not perf:
        return AIGatingReport(
            total_signals=stats.get("total_signals", 0),
            total_with_gate=stats.get("total_with_gate", 0),
            blocked_count=stats.get("blocked", 0),
            caution_count=stats.get("caution", 0),
            allow_count=stats.get("allow", 0),
        )

    def _build_group(label: str, data: dict | None) -> GatingGroupStats:
        if data is None:
            return GatingGroupStats(label)
        return GatingGroupStats(
            label=label,
            total=data.get("total", 0),
            resolved=data.get("resolved", 0),
            wins=data.get("wins", 0),
            losses=data.get("losses", 0),
            neutrals=data.get("neutrals", 0),
            avg_return=data.get("avg_return"),
        )

    baseline = _build_group("baseline", perf.get("baseline"))
    ai_filtered = _build_group("ai_filtered", perf.get("ai_filtered"))
    blocked = _build_group("blocked", perf.get("blocked"))

    return AIGatingReport(
        total_signals=stats.get("total_signals", 0),
        total_with_gate=stats.get("total_with_gate", 0),
        blocked_count=stats.get("blocked", 0),
        caution_count=stats.get("caution", 0),
        allow_count=stats.get("allow", 0),
        baseline=baseline,
        ai_filtered=ai_filtered,
        blocked=blocked,
    )


# -- Formatter --


def _pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.0%}"


def _ret(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.4f}"


def _ev_str(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.4f}"


def _format_group(g: GatingGroupStats) -> list[str]:
    return [
        f"  win_rate: {_pct(g.win_rate)}",
        f"  avg_return: {_ret(g.avg_return)}",
        f"  EV: {_ev_str(g.ev)}",
        f"  (resolved={g.resolved}, wins={g.wins}, losses={g.losses})",
    ]


def format_ai_gating_report(report: AIGatingReport) -> str:
    """Format the gating report for CLI output."""
    lines: list[str] = []

    lines.append("AI gating report (SHADOW MODE)")
    lines.append(f"  total signals: {report.total_signals}")
    lines.append(f"  with gate decision: {report.total_with_gate}")
    lines.append(
        f"  blocked: {report.blocked_count} ({_pct(report.blocked_pct)})",
    )
    lines.append(f"  caution: {report.caution_count}")
    lines.append(f"  allow: {report.allow_count}")
    lines.append("")

    if report.baseline.resolved == 0 and report.ai_filtered.resolved == 0:
        lines.append("  no resolved signals yet")
        return "\n".join(lines)

    lines.append("Baseline (all delivered):")
    lines.extend(_format_group(report.baseline))
    lines.append("")

    lines.append("AI-filtered (BLOCK removed):")
    lines.extend(_format_group(report.ai_filtered))
    lines.append("")

    lines.append("Blocked signals:")
    lines.extend(_format_group(report.blocked))

    # -- Insights --
    insights: list[str] = []

    b_wr = report.baseline.win_rate
    f_wr = report.ai_filtered.win_rate
    bl_wr = report.blocked.win_rate

    if (
        b_wr is not None
        and f_wr is not None
        and report.ai_filtered.resolved >= 3
    ):
        delta = f_wr - b_wr
        if delta > 0.02:
            insights.append(
                f"  + AI filtering improves win rate by "
                f"{delta:+.0%} ({_pct(b_wr)} -> {_pct(f_wr)})",
            )
        elif delta < -0.02:
            insights.append(
                f"  - AI filtering reduces win rate by "
                f"{delta:+.0%} ({_pct(b_wr)} -> {_pct(f_wr)})",
            )

    if (
        report.blocked.resolved >= 3
        and report.blocked.avg_return is not None
        and report.blocked.avg_return < 0
    ):
        insights.append(
            f"  + AI BLOCK removes negative-EV signals "
            f"(avg_return={_ret(report.blocked.avg_return)})",
        )

    if (
        bl_wr is not None
        and report.blocked.resolved >= 3
        and bl_wr > 0.5
    ):
        insights.append(
            f"  ! Blocked signals have {bl_wr:.0%} win rate "
            f"-- some profitable signals are filtered out (trade-off)",
        )

    if insights:
        lines.append("")
        lines.append("Insights:")
        lines.extend(insights)

    return "\n".join(lines)
