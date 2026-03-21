"""Signal divergence tracking -- measures funnel conversion and leakage.

Computes how signals flow through the pipeline stages and where they
are filtered out. Highlights rejected signals that would have been
profitable (edge leakage).

No ML. No adaptive tuning. Pure measurement and reporting.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


# -- Pipeline stage constants --

STAGE_GENERATED = "generated"
STAGE_REJECTED_CALIBRATION = "rejected_calibration"
STAGE_REJECTED_BINDING = "rejected_binding"
STAGE_REJECTED_SAFETY = "rejected_safety"
STAGE_DELIVERED = "delivered"

REJECTION_STAGES = (
    STAGE_REJECTED_CALIBRATION,
    STAGE_REJECTED_BINDING,
    STAGE_REJECTED_SAFETY,
)

ALL_STAGES = (
    STAGE_GENERATED,
    STAGE_REJECTED_CALIBRATION,
    STAGE_REJECTED_BINDING,
    STAGE_REJECTED_SAFETY,
    STAGE_DELIVERED,
)


@dataclass(frozen=True)
class StageStats:
    """Performance stats for a single pipeline stage."""

    stage: str
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
    def loss_rate(self) -> float | None:
        if self.resolved == 0:
            return None
        return self.losses / self.resolved


@dataclass(frozen=True)
class DivergenceReport:
    """Full funnel divergence report."""

    total: int = 0
    generated: int = 0
    rejected_calibration: int = 0
    rejected_binding: int = 0
    rejected_safety: int = 0
    delivered: int = 0
    untracked: int = 0
    by_stage: list[StageStats] = field(default_factory=list)


def build_divergence_report(
    repository: TradingRepository,
) -> DivergenceReport:
    """Build a divergence report from the database."""
    counts = repository.get_divergence_stats()
    if not counts:
        return DivergenceReport()

    by_stage_raw = repository.get_divergence_stats_by_stage()
    by_stage = [
        StageStats(
            stage=row["stage"],
            total=row["total"],
            resolved=row["resolved"],
            wins=row["wins"],
            losses=row["losses"],
            neutrals=row["neutrals"],
            avg_return=row["avg_return"],
        )
        for row in by_stage_raw
    ]

    return DivergenceReport(
        total=counts.get("total", 0),
        generated=counts.get("generated", 0),
        rejected_calibration=counts.get("rejected_calibration", 0),
        rejected_binding=counts.get("rejected_binding", 0),
        rejected_safety=counts.get("rejected_safety", 0),
        delivered=counts.get("delivered", 0),
        untracked=counts.get("untracked", 0),
        by_stage=by_stage,
    )


def _pct(part: int, whole: int) -> str:
    """Format percentage string."""
    if whole == 0:
        return "n/a"
    return f"{part / whole:.0%}"


def _win_rate_str(stats: StageStats) -> str:
    wr = stats.win_rate
    if wr is None:
        return "n/a"
    return f"{wr:.0%}"


def _avg_return_str(stats: StageStats) -> str:
    if stats.avg_return is None:
        return "n/a"
    return f"{stats.avg_return:+.4f}"


def format_divergence_report(report: DivergenceReport) -> str:
    """Format the divergence report for CLI output."""
    lines: list[str] = []

    # -- Funnel --
    tracked = report.total - report.untracked
    lines.append("Signal funnel:")
    lines.append(f"  total signals: {report.total}")
    if report.untracked > 0:
        lines.append(f"  untracked (pre-v1): {report.untracked}")
    lines.append(f"  tracked: {tracked}")
    lines.append("")

    # Funnel breakdown
    gen = report.generated + report.rejected_calibration + \
        report.rejected_binding + report.rejected_safety + report.delivered
    if gen == 0:
        lines.append("  no tracked signals yet")
        return "\n".join(lines)

    after_cal = gen - report.rejected_calibration
    after_bind = after_cal - report.rejected_binding
    after_safety = after_bind - report.rejected_safety

    lines.append(f"  generated:          {gen}")
    lines.append(
        f"  after calibration:  {after_cal} ({_pct(after_cal, gen)})",
    )
    lines.append(
        f"  after binding:      {after_bind} ({_pct(after_bind, after_cal)})"
        if after_cal > 0
        else f"  after binding:      {after_bind}",
    )
    lines.append(
        f"  after safety:       {after_safety} ({_pct(after_safety, after_bind)})"
        if after_bind > 0
        else f"  after safety:       {after_safety}",
    )
    lines.append(
        f"  delivered:          {report.delivered} ({_pct(report.delivered, after_safety)})"
        if after_safety > 0
        else f"  delivered:          {report.delivered}",
    )

    # Stage-only signals (stuck at 'generated' = not yet processed)
    still_generated = report.generated
    if still_generated > 0:
        lines.append(f"  pending (generated): {still_generated}")

    # -- Win rates per stage --
    lines.append("")
    lines.append("Win rates by stage:")

    stage_map = {s.stage: s for s in report.by_stage}

    for stage_name in ALL_STAGES:
        stats = stage_map.get(stage_name)
        if stats is None or stats.resolved == 0:
            continue
        wr = _win_rate_str(stats)
        avg_r = _avg_return_str(stats)
        lines.append(
            f"  {stage_name:<25} "
            f"win={wr:<6} avg_return={avg_r:<10} "
            f"(n={stats.resolved})",
        )

    # -- Insights --
    delivered_stats = stage_map.get(STAGE_DELIVERED)
    insights: list[str] = []

    for rej_stage in REJECTION_STAGES:
        rej_stats = stage_map.get(rej_stage)
        if rej_stats is None or rej_stats.resolved < 3:
            continue
        if delivered_stats is None or delivered_stats.resolved < 3:
            continue

        rej_wr = rej_stats.win_rate or 0.0
        del_wr = delivered_stats.win_rate or 0.0

        if rej_wr > del_wr + 0.05:
            insights.append(
                f"  ! {rej_stage} rejects signals with HIGHER win rate "
                f"({rej_wr:.0%}) than delivered ({del_wr:.0%})",
            )

        if (
            rej_stats.avg_return is not None
            and delivered_stats.avg_return is not None
            and rej_stats.avg_return > delivered_stats.avg_return
        ):
            insights.append(
                f"  ! {rej_stage} rejects signals with HIGHER avg return "
                f"({rej_stats.avg_return:+.4f}) than delivered "
                f"({delivered_stats.avg_return:+.4f})",
            )

    if insights:
        lines.append("")
        lines.append("Insights:")
        lines.extend(insights)

    return "\n".join(lines)
