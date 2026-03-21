"""Telegram source performance attribution service."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass
class SourceStats:
    """Performance stats for a single Telegram source."""

    source_channel: str
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
        wr = self.win_rate
        if wr is None or self.avg_return is None:
            return None
        return wr * self.avg_return


@dataclass
class SourceTickerStats:
    """Performance stats for a source + ticker combination."""

    source_channel: str
    ticker: str
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
        wr = self.win_rate
        if wr is None or self.avg_return is None:
            return None
        return wr * self.avg_return


@dataclass
class SourcePerformanceReport:
    """Full source performance attribution report."""

    by_source: list[SourceStats] = field(default_factory=list)
    by_source_ticker: list[SourceTickerStats] = field(default_factory=list)


def build_source_performance_report(
    repository: TradingRepository,
) -> SourcePerformanceReport:
    """Build source performance report from DB aggregates."""
    by_source_raw = repository.get_signal_stats_by_source()
    by_source = [
        SourceStats(
            source_channel=r["source_channel"],
            total=r["total"],
            resolved=r["resolved"],
            wins=r["wins"],
            losses=r["losses"],
            neutrals=r["neutrals"],
            avg_return=r["avg_return"],
        )
        for r in by_source_raw
    ]

    by_st_raw = repository.get_signal_stats_by_source_and_ticker()
    by_source_ticker = [
        SourceTickerStats(
            source_channel=r["source_channel"],
            ticker=r["ticker"],
            total=r["total"],
            resolved=r["resolved"],
            wins=r["wins"],
            losses=r["losses"],
            neutrals=r["neutrals"],
            avg_return=r["avg_return"],
        )
        for r in by_st_raw
    ]

    return SourcePerformanceReport(
        by_source=by_source,
        by_source_ticker=by_source_ticker,
    )


def format_source_performance_report(
    report: SourcePerformanceReport,
    min_resolved: int = 0,
) -> str:
    """Format the source performance report for CLI output."""
    lines: list[str] = ["telegram source performance", ""]

    sources = report.by_source
    if min_resolved > 0:
        sources = [s for s in sources if s.resolved >= min_resolved]

    if not sources:
        lines.append("no source-attributed signals found")
        return "\n".join(lines)

    # Sort by EV descending (None last)
    sources = sorted(
        sources,
        key=lambda s: s.ev if s.ev is not None else float("-inf"),
        reverse=True,
    )

    lines.append("sources:")
    for s in sources:
        wr = f"{s.win_rate * 100:.1f}%" if s.win_rate is not None else "n/a"
        ar = f"{s.avg_return:+.4f}" if s.avg_return is not None else "n/a"
        ev = f"{s.ev:+.4f}" if s.ev is not None else "n/a"
        lines.append(
            f"  {s.source_channel}: "
            f"total={s.total} resolved={s.resolved} "
            f"win_rate={wr} avg_return={ar} EV={ev}",
        )

    # Best source/ticker combos (top 5 by EV)
    st_with_ev = [
        st for st in report.by_source_ticker
        if st.ev is not None and st.resolved >= max(min_resolved, 1)
    ]
    if st_with_ev:
        best = sorted(
            st_with_ev,
            key=lambda s: s.ev if s.ev is not None else float("-inf"),
            reverse=True,
        )[:5]
        lines.append("")
        lines.append("best source/ticker combos:")
        for st in best:
            wr = f"{st.win_rate * 100:.1f}%" if st.win_rate is not None else "n/a"
            ev = f"{st.ev:+.4f}" if st.ev is not None else "n/a"
            lines.append(
                f"  {st.source_channel} / {st.ticker}: "
                f"resolved={st.resolved} win_rate={wr} EV={ev}",
            )

    # Weak sources (negative EV or win_rate < 50%)
    weak = [
        s for s in sources
        if s.ev is not None and s.ev < 0
    ]
    if weak:
        lines.append("")
        lines.append("weak sources:")
        for s in weak:
            ev = f"{s.ev:+.4f}" if s.ev is not None else "n/a"
            lines.append(f"  {s.source_channel} (EV={ev})")

    return "\n".join(lines)
