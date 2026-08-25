"""Read-only operational report and informational readiness assessment."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.app.config import AppConfig, OperationalReadinessConfig
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass(frozen=True)
class SourceFreshness:
    name: str
    latest_at: datetime | None
    age_minutes: int | None
    required: bool
    fresh: bool | None


@dataclass
class OperationalReport:
    generated_at: datetime
    status: str
    execution_allowed: bool = False
    sources: list[SourceFreshness] = field(default_factory=list)
    paper_portfolio: dict | None = None
    activity_portfolios: list[dict] = field(default_factory=list)
    activity_decisions: dict[str, int] = field(default_factory=dict)
    activity_skip_reasons: list[dict] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    collecting_reasons: list[str] = field(default_factory=list)


_CONTINUOUS_SOURCES = {
    "telegram",
    "quotes",
    "market_activity",
    "global_market_data",
}


def build_operational_report(
    repository: TradingRepository,
    app_config: AppConfig,
    readiness_config: OperationalReadinessConfig,
    *,
    lookback_hours: int = 24,
    now: datetime | None = None,
) -> OperationalReport:
    """Collect current operational state and evaluate review readiness."""
    current_time = now or datetime.now(UTC)
    if not readiness_config.enabled:
        return OperationalReport(generated_at=current_time, status="DISABLED")

    raw = repository.get_operational_readiness_data(
        lookback_hours=lookback_hours,
    )
    enabled_sources = _enabled_sources(app_config)
    latest_by_source = raw.get("source_latest_at", {})
    sources = [
        _source_freshness(
            name=name,
            latest_at=latest_by_source.get(name),
            required=name in _CONTINUOUS_SOURCES,
            max_age_minutes=readiness_config.max_data_age_minutes,
            now=current_time,
        )
        for name in enabled_sources
    ]

    paper_summary = None
    if app_config.paper_portfolio.enabled:
        paper_summary = repository.get_paper_portfolio_summary(
            app_config.paper_portfolio.name,
        )

    activity_summaries = []
    if app_config.activity_paper.enabled:
        for name in _activity_portfolio_names(app_config):
            summary = repository.get_activity_paper_summary(name)
            if summary is not None:
                activity_summaries.append(summary)

    blockers: list[str] = []
    collecting: list[str] = []

    if not app_config.background.enabled:
        blockers.append("background runner is disabled")

    if app_config.paper_portfolio.enabled:
        if not app_config.signal_generation.enabled:
            blockers.append("signal generation is disabled")
        if paper_summary is None:
            collecting.append("primary paper portfolio is not initialized")
        else:
            _evaluate_paper_performance(
                paper_summary,
                readiness_config,
                blockers,
                collecting,
            )
    else:
        collecting.append("primary paper portfolio is disabled")

    for source in sources:
        if not source.required:
            continue
        if source.latest_at is None:
            blockers.append(f"{source.name} has no data")
        elif source.fresh is False:
            blockers.append(
                f"{source.name} data is stale ({source.age_minutes}m)",
            )

    if blockers:
        status = "NOT_READY"
    elif collecting:
        status = "COLLECTING"
    else:
        status = "READY_FOR_REVIEW"

    return OperationalReport(
        generated_at=current_time,
        status=status,
        sources=sources,
        paper_portfolio=paper_summary,
        activity_portfolios=activity_summaries,
        activity_decisions=raw.get("activity_decisions", {}),
        activity_skip_reasons=raw.get("activity_skip_reasons", []),
        blockers=blockers,
        collecting_reasons=collecting,
    )


def format_operational_report(
    report: OperationalReport,
    *,
    compact: bool = False,
) -> str:
    """Format an operator-friendly readiness report."""
    lines = [
        f"Operational Readiness: {report.status}",
        "Real orders: BLOCKED (informational review only)",
    ]
    if report.status == "DISABLED":
        return "\n".join(lines)

    if report.sources:
        rendered_sources = []
        for source in report.sources:
            age = "no data" if source.age_minutes is None else f"{source.age_minutes}m"
            if source.fresh is True:
                state = "OK"
            elif source.required:
                state = "STALE"
            else:
                state = "INFO"
            rendered_sources.append(f"{source.name} {age} {state}")
        lines.append("Data: " + " | ".join(rendered_sources))

    if report.paper_portfolio:
        paper = report.paper_portfolio
        closed = int(paper.get("closed_positions", 0))
        wins = int(paper.get("wins", 0))
        win_rate = wins / closed if closed else None
        win_rate_text = "n/a" if win_rate is None else f"{win_rate:.0%}"
        lines.append(
            "Paper "
            f"{paper['name']}: {paper.get('open_positions', 0)} open / "
            f"{closed} closed | PnL {paper.get('realized_pnl', 0.0):.2f} | "
            f"WR {win_rate_text}",
        )

    decisions = report.activity_decisions
    if decisions:
        lines.append(
            "Activity 24h: "
            f"enter {decisions.get('enter', 0)} | skip {decisions.get('skip', 0)}",
        )
    if report.activity_skip_reasons:
        reasons = ", ".join(
            f"{item['reason']}={item['count']}"
            for item in report.activity_skip_reasons
        )
        lines.append(f"Top skips: {reasons}")

    issues = report.blockers or report.collecting_reasons
    if issues:
        label = "Blockers" if report.blockers else "Waiting"
        limit = 2 if compact else len(issues)
        lines.append(f"{label}: " + "; ".join(issues[:limit]))

    if not compact and report.activity_portfolios:
        lines.append("Activity portfolios:")
        for portfolio in report.activity_portfolios:
            closed = int(portfolio.get("closed_positions", 0))
            wins = int(portfolio.get("wins", 0))
            win_rate = wins / closed if closed else None
            win_rate_text = "n/a" if win_rate is None else f"{win_rate:.0%}"
            lines.append(
                f"  {portfolio['name']}: {portfolio.get('open_positions', 0)} open / "
                f"{closed} closed | PnL "
                f"{portfolio.get('realized_pnl', 0.0):.2f} | WR {win_rate_text}",
            )

    return "\n".join(lines)


def _enabled_sources(config: AppConfig) -> list[str]:
    flags = (
        ("telegram", config.sentiment.enabled),
        ("quotes", config.quote_sync.enabled),
        ("market_activity", config.market_activity.enabled),
        ("broker_events", config.broker_events.enabled),
        ("cbr", config.cbr.enabled),
        ("moex", config.moex.enabled),
        ("global_context", config.global_context.enabled),
        ("global_market_data", config.global_market_data.enabled),
    )
    return [name for name, enabled in flags if enabled]


def _activity_portfolio_names(config: AppConfig) -> list[str]:
    names = [
        config.activity_paper.momentum_portfolio_name,
        config.activity_paper.reversion_portfolio_name,
    ]
    if config.activity_paper.volume_confirmed_enabled:
        names.append(config.activity_paper.volume_confirmed_portfolio_name)
    return names


def _source_freshness(
    *,
    name: str,
    latest_at: datetime | None,
    required: bool,
    max_age_minutes: int,
    now: datetime,
) -> SourceFreshness:
    if latest_at is None:
        return SourceFreshness(name, None, None, required, None)
    if latest_at.tzinfo is None:
        latest_at = latest_at.replace(tzinfo=UTC)
    age_minutes = max(0, int((now - latest_at).total_seconds() / 60))
    return SourceFreshness(
        name=name,
        latest_at=latest_at,
        age_minutes=age_minutes,
        required=required,
        fresh=age_minutes <= max_age_minutes,
    )


def _evaluate_paper_performance(
    summary: dict,
    config: OperationalReadinessConfig,
    blockers: list[str],
    collecting: list[str],
) -> None:
    closed = int(summary.get("closed_positions", 0))
    if closed < config.min_closed_positions:
        collecting.append(
            f"paper sample {closed}/{config.min_closed_positions} closed positions",
        )
        return

    wins = int(summary.get("wins", 0))
    win_rate = wins / closed
    if win_rate < config.min_win_rate:
        blockers.append(
            f"paper win rate {win_rate:.1%} below {config.min_win_rate:.1%}",
        )

    avg_return = summary.get("avg_net_return_pct")
    if avg_return is None:
        blockers.append("paper average net return is unavailable")
    elif float(avg_return) < config.min_avg_net_return_pct:
        blockers.append(
            "paper average net return "
            f"{float(avg_return):.2%} below "
            f"{config.min_avg_net_return_pct:.2%}",
        )
