"""Read-only cost comparison for existing virtual paper positions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from collections.abc import Iterable

    from tinvest_trader.app.config import PaperTariffComparisonConfig

_MOSCOW = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True)
class TariffProfile:
    """One counterfactual broker cost profile."""

    name: str
    commission_rate: float
    monthly_fee: float


@dataclass(frozen=True)
class TariffComparison:
    """Aggregated result for one portfolio and tariff profile."""

    portfolio_name: str
    portfolio_type: str
    tariff: str
    closed_positions: int
    active_months: int
    executed_turnover: float
    gross_pnl: float
    broker_commission: float
    slippage_cost: float
    subscription_cost: float
    net_pnl: float


def build_tariff_profiles(
    config: PaperTariffComparisonConfig,
) -> tuple[TariffProfile, ...]:
    """Build paid and fee-waived scenarios from configurable base rates."""
    return (
        TariffProfile("investor", max(0.0, config.investor_commission_rate), 0.0),
        TariffProfile(
            "trader_paid",
            max(0.0, config.trader_commission_rate),
            max(0.0, config.trader_monthly_fee),
        ),
        TariffProfile("trader_free", max(0.0, config.trader_commission_rate), 0.0),
        TariffProfile(
            "premium_paid",
            max(0.0, config.premium_commission_rate),
            max(0.0, config.premium_monthly_fee),
        ),
        TariffProfile("premium_free", max(0.0, config.premium_commission_rate), 0.0),
    )


def compare_tariffs(
    positions: Iterable[dict],
    config: PaperTariffComparisonConfig,
) -> list[TariffComparison]:
    """Reprice closed paper positions without modifying stored results."""
    grouped: dict[tuple[str, str], list[dict]] = {}
    all_positions: list[dict] = []
    for position in positions:
        key = (position["portfolio_name"], position["portfolio_type"])
        grouped.setdefault(key, []).append(position)
        all_positions.append(position)

    if all_positions:
        grouped[("all_paper", "combined")] = all_positions

    results: list[TariffComparison] = []
    slippage_rate = max(0.0, config.slippage_rate)
    for (portfolio_name, portfolio_type), items in sorted(grouped.items()):
        turnover = sum(max(0.0, float(item["notional"])) * 2 for item in items)
        gross_pnl = sum(float(item["gross_pnl"]) for item in items)
        active_months = len({_moscow_month(item["exit_time"]) for item in items})
        slippage_cost = turnover * slippage_rate
        for profile in build_tariff_profiles(config):
            broker_commission = turnover * profile.commission_rate
            subscription_cost = active_months * profile.monthly_fee
            net_pnl = (
                gross_pnl - broker_commission - slippage_cost - subscription_cost
            )
            results.append(TariffComparison(
                portfolio_name=portfolio_name,
                portfolio_type=portfolio_type,
                tariff=profile.name,
                closed_positions=len(items),
                active_months=active_months,
                executed_turnover=turnover,
                gross_pnl=gross_pnl,
                broker_commission=broker_commission,
                slippage_cost=slippage_cost,
                subscription_cost=subscription_cost,
                net_pnl=net_pnl,
            ))
    return results


def format_tariff_comparison(results: Iterable[TariffComparison]) -> str:
    """Format an operator-friendly tariff comparison report."""
    rows = list(results)
    if not rows:
        return "no closed paper positions in selected period"

    lines: list[str] = []
    current_portfolio: tuple[str, str] | None = None
    for row in rows:
        portfolio = (row.portfolio_name, row.portfolio_type)
        if portfolio != current_portfolio:
            if lines:
                lines.append("")
            lines.append(f"portfolio: {row.portfolio_name} ({row.portfolio_type})")
            lines.append(
                f"closed: {row.closed_positions} | active_months: {row.active_months} "
                f"| turnover: {row.executed_turnover:.2f} RUB "
                f"| gross_pnl: {row.gross_pnl:.2f} RUB",
            )
            current_portfolio = portfolio
        lines.append(
            f"  {row.tariff}: net={row.net_pnl:.2f} "
            f"commission={row.broker_commission:.2f} "
            f"slippage={row.slippage_cost:.2f} "
            f"subscription={row.subscription_cost:.2f} RUB",
        )
    return "\n".join(lines)


def _moscow_month(value: datetime) -> tuple[int, int]:
    if value.tzinfo is None:
        value = value.replace(tzinfo=_MOSCOW)
    local = value.astimezone(_MOSCOW)
    return local.year, local.month
