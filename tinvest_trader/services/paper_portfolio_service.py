"""Persistent virtual portfolio for measuring live shadow-trading performance.

The service only reads resolved signal predictions and writes paper portfolio
records. It does not depend on the broker client, execution engine, or orders.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.app.config import PaperPortfolioConfig
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass(frozen=True)
class PaperPortfolioCycleResult:
    """Counts produced by one idempotent virtual portfolio cycle."""

    opened: int = 0
    closed: int = 0
    expired: int = 0
    skipped_capacity: int = 0


class PaperPortfolioService:
    """Open and close virtual positions from the existing signal lifecycle."""

    def __init__(
        self,
        repository: TradingRepository,
        config: PaperPortfolioConfig,
        logger: logging.Logger,
    ) -> None:
        self._repository = repository
        self._config = config
        self._logger = logger

    def run_cycle(self, now: datetime | None = None) -> PaperPortfolioCycleResult:
        """Close resolved positions, then open eligible new virtual positions."""
        if now is None:
            now = datetime.now(UTC)
        self._repository.ensure_paper_portfolio(
            name=self._config.name,
            initial_cash=self._config.initial_cash,
        )
        closed = self._close_resolved_positions()
        expired = self._expire_unresolved_positions(now)
        opened, skipped_capacity = self._open_new_positions()
        result = PaperPortfolioCycleResult(
            opened=opened,
            closed=closed,
            expired=expired,
            skipped_capacity=skipped_capacity,
        )
        self._logger.info(
            "paper portfolio cycle complete",
            extra={
                "component": "paper_portfolio",
                "portfolio": self._config.name,
                "opened": result.opened,
                "closed": result.closed,
                "expired": result.expired,
                "skipped_capacity": result.skipped_capacity,
            },
        )
        return result

    def _close_resolved_positions(self) -> int:
        closed = 0
        total_cost_rate = 2 * max(
            0.0,
            self._config.commission_rate + self._config.slippage_rate,
        )
        for position in self._repository.list_resolved_open_paper_positions(
            self._config.name,
        ):
            direction_multiplier = 1.0 if position["direction"] == "up" else -1.0
            gross_return_pct = direction_multiplier * position["return_pct"]
            gross_pnl = position["notional"] * gross_return_pct
            costs = position["notional"] * total_cost_rate
            net_pnl = gross_pnl - costs
            net_return_pct = gross_return_pct - total_cost_rate
            if self._repository.close_paper_position(
                position_id=position["id"],
                exit_price=position["exit_price"],
                exit_time=position["resolved_at"],
                gross_return_pct=gross_return_pct,
                net_return_pct=net_return_pct,
                gross_pnl=gross_pnl,
                costs=costs,
                net_pnl=net_pnl,
            ):
                closed += 1
        return closed

    def _expire_unresolved_positions(self, now: datetime) -> int:
        expiry_minutes = max(
            0,
            self._config.unresolved_position_expiry_minutes,
        )
        if expiry_minutes == 0:
            return 0
        return self._repository.expire_stale_paper_positions(
            portfolio_name=self._config.name,
            before=now - timedelta(minutes=expiry_minutes),
            expired_at=now,
        )

    def _open_new_positions(self) -> tuple[int, int]:
        summary = self._repository.get_paper_portfolio_summary(self._config.name)
        if summary is None:
            raise RuntimeError(
                f"paper portfolio is unavailable: {self._config.name}",
            )

        open_positions = int(summary["open_positions"])
        available_cash = max(
            0.0,
            summary["initial_cash"] + summary["realized_pnl"]
            - summary["open_notional"],
        )
        target_notional = max(
            0.0,
            summary["initial_cash"] * self._config.position_fraction,
        )
        max_positions = max(0, self._config.max_open_positions)
        opened = 0
        skipped_capacity = 0

        for candidate in self._repository.list_paper_entry_candidates(
            self._config.name,
            self._config.entry_stages,
        ):
            if open_positions >= max_positions or available_cash <= 0:
                skipped_capacity += 1
                continue

            notional = min(target_notional, available_cash)
            if notional <= 0:
                skipped_capacity += 1
                continue

            position_id = self._repository.insert_paper_position(
                portfolio_name=self._config.name,
                prediction_id=candidate["id"],
                ticker=candidate["ticker"],
                direction=candidate["direction"],
                entry_price=candidate["entry_price"],
                entry_time=candidate["entry_time"],
                notional=notional,
            )
            if position_id is None:
                continue

            opened += 1
            open_positions += 1
            available_cash -= notional

        return opened, skipped_capacity


def format_paper_portfolio_summary(summary: dict | None) -> str:
    """Format a concise, realized-only summary for the operational CLI."""
    if summary is None:
        return "paper portfolio has not started yet"

    closed = summary["closed_positions"]
    wins = summary["wins"]
    win_rate = wins / closed if closed else None
    realized_equity = summary["initial_cash"] + summary["realized_pnl"]
    average_return = summary["avg_net_return_pct"]

    lines = [
        f"paper_portfolio: {summary['name']}",
        f"started_at: {summary['started_at']}",
        f"initial_cash: {summary['initial_cash']:.2f} {summary['currency']}",
        f"realized_equity: {realized_equity:.2f} {summary['currency']}",
        f"realized_pnl: {summary['realized_pnl']:.2f} {summary['currency']}",
        f"open_positions: {summary['open_positions']}",
        f"open_notional: {summary['open_notional']:.2f} {summary['currency']}",
        f"closed_positions: {closed}",
        f"expired_positions: {summary.get('expired_positions', 0)}",
        f"win_rate: {win_rate:.1%}" if win_rate is not None else "win_rate: n/a",
        (
            f"avg_net_return: {average_return:.3%}"
            if average_return is not None
            else "avg_net_return: n/a"
        ),
    ]
    return "\n".join(lines)
