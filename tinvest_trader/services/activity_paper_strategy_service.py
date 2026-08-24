"""A/B virtual portfolios for market-activity momentum and reversion.

This service has no broker client, execution engine, order, or signal dependency.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.app.config import ActivityPaperConfig
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass(frozen=True)
class ActivityPaperCycleResult:
    """Summary of one safe and idempotent A/B portfolio cycle."""

    opened: int = 0
    closed: int = 0
    skipped: int = 0
    failed_portfolios: int = 0


class ActivityPaperStrategyService:
    """Simulate constrained momentum and reversion positions after spikes."""

    def __init__(
        self,
        repository: TradingRepository,
        config: ActivityPaperConfig,
        logger: logging.Logger,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._repository = repository
        self._config = config
        self._logger = logger
        self._now_fn = now_fn or (lambda: datetime.now(UTC))

    def run_cycle(self) -> ActivityPaperCycleResult:
        """Close resolved positions, then evaluate fresh spikes for both arms."""
        now = self._normalized_now()
        opened = closed = skipped = failed = 0
        for name, strategy in self._experiments():
            try:
                portfolio = self._ensure_portfolio(name, strategy, now)
                closed += self._close_resolved(name)
                arm_opened, arm_skipped = self._open_candidates(
                    portfolio=portfolio,
                    strategy=strategy,
                    now=now,
                )
                opened += arm_opened
                skipped += arm_skipped
            except Exception:
                failed += 1
                self._logger.exception(
                    "activity paper portfolio cycle failed",
                    extra={
                        "component": "activity_paper_strategy",
                        "portfolio": name,
                        "strategy": strategy,
                    },
                )

        result = ActivityPaperCycleResult(
            opened=opened,
            closed=closed,
            skipped=skipped,
            failed_portfolios=failed,
        )
        self._logger.info(
            "activity paper strategy cycle complete",
            extra={"component": "activity_paper_strategy", **result.__dict__},
        )
        return result

    def _experiments(self) -> tuple[tuple[str, str], ...]:
        return (
            (self._config.momentum_portfolio_name, "momentum"),
            (self._config.reversion_portfolio_name, "reversion"),
        )

    def _ensure_portfolio(
        self,
        name: str,
        strategy: str,
        now: datetime,
    ) -> dict:
        portfolio = self._repository.ensure_activity_paper_portfolio(
            name=name,
            strategy=strategy,
            horizon=self._config.horizon,
            initial_cash=self._config.initial_cash,
            now=now,
        )
        if portfolio["strategy"] != strategy or portfolio["horizon"] != self._config.horizon:
            raise RuntimeError(
                f"activity paper portfolio config mismatch: {name}",
            )
        return portfolio

    def _close_resolved(self, portfolio_name: str) -> int:
        closed = 0
        total_cost_rate = 2 * max(
            0.0,
            self._config.commission_rate + self._config.slippage_rate,
        )
        for position in self._repository.list_resolved_activity_paper_positions(
            portfolio_name,
        ):
            multiplier = 1.0 if position["direction"] == "up" else -1.0
            gross_return = multiplier * position["raw_return_pct"]
            gross_pnl = position["notional"] * gross_return
            costs = position["notional"] * total_cost_rate
            net_return = gross_return - total_cost_rate
            net_pnl = gross_pnl - costs
            if self._repository.close_activity_paper_position(
                position_id=position["id"],
                exit_price=position["exit_price"],
                exit_time=position["exit_time"],
                gross_return_pct=gross_return,
                net_return_pct=net_return,
                gross_pnl=gross_pnl,
                costs=costs,
                net_pnl=net_pnl,
            ):
                closed += 1
        return closed

    def _open_candidates(
        self,
        *,
        portfolio: dict,
        strategy: str,
        now: datetime,
    ) -> tuple[int, int]:
        summary = self._repository.get_activity_paper_summary(portfolio["name"])
        if summary is None:
            raise RuntimeError(f"activity paper portfolio unavailable: {portfolio['name']}")

        open_positions = self._repository.list_open_activity_paper_positions(
            portfolio["name"],
        )
        open_by_ticker = Counter(item["ticker"] for item in open_positions)
        open_count = len(open_positions)
        available_cash = max(
            0.0,
            summary["initial_cash"] + summary["realized_pnl"]
            - summary["open_notional"],
        )
        target_notional = max(
            0.0,
            summary["initial_cash"] * self._config.position_fraction,
        )
        latest_by_ticker: dict[str, datetime] = {}
        opened = skipped = 0

        for candidate in self._repository.list_activity_paper_entry_candidates(
            portfolio["name"],
        ):
            ticker = candidate["ticker"]
            stored_latest = candidate.get("latest_entry_time")
            if stored_latest is not None:
                stored_latest = self._as_aware(stored_latest)
                current_latest = latest_by_ticker.get(ticker)
                if current_latest is None or stored_latest > current_latest:
                    latest_by_ticker[ticker] = stored_latest
            reason = self._skip_reason(
                candidate=candidate,
                now=now,
                open_count=open_count,
                open_for_ticker=open_by_ticker[ticker],
                available_cash=available_cash,
                latest_entry_time=latest_by_ticker.get(ticker),
            )
            if reason is not None:
                self._record_decision(portfolio["name"], candidate, "skip", reason, now)
                skipped += 1
                continue

            direction = self._direction(candidate["price_change_pct"], strategy)
            notional = min(target_notional, available_cash)
            position_id = self._repository.insert_activity_paper_position({
                "portfolio_name": portfolio["name"],
                "spike_id": candidate["spike_id"],
                "strategy": strategy,
                "horizon": self._config.horizon,
                "ticker": candidate["ticker"],
                "figi": candidate["figi"],
                "spike_type": candidate["spike_type"],
                "severity": candidate["severity"],
                "score": candidate["score"],
                "direction": direction,
                "entry_price": candidate["entry_price"],
                "entry_time": candidate["entry_time"],
                "notional": notional,
            })
            if position_id is None:
                continue
            self._record_decision(portfolio["name"], candidate, "enter", "eligible", now)
            opened += 1
            open_count += 1
            open_by_ticker[ticker] += 1
            latest_by_ticker[ticker] = self._as_aware(candidate["entry_time"])
            available_cash -= notional
        return opened, skipped

    def _skip_reason(
        self,
        *,
        candidate: dict,
        now: datetime,
        open_count: int,
        open_for_ticker: int,
        available_cash: float,
        latest_entry_time: datetime | None,
    ) -> str | None:
        if candidate["price_change_pct"] == 0:
            return "flat_direction"
        if candidate["score"] < self._config.min_score:
            return "score_below_minimum"
        if candidate["severity"] not in self._config.allowed_severities:
            return "severity_not_allowed"
        if candidate["spike_type"] not in self._config.allowed_spike_types:
            return "spike_type_not_allowed"
        max_age = timedelta(minutes=max(0, self._config.max_candidate_age_minutes))
        if now - self._as_aware(candidate["entry_time"]) > max_age:
            return "candidate_stale"
        if open_count >= max(0, self._config.max_open_positions):
            return "portfolio_capacity"
        if open_for_ticker >= max(0, self._config.max_open_positions_per_ticker):
            return "ticker_capacity"
        if available_cash <= 0 or self._config.position_fraction <= 0:
            return "insufficient_virtual_cash"
        if latest_entry_time is not None:
            cooldown = timedelta(minutes=max(0, self._config.cooldown_minutes))
            if self._as_aware(candidate["entry_time"]) - latest_entry_time < cooldown:
                return "ticker_cooldown"
        return None

    def _record_decision(
        self,
        portfolio_name: str,
        candidate: dict,
        decision: str,
        reason: str,
        now: datetime,
    ) -> None:
        self._repository.insert_activity_paper_decision(
            portfolio_name=portfolio_name,
            spike_id=candidate["spike_id"],
            decision=decision,
            reason=reason,
            recorded_at=now,
        )

    @staticmethod
    def _direction(price_change_pct: float, strategy: str) -> str:
        momentum = "up" if price_change_pct > 0 else "down"
        if strategy == "momentum":
            return momentum
        return "down" if momentum == "up" else "up"

    def _normalized_now(self) -> datetime:
        return self._as_aware(self._now_fn()).astimezone(UTC)

    @staticmethod
    def _as_aware(value: datetime) -> datetime:
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def format_activity_paper_summary(rows: list[dict | None]) -> str:
    """Format both A/B portfolio summaries for operational inspection."""
    available = [row for row in rows if row is not None]
    if not available:
        return "activity paper strategy has not started yet"
    lines = [
        "portfolio                  strategy   horizon  open  closed  win rate  pnl",
        "-------------------------  ---------  -------  ----  ------  --------  --------",
    ]
    for row in available:
        closed = int(row["closed_positions"])
        win_rate = row["wins"] / closed if closed else None
        win_text = f"{win_rate:>7.1%}" if win_rate is not None else "    n/a"
        lines.append(
            f"{row['name']:<25}  {row['strategy']:<9}  {row['horizon']:<7}  "
            f"{int(row['open_positions']):>4}  {closed:>6}  {win_text}  "
            f"{row['realized_pnl']:>8.2f}"
        )
    return "\n".join(lines)
