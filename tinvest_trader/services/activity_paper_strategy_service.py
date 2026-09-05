"""A/B/C virtual portfolios for market-activity experiments.

This service has no broker client, execution engine, order, or signal dependency.
"""

from __future__ import annotations

import logging
import math
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from tinvest_trader.app.config import ActivityPaperConfig
    from tinvest_trader.infra.storage.repository import TradingRepository

_MOSCOW = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True)
class ActivityPaperCycleResult:
    """Summary of one safe and idempotent virtual portfolio cycle."""

    opened: int = 0
    closed: int = 0
    expired: int = 0
    skipped: int = 0
    deferred: int = 0
    failed_portfolios: int = 0


class ActivityPaperStrategyService:
    """Simulate constrained activity-spike experiments without broker orders."""

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
        if config.strict_entries_enabled:
            self._validate_strict_config()

    def _validate_strict_config(self) -> None:
        horizon = self._config.horizon
        if not (horizon.endswith("m") and horizon[:-1].isdigit() and int(horizon[:-1]) > 0):
            raise ValueError("activity paper strict entries require a positive minute horizon")
        for name, value in vars(self._config).items():
            if (
                name.startswith("strict_") and not isinstance(value, bool)
                and (not math.isfinite(value) or value < 0)
            ):
                raise ValueError(f"activity paper {name} must be finite and nonnegative")
        if not 0 < self._config.strict_min_confirmation_move_pct <= (
            self._config.strict_max_confirmation_move_pct
        ):
            raise ValueError("activity paper strict confirmation range is invalid")
        if self._config.strict_min_cost_multiple < 1:
            raise ValueError("activity paper strict cost multiple must be at least 1")
        for rate in (self._config.commission_rate, self._config.slippage_rate):
            if not math.isfinite(rate) or rate < 0:
                raise ValueError("activity paper costs must be finite and nonnegative")

    def _strict_entries(self, strategy: str) -> bool:
        return self._config.strict_entries_enabled and strategy in {
            "momentum", "volume_confirmed_v2",
        }

    def run_cycle(self) -> ActivityPaperCycleResult:
        """Close resolved positions, then evaluate fresh spikes for every arm."""
        now = self._normalized_now()
        opened = closed = expired = skipped = deferred = failed = 0
        for name, strategy in self._experiments():
            try:
                portfolio = self._ensure_portfolio(name, strategy, now)
                closed += self._close_resolved(name)
                expired += self._expire_unresolved(name, now)
                arm_opened, arm_skipped, arm_deferred = self._open_candidates(
                    portfolio=portfolio,
                    strategy=strategy,
                    now=now,
                )
                opened += arm_opened
                skipped += arm_skipped
                deferred += arm_deferred
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
            expired=expired,
            skipped=skipped,
            deferred=deferred,
            failed_portfolios=failed,
        )
        self._logger.info(
            "activity paper strategy cycle complete",
            extra={"component": "activity_paper_strategy", **result.__dict__},
        )
        return result

    def _experiments(self) -> tuple[tuple[str, str], ...]:
        experiments = [
            (self._config.momentum_portfolio_name, "momentum"),
            (self._config.reversion_portfolio_name, "reversion"),
        ]
        if self._config.volume_confirmed_enabled:
            experiments.append((
                self._config.volume_confirmed_portfolio_name,
                "volume_confirmed",
            ))
        if self._config.volume_confirmed_v2_enabled:
            experiments.append((
                self._config.volume_confirmed_v2_portfolio_name,
                "volume_confirmed_v2",
            ))
        return tuple(experiments)

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

    def _expire_unresolved(self, portfolio_name: str, now: datetime) -> int:
        expiry_minutes = max(
            0,
            self._config.unresolved_position_expiry_minutes,
        )
        if expiry_minutes == 0:
            return 0
        return self._repository.expire_stale_activity_paper_positions(
            portfolio_name=portfolio_name,
            before=now - timedelta(minutes=expiry_minutes),
            expired_at=now,
        )

    def _open_candidates(
        self,
        *,
        portfolio: dict,
        strategy: str,
        now: datetime,
    ) -> tuple[int, int, int]:
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
        entries_today = 0
        if strategy == "volume_confirmed_v2" or self._strict_entries(strategy):
            entries_today = self._repository.count_activity_paper_entries_since(
                portfolio["name"],
                self._moscow_day_start(now),
            )
        opened = skipped = deferred = 0

        for raw_candidate in self._repository.list_activity_paper_entry_candidates(
            portfolio["name"],
        ):
            ticker = raw_candidate["ticker"]
            stored_latest = raw_candidate.get("latest_entry_time")
            if stored_latest is not None:
                stored_latest = self._as_aware(stored_latest)
                current_latest = latest_by_ticker.get(ticker)
                if current_latest is None or stored_latest > current_latest:
                    latest_by_ticker[ticker] = stored_latest

            reason = self._quality_skip_reason(raw_candidate, strategy)
            if reason is not None:
                self._record_decision(
                    portfolio["name"], raw_candidate, "skip", reason, now,
                )
                skipped += 1
                continue

            candidate, confirmation_reason = self._prepare_candidate(
                candidate=raw_candidate,
                strategy=strategy,
                now=now,
            )
            if confirmation_reason == "confirmation_pending":
                deferred += 1
                continue
            if confirmation_reason is not None:
                self._record_decision(
                    portfolio["name"], raw_candidate, "skip",
                    confirmation_reason, now,
                )
                skipped += 1
                continue
            if candidate is None:
                raise RuntimeError("activity paper candidate preparation failed")

            reason = self._risk_skip_reason(
                candidate=candidate,
                strategy=strategy,
                now=now,
                open_count=open_count,
                open_for_ticker=open_by_ticker[ticker],
                available_cash=available_cash,
                latest_entry_time=latest_by_ticker.get(ticker),
                entries_today=entries_today,
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
            entry_reason = "strict_eligible" if self._strict_entries(strategy) else "eligible"
            self._record_decision(portfolio["name"], candidate, "enter", entry_reason, now)
            opened += 1
            open_count += 1
            open_by_ticker[ticker] += 1
            latest_by_ticker[ticker] = self._as_aware(candidate["entry_time"])
            available_cash -= notional
            entries_today += 1
        return opened, skipped, deferred

    def _quality_skip_reason(self, candidate: dict, strategy: str) -> str | None:
        if self._strict_entries(strategy):
            reason = self._strict_quality_skip_reason(candidate, strategy)
            if reason is not None:
                return reason
        if candidate["score"] < self._config.min_score:
            return "score_below_minimum"
        if candidate["severity"] not in self._config.allowed_severities:
            return "severity_not_allowed"
        if strategy == "volume_confirmed_v2":
            if candidate["spike_type"] != "volume":
                return "spike_type_not_volume"
            if candidate["severity"] != "high":
                return "v2_severity_below_high"
            if candidate["score"] < self._config.volume_confirmed_v2_min_score:
                return "v2_score_below_minimum"
            volume_ratio = candidate.get("volume_ratio")
            if volume_ratio is None:
                return "v2_volume_ratio_unavailable"
            if volume_ratio < self._config.volume_confirmed_v2_min_volume_ratio:
                return "v2_volume_ratio_below_minimum"
            return None
        if strategy == "volume_confirmed":
            if candidate["spike_type"] != "volume":
                return "spike_type_not_volume"
            return None
        if candidate["price_change_pct"] == 0:
            return "flat_direction"
        if candidate["spike_type"] not in self._config.allowed_spike_types:
            return "spike_type_not_allowed"
        return None

    def _strict_quality_skip_reason(self, candidate: dict, strategy: str) -> str | None:
        for name in ("score", "entry_price", "price_change_pct", "volume_ratio"):
            value = candidate.get(name)
            if value is None or not math.isfinite(value):
                return "strict_market_data_unavailable"
        if candidate["entry_price"] <= 0:
            return "strict_market_data_unavailable"
        if candidate.get("candle_interval") != "CANDLE_INTERVAL_1_MIN":
            return "strict_requires_minute_candles"
        expected_type = "volume_price" if strategy == "momentum" else "volume"
        if candidate["spike_type"] != expected_type:
            return "strict_spike_type_not_allowed"
        if candidate["severity"] != "high":
            return "strict_severity_below_high"
        if candidate["score"] < self._config.strict_min_score:
            return "strict_score_below_minimum"
        if candidate["volume_ratio"] < self._config.strict_min_volume_ratio:
            return "strict_volume_ratio_below_minimum"
        if candidate["price_change_pct"] == 0:
            return "strict_flat_spike"
        if abs(candidate["price_change_pct"]) > self._config.strict_max_spike_move_pct:
            return "strict_spike_overextended"
        return None

    def _prepare_candidate(
        self,
        *,
        candidate: dict,
        strategy: str,
        now: datetime,
    ) -> tuple[dict | None, str | None]:
        if self._strict_entries(strategy):
            return self._prepare_strict_candidate(candidate, strategy, now)
        if strategy not in {"volume_confirmed", "volume_confirmed_v2"}:
            return candidate, None

        confirmation_time = candidate.get("confirmation_time")
        if confirmation_time is None:
            max_age = timedelta(minutes=max(
                0,
                self._config.max_candidate_age_minutes,
            ))
            if now - self._as_aware(candidate["entry_time"]) > max_age:
                return None, "confirmation_missing"
            return None, "confirmation_pending"

        spike_time = self._as_aware(candidate["entry_time"])
        confirmation_time = self._as_aware(confirmation_time)
        if strategy == "volume_confirmed_v2":
            max_delay_minutes = self._config.volume_confirmed_v2_max_delay_minutes
            min_move_pct = self._config.volume_confirmed_v2_min_move_pct
        else:
            max_delay_minutes = self._config.volume_confirmation_max_delay_minutes
            min_move_pct = self._config.volume_confirmation_min_move_pct
        max_delay = timedelta(minutes=max(0, max_delay_minutes))
        if confirmation_time <= spike_time or confirmation_time - spike_time > max_delay:
            return None, "confirmation_too_late"

        confirmation_price = candidate.get("confirmation_price")
        confirmation_move = candidate.get("confirmation_move_pct")
        if confirmation_price is None or confirmation_move is None:
            return None, "confirmation_unavailable"
        min_move = max(0.0, min_move_pct)
        if abs(confirmation_move) < min_move:
            return None, "confirmation_move_below_minimum"

        return {
            **candidate,
            "entry_time": confirmation_time,
            "entry_price": confirmation_price,
            "price_change_pct": confirmation_move,
        }, None

    def _prepare_strict_candidate(
        self, candidate: dict, strategy: str, now: datetime,
    ) -> tuple[dict | None, str | None]:
        spike_time = self._as_aware(candidate["entry_time"])
        max_delay = self._config.strict_confirmation_max_delay_minutes
        if strategy == "volume_confirmed_v2":
            max_delay = min(max_delay, self._config.volume_confirmed_v2_max_delay_minutes)
        confirmation_time = candidate.get("confirmation_time")
        if confirmation_time is None:
            deadline = spike_time + timedelta(minutes=max_delay + 1)
            reason = "strict_confirmation_missing" if now > deadline else "confirmation_pending"
            return None, reason
        confirmation_time = self._as_aware(confirmation_time)
        if not spike_time < confirmation_time <= spike_time + timedelta(minutes=max_delay):
            return None, "strict_confirmation_too_late"

        # T-Bank candle timestamps denote the open; a close is usable one minute later.
        entry_time = confirmation_time + timedelta(minutes=1)
        if entry_time > now:
            return None, "confirmation_pending"
        horizon = self._config.horizon
        if horizon.endswith("m") and entry_time >= spike_time + timedelta(
            minutes=int(horizon[:-1]),
        ):
            return None, "strict_confirmation_after_horizon"
        price = candidate.get("confirmation_price")
        if price is None or not math.isfinite(price) or price <= 0:
            return None, "strict_confirmation_unavailable"
        move = price / candidate["entry_price"] - 1
        if not math.isfinite(move):
            return None, "strict_confirmation_unavailable"
        if move * candidate["price_change_pct"] <= 0:
            return None, "strict_confirmation_direction_mismatch"
        minimum = self._config.strict_min_confirmation_move_pct
        if strategy == "volume_confirmed_v2":
            minimum = max(minimum, self._config.volume_confirmed_v2_min_move_pct)
        if abs(move) + 1e-12 < minimum:
            return None, "strict_confirmation_move_below_minimum"
        if abs(move) > self._config.strict_max_confirmation_move_pct + 1e-12:
            return None, "strict_confirmation_overextended"
        cost_floor = 2 * (
            self._config.commission_rate + self._config.slippage_rate
        ) * self._config.strict_min_cost_multiple
        if not math.isfinite(cost_floor) or abs(move) + 1e-12 < cost_floor:
            return None, "strict_confirmation_below_cost_floor"
        return {
            **candidate,
            "entry_time": entry_time,
            "entry_price": price,
            "price_change_pct": move,
        }, None

    def _risk_skip_reason(
        self,
        *,
        candidate: dict,
        strategy: str,
        now: datetime,
        open_count: int,
        open_for_ticker: int,
        available_cash: float,
        latest_entry_time: datetime | None,
        entries_today: int,
    ) -> str | None:
        age_minutes = self._config.max_candidate_age_minutes
        if self._strict_entries(strategy):
            age_minutes = min(age_minutes, self._config.strict_max_entry_age_minutes)
            entry_time = self._as_aware(candidate["entry_time"])
            if entry_time.astimezone(_MOSCOW).date() != now.astimezone(_MOSCOW).date():
                return "strict_entry_day_mismatch"
            daily_limit = self._config.strict_max_entries_per_day
            if strategy == "volume_confirmed_v2":
                daily_limit = min(
                    daily_limit, self._config.volume_confirmed_v2_max_entries_per_day,
                )
            if entries_today >= daily_limit:
                return "strict_daily_entry_limit"
        max_age = timedelta(minutes=max(0, age_minutes))
        if now - self._as_aware(candidate["entry_time"]) > max_age:
            return "candidate_stale"
        if strategy == "volume_confirmed_v2" and entries_today >= max(
            0,
            self._config.volume_confirmed_v2_max_entries_per_day,
        ):
            return "v2_daily_entry_limit"
        if open_count >= max(0, self._config.max_open_positions):
            return "portfolio_capacity"
        if open_for_ticker >= max(0, self._config.max_open_positions_per_ticker):
            return "ticker_capacity"
        if available_cash <= 0 or self._config.position_fraction <= 0:
            return "insufficient_virtual_cash"
        if latest_entry_time is not None:
            cooldown_minutes = (
                self._config.volume_confirmed_v2_cooldown_minutes
                if strategy == "volume_confirmed_v2"
                else self._config.cooldown_minutes
            )
            if self._strict_entries(strategy):
                cooldown_minutes = max(cooldown_minutes, self._config.strict_cooldown_minutes)
            cooldown = timedelta(minutes=max(0, cooldown_minutes))
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
        if strategy in {"momentum", "volume_confirmed", "volume_confirmed_v2"}:
            return momentum
        if strategy == "reversion":
            return "down" if momentum == "up" else "up"
        raise ValueError(f"unsupported activity paper strategy: {strategy}")

    def _normalized_now(self) -> datetime:
        return self._as_aware(self._now_fn()).astimezone(UTC)

    @staticmethod
    def _moscow_day_start(now: datetime) -> datetime:
        return now.astimezone(_MOSCOW).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ).astimezone(UTC)

    @staticmethod
    def _as_aware(value: datetime) -> datetime:
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def format_activity_paper_summary(rows: list[dict | None]) -> str:
    """Format enabled virtual portfolio summaries for operational inspection."""
    available = [row for row in rows if row is not None]
    if not available:
        return "activity paper strategy has not started yet"
    lines = [
        "portfolio                       strategy          horizon  "
        "open  closed  expired  win rate  pnl",
        "------------------------------  ----------------  -------  "
        "----  ------  -------  --------  --------",
    ]
    for row in available:
        closed = int(row["closed_positions"])
        win_rate = row["wins"] / closed if closed else None
        win_text = f"{win_rate:>7.1%}" if win_rate is not None else "    n/a"
        lines.append(
            f"{row['name']:<30}  {row['strategy']:<16}  {row['horizon']:<7}  "
            f"{int(row['open_positions']):>4}  {closed:>6}  "
            f"{int(row.get('expired_positions', 0)):>7}  {win_text}  "
            f"{row['realized_pnl']:>8.2f}"
        )
    return "\n".join(lines)


def format_activity_paper_direction_summary(rows: list[dict]) -> str:
    """Show separate cohorts so tightening entries does not hide old results."""
    lines = [
        "By entry policy and direction (all time, virtual only):",
        "portfolio                       policy  side   closed  win rate     costs   net pnl",
    ]
    for row in rows:
        closed = row["closed_positions"]
        win_text = f"{row['wins'] / closed:>7.1%}" if closed else "    n/a"
        lines.append(
            f"{row['portfolio_name']:<30}  {row['entry_policy']:<6}  {row['side']:<5}  "
            f"{closed:>6}  {win_text}  {row['costs']:>8.2f}  {row['net_pnl']:>8.2f}"
        )
    return "\n".join(lines)
