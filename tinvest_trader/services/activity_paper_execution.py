"""Causal virtual fills from persisted quotes only. No broker/execution dependency."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.app.config import ActivityPaperConfig
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass
class ExecutionProgress:
    opened: int = 0
    closed: int = 0
    expired: int = 0
    cancelled: int = 0


def validate_causal_config(config: ActivityPaperConfig) -> None:
    horizon = config.horizon
    if not (horizon.endswith("m") and horizon[:-1].isdigit() and int(horizon[:-1]) > 0):
        raise ValueError("reversion v2 requires a positive minute horizon")
    if (config.reversion_v2_quote_wait_seconds <= 0
            or config.reversion_v2_max_quote_age_seconds <= 0):
        raise ValueError("reversion v2 quote timeouts must be positive")
    other_names = {
        config.momentum_portfolio_name, config.reversion_portfolio_name,
        config.volume_confirmed_portfolio_name, config.volume_confirmed_v2_portfolio_name,
    }
    if (not config.reversion_v2_portfolio_name.strip()
            or config.reversion_v2_portfolio_name in other_names):
        raise ValueError("reversion v2 requires a separate portfolio name")
    for value in (config.commission_rate, config.slippage_rate):
        if not math.isfinite(value) or value < 0:
            raise ValueError("reversion v2 costs must be finite and nonnegative")


def causal_signal_error(candidate: dict, now: datetime) -> str | None:
    """Data availability guard, without changing the v1 signal thresholds."""
    if candidate.get("candle_interval") != "CANDLE_INTERVAL_1_MIN":
        return "causal_requires_minute_candles"
    if candidate["entry_time"] + timedelta(minutes=1) > now:
        return "causal_signal_not_closed"
    for field in ("entry_price", "price_change_pct", "score"):
        value = candidate.get(field)
        if value is None or not math.isfinite(value):
            return "causal_signal_invalid"
    if candidate["entry_price"] <= 0:
        return "causal_signal_invalid"
    return None


def process_causal_execution(repository: TradingRepository, name: str,
                             now: datetime) -> ExecutionProgress:
    """Replay durable requests, not prices that preceded the request.

    A quote received within the persisted deadline can be processed after a
    restart. Its reception time is the virtual fill time, not the old tick time
    or the time at which this worker eventually resumes.
    """
    result = ExecutionProgress()
    for request in repository.list_activity_paper_execution(name):
        pending = request["status"] == "pending"
        after = request["decision_at"] if pending else request["exit_due_at"]
        until = request["entry_deadline"] if pending else (
            after + timedelta(seconds=request["quote_wait_seconds"])
        )
        if now <= after:
            continue
        quote = repository.first_activity_execution_quote(
            request["figi"], after=after, until=until, as_of=now,
            max_age_seconds=request["max_quote_age_seconds"],
        )
        if quote is not None:
            # Validate the repository contract too; fakes and corrupt input must
            # never turn a future, stale, or invalid quote into virtual PnL.
            price = float(quote["price"])
            if not (
                math.isfinite(price) and price > 0
                and after < quote["source_time"] <= quote["fetched_at"] <= min(now, until)
                and quote["fetched_at"]-quote["source_time"] <= timedelta(
                    seconds=request["max_quote_age_seconds"],
                )
            ):
                raise ValueError("invalid causal execution quote")
            if pending:
                due = quote["fetched_at"] + timedelta(minutes=int(request["horizon"][:-1]))
                result.opened += repository.fill_activity_paper_entry(
                    request, quote, exit_due_at=due, processed_at=now,
                )
            else:
                multiplier = 1 if request["direction"] == "up" else -1
                gross = multiplier * (price / float(request["entry_price"]) - 1)
                result.closed += repository.finish_activity_paper_execution(
                    request, status="closed", reason="timed_quote_exit", now=now,
                    quote=quote, gross_return=gross,
                    costs=float(request["notional"])*float(request["cost_rate"]),
                )
        elif now >= until:
            changed = repository.finish_activity_paper_execution(
                request, status="cancelled" if pending else "expired",
                reason="entry_quote_timeout" if pending else "exit_quote_timeout", now=now,
            )
            if pending:
                result.cancelled += changed
            else:
                result.expired += changed
    return result
