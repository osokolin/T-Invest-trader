"""Daily long-only medium-term virtual strategy experiments.

The service reads stored MOEX daily bars and writes only dedicated paper
records. It has no broker client, order, execution, or signal-pipeline access.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.app.config import MediumTermPaperConfig
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass(frozen=True)
class MediumTermSignal:
    eligible: bool
    reason: str
    atr: float | None
    metrics: dict[str, float | int | str]


@dataclass(frozen=True)
class MediumTermPaperCycleResult:
    opened: int = 0
    closed: int = 0
    stop_updates: int = 0
    skipped: int = 0
    failed_portfolios: int = 0


class MediumTermPaperStrategyService:
    """Run staircase, ATR, and hybrid daily virtual portfolios."""

    def __init__(
        self,
        repository: TradingRepository,
        config: MediumTermPaperConfig,
        logger: logging.Logger,
        tracked_tickers: tuple[str, ...],
    ) -> None:
        self._repository = repository
        self._config = config
        self._logger = logger
        self._tracked_tickers = tuple(sorted({item.upper() for item in tracked_tickers}))

    def run_cycle(self) -> MediumTermPaperCycleResult:
        """Advance open positions and evaluate one next-day entry per ticker."""
        opened = closed = stop_updates = skipped = failed = 0
        for portfolio_name, strategy in self._experiments():
            try:
                portfolio = self._ensure_portfolio(portfolio_name, strategy)
                arm_closed, arm_updates = self._advance_open_positions(portfolio_name)
                arm_opened, arm_skipped = self._open_candidates(portfolio, strategy)
                opened += arm_opened
                closed += arm_closed
                stop_updates += arm_updates
                skipped += arm_skipped
            except Exception:
                failed += 1
                self._logger.exception(
                    "medium-term paper portfolio cycle failed",
                    extra={
                        "component": "medium_term_paper",
                        "portfolio": portfolio_name,
                        "strategy": strategy,
                    },
                )
        result = MediumTermPaperCycleResult(
            opened=opened,
            closed=closed,
            stop_updates=stop_updates,
            skipped=skipped,
            failed_portfolios=failed,
        )
        self._logger.info(
            "medium-term paper strategy cycle complete",
            extra={"component": "medium_term_paper", **result.__dict__},
        )
        return result

    def _experiments(self) -> tuple[tuple[str, str], ...]:
        return (
            (self._config.staircase_portfolio_name, "staircase"),
            (self._config.atr_portfolio_name, "atr"),
            (self._config.hybrid_portfolio_name, "hybrid"),
        )

    def _ensure_portfolio(self, name: str, strategy: str) -> dict:
        portfolio = self._repository.ensure_medium_term_paper_portfolio(
            name=name,
            strategy=strategy,
            initial_cash=self._config.initial_cash,
        )
        if portfolio["strategy"] != strategy:
            raise RuntimeError(f"medium-term portfolio strategy mismatch: {name}")
        return portfolio

    def _advance_open_positions(self, portfolio_name: str) -> tuple[int, int]:
        closed = stop_updates = 0
        history_limit = max(
            self._config.history_bars,
            self._config.max_holding_sessions + self._config.atr_period + 5,
        )
        for position in self._repository.list_open_medium_term_positions(
            portfolio_name,
        ):
            bars = self._repository.list_moex_daily_history(
                position["ticker"], history_limit,
            )
            for index, bar in enumerate(bars):
                if bar["trade_date"] <= position["last_evaluated_date"]:
                    continue
                did_close, did_update = self._advance_position(
                    position=position,
                    bars=bars,
                    bar_index=index,
                )
                stop_updates += did_update
                if did_close:
                    closed += 1
                    break
        return closed, stop_updates

    def _advance_position(
        self,
        *,
        position: dict,
        bars: list[dict],
        bar_index: int,
    ) -> tuple[bool, int]:
        bar = bars[bar_index]
        stop = position["current_stop"]
        held_sessions = position["held_sessions"] + 1

        if bar["open"] <= stop:
            return self._close_position(
                position, bar, bar["open"], "stop_gap", held_sessions,
            ), 0
        if bar["low"] <= stop:
            return self._close_position(
                position, bar, stop, "stop_intraday", held_sessions,
            ), 0
        if held_sessions >= max(1, self._config.max_holding_sessions):
            return self._close_position(
                position, bar, bar["close"], "max_holding", held_sessions,
            ), 0

        highest_close = max(position["highest_close"], bar["close"])
        atr = calculate_atr(bars[:bar_index + 1], self._config.atr_period)
        if atr is None:
            atr = position["atr_at_entry"]
        new_stop, reason = self._next_stop(
            strategy=position["strategy"],
            entry_price=position["entry_price"],
            current_stop=stop,
            highest_close=highest_close,
            atr=atr,
            latest_close=bar["close"],
        )
        self._repository.update_medium_term_position_state(
            position_id=position["id"],
            trade_date=bar["trade_date"],
            held_sessions=held_sessions,
            highest_close=highest_close,
            previous_stop=stop,
            new_stop=new_stop,
            stop_reason=reason,
        )
        position["held_sessions"] = held_sessions
        position["last_evaluated_date"] = bar["trade_date"]
        position["highest_close"] = highest_close
        position["current_stop"] = new_stop
        return False, int(new_stop > stop)

    def _close_position(
        self,
        position: dict,
        bar: dict,
        exit_price: float,
        reason: str,
        held_sessions: int,
    ) -> bool:
        gross_return = (exit_price - position["entry_price"]) / position["entry_price"]
        total_cost_rate = 2 * max(
            0.0,
            self._config.commission_rate + self._config.slippage_rate,
        )
        gross_pnl = position["notional"] * gross_return
        costs = position["notional"] * total_cost_rate
        return self._repository.close_medium_term_position(
            position_id=position["id"],
            exit_date=bar["trade_date"],
            exit_price=exit_price,
            exit_reason=reason,
            held_sessions=held_sessions,
            gross_return_pct=gross_return,
            net_return_pct=gross_return - total_cost_rate,
            gross_pnl=gross_pnl,
            costs=costs,
            net_pnl=gross_pnl - costs,
        )

    def _next_stop(
        self,
        *,
        strategy: str,
        entry_price: float,
        current_stop: float,
        highest_close: float,
        atr: float,
        latest_close: float,
    ) -> tuple[float, str | None]:
        gain = max(0.0, highest_close / entry_price - 1)
        target = current_stop
        reason: str | None = None
        if strategy == "staircase":
            trigger = max(self._config.staircase_trigger_pct, 1e-9)
            steps = math.floor((gain + 1e-12) / trigger)
            target = entry_price * (
                1 - self._config.initial_stop_pct
                + steps * self._config.staircase_raise_pct
            )
            reason = "staircase"
        elif strategy == "atr":
            target = highest_close - self._config.atr_multiplier * atr
            reason = "atr_trail"
        elif strategy == "hybrid" and (
            gain >= self._config.hybrid_breakeven_trigger_pct
        ):
            target = max(
                entry_price,
                highest_close - self._config.hybrid_trailing_atr_multiplier * atr,
            )
            reason = "hybrid_breakeven_atr"
        target = min(target, latest_close)
        new_stop = max(current_stop, target)
        return new_stop, reason if new_stop > current_stop else None

    def _open_candidates(self, portfolio: dict, strategy: str) -> tuple[int, int]:
        summary = self._repository.get_medium_term_paper_summary(portfolio["name"])
        if summary is None:
            raise RuntimeError(f"medium-term portfolio unavailable: {portfolio['name']}")
        open_positions = self._repository.list_open_medium_term_positions(
            portfolio["name"],
        )
        open_tickers = {item["ticker"] for item in open_positions}
        open_count = len(open_positions)
        equity = max(0.0, summary["initial_cash"] + summary["realized_pnl"])
        available_cash = max(0.0, equity - summary["open_notional"])
        opened = skipped = 0

        for ticker in self._tracked_tickers:
            bars = self._repository.list_moex_daily_history(
                ticker,
                max(2, self._config.history_bars),
            )
            if len(bars) < 2:
                skipped += self._record_skip(
                    portfolio["name"], ticker, bars, "insufficient_history", {},
                )
                continue
            signal_bars = bars[:-1]
            entry_bar = bars[-1]
            signal = evaluate_medium_term_signal(signal_bars, self._config)
            signal_date = signal_bars[-1]["trade_date"]
            metrics = dict(signal.metrics)
            metrics["entry_date"] = str(entry_bar["trade_date"])
            if not signal.eligible or signal.atr is None:
                skipped += self._record_decision(
                    portfolio["name"], ticker, signal_date,
                    "skip", signal.reason, metrics,
                )
                continue
            if ticker in open_tickers:
                skipped += self._record_decision(
                    portfolio["name"], ticker, signal_date,
                    "skip", "ticker_already_open", metrics,
                )
                continue
            if open_count >= max(0, self._config.max_open_positions):
                skipped += self._record_decision(
                    portfolio["name"], ticker, signal_date,
                    "skip", "portfolio_capacity", metrics,
                )
                continue
            entry_price = entry_bar["open"]
            if entry_price <= 0:
                skipped += self._record_decision(
                    portfolio["name"], ticker, signal_date,
                    "skip", "invalid_entry_price", metrics,
                )
                continue
            stop_distance_pct = self._initial_stop_distance_pct(
                strategy, entry_price, signal.atr,
            )
            metrics["initial_stop_distance_pct"] = stop_distance_pct
            if stop_distance_pct > self._config.max_stop_distance_pct:
                skipped += self._record_decision(
                    portfolio["name"], ticker, signal_date,
                    "skip", "stop_too_wide", metrics,
                )
                continue
            risk_budget = equity * max(0.0, self._config.risk_per_position)
            notional = min(
                risk_budget / max(stop_distance_pct, 1e-9),
                equity * max(0.0, self._config.max_position_fraction),
                available_cash,
            )
            if notional <= 0:
                skipped += self._record_decision(
                    portfolio["name"], ticker, signal_date,
                    "skip", "insufficient_cash", metrics,
                )
                continue
            if not self._repository.insert_medium_term_decision({
                "portfolio_name": portfolio["name"],
                "ticker": ticker,
                "signal_date": signal_date,
                "decision": "enter",
                "reason": "trend_breakout_volume",
                "metrics": metrics,
            }):
                continue
            initial_stop = entry_price * (1 - stop_distance_pct)
            position_id = self._repository.insert_medium_term_position({
                "portfolio_name": portfolio["name"],
                "strategy": strategy,
                "ticker": ticker,
                "signal_date": signal_date,
                "entry_date": entry_bar["trade_date"],
                "entry_price": entry_price,
                "notional": notional,
                "atr_at_entry": signal.atr,
                "initial_stop": initial_stop,
            })
            if position_id is None:
                continue
            opened += 1
            open_count += 1
            open_tickers.add(ticker)
            available_cash -= notional
        return opened, skipped

    def _record_skip(
        self,
        portfolio_name: str,
        ticker: str,
        bars: list[dict],
        reason: str,
        metrics: dict,
    ) -> int:
        if not bars:
            return 0
        return self._record_decision(
            portfolio_name, ticker, bars[-1]["trade_date"],
            "skip", reason, metrics,
        )

    def _record_decision(
        self,
        portfolio_name: str,
        ticker: str,
        signal_date: object,
        decision: str,
        reason: str,
        metrics: dict,
    ) -> int:
        inserted = self._repository.insert_medium_term_decision({
            "portfolio_name": portfolio_name,
            "ticker": ticker,
            "signal_date": signal_date,
            "decision": decision,
            "reason": reason,
            "metrics": metrics,
        })
        return int(inserted)

    def _initial_stop_distance_pct(
        self,
        strategy: str,
        entry_price: float,
        atr: float,
    ) -> float:
        initial = max(0.0, self._config.initial_stop_pct)
        if strategy == "staircase":
            return initial
        return max(initial, self._config.atr_multiplier * atr / entry_price)


def evaluate_medium_term_signal(
    bars: list[dict],
    config: MediumTermPaperConfig,
) -> MediumTermSignal:
    """Evaluate a deterministic signal using completed bars only."""
    required = max(
        config.sma_long_period + 1,
        config.breakout_period + 1,
        config.volume_period + 1,
        config.atr_period + 1,
    )
    if len(bars) < required:
        return MediumTermSignal(False, "insufficient_history", None, {
            "available_bars": len(bars), "required_bars": required,
        })

    closes = [float(item["close"]) for item in bars]
    highs = [float(item["high"]) for item in bars]
    volumes = [float(item["volume"]) for item in bars]
    close = closes[-1]
    sma_short = _average(closes[-config.sma_short_period:])
    sma_long = _average(closes[-config.sma_long_period:])
    previous_sma_long = _average(closes[-config.sma_long_period - 1:-1])
    breakout_level = max(highs[-config.breakout_period - 1:-1])
    average_volume = _average(volumes[-config.volume_period - 1:-1])
    volume_ratio = volumes[-1] / average_volume if average_volume > 0 else 0.0
    atr = calculate_atr(bars, config.atr_period)
    metrics: dict[str, float | int | str] = {
        "signal_date": str(bars[-1]["trade_date"]),
        "close": close,
        "sma_short": sma_short,
        "sma_long": sma_long,
        "previous_sma_long": previous_sma_long,
        "breakout_level": breakout_level,
        "volume_ratio": volume_ratio,
        "atr": atr or 0.0,
    }
    if not (close > sma_short > sma_long and sma_long > previous_sma_long):
        return MediumTermSignal(False, "trend_not_confirmed", atr, metrics)
    if close < breakout_level:
        return MediumTermSignal(False, "breakout_not_confirmed", atr, metrics)
    if volume_ratio < config.volume_multiplier:
        return MediumTermSignal(False, "volume_not_confirmed", atr, metrics)
    if atr is None or atr <= 0:
        return MediumTermSignal(False, "atr_unavailable", atr, metrics)
    return MediumTermSignal(True, "eligible", atr, metrics)


def calculate_atr(bars: list[dict], period: int) -> float | None:
    """Calculate simple average true range for the latest completed bars."""
    period = max(1, period)
    if len(bars) < period + 1:
        return None
    ranges = []
    for index in range(len(bars) - period, len(bars)):
        bar = bars[index]
        previous_close = float(bars[index - 1]["close"])
        ranges.append(max(
            float(bar["high"]) - float(bar["low"]),
            abs(float(bar["high"]) - previous_close),
            abs(float(bar["low"]) - previous_close),
        ))
    return _average(ranges)


def format_medium_term_paper_summary(summaries: list[dict | None]) -> str:
    """Format concise A/B/C medium-term paper results."""
    lines = ["medium-term paper portfolios (virtual only):"]
    for summary in summaries:
        if summary is None:
            lines.append("  not started")
            continue
        closed = summary["closed_positions"]
        win_rate = summary["wins"] / closed if closed else None
        avg_return = summary["avg_net_return_pct"]
        win_rate_text = f"{win_rate:.1%}" if win_rate is not None else "n/a"
        lines.append(
            f"  {summary['name']} [{summary['strategy']}]: "
            f"open={summary['open_positions']} closed={closed} "
            f"pnl={summary['realized_pnl']:.2f} RUB "
            f"win_rate={win_rate_text}",
        )
        if avg_return is not None:
            lines.append(f"    avg_net_return={avg_return:.3%}")
    return "\n".join(lines)


def _average(values: list[float]) -> float:
    return sum(values) / len(values)
