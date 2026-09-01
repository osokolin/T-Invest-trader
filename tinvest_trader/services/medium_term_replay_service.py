"""Historical replay for the medium-term virtual strategy experiments."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import TYPE_CHECKING

from tinvest_trader.strategy.medium_term import (
    MediumTermSignal,
    calculate_atr,
    evaluate_medium_term_signal,
    initial_stop_distance_pct,
    next_medium_term_stop,
)

if TYPE_CHECKING:
    from tinvest_trader.app.config import MediumTermPaperConfig
    from tinvest_trader.infra.storage.repository import TradingRepository


BENCHMARK_ARM = "benchmark_equal_weight"
STRATEGY_ARMS = ("staircase", "atr", "hybrid")


@dataclass(frozen=True)
class ReplayCandidate:
    ticker: str
    signal_date: date
    entry_date: date
    entry_price: float
    signal: MediumTermSignal


@dataclass
class ReplayPosition:
    ticker: str
    signal_date: date
    entry_date: date
    entry_price: float
    notional: float
    atr_at_entry: float
    initial_stop: float
    current_stop: float
    highest_close: float
    held_sessions: int = 0


@dataclass
class ReplayArmState:
    arm: str
    initial_cash: float
    cash: float
    realized_pnl: float = 0.0
    positions: dict[str, ReplayPosition] = field(default_factory=dict)
    trades: list[dict] = field(default_factory=list)
    equity: list[dict] = field(default_factory=list)
    peak_equity: float = 0.0


@dataclass(frozen=True)
class MediumTermReplayResult:
    run_id: int | None
    run_name: str
    start_date: date
    end_date: date
    tickers: tuple[str, ...]
    summaries: tuple[dict, ...]
    trade_count: int
    equity_row_count: int


class MediumTermReplayService:
    """Run and persist one deterministic, network-free historical replay."""

    def __init__(
        self,
        repository: TradingRepository,
        config: MediumTermPaperConfig,
        logger: logging.Logger,
    ) -> None:
        self._repository = repository
        self._config = config
        self._logger = logger

    def run(
        self,
        *,
        name: str,
        start_date: date,
        end_date: date,
        tickers: tuple[str, ...],
    ) -> MediumTermReplayResult:
        if start_date >= end_date:
            raise ValueError("replay start_date must be before end_date")
        normalized_tickers = tuple(sorted({item.upper() for item in tickers if item}))
        if not normalized_tickers:
            raise ValueError("replay requires at least one ticker")

        run_id = self._repository.create_medium_term_replay_run(
            name=name,
            start_date=start_date,
            end_date=end_date,
            tickers=normalized_tickers,
            config=asdict(self._config),
        )
        try:
            bars_by_ticker = self._repository.list_moex_daily_history_range(
                normalized_tickers,
                start_date=start_date,
                end_date=end_date,
                warmup_bars=max(2, self._config.history_bars),
            )
            simulation = simulate_medium_term_replay(
                bars_by_ticker=bars_by_ticker,
                config=self._config,
                start_date=start_date,
                end_date=end_date,
            )
            self._repository.insert_medium_term_replay_results(
                run_id=run_id,
                trades=simulation["trades"],
                equity_rows=simulation["equity"],
            )
            self._repository.complete_medium_term_replay_run(run_id)
        except Exception as exc:
            self._repository.fail_medium_term_replay_run(run_id, str(exc))
            raise

        result = MediumTermReplayResult(
            run_id=run_id,
            run_name=name,
            start_date=start_date,
            end_date=end_date,
            tickers=normalized_tickers,
            summaries=tuple(simulation["summaries"]),
            trade_count=len(simulation["trades"]),
            equity_row_count=len(simulation["equity"]),
        )
        self._logger.info(
            "medium-term historical replay complete",
            extra={
                "component": "medium_term_replay",
                "run_id": run_id,
                "run_name": name,
                "trade_count": result.trade_count,
                "equity_rows": result.equity_row_count,
            },
        )
        return result


def simulate_medium_term_replay(
    *,
    bars_by_ticker: dict[str, list[dict]],
    config: MediumTermPaperConfig,
    start_date: date,
    end_date: date,
) -> dict[str, list[dict]]:
    """Replay all strategy arms and a benchmark without future information."""
    bars_by_ticker = {
        ticker.upper(): sorted(items, key=lambda item: item["trade_date"])
        for ticker, items in bars_by_ticker.items()
        if items
    }
    calendar = sorted({
        bar["trade_date"]
        for bars in bars_by_ticker.values()
        for bar in bars
        if start_date <= bar["trade_date"] <= end_date
    })
    if not calendar:
        raise ValueError("no MOEX daily bars available in replay range")

    bar_indexes = {
        ticker: {bar["trade_date"]: index for index, bar in enumerate(bars)}
        for ticker, bars in bars_by_ticker.items()
    }
    candidates = _build_candidates(
        bars_by_ticker,
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    trades: list[dict] = []
    equity_rows: list[dict] = []
    summaries: list[dict] = []
    for arm in STRATEGY_ARMS:
        state = _simulate_strategy_arm(
            arm=arm,
            calendar=calendar,
            candidates=candidates,
            bars_by_ticker=bars_by_ticker,
            bar_indexes=bar_indexes,
            config=config,
        )
        trades.extend(state.trades)
        equity_rows.extend(state.equity)
        summaries.append(_summarize_arm(state))

    benchmark_equity = _simulate_equal_weight_benchmark(
        calendar=calendar,
        bars_by_ticker=bars_by_ticker,
        bar_indexes=bar_indexes,
        config=config,
    )
    equity_rows.extend(benchmark_equity)
    summaries.append(_summarize_benchmark(benchmark_equity, config.initial_cash))
    return {"trades": trades, "equity": equity_rows, "summaries": summaries}


def _build_candidates(
    bars_by_ticker: dict[str, list[dict]],
    *,
    config: MediumTermPaperConfig,
    start_date: date,
    end_date: date,
) -> dict[date, list[ReplayCandidate]]:
    result: dict[date, list[ReplayCandidate]] = {}
    history_limit = max(2, config.history_bars)
    for ticker, bars in bars_by_ticker.items():
        for entry_index in range(1, len(bars)):
            entry_bar = bars[entry_index]
            entry_date = entry_bar["trade_date"]
            if entry_date < start_date or entry_date > end_date:
                continue
            signal_bars = bars[max(0, entry_index - history_limit):entry_index]
            signal = evaluate_medium_term_signal(signal_bars, config)
            if not signal.eligible or signal.atr is None:
                continue
            result.setdefault(entry_date, []).append(ReplayCandidate(
                ticker=ticker,
                signal_date=signal_bars[-1]["trade_date"],
                entry_date=entry_date,
                entry_price=float(entry_bar["open"]),
                signal=signal,
            ))
    for items in result.values():
        items.sort(key=lambda item: item.ticker)
    return result


def _simulate_strategy_arm(
    *,
    arm: str,
    calendar: list[date],
    candidates: dict[date, list[ReplayCandidate]],
    bars_by_ticker: dict[str, list[dict]],
    bar_indexes: dict[str, dict[date, int]],
    config: MediumTermPaperConfig,
) -> ReplayArmState:
    state = ReplayArmState(
        arm=arm,
        initial_cash=config.initial_cash,
        cash=config.initial_cash,
        peak_equity=config.initial_cash,
    )
    latest_closes: dict[str, float] = {}
    for trade_date in calendar:
        bars_today = _bars_for_date(
            trade_date, bars_by_ticker=bars_by_ticker, bar_indexes=bar_indexes,
        )
        latest_closes.update({ticker: bar["close"] for ticker, bar in bars_today.items()})

        # Overnight gaps are known at the open and release virtual cash first.
        for ticker in sorted(tuple(state.positions)):
            bar = bars_today.get(ticker)
            position = state.positions.get(ticker)
            if bar is None or position is None:
                continue
            if bar["open"] <= position.current_stop:
                _close_replay_position(
                    state, position, trade_date, bar["open"], "stop_gap",
                    position.held_sessions + 1, config,
                )

        for candidate in candidates.get(trade_date, []):
            _try_open_replay_position(state, candidate, config)

        # Intraday lows and close-derived trailing updates happen after all opens.
        for ticker in sorted(tuple(state.positions)):
            bar = bars_today.get(ticker)
            position = state.positions.get(ticker)
            if bar is None or position is None:
                continue
            held_sessions = position.held_sessions + 1
            if bar["low"] <= position.current_stop:
                _close_replay_position(
                    state, position, trade_date, position.current_stop,
                    "stop_intraday", held_sessions, config,
                )
                continue
            if held_sessions >= max(1, config.max_holding_sessions):
                _close_replay_position(
                    state, position, trade_date, bar["close"],
                    "max_holding", held_sessions, config,
                )
                continue
            position.held_sessions = held_sessions
            position.highest_close = max(position.highest_close, bar["close"])
            index = bar_indexes[ticker][trade_date]
            history = bars_by_ticker[ticker][:index + 1]
            atr = calculate_atr(history, config.atr_period) or position.atr_at_entry
            position.current_stop, _ = next_medium_term_stop(
                config,
                strategy=arm,
                entry_price=position.entry_price,
                current_stop=position.current_stop,
                highest_close=position.highest_close,
                atr=atr,
                latest_close=bar["close"],
            )

        _record_strategy_equity(
            state, trade_date, latest_closes=latest_closes, config=config,
        )
    return state


def _try_open_replay_position(
    state: ReplayArmState,
    candidate: ReplayCandidate,
    config: MediumTermPaperConfig,
) -> None:
    if candidate.ticker in state.positions:
        return
    if len(state.positions) >= max(0, config.max_open_positions):
        return
    if candidate.entry_price <= 0 or candidate.signal.atr is None:
        return
    stop_distance = initial_stop_distance_pct(
        config, state.arm, candidate.entry_price, candidate.signal.atr,
    )
    if stop_distance <= 0 or stop_distance > config.max_stop_distance_pct:
        return
    capital_basis = max(0.0, state.initial_cash + state.realized_pnl)
    risk_budget = capital_basis * max(0.0, config.risk_per_position)
    notional = min(
        risk_budget / stop_distance,
        capital_basis * max(0.0, config.max_position_fraction),
        state.cash,
    )
    if notional <= 0:
        return
    initial_stop = candidate.entry_price * (1 - stop_distance)
    state.positions[candidate.ticker] = ReplayPosition(
        ticker=candidate.ticker,
        signal_date=candidate.signal_date,
        entry_date=candidate.entry_date,
        entry_price=candidate.entry_price,
        notional=notional,
        atr_at_entry=candidate.signal.atr,
        initial_stop=initial_stop,
        current_stop=initial_stop,
        highest_close=candidate.entry_price,
    )
    state.cash -= notional


def _close_replay_position(
    state: ReplayArmState,
    position: ReplayPosition,
    exit_date: date,
    exit_price: float,
    exit_reason: str,
    held_sessions: int,
    config: MediumTermPaperConfig,
) -> None:
    gross_return = exit_price / position.entry_price - 1
    total_cost_rate = _total_cost_rate(config)
    gross_pnl = position.notional * gross_return
    costs = position.notional * total_cost_rate
    net_pnl = gross_pnl - costs
    state.cash += position.notional + net_pnl
    state.realized_pnl += net_pnl
    state.trades.append({
        "arm": state.arm,
        "ticker": position.ticker,
        "signal_date": position.signal_date,
        "entry_date": position.entry_date,
        "exit_date": exit_date,
        "entry_price": position.entry_price,
        "exit_price": exit_price,
        "notional": position.notional,
        "initial_stop": position.initial_stop,
        "exit_reason": exit_reason,
        "held_sessions": held_sessions,
        "gross_return_pct": gross_return,
        "net_return_pct": gross_return - total_cost_rate,
        "gross_pnl": gross_pnl,
        "costs": costs,
        "net_pnl": net_pnl,
    })
    del state.positions[position.ticker]


def _record_strategy_equity(
    state: ReplayArmState,
    trade_date: date,
    *,
    latest_closes: dict[str, float],
    config: MediumTermPaperConfig,
) -> None:
    total_cost_rate = _total_cost_rate(config)
    position_value = sum(
        position.notional * latest_closes.get(ticker, position.entry_price)
        / position.entry_price
        - position.notional * total_cost_rate
        for ticker, position in state.positions.items()
    )
    total_equity = state.cash + position_value
    state.peak_equity = max(state.peak_equity, total_equity)
    drawdown = (
        (state.peak_equity - total_equity) / state.peak_equity
        if state.peak_equity > 0 else 0.0
    )
    state.equity.append({
        "arm": state.arm,
        "trade_date": trade_date,
        "cash": state.cash,
        "position_value": position_value,
        "total_equity": total_equity,
        "drawdown_pct": drawdown,
        "open_positions": len(state.positions),
    })


def _simulate_equal_weight_benchmark(
    *,
    calendar: list[date],
    bars_by_ticker: dict[str, list[dict]],
    bar_indexes: dict[str, dict[date, int]],
    config: MediumTermPaperConfig,
) -> list[dict]:
    first_entries = {
        ticker: next(
            (bar for bar in bars if bar["trade_date"] >= calendar[0]),
            None,
        )
        for ticker, bars in bars_by_ticker.items()
    }
    first_entries = {ticker: bar for ticker, bar in first_entries.items() if bar}
    if not first_entries:
        return []
    allocation = config.initial_cash / len(first_entries)
    cash = config.initial_cash
    positions: dict[str, tuple[float, float]] = {}
    latest_closes: dict[str, float] = {}
    peak = config.initial_cash
    total_cost_rate = _total_cost_rate(config)
    rows = []
    for trade_date in calendar:
        bars_today = _bars_for_date(
            trade_date, bars_by_ticker=bars_by_ticker, bar_indexes=bar_indexes,
        )
        latest_closes.update({ticker: bar["close"] for ticker, bar in bars_today.items()})
        for ticker, entry_bar in first_entries.items():
            if ticker in positions or entry_bar["trade_date"] != trade_date:
                continue
            positions[ticker] = (allocation, float(entry_bar["open"]))
            cash -= allocation
        position_value = sum(
            notional * latest_closes.get(ticker, entry_price) / entry_price
            - notional * total_cost_rate
            for ticker, (notional, entry_price) in positions.items()
        )
        total_equity = cash + position_value
        peak = max(peak, total_equity)
        rows.append({
            "arm": BENCHMARK_ARM,
            "trade_date": trade_date,
            "cash": cash,
            "position_value": position_value,
            "total_equity": total_equity,
            "drawdown_pct": (peak - total_equity) / peak if peak > 0 else 0.0,
            "open_positions": len(positions),
        })
    return rows


def _bars_for_date(
    trade_date: date,
    *,
    bars_by_ticker: dict[str, list[dict]],
    bar_indexes: dict[str, dict[date, int]],
) -> dict[str, dict]:
    return {
        ticker: bars[indexes[trade_date]]
        for ticker, indexes in bar_indexes.items()
        if trade_date in indexes
        for bars in (bars_by_ticker[ticker],)
    }


def _summarize_arm(state: ReplayArmState) -> dict:
    final_equity = state.equity[-1]["total_equity"]
    closed = len(state.trades)
    wins = sum(item["net_pnl"] > 0 for item in state.trades)
    return {
        "arm": state.arm,
        "final_equity": final_equity,
        "total_return_pct": final_equity / state.initial_cash - 1,
        "max_drawdown_pct": max(
            (item["drawdown_pct"] for item in state.equity), default=0.0,
        ),
        "closed_trades": closed,
        "open_positions": len(state.positions),
        "win_rate": wins / closed if closed else None,
        "avg_net_return_pct": (
            sum(item["net_return_pct"] for item in state.trades) / closed
            if closed else None
        ),
    }


def _summarize_benchmark(rows: list[dict], initial_cash: float) -> dict:
    final_equity = rows[-1]["total_equity"] if rows else initial_cash
    return {
        "arm": BENCHMARK_ARM,
        "final_equity": final_equity,
        "total_return_pct": final_equity / initial_cash - 1,
        "max_drawdown_pct": max(
            (item["drawdown_pct"] for item in rows), default=0.0,
        ),
        "closed_trades": 0,
        "open_positions": rows[-1]["open_positions"] if rows else 0,
        "win_rate": None,
        "avg_net_return_pct": None,
    }


def format_medium_term_replay_result(result: MediumTermReplayResult) -> str:
    lines = [
        f"medium-term replay: {result.run_name} (run_id={result.run_id})",
        f"range: {result.start_date} .. {result.end_date}",
        f"tickers: {', '.join(result.tickers)}",
    ]
    for summary in result.summaries:
        win_rate = summary["win_rate"]
        win_text = f"{win_rate:.1%}" if win_rate is not None else "n/a"
        lines.append(
            f"  {summary['arm']}: equity={summary['final_equity']:.2f} "
            f"return={summary['total_return_pct']:.2%} "
            f"max_dd={summary['max_drawdown_pct']:.2%} "
            f"closed={summary['closed_trades']} win_rate={win_text}",
        )
    return "\n".join(lines)


def _total_cost_rate(config: MediumTermPaperConfig) -> float:
    return 2 * max(0.0, config.commission_rate + config.slippage_rate)
