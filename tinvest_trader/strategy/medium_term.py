"""Pure medium-term signal and virtual stop policies."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.app.config import MediumTermPaperConfig


@dataclass(frozen=True)
class MediumTermSignal:
    eligible: bool
    reason: str
    atr: float | None
    metrics: dict[str, float | int | str]


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


def initial_stop_distance_pct(
    config: MediumTermPaperConfig,
    strategy: str,
    entry_price: float,
    atr: float,
) -> float:
    """Return the shared initial virtual stop distance for one strategy arm."""
    initial = max(0.0, config.initial_stop_pct)
    if strategy == "staircase":
        return initial
    if entry_price <= 0:
        return float("inf")
    return max(initial, config.atr_multiplier * atr / entry_price)


def next_medium_term_stop(
    config: MediumTermPaperConfig,
    *,
    strategy: str,
    entry_price: float,
    current_stop: float,
    highest_close: float,
    atr: float,
    latest_close: float,
) -> tuple[float, str | None]:
    """Raise, but never lower, a virtual stop after a completed close."""
    gain = max(0.0, highest_close / entry_price - 1)
    target = current_stop
    reason: str | None = None
    if strategy == "staircase":
        trigger = max(config.staircase_trigger_pct, 1e-9)
        steps = math.floor((gain + 1e-12) / trigger)
        target = entry_price * (
            1 - config.initial_stop_pct
            + steps * config.staircase_raise_pct
        )
        reason = "staircase"
    elif strategy == "atr":
        target = highest_close - config.atr_multiplier * atr
        reason = "atr_trail"
    elif strategy == "hybrid" and (
        gain >= config.hybrid_breakeven_trigger_pct
    ):
        target = max(
            entry_price,
            highest_close - config.hybrid_trailing_atr_multiplier * atr,
        )
        reason = "hybrid_breakeven_atr"
    target = min(target, latest_close)
    new_stop = max(current_stop, target)
    return new_stop, reason if new_stop > current_stop else None


def _average(values: list[float]) -> float:
    return sum(values) / len(values)
