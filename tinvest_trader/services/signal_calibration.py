"""Signal calibration -- filters low-quality signals before execution.

Uses historical signal performance to decide whether a signal should
be executed. Rules are deterministic and config-driven:

1. Global confidence threshold
2. Per-ticker win rate and return filter
3. Per-signal-type enable/disable
4. Expected value (EV) filter

No ML. No dynamic learning. Pure rule-based filtering.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CalibrationConfig:
    """Config for signal calibration gate."""

    min_confidence: float = 0.0
    min_win_rate: float = 0.0
    min_ev: float = 0.0
    enable_up: bool = True
    enable_down: bool = True
    min_resolved_for_filter: int = 5


@dataclass(frozen=True)
class CalibrationDecision:
    """Result of calibration check."""

    allowed: bool
    reasons: list[str] = field(default_factory=list)


def compute_ev(win_rate: float, avg_return: float) -> float:
    """Compute simple expected value."""
    return win_rate * avg_return


def should_execute_signal(
    ticker: str,
    signal_type: str,
    confidence: float | None,
    ticker_stats: dict | None,
    type_stats: dict | None,
    config: CalibrationConfig,
) -> CalibrationDecision:
    """Decide whether a signal should proceed to execution.

    Args:
        ticker: instrument ticker
        signal_type: "up" or "down"
        confidence: signal confidence score (0..1), None if unavailable
        ticker_stats: stats dict for this ticker (from get_signal_stats_by_ticker)
        type_stats: stats dict for this signal_type (from get_signal_stats_by_type)
        config: calibration thresholds

    Returns:
        CalibrationDecision with allowed=True/False and rejection reasons.
    """
    reasons: list[str] = []

    # 1. Signal type enabled check
    if signal_type == "up" and not config.enable_up:
        reasons.append("signal_type_up_disabled")
    elif signal_type == "down" and not config.enable_down:
        reasons.append("signal_type_down_disabled")

    # 2. Confidence threshold
    if config.min_confidence > 0 and confidence is not None and confidence < config.min_confidence:
        reasons.append(
            f"low_confidence({confidence:.3f}<{config.min_confidence:.3f})",
        )

    # 3. Per-ticker filter (only if enough data)
    if ticker_stats and ticker_stats.get("resolved", 0) >= config.min_resolved_for_filter:
        resolved = ticker_stats["resolved"]
        wins = ticker_stats.get("wins", 0)
        avg_return = ticker_stats.get("avg_return")

        win_rate = wins / resolved if resolved > 0 else 0.0

        if config.min_win_rate > 0 and win_rate < config.min_win_rate:
            reasons.append(
                f"low_ticker_win_rate({win_rate:.3f}<{config.min_win_rate:.3f})",
            )

        if avg_return is not None and avg_return < 0:
            reasons.append(f"negative_ticker_return({avg_return:.6f})")

        # 4. EV filter
        if config.min_ev > 0 and avg_return is not None:
            ev = compute_ev(win_rate, avg_return)
            if ev < config.min_ev:
                reasons.append(f"low_ev({ev:.6f}<{config.min_ev})")

    # 5. Per-type filter (only if enough data)
    if type_stats and type_stats.get("resolved", 0) >= config.min_resolved_for_filter:
        resolved_t = type_stats["resolved"]
        wins_t = type_stats.get("wins", 0)

        win_rate_t = wins_t / resolved_t if resolved_t > 0 else 0.0

        if config.min_win_rate > 0 and win_rate_t < config.min_win_rate:
            reasons.append(
                f"low_type_win_rate({win_rate_t:.3f}<{config.min_win_rate:.3f})",
            )

    return CalibrationDecision(allowed=len(reasons) == 0, reasons=reasons)


def format_calibration_report(
    config: CalibrationConfig,
    by_ticker: list[dict],
    by_type: list[dict],
) -> str:
    """Format calibration report for CLI output."""
    lines: list[str] = ["signal calibration report"]
    lines.append(f"min_confidence: {config.min_confidence}")
    lines.append(f"min_win_rate: {config.min_win_rate}")
    lines.append(f"min_ev: {config.min_ev}")
    lines.append(f"enable_up: {config.enable_up}")
    lines.append(f"enable_down: {config.enable_down}")
    lines.append(f"min_resolved_for_filter: {config.min_resolved_for_filter}")

    if by_ticker:
        lines.append("tickers:")
        for t in by_ticker:
            resolved = t.get("resolved", 0)
            if resolved == 0:
                lines.append(f"  {t['ticker']}: pending (n={t['total']})")
                continue
            wins = t.get("wins", 0)
            wr = wins / resolved
            avg_ret = t.get("avg_return")
            avg_ret_str = f"{avg_ret * 100:+.2f}%" if avg_ret is not None else "n/a"
            ev = compute_ev(wr, avg_ret) if avg_ret is not None else 0.0
            ev_str = f"{ev:+.6f}"

            disabled_parts: list[str] = []
            if config.min_win_rate > 0 and wr < config.min_win_rate:
                disabled_parts.append("low_wr")
            if avg_ret is not None and avg_ret < 0:
                disabled_parts.append("neg_ret")
            if config.min_ev > 0 and ev < config.min_ev:
                disabled_parts.append("low_ev")

            status = f" (FILTERED: {','.join(disabled_parts)})" if disabled_parts else ""
            lines.append(
                f"  {t['ticker']}: win_rate={wr:.0%}, "
                f"avg_return={avg_ret_str}, EV={ev_str}{status}",
            )

    if by_type:
        lines.append("signal_types:")
        for s in by_type:
            resolved = s.get("resolved", 0)
            if resolved == 0:
                lines.append(
                    f"  {s['signal_type']}: pending (n={s['total']})",
                )
                continue
            wins = s.get("wins", 0)
            wr = wins / resolved
            avg_ret = s.get("avg_return")
            avg_ret_str = f"{avg_ret * 100:+.2f}%" if avg_ret is not None else "n/a"

            enabled = not (
                (s["signal_type"] == "up" and not config.enable_up)
                or (s["signal_type"] == "down" and not config.enable_down)
            )

            status = "" if enabled else " (disabled)"
            lines.append(
                f"  {s['signal_type']}: win_rate={wr:.0%}, "
                f"avg_return={avg_ret_str}{status}",
            )

    return "\n".join(lines)
