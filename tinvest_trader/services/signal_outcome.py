"""Signal outcome evaluation -- resolves pending predictions.

For each unresolved prediction older than evaluation_window:
1. Look up first local quote after signal was created
2. Compute return
3. Classify win/loss/neutral
4. Update prediction row

No retries. No external API calls. Deterministic classification.
All price data comes from local market_quotes table.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository

# Neutral zone: returns within this band are classified as neutral
_NEUTRAL_THRESHOLD = 0.0005  # 0.05%


def classify_outcome(signal_type: str, return_pct: float) -> str:
    """Classify return into win/loss/neutral based on signal direction."""
    if abs(return_pct) < _NEUTRAL_THRESHOLD:
        return "neutral"
    if signal_type == "up":
        return "win" if return_pct > 0 else "loss"
    if signal_type == "down":
        return "win" if return_pct < 0 else "loss"
    return "neutral"


def resolve_pending_signals(
    repository: TradingRepository,
    logger: logging.Logger,
    eval_window_seconds: int = 300,
    now: datetime | None = None,
) -> int:
    """Resolve all pending signal predictions older than eval_window.

    Uses local quotes from market_quotes table (no external API calls).
    For each pending prediction, finds the first quote after signal creation.
    Returns count of resolved predictions.
    """
    if now is None:
        now = datetime.now(UTC)

    cutoff = now - timedelta(seconds=eval_window_seconds)
    pending = repository.list_pending_predictions(before=cutoff)

    if not pending:
        return 0

    resolved_count = 0
    for pred in pending:
        ticker = pred["ticker"]
        price_at_signal = pred["price_at_signal"]

        if price_at_signal is None or price_at_signal == 0:
            logger.warning(
                "signal_outcome: skipping prediction %d, no signal price",
                pred["id"],
                extra={"component": "signal_outcome"},
            )
            continue

        quote = repository.get_first_quote_after(ticker, pred["created_at"])

        if quote is None:
            logger.debug(
                "signal_outcome: ticker=%s resolved=false reason=no_quote_yet",
                ticker,
                extra={"component": "signal_outcome"},
            )
            continue

        price_now = quote["price"]
        return_pct = (price_now - price_at_signal) / price_at_signal
        outcome_label = classify_outcome(pred["signal_type"], return_pct)

        repository.resolve_prediction(
            prediction_id=pred["id"],
            price_at_outcome=price_now,
            return_pct=return_pct,
            outcome_label=outcome_label,
            resolved_at=now,
        )

        logger.debug(
            "signal_outcome: ticker=%s resolved=true "
            "price_signal=%.4f price_outcome=%.4f return=%+.4f%% outcome=%s",
            ticker, price_at_signal, price_now,
            return_pct * 100, outcome_label,
            extra={"component": "signal_outcome"},
        )
        resolved_count += 1

    return resolved_count


def format_signal_stats(
    stats: dict,
    by_ticker: list[dict],
    by_type: list[dict],
) -> str:
    """Format signal prediction statistics for CLI output."""
    lines: list[str] = []
    lines.append("signal prediction stats")

    total = stats.get("total", 0)
    resolved = stats.get("resolved", 0)
    wins = stats.get("wins", 0)
    avg_ret = stats.get("avg_return")

    lines.append(f"total_signals: {total}")
    lines.append(f"resolved: {resolved}")

    if resolved > 0:
        win_rate = (wins / resolved) * 100
        lines.append(f"win_rate: {win_rate:.1f}%")
    else:
        lines.append("win_rate: n/a")

    if avg_ret is not None:
        lines.append(f"avg_return: {avg_ret * 100:+.2f}%")
    else:
        lines.append("avg_return: n/a")

    if by_ticker:
        lines.append("by_ticker:")
        for t in by_ticker:
            resolved_t = t.get("resolved", 0)
            if resolved_t > 0:
                wr = (t["wins"] / resolved_t) * 100
                ret = t.get("avg_return")
                ret_str = f"{ret * 100:+.2f}%" if ret is not None else "n/a"
                lines.append(
                    f"  {t['ticker']}: win_rate={wr:.0f}%, "
                    f"avg_return={ret_str} (n={t['total']})",
                )
            else:
                lines.append(
                    f"  {t['ticker']}: pending (n={t['total']})",
                )

    if by_type:
        lines.append("by_signal_type:")
        for s in by_type:
            resolved_s = s.get("resolved", 0)
            if resolved_s > 0:
                wr = (s["wins"] / resolved_s) * 100
                lines.append(
                    f"  {s['signal_type']}: win_rate={wr:.0f}% (n={s['total']})",
                )
            else:
                lines.append(
                    f"  {s['signal_type']}: pending (n={s['total']})",
                )

    return "\n".join(lines)
