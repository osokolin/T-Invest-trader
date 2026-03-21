"""Telegram bot command handlers -- read-only operator queries.

Pure formatting and routing logic. No Telegram API calls here.
All commands are read-only: no trades, no config changes.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


# -- Command parsing --


def parse_command(text: str) -> tuple[str, str]:
    """Parse a bot command from message text.

    Returns (command, args) where command is lowercase without '/'.
    Examples:
        "/last_signals 10" -> ("last_signals", "10")
        "/signal 42"       -> ("signal", "42")
        "/stats"           -> ("stats", "")
        "hello"            -> ("", "hello")
    """
    text = text.strip()
    if not text.startswith("/"):
        return ("", text)
    # Strip @botname suffix if present (e.g. "/stats@my_bot")
    parts = text.split(None, 1)
    cmd_part = parts[0].lower()
    if "@" in cmd_part:
        cmd_part = cmd_part.split("@")[0]
    command = cmd_part.lstrip("/")
    args = parts[1].strip() if len(parts) > 1 else ""
    return (command, args)


# -- Formatters --


def _fmt_time(dt: datetime | None) -> str:
    if dt is None:
        return "?"
    if isinstance(dt, datetime):
        return dt.strftime("%H:%M")
    return str(dt)


def _fmt_conf(conf: float | None) -> str:
    if conf is None:
        return "?"
    return f"{conf:.2f}"


def _fmt_pct(val: float | None) -> str:
    if val is None:
        return "n/a"
    return f"{val:+.2f}%"


def _fmt_stage(stage: str | None) -> str:
    return stage or "unknown"


# -- Command handlers (return response text) --


def handle_last_signals(
    repository: TradingRepository,
    args: str,
) -> str:
    """Handle /last_signals [N] command."""
    limit = 5
    if args:
        try:
            limit = int(args)
        except ValueError:
            return "Usage: /last_signals [N]  (N = 1..10)"
        if limit < 1 or limit > 10:
            return "Usage: /last_signals [N]  (N = 1..10)"

    signals = repository.list_recent_signals(limit)
    if not signals:
        return "No signals yet"

    lines = ["Recent signals:"]
    for s in signals:
        direction = (s.get("signal_type") or "?").upper()
        stage = _fmt_stage(s.get("pipeline_stage"))
        line = (
            f"#{s['id']} {s['ticker']} {direction} "
            f"{stage} conf={_fmt_conf(s.get('confidence'))} "
            f"{_fmt_time(s.get('created_at'))}"
        )
        lines.append(line)
    return "\n".join(lines)


def handle_signal(
    repository: TradingRepository,
    args: str,
) -> str:
    """Handle /signal <id> command."""
    if not args:
        return "Usage: /signal <id>"
    try:
        signal_id = int(args)
    except ValueError:
        return "Usage: /signal <id>"

    signal = repository.get_signal_detail(signal_id)
    if signal is None:
        return f"Signal #{signal_id} not found"

    direction = (signal.get("signal_type") or "?").upper()

    lines = [
        f"Signal #{signal['id']}",
        f"Ticker: {signal['ticker']}",
        f"Direction: {direction}",
        f"Confidence: {_fmt_conf(signal.get('confidence'))}",
        f"Stage: {_fmt_stage(signal.get('pipeline_stage'))}",
    ]

    if signal.get("source_channel"):
        lines.append(f"Source: {signal['source_channel']}")

    if signal.get("rejection_reason"):
        lines.append(f"Rejected: {signal['rejection_reason']}")

    if signal.get("created_at"):
        dt = signal["created_at"]
        if isinstance(dt, datetime):
            lines.append(f"Created: {dt.strftime('%Y-%m-%d %H:%M')}")

    if signal.get("delivered_at"):
        dt = signal["delivered_at"]
        if isinstance(dt, datetime):
            lines.append(f"Delivered: {dt.strftime('%Y-%m-%d %H:%M')}")

    if signal.get("outcome_label"):
        outcome = signal["outcome_label"]
        ret = _fmt_pct(signal.get("return_pct"))
        lines.append(f"Outcome: {outcome} ({ret})")

    return "\n".join(lines)


def handle_stats(repository: TradingRepository) -> str:
    """Handle /stats command."""
    stats = repository.get_signal_stats()
    if not stats or stats.get("total", 0) == 0:
        return "No signal data yet"

    total = stats["total"]
    resolved = stats["resolved"]
    wins = stats["wins"]
    avg_ret = stats.get("avg_return")

    win_rate_str = "n/a"
    if resolved > 0:
        win_rate_str = f"{wins / resolved:.0%}"

    avg_ret_str = _fmt_pct(avg_ret * 100 if avg_ret is not None else None)

    lines = [
        "Stats:",
        f"  total signals: {total}",
        f"  resolved: {resolved}",
        f"  win rate: {win_rate_str}",
        f"  avg return: {avg_ret_str}",
    ]

    # Top source by volume
    try:
        sources = repository.get_signal_stats_by_source()
        if sources:
            top = sources[0]
            lines.append(f"  top source: {top['source_channel']}")
    except Exception:
        pass

    return "\n".join(lines)


def handle_help() -> str:
    """Handle /help command."""
    return (
        "Commands:\n"
        "/last_signals [N] -- recent signals (default 5)\n"
        "/signal <id> -- signal details\n"
        "/ai <id> -- AI analysis for signal\n"
        "/stats -- quick summary\n"
        "/help -- this message"
    )
