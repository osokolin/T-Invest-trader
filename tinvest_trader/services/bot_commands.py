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


# -- Inline keyboard builders --


def build_signal_list_keyboard(
    signals: list[dict],
) -> list[list[dict]]:
    """Build inline keyboard for signal list (one button per signal).

    Returns list-of-rows for Telegram inline_keyboard.
    """
    rows: list[list[dict]] = []
    for s in signals:
        sid = s["id"]
        ticker = s.get("ticker", "?")
        direction = (s.get("signal_type") or "?").upper()
        arrow = "\u2191" if direction == "UP" else "\u2193"
        label = f"{arrow} #{sid} {ticker}"
        rows.append([
            {"text": label, "callback_data": f"signal:{sid}:details"},
        ])
    return rows


def build_signal_detail_keyboard(signal_id: int) -> list[list[dict]]:
    """Build inline keyboard for signal detail view.

    Row 1: [AI] [Stats]
    Row 2: [Back]
    """
    return [
        [
            {
                "text": "\U0001f916 AI",
                "callback_data": f"signal:{signal_id}:ai",
            },
            {
                "text": "\U0001f4ca \u0421\u0442\u0430\u0442\u044b",
                "callback_data": f"signal:{signal_id}:stats",
            },
        ],
        [
            {
                "text": "\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434",
                "callback_data": "nav:last_signals",
            },
        ],
    ]


def build_delivery_keyboard(signal_id: int) -> list[list[dict]]:
    """Build inline keyboard for delivered signal messages.

    Row 1: [Details] [AI]
    Row 2: [Stats]
    """
    return [
        [
            {
                "text": "\U0001f4c4 \u0414\u0435\u0442\u0430\u043b\u0438",
                "callback_data": f"signal:{signal_id}:details",
            },
            {
                "text": "\U0001f916 AI",
                "callback_data": f"signal:{signal_id}:ai",
            },
        ],
        [
            {
                "text": "\U0001f4ca \u0421\u0442\u0430\u0442\u044b",
                "callback_data": f"signal:{signal_id}:stats",
            },
        ],
    ]


def handle_last_signals_with_buttons(
    repository: TradingRepository,
    args: str,
) -> tuple[str, list[list[dict]]]:
    """Handle /last_signals with inline buttons.

    Returns (text, keyboard_rows). If error/empty, keyboard is empty.
    """
    limit = 5
    if args:
        try:
            limit = int(args)
        except ValueError:
            return ("Usage: /last_signals [N]  (N = 1..10)", [])
        if limit < 1 or limit > 10:
            return ("Usage: /last_signals [N]  (N = 1..10)", [])

    signals = repository.list_recent_signals(limit)
    if not signals:
        return ("No signals yet", [])

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

    text = "\n".join(lines)
    keyboard = build_signal_list_keyboard(signals)
    return (text, keyboard)


def handle_signal_detail_with_buttons(
    repository: TradingRepository,
    signal_id: int,
) -> tuple[str, list[list[dict]]]:
    """Load signal detail and return (text, keyboard).

    Reuses handle_signal formatting, adds action buttons.
    """
    text = handle_signal(repository, str(signal_id))
    if "not found" in text:
        return (text, [])
    keyboard = build_signal_detail_keyboard(signal_id)
    return (text, keyboard)


def handle_ticker_stats(
    repository: TradingRepository,
    signal_id: int,
) -> str:
    """Return ticker-level stats for a signal."""
    signal = repository.get_signal_detail(signal_id)
    if signal is None:
        return f"Signal #{signal_id} not found"

    ticker = signal.get("ticker", "?")

    try:
        by_ticker = repository.get_signal_stats_by_ticker()
    except Exception:
        return f"Stats unavailable for {ticker}"

    ticker_stats: dict | None = None
    for row in by_ticker:
        if row.get("ticker") == ticker:
            ticker_stats = row
            break

    if not ticker_stats or ticker_stats.get("total", 0) == 0:
        return f"No stats for {ticker} yet"

    total = ticker_stats["total"]
    resolved = ticker_stats.get("resolved", 0)
    wins = ticker_stats.get("wins", 0)
    avg_ret = ticker_stats.get("avg_return")

    win_rate_str = "n/a"
    if resolved > 0:
        win_rate_str = f"{wins / resolved:.0%}"

    avg_ret_str = _fmt_pct(avg_ret * 100 if avg_ret is not None else None)

    return (
        f"Stats for {ticker}:\n"
        f"  total: {total}\n"
        f"  resolved: {resolved}\n"
        f"  win rate: {win_rate_str}\n"
        f"  avg return: {avg_ret_str}"
    )
