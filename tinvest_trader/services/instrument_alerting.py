"""Instrument health alerting -- notify operator about registry problems."""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.services.instrument_health import InstrumentHealthReport

_MAX_EXAMPLES = 10
_TELEGRAM_API = "https://api.telegram.org"

logger = logging.getLogger("tinvest_trader")


def _build_alert_message(report: InstrumentHealthReport) -> str:
    """Build a compact, actionable alert message from health report."""
    lines = [
        "\U0001f6a8 Instrument Health Issues",
        f"tracked: {report.total_tracked}",
        f"complete: {report.complete}",
        "problems:",
    ]
    if report.placeholder_figi_count:
        lines.append(f"- placeholder_figi: {report.placeholder_figi_count}")
    if report.missing_metadata_count:
        lines.append(f"- missing_metadata: {report.missing_metadata_count}")
    if report.stale_count:
        lines.append(f"- stale: {report.stale_count}")

    examples = report.instruments_with_issues[:_MAX_EXAMPLES]
    if examples:
        lines.append("examples:")
        for item in examples:
            first_issue = item.issues[0] if item.issues else "unknown"
            lines.append(f"- {item.ticker}: {first_issue}")

    return "\n".join(lines)


def _send_telegram_message(bot_token: str, chat_id: str, text: str) -> bool:
    """Send a message via Telegram Bot API. Returns True on success."""
    url = f"{_TELEGRAM_API}/bot{bot_token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": text}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception:
        logger.exception(
            "failed to send Telegram alert",
            extra={"component": "instrument_alerting"},
        )
        return False


def send_instrument_health_alert(
    report: InstrumentHealthReport,
    *,
    bot_token: str = "",
    chat_id: str = "",
) -> bool:
    """Send health alert if report has issues.

    Returns True if alert was sent (or logged), False if no issues.
    Reads TINVEST_ALERT_BOT_TOKEN and TINVEST_ALERT_CHAT_ID from env
    when bot_token/chat_id are not provided.
    """
    if not report.has_issues:
        return False

    message = _build_alert_message(report)

    token = bot_token or os.environ.get("TINVEST_ALERT_BOT_TOKEN", "")
    cid = chat_id or os.environ.get("TINVEST_ALERT_CHAT_ID", "")

    if token and cid:
        sent = _send_telegram_message(token, cid, message)
        if sent:
            logger.info(
                "instrument health alert sent via Telegram",
                extra={"component": "instrument_alerting"},
            )
            return True
        # Telegram failed -- fall through to logging

    logger.warning(
        "instrument health alert (Telegram not available):\n%s",
        message,
        extra={"component": "instrument_alerting"},
    )
    return True
