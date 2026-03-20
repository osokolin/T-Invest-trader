"""Broker fetch policy observability -- operator visibility into fetch state."""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from tinvest_trader.services.tbank_event_fetch_policy import (
    FetchPolicyConfig,
    should_fetch,
)

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository

logger = logging.getLogger("tinvest_trader")

_MAX_EXAMPLES = 10
_TELEGRAM_API = "https://api.telegram.org"


@dataclass
class FetchPolicyReport:
    """Aggregate report on broker fetch policy health."""

    tracked_total: int = 0
    placeholder_figi_count: int = 0
    total_pairs: int = 0
    eligible_now: int = 0
    skipped_ttl: int = 0
    skipped_cooldown: int = 0
    blocked_max_failures: int = 0
    never_succeeded: int = 0
    stale: int = 0
    recent_failures: int = 0

    blocked_examples: list[dict] = field(default_factory=list)
    never_succeeded_examples: list[dict] = field(default_factory=list)
    stale_examples: list[dict] = field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        return (
            self.blocked_max_failures > 0
            or self.stale > 0
            or self.never_succeeded > 0
            or self.recent_failures > 0
        )


def build_fetch_policy_report(
    repository: TradingRepository,
    policy_config: FetchPolicyConfig,
    *,
    stale_seconds: int = 172800,
    limit: int = 10,
    now: datetime | None = None,
) -> FetchPolicyReport:
    """Build a fetch policy health report from DB state.

    Combines tracked instruments, fetch state, and policy config
    to produce a comprehensive status report.
    """
    now = now or datetime.now(tz=UTC)
    report = FetchPolicyReport()

    # Tracked instruments
    tracked = repository.list_tracked_instruments()
    report.tracked_total = len(tracked)

    tracked_figis: list[str] = []
    for inst in tracked:
        figi = inst.get("figi", "")
        if figi.startswith("TICKER:"):
            report.placeholder_figi_count += 1
        else:
            tracked_figis.append(figi)

    # Load all fetch states
    all_states = repository.get_all_fetch_states()
    states_by_key: dict[tuple[str, str], dict] = {}
    for state in all_states:
        key = (state["figi"], state["event_type"])
        states_by_key[key] = state

    # Classify each (figi, event_type) pair
    event_types = ("dividends", "reports", "insider_deals")
    report.total_pairs = len(tracked_figis) * len(event_types)

    for event_type in event_types:
        for figi in tracked_figis:
            state = states_by_key.get((figi, event_type))
            if should_fetch(policy_config, event_type, state, now):
                report.eligible_now += 1
            elif state is not None:
                error_count = state.get("error_count", 0)
                if error_count >= policy_config.max_consecutive_failures:
                    report.blocked_max_failures += 1
                elif state.get("last_success_at") is not None:
                    report.skipped_ttl += 1
                else:
                    report.skipped_cooldown += 1

    # DB-backed detail queries
    db_summary = repository.get_broker_fetch_policy_summary()
    report.never_succeeded = db_summary["never_succeeded"]
    report.recent_failures = db_summary["recent_failures"]

    # Stale entries
    stale_rows = repository.list_broker_fetch_stale(
        stale_seconds=stale_seconds, limit=limit,
    )
    report.stale = len(stale_rows)
    report.stale_examples = stale_rows[:limit]

    # Blocked examples
    blocked_rows = repository.list_broker_fetch_failures(
        min_error_count=policy_config.max_consecutive_failures,
        limit=limit,
    )
    report.blocked_examples = blocked_rows[:limit]

    # Never succeeded examples
    never_rows = repository.list_broker_fetch_never_succeeded(limit=limit)
    report.never_succeeded_examples = never_rows[:limit]

    return report


def format_report(report: FetchPolicyReport) -> str:
    """Format report as compact CLI-friendly text."""
    lines = [
        "broker fetch policy status",
        f"tracked_total: {report.tracked_total}",
        f"placeholder_figi: {report.placeholder_figi_count}",
        f"total_pairs: {report.total_pairs}",
        f"eligible_now: {report.eligible_now}",
        f"skipped_ttl: {report.skipped_ttl}",
        f"skipped_cooldown: {report.skipped_cooldown}",
        f"blocked_max_failures: {report.blocked_max_failures}",
        f"never_succeeded: {report.never_succeeded}",
        f"stale: {report.stale}",
        f"recent_failures: {report.recent_failures}",
    ]

    if report.blocked_examples:
        lines.append("blocked:")
        for row in report.blocked_examples[:_MAX_EXAMPLES]:
            ticker = row.get("ticker") or row.get("figi", "?")
            lines.append(
                f"  {ticker} {row['event_type']}: "
                f"errors={row['error_count']}",
            )

    if report.never_succeeded_examples:
        lines.append("never_succeeded:")
        for row in report.never_succeeded_examples[:_MAX_EXAMPLES]:
            ticker = row.get("ticker") or row.get("figi", "?")
            lines.append(f"  {ticker} {row['event_type']}")

    if report.stale_examples:
        lines.append("stale:")
        for row in report.stale_examples[:_MAX_EXAMPLES]:
            ticker = row.get("ticker") or row.get("figi", "?")
            last = row.get("last_success_at") or row.get("last_checked_at")
            ts = last.isoformat() if last else "never"
            lines.append(f"  {ticker} {row['event_type']}: last={ts}")

    return "\n".join(lines)


def _build_alert_message(report: FetchPolicyReport) -> str:
    """Build a compact alert message for degraded policy health."""
    lines = [
        "\U0001f6a8 Broker Fetch Policy Issues",
        f"tracked: {report.tracked_total}",
        f"eligible_now: {report.eligible_now}",
        f"blocked_max_failures: {report.blocked_max_failures}",
        f"never_succeeded: {report.never_succeeded}",
        f"stale: {report.stale}",
        f"recent_failures: {report.recent_failures}",
    ]

    examples: list[str] = []
    for row in report.blocked_examples[:5]:
        ticker = row.get("ticker") or row.get("figi", "?")
        examples.append(f"- {ticker} {row['event_type']}: max_failures")
    for row in report.stale_examples[:3]:
        ticker = row.get("ticker") or row.get("figi", "?")
        examples.append(f"- {ticker} {row['event_type']}: stale")
    for row in report.never_succeeded_examples[:3]:
        ticker = row.get("ticker") or row.get("figi", "?")
        examples.append(f"- {ticker} {row['event_type']}: never succeeded")

    if examples:
        lines.append("examples:")
        lines.extend(examples[:_MAX_EXAMPLES])

    return "\n".join(lines)


def send_fetch_policy_alert(
    report: FetchPolicyReport,
    *,
    bot_token: str = "",
    chat_id: str = "",
) -> bool:
    """Send alert if report has issues.

    Returns True if alert was sent (or logged), False if no issues.
    """
    if not report.has_issues:
        return False

    message = _build_alert_message(report)
    token = bot_token or os.environ.get("TINVEST_ALERT_BOT_TOKEN", "")
    cid = chat_id or os.environ.get("TINVEST_ALERT_CHAT_ID", "")

    if token and cid:
        url = f"{_TELEGRAM_API}/bot{token}/sendMessage"
        payload = json.dumps({"chat_id": cid, "text": message}).encode()
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    logger.info(
                        "fetch policy alert sent via Telegram",
                        extra={"component": "fetch_policy_observability"},
                    )
                    return True
        except Exception:
            logger.exception(
                "failed to send Telegram fetch policy alert",
                extra={"component": "fetch_policy_observability"},
            )

    logger.warning(
        "fetch policy alert (Telegram not available):\n%s",
        message,
        extra={"component": "fetch_policy_observability"},
    )
    return True
