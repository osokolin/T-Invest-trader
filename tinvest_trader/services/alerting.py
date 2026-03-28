"""Operator alerting service.

Evaluates system health checks, applies cooldown/dedup,
and sends alerts via Telegram when conditions are met.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import TYPE_CHECKING

# Moscow timezone (UTC+3, no DST)
_MSK = timezone(timedelta(hours=3))

if TYPE_CHECKING:
    from tinvest_trader.app.config import AlertingConfig, SignalDeliveryConfig
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass
class Alert:
    key: str
    category: str
    severity: str
    title: str
    message: str


@dataclass
class AlertCheckResult:
    alerts_evaluated: int = 0
    alerts_fired: int = 0
    alerts_sent: int = 0
    alerts_cooled_down: int = 0
    details: list[str] | None = None


def evaluate_alerts(
    config: AlertingConfig,
    repository: TradingRepository,
    logger: logging.Logger,
) -> list[Alert]:
    """Evaluate all alert checks against current system state."""
    health = repository.get_alerting_health_data()
    if not health:
        logger.warning(
            "alerting: no health data available",
            extra={"component": "alerting"},
        )
        return []

    now = datetime.now(UTC)
    alerts: list[Alert] = []

    # Moscow Exchange trades weekdays 09:50–18:50 MSK.
    # Skip gap-based alerts outside market hours (evenings, nights, weekends).
    # Win rate and pending checks still apply at all times.
    now_msk = now.astimezone(_MSK)
    msk_minutes = now_msk.hour * 60 + now_msk.minute
    _MARKET_OPEN_MIN = 9 * 60 + 50   # 09:50 MSK
    _MARKET_CLOSE_MIN = 18 * 60 + 50  # 18:50 MSK
    _GRACE_PERIOD_MIN = 60             # skip first 60 min after open

    market_open = (
        now_msk.weekday() < 5
        and _MARKET_OPEN_MIN <= msk_minutes <= _MARKET_CLOSE_MIN
    )
    in_grace_period = (
        market_open
        and msk_minutes < _MARKET_OPEN_MIN + _GRACE_PERIOD_MIN
    )
    suppress_gap_alerts = not market_open or in_grace_period

    # -- Signal pipeline alerts --

    # No signals generated recently
    if not suppress_gap_alerts:
        latest_signal = health.get("latest_signal_at")
        if latest_signal is not None:
            gap = now - latest_signal
            threshold = timedelta(minutes=config.signal_gap_minutes)
            if gap > threshold:
                gap_min = int(gap.total_seconds() / 60)
                alerts.append(Alert(
                    key="signal_gap",
                    category="signal_pipeline",
                    severity="warning",
                    title=f"No new signals for {gap_min}m",
                    message=(
                        f"Last signal generated {gap_min} minutes ago "
                        f"(threshold: {config.signal_gap_minutes}m)."
                    ),
                ))

    # Too many pending (unresolved) signals
    pending = health.get("pending_signals", 0)
    if (
        config.pending_signals_alert_enabled
        and pending > config.pending_signals_max
    ):
        alerts.append(Alert(
            key="pending_signals_high",
            category="signal_pipeline",
            severity="warning",
            title=f"Pending signals: {pending}",
            message=(
                f"{pending} delivered signals are unresolved "
                f"(threshold: {config.pending_signals_max})."
            ),
        ))

    # Win rate drop (suppress on weekends — no actionable trades)
    is_weekend = now_msk.weekday() >= 5
    wr = health.get("win_rate_7d")
    wr_resolved = health.get("win_rate_7d_resolved", 0)
    if (
        not is_weekend
        and wr is not None
        and wr_resolved >= config.win_rate_min_resolved
        and wr < config.win_rate_min
    ):
        alerts.append(Alert(
            key="win_rate_low",
            category="analytics",
            severity="critical",
            title=f"Win rate dropped to {wr:.1%}",
            message=(
                f"7d delivered win rate is {wr:.1%} "
                f"({wr_resolved} resolved, "
                f"threshold: {config.win_rate_min:.0%})."
            ),
        ))

    # -- Data/ingestion alerts (skip on weekends) --

    if not suppress_gap_alerts:
        # Telegram ingestion gap
        latest_tg = health.get("latest_telegram_at")
        if latest_tg is not None:
            gap = now - latest_tg
            threshold = timedelta(minutes=config.telegram_gap_minutes)
            if gap > threshold:
                gap_min = int(gap.total_seconds() / 60)
                alerts.append(Alert(
                    key="telegram_gap",
                    category="data_ingestion",
                    severity="warning",
                    title=f"No Telegram messages for {gap_min}m",
                    message=(
                        f"Last Telegram message recorded {gap_min} minutes ago "
                        f"(threshold: {config.telegram_gap_minutes}m)."
                    ),
                ))

        # Quote sync gap
        latest_quote = health.get("latest_quote_at")
        if latest_quote is not None:
            gap = now - latest_quote
            threshold = timedelta(minutes=config.quote_gap_minutes)
            if gap > threshold:
                gap_min = int(gap.total_seconds() / 60)
                alerts.append(Alert(
                    key="quote_gap",
                    category="data_ingestion",
                    severity="warning",
                    title=f"No quotes for {gap_min}m",
                    message=(
                        f"Last quote fetched {gap_min} minutes ago "
                        f"(threshold: {config.quote_gap_minutes}m)."
                    ),
                ))

        # Global context gap
        latest_gc = health.get("latest_global_context_at")
        if latest_gc is not None:
            gap = now - latest_gc
            threshold = timedelta(minutes=config.global_context_gap_minutes)
            if gap > threshold:
                gap_min = int(gap.total_seconds() / 60)
                alerts.append(Alert(
                    key="global_context_gap",
                    category="data_ingestion",
                    severity="info",
                    title=f"No global context events for {gap_min}m",
                    message=(
                        f"Last global context event fetched {gap_min} minutes ago "
                        f"(threshold: {config.global_context_gap_minutes}m)."
                    ),
                ))

    return alerts


def check_cooldown(
    alert: Alert,
    repository: TradingRepository,
    cooldown_seconds: int,
) -> bool:
    """Return True if alert is within cooldown period (should be skipped)."""
    last_fired = repository.get_last_alert_fired_at(alert.key)
    if last_fired is None:
        return False
    now = datetime.now(UTC)
    return (now - last_fired).total_seconds() < cooldown_seconds


def format_alert_telegram_message(alert: Alert) -> str:
    """Format an alert for Telegram delivery."""
    severity_emoji = {
        "critical": "\u26a0\ufe0f",
        "warning": "\u26a0\ufe0f",
        "info": "\u2139\ufe0f",
    }
    emoji = severity_emoji.get(alert.severity, "\u2139\ufe0f")
    return (
        f"{emoji} [{alert.severity.upper()}] {alert.title}\n\n"
        f"{alert.message}\n\n"
        f"Category: {alert.category}\n"
        f"Key: {alert.key}"
    )


def run_alert_check(
    alerting_config: AlertingConfig,
    delivery_config: SignalDeliveryConfig | None,
    repository: TradingRepository,
    logger: logging.Logger,
    *,
    send: bool = True,
    dry_run: bool = False,
) -> AlertCheckResult:
    """Run all alert checks, apply cooldown, optionally send via Telegram.

    Args:
        send: If True, send alerts via Telegram (requires delivery_config).
        dry_run: If True, evaluate and print but don't persist or send.
    """
    alerts = evaluate_alerts(alerting_config, repository, logger)
    result = AlertCheckResult(
        alerts_evaluated=len(alerts),
        details=[],
    )

    for alert in alerts:
        # Check cooldown
        if not dry_run and check_cooldown(
            alert, repository, alerting_config.cooldown_seconds,
        ):
            result.alerts_cooled_down += 1
            result.details.append(f"[COOLDOWN] {alert.key}: {alert.title}")
            logger.info(
                "alert cooldown active",
                extra={
                    "component": "alerting",
                    "alert_key": alert.key,
                },
            )
            continue

        result.alerts_fired += 1
        text = format_alert_telegram_message(alert)

        # Send via Telegram
        sent = False
        if send and not dry_run and delivery_config is not None:
            sent = _send_alert(alert, text, delivery_config, logger)
            if sent:
                result.alerts_sent += 1

        # Persist alert event
        if not dry_run:
            repository.insert_alert_event(
                alert_key=alert.key,
                alert_category=alert.category,
                severity=alert.severity,
                title=alert.title,
                message=alert.message,
                sent=sent,
            )

        status = "SENT" if sent else ("DRY_RUN" if dry_run else "NOT_SENT")
        result.details.append(f"[{status}] {alert.key}: {alert.title}")

        logger.info(
            "alert fired",
            extra={
                "component": "alerting",
                "alert_key": alert.key,
                "severity": alert.severity,
                "sent": sent,
                "dry_run": dry_run,
            },
        )

    return result


def _send_alert(
    alert: Alert,
    text: str,
    delivery_config: SignalDeliveryConfig,
    logger: logging.Logger,
) -> bool:
    """Send alert message via Telegram."""
    from tinvest_trader.services.signal_delivery import send_telegram_message

    if not delivery_config.bot_token or not delivery_config.chat_id:
        logger.warning(
            "alerting: Telegram credentials not configured",
            extra={"component": "alerting"},
        )
        return False

    return send_telegram_message(
        delivery_config.bot_token,
        delivery_config.chat_id,
        text,
        proxy_host=delivery_config.proxy_host,
        proxy_port=delivery_config.proxy_port,
        proxy_user=delivery_config.proxy_user,
        proxy_pass=delivery_config.proxy_pass,
    )
