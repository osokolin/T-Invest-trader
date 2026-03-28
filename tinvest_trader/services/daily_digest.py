"""Daily digest -- concise operator summary sent via Telegram.

Aggregates 24h signal performance, pipeline stats, top sources/tickers,
AI agreement, and shadow experiment results into a short, readable message.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.app.config import SignalDeliveryConfig
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass
class DigestData:
    """Structured daily digest data."""

    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    signals_total: int = 0
    signals_delivered: int = 0
    resolved: int = 0
    win_rate: float | None = None
    avg_return: float | None = None
    rejected_calibration: int = 0
    rejected_binding: int = 0
    rejected_safety: int = 0
    top_sources: list[dict] = field(default_factory=list)
    top_tickers: list[dict] = field(default_factory=list)
    ai_total: int = 0
    ai_agreed: int = 0
    shadow_weight_ev_strong: float | None = None
    shadow_weight_ev_weak: float | None = None
    shadow_ai_gating: dict | None = None
    shadow_global_alignment: dict | None = None
    best_signal: dict | None = None
    worst_signal: dict | None = None


def build_daily_digest(
    repository: TradingRepository,
    lookback_hours: int = 24,
) -> DigestData:
    """Build digest data from repository queries."""
    raw = repository.get_daily_digest_data(lookback_hours=lookback_hours)
    return DigestData(
        signals_total=raw.get("signals_total", 0),
        signals_delivered=raw.get("signals_delivered", 0),
        resolved=raw.get("resolved", 0),
        win_rate=raw.get("win_rate"),
        avg_return=raw.get("avg_return"),
        rejected_calibration=raw.get("rejected_calibration", 0),
        rejected_binding=raw.get("rejected_binding", 0),
        rejected_safety=raw.get("rejected_safety", 0),
        top_sources=raw.get("top_sources", []),
        top_tickers=raw.get("top_tickers", []),
        ai_total=raw.get("ai_total", 0),
        ai_agreed=raw.get("ai_agreed", 0),
        shadow_weight_ev_strong=raw.get("shadow_weight_ev_strong"),
        shadow_weight_ev_weak=raw.get("shadow_weight_ev_weak"),
        shadow_ai_gating=raw.get("shadow_ai_gating"),
        shadow_global_alignment=raw.get("shadow_global_alignment"),
        best_signal=raw.get("best_signal"),
        worst_signal=raw.get("worst_signal"),
    )


def format_daily_digest(data: DigestData, *, is_weekly: bool = False) -> str:
    """Format digest data into a compact Telegram-friendly message."""
    lines: list[str] = []

    # Header
    header = "Weekly Summary (7d)" if is_weekly else "Daily Summary (24h)"
    lines.append(header)
    lines.append("")

    # Core metrics
    if data.signals_total == 0:
        lines.append("No signals generated.")
        return "\n".join(lines)

    delivery = f"Signals: {data.signals_total}"
    if data.signals_delivered > 0:
        delivery += f" -> {data.signals_delivered} delivered"
    lines.append(delivery)

    if data.resolved > 0 and data.win_rate is not None:
        parts = [f"Win rate: {data.win_rate:.0%}"]
        if data.avg_return is not None:
            sign = "+" if data.avg_return >= 0 else ""
            parts.append(f"Avg return: {sign}{data.avg_return:.2%}")
        lines.append(" | ".join(parts))

    # Pipeline rejections (only non-zero)
    rejections = []
    if data.rejected_calibration > 0:
        rejections.append(f"calibration: {data.rejected_calibration}")
    if data.rejected_binding > 0:
        rejections.append(f"binding: {data.rejected_binding}")
    if data.rejected_safety > 0:
        rejections.append(f"safety: {data.rejected_safety}")
    if rejections:
        lines.append(f"Rejected: {', '.join(rejections)}")

    # Top sources
    if data.top_sources:
        lines.append("")
        lines.append("Top sources:")
        for src in data.top_sources:
            ev = src.get("ev")
            if ev is not None:
                ev_f = float(ev)
                sign = "+" if ev_f >= 0 else ""
                lines.append(
                    f"  {src['source_channel']} ({sign}{ev_f:.2%} EV)",
                )

    # Top tickers
    if data.top_tickers:
        lines.append("")
        lines.append("Top tickers:")
        for t in data.top_tickers:
            ret = t.get("avg_return")
            if ret is not None:
                ret_f = float(ret)
                sign = "+" if ret_f >= 0 else ""
                lines.append(f"  {t['ticker']} ({sign}{ret_f:.2%})")

    # AI agreement
    if data.ai_total > 0:
        rate = data.ai_agreed / data.ai_total
        lines.append("")
        lines.append(f"AI agreement: {rate:.0%} ({data.ai_total} analyzed)")

    # Shadow experiments (only if data exists)
    shadow_lines: list[str] = []

    if (
        data.shadow_weight_ev_strong is not None
        and data.shadow_weight_ev_weak is not None
    ):
        delta = data.shadow_weight_ev_strong - data.shadow_weight_ev_weak
        sign = "+" if delta >= 0 else ""
        shadow_lines.append(f"  weighting: {sign}{delta:.2%} EV delta")

    if data.shadow_ai_gating:
        allow_ev = data.shadow_ai_gating.get("ALLOW")
        block_ev = data.shadow_ai_gating.get("BLOCK")
        if allow_ev is not None and block_ev is not None:
            delta = allow_ev - block_ev
            sign = "+" if delta >= 0 else ""
            shadow_lines.append(f"  AI gating: {sign}{delta:.2%} EV delta")
        elif allow_ev is not None:
            sign = "+" if allow_ev >= 0 else ""
            shadow_lines.append(f"  AI gating: ALLOW {sign}{allow_ev:.2%} EV")

    if data.shadow_global_alignment:
        aligned = data.shadow_global_alignment.get("aligned")
        against = data.shadow_global_alignment.get("against")
        if aligned and against:
            a_wr = aligned.get("win_rate")
            g_wr = against.get("win_rate")
            if a_wr is not None and g_wr is not None:
                delta = a_wr - g_wr
                sign = "+" if delta >= 0 else ""
                shadow_lines.append(
                    f"  global alignment: {sign}{delta:.0%} win rate delta",
                )

    if shadow_lines:
        lines.append("")
        lines.append("Shadow:")
        lines.extend(shadow_lines)

    # Best/worst signal
    extremes: list[str] = []
    if data.best_signal:
        ret = data.best_signal["return_pct"]
        extremes.append(
            f"Best: {data.best_signal['ticker']} +{ret:.2%}",
        )
    if data.worst_signal:
        ret = data.worst_signal["return_pct"]
        extremes.append(
            f"Worst: {data.worst_signal['ticker']} {ret:.2%}",
        )
    if extremes:
        lines.append("")
        lines.extend(extremes)

    text = "\n".join(lines)
    # Truncate to Telegram-safe length
    if len(text) > 1000:
        text = text[:997] + "..."
    return text


def send_daily_digest(
    repository: TradingRepository,
    delivery_config: SignalDeliveryConfig,
    logger: logging.Logger,
    *,
    dry_run: bool = False,
    lookback_hours: int = 24,
    skip_weekends: bool = True,
) -> dict:
    """Build, format, and optionally send daily digest.

    On weekends (Saturday/Sunday MSK) the digest is skipped.
    On Friday evenings it becomes a weekly summary (7d lookback).

    Returns dict with keys: text, sent, dry_run.
    """
    from zoneinfo import ZoneInfo

    now_msk = datetime.now(ZoneInfo("Europe/Moscow"))
    weekday = now_msk.weekday()  # Mon=0 … Sun=6

    if skip_weekends and weekday >= 5:
        logger.info(
            "daily digest skipped (weekend)",
            extra={"component": "daily_digest", "weekday": weekday},
        )
        return {"text": "", "sent": False, "dry_run": dry_run,
                "signals_total": 0, "signals_delivered": 0, "skipped": True}

    # Friday → weekly summary
    is_weekly = weekday == 4
    effective_lookback = 168 if is_weekly else lookback_hours

    data = build_daily_digest(repository, lookback_hours=effective_lookback)
    text = format_daily_digest(data, is_weekly=is_weekly)

    result = {
        "text": text,
        "sent": False,
        "dry_run": dry_run,
        "signals_total": data.signals_total,
        "signals_delivered": data.signals_delivered,
    }

    logger.info(
        "daily_digest",
        extra={
            "component": "daily_digest",
            "signals": data.signals_total,
            "delivered": data.signals_delivered,
            "dry_run": dry_run,
        },
    )

    if dry_run:
        return result

    from tinvest_trader.services.signal_delivery import send_telegram_message

    if not delivery_config.bot_token or not delivery_config.chat_id:
        logger.warning(
            "daily digest: Telegram credentials not configured",
            extra={"component": "daily_digest"},
        )
        return result

    sent = send_telegram_message(
        delivery_config.bot_token,
        delivery_config.chat_id,
        text,
        proxy_host=delivery_config.proxy_host,
        proxy_port=delivery_config.proxy_port,
        proxy_user=delivery_config.proxy_user,
        proxy_pass=delivery_config.proxy_pass,
    )
    result["sent"] = sent

    if sent:
        # Record in alert_events for dedup
        repository.insert_alert_event(
            alert_key="daily_digest",
            alert_category="digest",
            severity="info",
            title="Daily digest sent",
            message="",
            sent=True,
        )

    return result


def is_digest_already_sent_today(
    repository: TradingRepository,
) -> bool:
    """Check if daily digest was already sent today (UTC)."""
    last_fired = repository.get_last_alert_fired_at("daily_digest")
    if last_fired is None:
        return False
    today = datetime.now(UTC).date()
    return last_fired.date() == today
