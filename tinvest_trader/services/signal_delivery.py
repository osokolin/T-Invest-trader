"""Telegram signal delivery service.

GUARDRAIL: DELIVERY layer -- transport only.
- Format and send messages via Telegram Bot API.
- No business logic, no signal filtering, no DB writes (except delivered_at).
- See SYSTEM_GUARDRAILS.md section 10.
"""

from __future__ import annotations

import logging
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


def _is_weekend_msk() -> bool:
    """Return True if current time in Moscow is Saturday or Sunday."""
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo("Europe/Moscow")).weekday() >= 5


@dataclass
class DeliveryResult:
    """Result of a signal delivery attempt."""

    signal_id: int
    sent: bool = False
    error: str | None = None


def format_signal_message(signal: dict) -> str:
    """Format a signal dict into a Telegram-friendly message (legacy)."""
    ticker = signal.get("ticker", "???")
    signal_type = signal.get("signal_type", "???")
    direction = signal_type.upper()

    confidence = signal.get("confidence")
    conf_str = f"{confidence:.2f}" if confidence is not None else "n/a"

    price = signal.get("price_at_signal")
    price_str = f"{price:.2f}" if price is not None else "n/a"

    created_at = signal.get("created_at")
    if isinstance(created_at, datetime):
        msk = timezone(timedelta(hours=3))
        created_msk = created_at.astimezone(msk) if created_at.tzinfo else created_at
        time_str = created_msk.strftime("%Y-%m-%d %H:%M") + " MSK"
    else:
        time_str = str(created_at) if created_at else "n/a"

    source_channel = signal.get("source_channel")
    source_str = source_channel if source_channel else signal.get("source", "n/a")

    outcome = signal.get("outcome_label")
    return_pct = signal.get("return_pct")

    lines = [
        f"\U0001f6a8 Signal: {ticker}",
        f"Direction: {direction}",
        f"Confidence: {conf_str}",
        f"Price: {price_str}",
        f"Time: {time_str}",
        f"Source: {source_str}",
    ]

    if outcome:
        outcome_emoji = {
            "win": "\u2705",
            "loss": "\u274c",
            "neutral": "\u2796",
        }.get(outcome, "")
        lines.append(f"Outcome: {outcome_emoji} {outcome}")

    if return_pct is not None:
        lines.append(f"Return: {return_pct:+.4%}")

    return "\n".join(lines)


def _build_socks5_opener(
    proxy_host: str,
    proxy_port: int,
    proxy_user: str | None = None,
    proxy_pass: str | None = None,
) -> urllib.request.OpenerDirector:
    """Build a urllib opener that tunnels HTTPS through a SOCKS5 proxy."""
    import http.client
    import socket
    import ssl

    import socks

    def _create_connection(
        address: tuple, timeout: float = 10.0,
        _source_address: object = None, **_kw: object,
    ) -> socket.socket:
        sock = socks.socksocket()
        sock.set_proxy(
            socks.SOCKS5, proxy_host, proxy_port,
            username=proxy_user, password=proxy_pass,
        )
        sock.settimeout(timeout)
        sock.connect(address)
        return sock

    class Socks5HTTPSHandler(urllib.request.HTTPSHandler):
        def https_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
            return self.do_open(self._make_connection, req)

        def _make_connection(
            self, host: str, **kwargs: object,
        ) -> http.client.HTTPSConnection:
            timeout = kwargs.get("timeout", 10.0)
            conn = http.client.HTTPSConnection(
                host, timeout=timeout,
                context=ssl.create_default_context(),
            )
            conn._create_connection = _create_connection  # type: ignore[attr-defined]
            return conn

    return urllib.request.build_opener(Socks5HTTPSHandler)


def send_telegram_message(
    bot_token: str,
    chat_id: str,
    text: str,
    *,
    timeout_sec: float = 10.0,
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
    reply_markup: str = "",
) -> bool:
    """Send a message via Telegram Bot API. Returns True on success."""
    import json as _json

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload: dict = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = _json.loads(reply_markup)

    data = _json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        if proxy_host and proxy_port:
            opener = _build_socks5_opener(
                proxy_host, proxy_port,
                proxy_user=proxy_user or None,
                proxy_pass=proxy_pass or None,
            )
            with opener.open(req, timeout=timeout_sec) as resp:
                return resp.status == 200
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            return resp.status == 200
    except (urllib.error.URLError, OSError):
        return False


def _lookup_stats_for_signal(
    signal: dict,
    repository: TradingRepository,
) -> tuple[dict | None, dict | None, dict | None]:
    """Fetch ticker/type/source stats for a signal. Never raises."""
    ticker = signal.get("ticker", "")
    signal_type = signal.get("signal_type", "")
    source_channel = signal.get("source_channel")

    ticker_stats: dict | None = None
    type_stats: dict | None = None
    source_stats: dict | None = None

    try:
        by_ticker = repository.get_signal_stats_by_ticker()
        for row in by_ticker:
            if row.get("ticker") == ticker:
                ticker_stats = row
                break
    except Exception:
        pass

    try:
        by_type = repository.get_signal_stats_by_type()
        for row in by_type:
            if row.get("signal_type") == signal_type:
                type_stats = row
                break
    except Exception:
        pass

    if source_channel:
        try:
            by_source = repository.get_signal_stats_by_source()
            for row in by_source:
                if row.get("source_channel") == source_channel:
                    source_stats = row
                    break
        except Exception:
            pass

    return ticker_stats, type_stats, source_stats


def deliver_signal(
    signal: dict,
    bot_token: str,
    chat_id: str,
    repository: TradingRepository | None = None,
    logger: logging.Logger | None = None,
    *,
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
    severity_config: object | None = None,
) -> DeliveryResult:
    """Format, send, and mark a signal as delivered."""
    from tinvest_trader.services.signal_severity import (
        SeverityConfig,
        classify_signal_severity,
        format_ai_snapshot,
        format_enriched_signal_message,
    )

    signal_id = signal["id"]

    # Compute severity + enriched message
    ticker_stats: dict | None = None
    type_stats: dict | None = None
    source_stats: dict | None = None

    if repository is not None:
        ticker_stats, type_stats, source_stats = _lookup_stats_for_signal(
            signal, repository,
        )

    sev_cfg = severity_config if isinstance(severity_config, SeverityConfig) else None
    severity = classify_signal_severity(
        signal,
        ticker_stats=ticker_stats,
        type_stats=type_stats,
        source_stats=source_stats,
        config=sev_cfg,
    )

    text = format_enriched_signal_message(
        signal, severity,
        ticker_stats=ticker_stats,
        type_stats=type_stats,
    )

    # Append AI agreement snapshot (cached only, never triggers AI)
    import contextlib

    ai_snapshot: dict | None = None
    if repository is not None:
        with contextlib.suppress(Exception):
            ai_snapshot = repository.get_ai_snapshot(signal_id)
    text += "\n" + format_ai_snapshot(ai_snapshot, severity.level)

    # Inline keyboard with action buttons
    import json as _json

    from tinvest_trader.services.bot_commands import build_delivery_keyboard

    keyboard = _json.dumps({
        "inline_keyboard": build_delivery_keyboard(signal_id),
    })

    sent = send_telegram_message(
        bot_token, chat_id, text,
        proxy_host=proxy_host, proxy_port=proxy_port,
        proxy_user=proxy_user, proxy_pass=proxy_pass,
        reply_markup=keyboard,
    )

    if not sent:
        if logger:
            logger.warning(
                "signal delivery failed",
                extra={
                    "component": "signal_delivery",
                    "signal_id": signal_id,
                },
            )
        return DeliveryResult(signal_id=signal_id, sent=False, error="send failed")

    if repository is not None:
        repository.mark_signal_delivered(signal_id)
        repository.update_signal_stage(signal_id, "delivered")

    if logger:
        logger.info(
            "signal delivered",
            extra={
                "component": "signal_delivery",
                "signal_id": signal_id,
                "ticker": signal.get("ticker"),
                "severity": severity.level,
            },
        )

    return DeliveryResult(signal_id=signal_id, sent=True)


def deliver_pending_signals(
    bot_token: str,
    chat_id: str,
    repository: TradingRepository,
    logger: logging.Logger,
    limit: int = 50,
    *,
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
    max_per_cycle: int = 0,
    severity_config: object | None = None,
    dedup_config: object | None = None,
    weekend_high_only: bool = True,
) -> int:
    """Deliver undelivered resolved signals, ordered by severity.

    If max_per_cycle > 0, send at most that many per cycle (HIGH first).
    Semantic dedup suppresses signals whose state hasn't changed
    meaningfully since the last delivery for the same ticker.
    """
    from tinvest_trader.services.signal_delivery_dedup import (
        DeliveryDedupConfig,
        should_deliver_signal,
    )
    from tinvest_trader.services.signal_severity import (
        SeverityConfig,
        classify_signal_severity,
        severity_sort_key,
    )

    signals = repository.list_undelivered_signals(limit=limit)
    if not signals:
        return 0

    dedup_cfg = (
        dedup_config
        if isinstance(dedup_config, DeliveryDedupConfig)
        else None
    )

    # Classify + sort by severity (HIGH first)
    sev_cfg = severity_config if isinstance(severity_config, SeverityConfig) else None

    classified: list[tuple[dict, str]] = []
    for signal in signals:
        ticker_stats, type_stats, source_stats = _lookup_stats_for_signal(
            signal, repository,
        )
        sev = classify_signal_severity(
            signal,
            ticker_stats=ticker_stats,
            type_stats=type_stats,
            source_stats=source_stats,
            config=sev_cfg,
        )
        signal["severity"] = sev.level
        classified.append((signal, sev.level))

    classified.sort(key=lambda x: severity_sort_key(x[1]))

    # Weekend filter: only HIGH signals on Sat/Sun (MSK)
    if weekend_high_only and _is_weekend_msk():
        weekend_before = len(classified)
        classified = [(s, lv) for s, lv in classified if lv == "HIGH"]
        skipped = weekend_before - len(classified)
        if skipped:
            logger.info(
                "weekend filter: suppressed non-HIGH signals",
                extra={
                    "component": "signal_delivery",
                    "suppressed": skipped,
                },
            )

    # Apply max-per-cycle limit
    if max_per_cycle > 0:
        classified = classified[:max_per_cycle]

    sent_count = 0
    suppressed_count = 0
    for signal, _sev_level in classified:
        # Semantic dedup: compare with last delivered for this ticker
        previous = repository.get_last_delivered_signal(signal["ticker"])
        if previous is not None:
            # Attach severity of previous signal for comparison
            prev_ticker_stats, prev_type_stats, prev_source_stats = (
                _lookup_stats_for_signal(previous, repository)
            )
            prev_sev = classify_signal_severity(
                previous,
                ticker_stats=prev_ticker_stats,
                type_stats=prev_type_stats,
                source_stats=prev_source_stats,
                config=sev_cfg,
            )
            previous["severity"] = prev_sev.level

        decision = should_deliver_signal(signal, previous, dedup_cfg)

        if not decision.deliver:
            suppressed_count += 1
            repository.update_signal_stage(
                signal["id"], "suppressed_delivery", decision.reason,
            )
            logger.info(
                "signal delivery suppressed",
                extra={
                    "component": "signal_delivery",
                    "signal_id": signal["id"],
                    "ticker": signal.get("ticker"),
                    "reason": decision.reason,
                },
            )
            continue

        result = deliver_signal(
            signal, bot_token, chat_id,
            repository=repository, logger=logger,
            proxy_host=proxy_host, proxy_port=proxy_port,
            proxy_user=proxy_user, proxy_pass=proxy_pass,
            severity_config=severity_config,
        )
        if result.sent:
            sent_count += 1

    logger.info(
        "delivery cycle complete",
        extra={
            "component": "signal_delivery",
            "total": len(signals),
            "sent": sent_count,
            "suppressed": suppressed_count,
        },
    )
    return sent_count
