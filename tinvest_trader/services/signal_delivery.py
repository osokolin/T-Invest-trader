"""Telegram signal delivery service."""

from __future__ import annotations

import logging
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass
class DeliveryResult:
    """Result of a signal delivery attempt."""

    signal_id: int
    sent: bool = False
    error: str | None = None


def format_signal_message(signal: dict) -> str:
    """Format a signal dict into a Telegram-friendly message."""
    ticker = signal.get("ticker", "???")
    signal_type = signal.get("signal_type", "???")
    direction = signal_type.upper()

    confidence = signal.get("confidence")
    conf_str = f"{confidence:.2f}" if confidence is not None else "n/a"

    price = signal.get("price_at_signal")
    price_str = f"{price:.2f}" if price is not None else "n/a"

    created_at = signal.get("created_at")
    if isinstance(created_at, datetime):
        time_str = created_at.strftime("%Y-%m-%d %H:%M")
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

    def _create_connection(address: tuple, timeout: float = 10.0, **_kw: object) -> socket.socket:
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
) -> bool:
    """Send a message via Telegram Bot API. Returns True on success."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }).encode("utf-8")

    req = urllib.request.Request(url, data=data, method="POST")
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
) -> DeliveryResult:
    """Format, send, and mark a signal as delivered."""
    signal_id = signal["id"]

    text = format_signal_message(signal)
    sent = send_telegram_message(
        bot_token, chat_id, text,
        proxy_host=proxy_host, proxy_port=proxy_port,
        proxy_user=proxy_user, proxy_pass=proxy_pass,
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

    if logger:
        logger.info(
            "signal delivered",
            extra={
                "component": "signal_delivery",
                "signal_id": signal_id,
                "ticker": signal.get("ticker"),
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
) -> int:
    """Deliver all undelivered resolved signals. Returns count sent."""
    signals = repository.list_undelivered_signals(limit=limit)
    if not signals:
        return 0

    sent_count = 0
    for signal in signals:
        result = deliver_signal(
            signal, bot_token, chat_id,
            repository=repository, logger=logger,
            proxy_host=proxy_host, proxy_port=proxy_port,
            proxy_user=proxy_user, proxy_pass=proxy_pass,
        )
        if result.sent:
            sent_count += 1

    logger.info(
        "delivery cycle complete",
        extra={
            "component": "signal_delivery",
            "total": len(signals),
            "sent": sent_count,
        },
    )
    return sent_count
