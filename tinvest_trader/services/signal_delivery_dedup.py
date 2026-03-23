"""Semantic deduplication for signal delivery.

Compares a pending signal with the last delivered signal for the same
ticker.  If the state has not changed meaningfully the delivery is
suppressed so that the user does not receive identical Telegram messages
every minute.

GUARDRAIL: DELIVERY layer only -- no generation / execution changes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class DeliveryDedupConfig:
    """Thresholds for semantic dedup."""

    enabled: bool = True
    confidence_delta: float = 0.10
    repeat_after_minutes: int = 240


@dataclass(frozen=True)
class DeliveryDecision:
    """Result of the should-deliver check."""

    deliver: bool
    reason: str


def should_deliver_signal(
    current: dict,
    previous: dict | None,
    config: DeliveryDedupConfig | None = None,
) -> DeliveryDecision:
    """Decide whether *current* signal should be sent to Telegram.

    Parameters
    ----------
    current:
        Signal row dict (from ``list_undelivered_signals``).
        Must contain: ticker, signal_type, confidence.
        May contain: severity (str).
    previous:
        Last delivered signal for the same ticker, or ``None``.
        Same shape plus ``delivered_at`` (datetime).
    config:
        Dedup thresholds.

    Returns
    -------
    DeliveryDecision with deliver=True/False and a human-readable reason.
    """
    cfg = config or DeliveryDedupConfig()

    if not cfg.enabled:
        return DeliveryDecision(deliver=True, reason="dedup_disabled")

    # No prior delivery -> always send
    if previous is None:
        return DeliveryDecision(deliver=True, reason="no_previous")

    # 1. Direction changed
    cur_dir = (current.get("signal_type") or "").lower()
    prev_dir = (previous.get("signal_type") or "").lower()
    if cur_dir != prev_dir:
        return DeliveryDecision(deliver=True, reason="direction_changed")

    # 2. Severity changed
    cur_sev = (current.get("severity") or "").upper()
    prev_sev = (previous.get("severity") or "").upper()
    if cur_sev and prev_sev and cur_sev != prev_sev:
        return DeliveryDecision(deliver=True, reason="severity_changed")

    # 3. Confidence jump
    cur_conf = current.get("confidence")
    prev_conf = previous.get("confidence")
    if cur_conf is not None and prev_conf is not None:
        delta = abs(float(cur_conf) - float(prev_conf))
        if delta >= cfg.confidence_delta:
            return DeliveryDecision(deliver=True, reason="confidence_jump")

    # 4. Repeat-after timeout
    prev_delivered = previous.get("delivered_at")
    if isinstance(prev_delivered, datetime):
        now = datetime.now(UTC)
        if prev_delivered.tzinfo is None:
            prev_delivered = prev_delivered.replace(tzinfo=UTC)
        elapsed = (now - prev_delivered).total_seconds() / 60.0
        if elapsed >= cfg.repeat_after_minutes:
            return DeliveryDecision(deliver=True, reason="repeat_after_timeout")

    # Nothing meaningful changed -> suppress
    return DeliveryDecision(deliver=False, reason="same_state_suppressed")
