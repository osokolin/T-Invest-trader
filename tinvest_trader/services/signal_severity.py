"""Signal severity classification -- deterministic, rule-based.

Assigns HIGH / MEDIUM / LOW severity to each signal based on
confidence, expected value, and source/ticker performance stats.
No ML, no adaptive scoring. Pure config-driven rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SeverityConfig:
    """Thresholds for severity classification."""

    high_confidence: float = 0.6
    high_ev: float = 0.02
    min_resolved_for_stats: int = 3


@dataclass(frozen=True)
class SeverityResult:
    """Severity classification outcome."""

    level: str  # HIGH, MEDIUM, LOW
    reasons: list[str] = field(default_factory=list)


def _compute_ev(win_rate: float, avg_return: float) -> float:
    return win_rate * avg_return


def classify_signal_severity(
    signal: dict,
    ticker_stats: dict | None = None,
    type_stats: dict | None = None,
    source_stats: dict | None = None,
    config: SeverityConfig | None = None,
) -> SeverityResult:
    """Classify a signal into HIGH / MEDIUM / LOW severity.

    Rules (deterministic, checked in order):
    - HIGH requires: confidence >= threshold AND positive EV (if stats available)
    - LOW if: confidence is near minimum OR negative EV
    - MEDIUM otherwise (default)

    Missing stats never block -- they just reduce severity confidence.
    """
    cfg = config or SeverityConfig()
    reasons: list[str] = []
    score = 0  # accumulate points; HIGH >= 3, LOW <= 0, else MEDIUM

    # -- confidence --
    confidence = signal.get("confidence")
    if confidence is not None:
        if confidence >= cfg.high_confidence:
            score += 2
            reasons.append(f"confidence {confidence:.2f} >= {cfg.high_confidence}")
        elif confidence >= cfg.high_confidence * 0.7:
            score += 1
            reasons.append(f"confidence {confidence:.2f} (moderate)")
        else:
            reasons.append(f"confidence {confidence:.2f} (low)")

    # -- ticker EV --
    if ticker_stats and ticker_stats.get("resolved", 0) >= cfg.min_resolved_for_stats:
        wins = ticker_stats.get("wins", 0)
        resolved = ticker_stats["resolved"]
        avg_ret = ticker_stats.get("avg_return", 0.0) or 0.0
        wr = wins / resolved if resolved else 0.0
        ev = _compute_ev(wr, avg_ret)
        if ev > cfg.high_ev:
            score += 2
            reasons.append(f"ticker EV {ev:+.4f} (strong)")
        elif ev > 0:
            score += 1
            reasons.append(f"ticker EV {ev:+.4f} (positive)")
        else:
            score -= 1
            reasons.append(f"ticker EV {ev:+.4f} (weak)")

    # -- signal type win rate --
    if type_stats and type_stats.get("resolved", 0) >= cfg.min_resolved_for_stats:
        wins = type_stats.get("wins", 0)
        resolved = type_stats["resolved"]
        wr = wins / resolved if resolved else 0.0
        if wr > 0.55:
            score += 1
            reasons.append(f"type win rate {wr:.0%}")
        elif wr < 0.40:
            score -= 1
            reasons.append(f"type win rate {wr:.0%} (low)")

    # -- source stats --
    if source_stats and source_stats.get("resolved", 0) >= cfg.min_resolved_for_stats:
        wins = source_stats.get("wins", 0)
        resolved = source_stats["resolved"]
        avg_ret = source_stats.get("avg_return", 0.0) or 0.0
        wr = wins / resolved if resolved else 0.0
        ev = _compute_ev(wr, avg_ret)
        if ev > 0:
            score += 1
            reasons.append(f"source EV {ev:+.4f}")

    # -- classify --
    if score >= 3:
        return SeverityResult(level="HIGH", reasons=reasons)
    if score <= 0:
        return SeverityResult(level="LOW", reasons=reasons)
    return SeverityResult(level="MEDIUM", reasons=reasons)


# -- Formatting helpers for enriched Telegram messages --

_SEVERITY_EMOJI = {
    "HIGH": "\U0001f525",   # fire
    "MEDIUM": "\u26a0\ufe0f",  # warning
    "LOW": "\u2139\ufe0f",     # info
}

_SEVERITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


def severity_sort_key(severity_level: str) -> int:
    """Sort key: HIGH=0, MEDIUM=1, LOW=2."""
    return _SEVERITY_ORDER.get(severity_level, 2)


def format_enriched_signal_message(
    signal: dict,
    severity: SeverityResult,
    ticker_stats: dict | None = None,
    type_stats: dict | None = None,
) -> str:
    """Format an enriched Telegram message for a signal."""
    from datetime import datetime

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
    source_str = source_channel or signal.get("source") or None

    emoji = _SEVERITY_EMOJI.get(severity.level, "")

    # Header
    lines = [f"{emoji} {severity.level} -- {ticker}"]

    # Core fields
    lines.append(f"Direction: {direction}")
    lines.append(f"Confidence: {conf_str}")
    lines.append(f"Price: {price_str}")
    lines.append(f"Time: {time_str}")
    if source_str:
        lines.append(f"Source: {source_str}")

    # Stats section (compact)
    stats_lines: list[str] = []
    if ticker_stats and ticker_stats.get("resolved", 0) > 0:
        wins = ticker_stats.get("wins", 0)
        resolved = ticker_stats["resolved"]
        avg_ret = ticker_stats.get("avg_return", 0.0) or 0.0
        wr = wins / resolved
        ev = _compute_ev(wr, avg_ret)
        stats_lines.append(f"Ticker: {wr:.0%} win, EV {ev:+.4f} ({resolved})")

    if type_stats and type_stats.get("resolved", 0) > 0:
        wins = type_stats.get("wins", 0)
        resolved = type_stats["resolved"]
        wr = wins / resolved
        stats_lines.append(f"Type {direction}: {wr:.0%} win ({resolved})")

    if stats_lines:
        lines.extend(stats_lines)

    # Outcome (for already resolved signals)
    outcome = signal.get("outcome_label")
    return_pct = signal.get("return_pct")

    if outcome:
        outcome_emoji = {
            "win": "\u2705", "loss": "\u274c", "neutral": "\u2796",
        }.get(outcome, "")
        lines.append(f"Outcome: {outcome_emoji} {outcome}")

    if return_pct is not None:
        lines.append(f"Return: {return_pct:+.4%}")

    # Pass reasons (compact, max 3)
    if severity.reasons:
        shown = severity.reasons[:3]
        lines.append("Passed: " + " | ".join(shown))

    return "\n".join(lines)
