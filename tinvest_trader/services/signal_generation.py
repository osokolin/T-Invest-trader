"""Signal generation service -- converts fused features into predictions.

Reads recent fused_signal_features rows, applies threshold-based
filtering, and inserts qualifying signals into signal_predictions.

This is the MISSING LINK between the fusion layer and delivery.

Design:
- Simple rule-based (min messages, min sentiment balance)
- Deterministic, idempotent (dedup by ticker + window + observation_time)
- No ML, no ranking, no portfolio logic
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass(frozen=True)
class SignalGenerationConfig:
    """Thresholds for signal generation from fused features."""

    min_message_count: int = 3
    min_sentiment_balance: float = 0.3
    lookback_minutes: int = 30
    cooldown_minutes: int = 30
    limit: int = 500


@dataclass
class SignalGenerationResult:
    """Summary of one signal generation cycle."""

    rows_seen: int = 0
    candidates_before_dedup: int = 0
    candidates: int = 0
    inserted: int = 0
    skipped_threshold: int = 0
    skipped_duplicate: int = 0
    skipped_ticker_dedup: int = 0
    failed: int = 0
    signals: list[dict] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"rows_seen={self.rows_seen} "
            f"candidates_before_dedup={self.candidates_before_dedup} "
            f"candidates={self.candidates} "
            f"inserted={self.inserted} "
            f"skipped_threshold={self.skipped_threshold} "
            f"skipped_duplicate={self.skipped_duplicate} "
            f"skipped_ticker_dedup={self.skipped_ticker_dedup} "
            f"failed={self.failed}"
        )


@dataclass(frozen=True)
class SignalCandidate:
    """A candidate signal derived from a fused feature row."""

    ticker: str
    direction: str  # "up" or "down"
    confidence: float
    window: str
    observation_time: str  # ISO string for dedup key
    features_json: dict


def evaluate_fused_row(
    row: dict,
    config: SignalGenerationConfig,
) -> SignalCandidate | None:
    """Evaluate a single fused feature row against thresholds.

    Returns a SignalCandidate if thresholds are met, None otherwise.
    """
    msg_count = row.get("sentiment_message_count") or 0
    balance = row.get("sentiment_balance")

    if msg_count < config.min_message_count:
        return None

    if balance is None:
        return None

    balance_f = float(balance)
    abs_balance = abs(balance_f)

    if abs_balance < config.min_sentiment_balance:
        return None

    direction = "up" if balance_f > 0 else "down"
    confidence = min(abs_balance, 1.0)

    return SignalCandidate(
        ticker=row["ticker"],
        direction=direction,
        confidence=round(confidence, 6),
        window=row.get("window", ""),
        observation_time=str(row.get("observation_time", "")),
        features_json={
            "sentiment_message_count": msg_count,
            "sentiment_balance": float(balance),
            "sentiment_positive_avg": (
                float(row["sentiment_positive_avg"])
                if row.get("sentiment_positive_avg") is not None
                else None
            ),
            "sentiment_negative_avg": (
                float(row["sentiment_negative_avg"])
                if row.get("sentiment_negative_avg") is not None
                else None
            ),
            "window": row.get("window", ""),
            "fused_feature_id": row.get("id"),
        },
    )


def select_best_per_ticker(
    candidates: list[SignalCandidate],
) -> list[SignalCandidate]:
    """Select one best candidate per ticker.

    Groups by ticker, picks the candidate with highest absolute
    sentiment balance (confidence). On tie, picks the latest
    observation_time.
    """
    by_ticker: dict[str, list[SignalCandidate]] = {}
    for c in candidates:
        by_ticker.setdefault(c.ticker, []).append(c)

    best: list[SignalCandidate] = []
    for _ticker, group in by_ticker.items():
        winner = max(
            group,
            key=lambda c: (c.confidence, c.observation_time),
        )
        best.append(winner)

    return best


def generate_signals(
    repository: TradingRepository,
    logger: logging.Logger,
    config: SignalGenerationConfig | None = None,
    *,
    dry_run: bool = False,
) -> SignalGenerationResult:
    """Generate signals from recent fused features.

    Reads fused_signal_features within lookback window, evaluates
    thresholds, selects best candidate per ticker, checks for
    duplicates, and inserts new predictions.

    Idempotent: uses (ticker, direction, window, observation_time) as
    dedup key — repeated runs produce the same result.
    """
    cfg = config or SignalGenerationConfig()
    result = SignalGenerationResult()

    rows = repository.list_recent_fused_features(
        lookback_minutes=cfg.lookback_minutes,
        limit=cfg.limit,
    )
    result.rows_seen = len(rows)

    # Phase 1: evaluate all rows against thresholds
    all_candidates: list[SignalCandidate] = []
    for row in rows:
        candidate = evaluate_fused_row(row, cfg)
        if candidate is None:
            result.skipped_threshold += 1
            continue
        all_candidates.append(candidate)

    result.candidates_before_dedup = len(all_candidates)

    # Phase 2: select best candidate per ticker
    final_candidates = select_best_per_ticker(all_candidates)
    result.skipped_ticker_dedup = len(all_candidates) - len(final_candidates)
    result.candidates = len(final_candidates)

    if result.skipped_ticker_dedup > 0:
        logger.info(
            "signal_generation: deduplicated per ticker",
            extra={
                "component": "signal_generation",
                "before": result.candidates_before_dedup,
                "after": result.candidates,
            },
        )

    # Phase 3: dedup check + insert
    for candidate in final_candidates:
        exists = repository.signal_exists_recent(
            ticker=candidate.ticker,
            direction=candidate.direction,
            cooldown_minutes=cfg.cooldown_minutes,
        )
        if exists:
            result.skipped_duplicate += 1
            continue

        if dry_run:
            result.inserted += 1
            result.signals.append({
                "ticker": candidate.ticker,
                "direction": candidate.direction,
                "confidence": candidate.confidence,
                "window": candidate.window,
                "dry_run": True,
            })
            continue

        signal_id = repository.insert_signal_prediction(
            ticker=candidate.ticker,
            signal_type=candidate.direction,
            price_at_signal=None,
            confidence=candidate.confidence,
            source="fusion",
            features_json=candidate.features_json,
        )

        if signal_id is not None:
            result.inserted += 1
            result.signals.append({
                "ticker": candidate.ticker,
                "direction": candidate.direction,
                "confidence": candidate.confidence,
                "window": candidate.window,
                "signal_id": signal_id,
            })
            logger.info(
                "signal_generation: inserted",
                extra={
                    "component": "signal_generation",
                    "ticker": candidate.ticker,
                    "direction": candidate.direction,
                    "confidence": candidate.confidence,
                    "window": candidate.window,
                    "signal_id": signal_id,
                },
            )
        else:
            result.failed += 1

    return result


def format_signal_generation_result(result: SignalGenerationResult) -> str:
    """Format generation result for CLI output."""
    lines = [
        "signal generation report",
        f"  rows_seen: {result.rows_seen}",
        f"  candidates_before_dedup: {result.candidates_before_dedup}",
        f"  candidates: {result.candidates}",
        f"  skipped_ticker_dedup: {result.skipped_ticker_dedup}",
        f"  inserted: {result.inserted}",
        f"  skipped_threshold: {result.skipped_threshold}",
        f"  skipped_duplicate: {result.skipped_duplicate}",
        f"  failed: {result.failed}",
    ]

    if result.signals:
        lines.append("")
        lines.append("signals:")
        for sig in result.signals:
            dr = " [DRY_RUN]" if sig.get("dry_run") else ""
            lines.append(
                f"  {sig['ticker']} {sig['direction']} "
                f"conf={sig['confidence']:.3f}{dr}",
            )

    return "\n".join(lines)
