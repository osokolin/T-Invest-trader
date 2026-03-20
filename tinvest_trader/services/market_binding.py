"""Market binding -- deterministic, explainable instrument selection.

Replaces 'pick best candidate' with:
    candidates -> validate -> classify -> bind OR reject

Never silently picks a wrong market.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum


class BindingStatus(Enum):
    """Outcome of a market binding attempt."""

    MATCHED = "matched"
    AMBIGUOUS = "ambiguous"
    NO_MATCH = "no_match"
    REJECTED = "rejected"


@dataclass(frozen=True)
class CandidateScore:
    """A scored candidate instrument from discovery."""

    ticker: str
    figi: str
    name: str
    score: float
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ValidationResult:
    """Validation outcome for a single candidate."""

    candidate: CandidateScore
    valid: bool
    checks_passed: list[str] = field(default_factory=list)
    checks_failed: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class MarketBindingResult:
    """Full result of a market binding attempt -- always returned."""

    status: BindingStatus
    selected_ticker: str | None = None
    selected_figi: str | None = None
    candidates: list[CandidateScore] = field(default_factory=list)
    validations: list[ValidationResult] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class BindingConfig:
    """Configuration for market binding thresholds."""

    min_score: float = 0.5
    min_gap: float = 0.2
    require_exact_ticker: bool = True
    require_trading_enabled: bool = True


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def normalize_ticker(raw: str) -> str:
    """Normalize ticker to canonical uppercase form."""
    return raw.strip().upper().replace(" ", "")


def normalize_direction(raw: str) -> str:
    """Normalize direction to 'up' or 'down'."""
    lower = raw.strip().lower()
    if lower in ("up", "rise", "buy", "long", "bull", "bullish"):
        return "up"
    if lower in ("down", "fall", "sell", "short", "bear", "bearish"):
        return "down"
    return lower


_WINDOW_ALIASES: dict[str, str] = {
    "1m": "1m", "1min": "1m",
    "5m": "5m", "5min": "5m",
    "15m": "15m", "15min": "15m",
    "1h": "1h", "60m": "1h", "60min": "1h",
    "1d": "1d", "day": "1d", "daily": "1d",
    "7d": "7d", "week": "7d", "weekly": "7d",
    "30d": "30d", "month": "30d", "monthly": "30d",
}


def normalize_window(raw: str) -> str:
    """Normalize window/timeframe to canonical form."""
    key = raw.strip().lower()
    return _WINDOW_ALIASES.get(key, key)


# ---------------------------------------------------------------------------
# Candidate scoring (wraps existing registry data)
# ---------------------------------------------------------------------------

def score_candidates(
    query_ticker: str,
    instruments: list[dict],
) -> list[CandidateScore]:
    """Score instruments against a query ticker.

    Uses deterministic exact/prefix matching -- no fuzzy logic.
    Each instrument dict must have keys: ticker, figi, name.
    """
    query = normalize_ticker(query_ticker)
    if not query:
        return []

    results: list[CandidateScore] = []
    for inst in instruments:
        ticker = (inst.get("ticker") or "").upper()
        figi = inst.get("figi") or ""
        name = inst.get("name") or ""

        # Skip placeholder FIGIs
        if figi.startswith("TICKER:"):
            continue

        score = 0.0
        reasons: list[str] = []

        # Exact ticker match is the primary signal
        if ticker == query:
            score = 1.0
            reasons.append("exact_ticker_match")
        elif ticker.startswith(query) or query.startswith(ticker):
            score = 0.3
            reasons.append("prefix_match")
        else:
            continue  # no match at all

        results.append(CandidateScore(
            ticker=ticker,
            figi=figi,
            name=name,
            score=score,
            reasons=reasons,
        ))

    # Sort by score descending, then by ticker for determinism
    results.sort(key=lambda c: (-c.score, c.ticker))
    return results


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_candidate(
    candidate: CandidateScore,
    query_ticker: str,
    instrument_details: dict | None,
    config: BindingConfig,
) -> ValidationResult:
    """Apply hard validation rules to a single candidate."""
    query = normalize_ticker(query_ticker)
    passed: list[str] = []
    failed: list[str] = []

    # Rule 1: Ticker must match exactly (if required)
    if config.require_exact_ticker:
        if candidate.ticker == query:
            passed.append("exact_ticker")
        else:
            failed.append("ticker_mismatch")
    else:
        passed.append("ticker_check_skipped")

    # Rule 2: FIGI must be real (not placeholder)
    if candidate.figi and not candidate.figi.startswith("TICKER:"):
        passed.append("real_figi")
    else:
        failed.append("placeholder_or_missing_figi")

    # Rule 3: Trading must be enabled (if required and details available)
    if config.require_trading_enabled and instrument_details is not None:
        if instrument_details.get("enabled"):
            passed.append("trading_enabled")
        else:
            # Not a hard failure -- instrument might just not be enabled yet
            passed.append("trading_not_enabled_soft")

    # Rule 4: Score must meet threshold
    if candidate.score >= config.min_score:
        passed.append("score_above_threshold")
    else:
        failed.append(f"score_below_threshold({candidate.score:.2f}<{config.min_score:.2f})")

    return ValidationResult(
        candidate=candidate,
        valid=len(failed) == 0,
        checks_passed=passed,
        checks_failed=failed,
    )


# ---------------------------------------------------------------------------
# Binding engine
# ---------------------------------------------------------------------------

def bind_market(
    query_ticker: str,
    instruments: list[dict],
    config: BindingConfig | None = None,
    logger: logging.Logger | None = None,
) -> MarketBindingResult:
    """Attempt to bind a signal to a specific market instrument.

    Flow: score candidates -> validate -> classify -> bind OR reject.
    """
    if config is None:
        config = BindingConfig()
    if logger is None:
        logger = logging.getLogger(__name__)

    query = normalize_ticker(query_ticker)
    if not query:
        return MarketBindingResult(
            status=BindingStatus.NO_MATCH,
            reasons=["empty_query_ticker"],
        )

    # Step 1: Score candidates
    candidates = score_candidates(query, instruments)

    if not candidates:
        logger.info(
            "market_binding: ticker=%s status=no_match candidates=0",
            query,
            extra={"component": "market_binding"},
        )
        return MarketBindingResult(
            status=BindingStatus.NO_MATCH,
            candidates=[],
            reasons=["no_candidates_found"],
        )

    # Step 2: Validate each candidate
    validations: list[ValidationResult] = []
    for cand in candidates:
        # Find instrument details for this ticker
        details = None
        for inst in instruments:
            if (inst.get("ticker") or "").upper() == cand.ticker:
                details = inst
                break
        validations.append(validate_candidate(cand, query, details, config))

    valid_candidates = [v for v in validations if v.valid]

    # Step 3: Classify result
    if not valid_candidates:
        reasons = []
        for v in validations:
            for f in v.checks_failed:
                reasons.append(f"{v.candidate.ticker}: {f}")
        logger.info(
            "market_binding: ticker=%s status=rejected candidates=%d reasons=%s",
            query, len(candidates), reasons,
            extra={"component": "market_binding"},
        )
        return MarketBindingResult(
            status=BindingStatus.REJECTED,
            candidates=candidates,
            validations=validations,
            reasons=reasons,
        )

    if len(valid_candidates) > 1:
        # Check if gap between top two is significant enough
        top_score = valid_candidates[0].candidate.score
        second_score = valid_candidates[1].candidate.score
        gap = top_score - second_score

        if gap >= config.min_gap:
            # Gap is significant -- take the top one
            selected = valid_candidates[0]
            logger.info(
                "market_binding: ticker=%s status=matched figi=%s score=%.2f gap=%.2f",
                query, selected.candidate.figi, top_score, gap,
                extra={"component": "market_binding"},
            )
            return MarketBindingResult(
                status=BindingStatus.MATCHED,
                selected_ticker=selected.candidate.ticker,
                selected_figi=selected.candidate.figi,
                candidates=candidates,
                validations=validations,
                reasons=[f"top_candidate_gap_sufficient({gap:.2f})"],
            )

        # Ambiguous -- multiple valid candidates with similar scores
        logger.info(
            "market_binding: ticker=%s status=ambiguous candidates=%d gap=%.2f",
            query, len(valid_candidates), gap,
            extra={"component": "market_binding"},
        )
        return MarketBindingResult(
            status=BindingStatus.AMBIGUOUS,
            candidates=candidates,
            validations=validations,
            reasons=[
                f"multiple_valid_candidates({len(valid_candidates)})",
                f"insufficient_gap({gap:.2f}<{config.min_gap:.2f})",
            ],
        )

    # Exactly one valid candidate -- matched
    selected = valid_candidates[0]
    logger.info(
        "market_binding: ticker=%s status=matched figi=%s score=%.2f",
        query, selected.candidate.figi, selected.candidate.score,
        extra={"component": "market_binding"},
    )
    return MarketBindingResult(
        status=BindingStatus.MATCHED,
        selected_ticker=selected.candidate.ticker,
        selected_figi=selected.candidate.figi,
        candidates=candidates,
        validations=validations,
        reasons=["single_valid_candidate"],
    )


# ---------------------------------------------------------------------------
# Debug / CLI output
# ---------------------------------------------------------------------------

def format_binding_debug(result: MarketBindingResult, query: str) -> str:
    """Format a MarketBindingResult for human-readable CLI output."""
    lines: list[str] = []
    lines.append("market binding debug")
    lines.append(f"query: {query}")
    lines.append(f"status: {result.status.value}")

    if result.selected_ticker:
        lines.append(f"selected_ticker: {result.selected_ticker}")
    if result.selected_figi:
        lines.append(f"selected_figi: {result.selected_figi}")

    lines.append(f"candidates: {len(result.candidates)}")

    if result.candidates:
        lines.append("scoring:")
        for c in result.candidates:
            lines.append(f"  {c.ticker} figi={c.figi} score={c.score:.2f} {c.reasons}")

    if result.validations:
        lines.append("validation:")
        for v in result.validations:
            status = "PASS" if v.valid else "FAIL"
            lines.append(f"  {v.candidate.ticker}: {status}")
            if v.checks_passed:
                lines.append(f"    passed: {v.checks_passed}")
            if v.checks_failed:
                lines.append(f"    failed: {v.checks_failed}")

    if result.reasons:
        lines.append(f"reasons: {result.reasons}")

    return "\n".join(lines)
