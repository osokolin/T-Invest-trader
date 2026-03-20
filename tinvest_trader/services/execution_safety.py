"""Execution safety layer -- pre-check, classify, finalize.

Wraps execution with:
    pre-check -> execute -> classify outcome -> enforce safe state

Never leaves an execution request in an undefined state.
Never retries automatically on unknown outcomes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum

# ---------------------------------------------------------------------------
# Terminal states
# ---------------------------------------------------------------------------

class ExecutionState(Enum):
    """Terminal state of an execution request."""

    EXECUTED = "executed"
    FAILED = "failed"
    REJECTED = "rejected"
    EXPIRED = "expired"
    PENDING_VERIFICATION = "pending_verification"


class ExecutionOutcome(Enum):
    """Outcome classification of an execution attempt."""

    SUCCESS = "success"
    FAILED = "failed"
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExecutionSafetyConfig:
    """Configuration for execution safety checks."""

    enabled: bool = True
    min_time_to_close_seconds: int = 90


@dataclass(frozen=True)
class PreCheckResult:
    """Result of pre-execution safety check."""

    allowed: bool
    reason: str

    @staticmethod
    def ok() -> PreCheckResult:
        return PreCheckResult(allowed=True, reason="pre_check_passed")


@dataclass(frozen=True)
class ExecutionSafetyResult:
    """Full result of execution safety classification."""

    state: ExecutionState
    outcome: ExecutionOutcome | None = None
    reason: str = ""


# ---------------------------------------------------------------------------
# Pre-execution close guard
# ---------------------------------------------------------------------------

def check_pre_execution(
    close_time: datetime | None,
    market_status: str | None,
    config: ExecutionSafetyConfig,
    now: datetime | None = None,
) -> PreCheckResult:
    """Check if execution is safe before submitting.

    Rejects if:
    - market is closed/expired
    - close_time - now < min_time_to_close_seconds
    - safety is disabled -> always allowed
    """
    if not config.enabled:
        return PreCheckResult(allowed=True, reason="safety_disabled")

    if market_status in ("closed", "expired"):
        return PreCheckResult(allowed=False, reason="market_closed")

    if close_time is not None:
        if now is None:
            now = datetime.now(UTC)
        if close_time <= now:
            return PreCheckResult(allowed=False, reason="market_closed")
        remaining = close_time - now
        min_remaining = timedelta(seconds=config.min_time_to_close_seconds)
        if remaining < min_remaining:
            return PreCheckResult(
                allowed=False,
                reason="too_late_to_execute",
            )

    return PreCheckResult.ok()


# ---------------------------------------------------------------------------
# Execution outcome classification
# ---------------------------------------------------------------------------

_UNKNOWN_ERROR_KEYWORDS = (
    "timeout",
    "timed out",
    "connection",
    "reset",
    "eof",
    "broken pipe",
)


def classify_execution_result(
    success: bool | None,
    error: str | None,
    exception: Exception | None = None,
) -> ExecutionOutcome:
    """Classify an execution attempt outcome.

    SUCCESS: explicit success response.
    FAILED: explicit rejection / validation error.
    UNKNOWN: timeout, connection error, ambiguous response.
    """
    # If we got an exception, check if it looks like a transport error
    if exception is not None:
        exc_str = str(exception).lower()
        for keyword in _UNKNOWN_ERROR_KEYWORDS:
            if keyword in exc_str:
                return ExecutionOutcome.UNKNOWN
        # Other exceptions are explicit failures
        return ExecutionOutcome.FAILED

    # Explicit success
    if success is True:
        return ExecutionOutcome.SUCCESS

    # Explicit failure with error message
    if success is False and error:
        error_lower = error.lower()
        for keyword in _UNKNOWN_ERROR_KEYWORDS:
            if keyword in error_lower:
                return ExecutionOutcome.UNKNOWN
        return ExecutionOutcome.FAILED

    # Ambiguous: success is None or False with no error
    if success is None:
        return ExecutionOutcome.UNKNOWN

    return ExecutionOutcome.FAILED


# ---------------------------------------------------------------------------
# Terminal state determination
# ---------------------------------------------------------------------------

def determine_terminal_state(
    pre_check: PreCheckResult | None,
    outcome: ExecutionOutcome | None,
) -> ExecutionSafetyResult:
    """Determine the terminal state for an execution request.

    pre_check fail -> rejected / expired
    success -> executed
    failure -> failed
    unknown -> pending_verification (NO retry)
    """
    # Pre-check blocked execution
    if pre_check is not None and not pre_check.allowed:
        if pre_check.reason in ("market_closed", "too_late_to_execute"):
            return ExecutionSafetyResult(
                state=ExecutionState.EXPIRED,
                reason=pre_check.reason,
            )
        return ExecutionSafetyResult(
            state=ExecutionState.REJECTED,
            reason=pre_check.reason,
        )

    # No outcome means we didn't even attempt
    if outcome is None:
        return ExecutionSafetyResult(
            state=ExecutionState.REJECTED,
            reason="no_execution_attempted",
        )

    if outcome == ExecutionOutcome.SUCCESS:
        return ExecutionSafetyResult(
            state=ExecutionState.EXECUTED,
            outcome=outcome,
            reason="execution_success",
        )

    if outcome == ExecutionOutcome.FAILED:
        return ExecutionSafetyResult(
            state=ExecutionState.FAILED,
            outcome=outcome,
            reason="execution_failed",
        )

    # UNKNOWN -> pending_verification. DO NOT retry.
    return ExecutionSafetyResult(
        state=ExecutionState.PENDING_VERIFICATION,
        outcome=outcome,
        reason="unknown_submit_result",
    )


# ---------------------------------------------------------------------------
# Post-close cleanup
# ---------------------------------------------------------------------------

def check_post_close_expiry(
    state: ExecutionState,
    close_time: datetime | None,
    now: datetime | None = None,
) -> ExecutionState:
    """If request is still pending and market is closed, expire it.

    Only affects PENDING_VERIFICATION state.
    """
    if state != ExecutionState.PENDING_VERIFICATION:
        return state

    if close_time is not None:
        if now is None:
            now = datetime.now(UTC)
        if close_time <= now:
            return ExecutionState.EXPIRED

    return state


# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------

def log_execution_safety(
    logger: logging.Logger,
    ticker: str,
    state: ExecutionState,
    reason: str,
    outcome: ExecutionOutcome | None = None,
) -> None:
    """Log execution safety decision in compact structured format."""
    extra: dict = {
        "component": "execution_safety",
        "ticker": ticker,
        "state": state.value,
        "reason": reason,
    }
    if outcome is not None:
        extra["outcome"] = outcome.value

    if state == ExecutionState.PENDING_VERIFICATION:
        logger.warning(
            "execution_safety: ticker=%s state=%s reason=%s",
            ticker, state.value, reason,
            extra=extra,
        )
    elif state in (ExecutionState.REJECTED, ExecutionState.EXPIRED):
        logger.info(
            "execution_safety: ticker=%s state=%s reason=%s",
            ticker, state.value, reason,
            extra=extra,
        )
    else:
        logger.info(
            "execution_safety: ticker=%s state=%s reason=%s",
            ticker, state.value, reason,
            extra=extra,
        )


# ---------------------------------------------------------------------------
# Debug / CLI output
# ---------------------------------------------------------------------------

def format_safety_debug(
    ticker: str,
    pre_check: PreCheckResult,
    close_time: datetime | None = None,
    config: ExecutionSafetyConfig | None = None,
    now: datetime | None = None,
) -> str:
    """Format a pre-check result for human-readable CLI output."""
    if config is None:
        config = ExecutionSafetyConfig()
    if now is None:
        now = datetime.now(UTC)

    lines: list[str] = []
    lines.append("execution safety debug")
    lines.append(f"ticker: {ticker}")
    lines.append(f"enabled: {config.enabled}")
    lines.append(
        f"min_time_to_close: {config.min_time_to_close_seconds}s",
    )

    if close_time is not None:
        remaining = close_time - now
        lines.append(f"close_time: {close_time.isoformat()}")
        lines.append(f"time_remaining: {remaining.total_seconds():.0f}s")
    else:
        lines.append("close_time: unknown")

    lines.append(f"allowed: {pre_check.allowed}")
    lines.append(f"reason: {pre_check.reason}")

    # Show classification examples
    lines.append("")
    lines.append("outcome classification examples:")
    lines.append(
        f"  success=True  -> {classify_execution_result(True, None).value}",
    )
    lines.append(
        f"  success=False error='rejected' "
        f"-> {classify_execution_result(False, 'rejected').value}",
    )
    lines.append(
        f"  success=False error='timeout' "
        f"-> {classify_execution_result(False, 'timeout').value}",
    )
    lines.append(
        f"  success=None  "
        f"-> {classify_execution_result(None, None).value}",
    )

    return "\n".join(lines)
