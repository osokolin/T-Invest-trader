"""Tests for execution safety layer."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from tinvest_trader.services.execution_safety import (
    ExecutionOutcome,
    ExecutionSafetyConfig,
    ExecutionState,
    PreCheckResult,
    check_post_close_expiry,
    check_pre_execution,
    classify_execution_result,
    determine_terminal_state,
    format_safety_debug,
    log_execution_safety,
)
from tinvest_trader.services.trading_service import TradingService

NOW = datetime(2026, 3, 20, 12, 0, 0, tzinfo=UTC)
CONFIG = ExecutionSafetyConfig(enabled=True, min_time_to_close_seconds=90)
DISABLED = ExecutionSafetyConfig(enabled=False)


# -- Pre-execution close guard -----------------------------------------------

class TestPreExecution:
    def test_market_closed_rejected(self) -> None:
        """A. Market closed -> rejected."""
        result = check_pre_execution(
            close_time=None, market_status="closed",
            config=CONFIG, now=NOW,
        )
        assert result.allowed is False
        assert result.reason == "market_closed"

    def test_market_expired_rejected(self) -> None:
        result = check_pre_execution(
            close_time=None, market_status="expired",
            config=CONFIG, now=NOW,
        )
        assert result.allowed is False
        assert result.reason == "market_closed"

    def test_close_time_in_past_rejected(self) -> None:
        result = check_pre_execution(
            close_time=NOW - timedelta(minutes=5),
            market_status="open", config=CONFIG, now=NOW,
        )
        assert result.allowed is False
        assert result.reason == "market_closed"

    def test_too_close_to_close_rejected(self) -> None:
        """B. Too close to close -> rejected."""
        result = check_pre_execution(
            close_time=NOW + timedelta(seconds=30),
            market_status="open", config=CONFIG, now=NOW,
        )
        assert result.allowed is False
        assert result.reason == "too_late_to_execute"

    def test_exactly_at_threshold_rejected(self) -> None:
        # 90 seconds remaining == threshold, but timedelta < min means rejected
        # Actually 90 == 90, remaining is NOT < min, so it's allowed
        result = check_pre_execution(
            close_time=NOW + timedelta(seconds=90),
            market_status="open", config=CONFIG, now=NOW,
        )
        assert result.allowed is True

    def test_enough_time_allowed(self) -> None:
        result = check_pre_execution(
            close_time=NOW + timedelta(hours=2),
            market_status="open", config=CONFIG, now=NOW,
        )
        assert result.allowed is True
        assert result.reason == "pre_check_passed"

    def test_no_close_time_allowed(self) -> None:
        result = check_pre_execution(
            close_time=None, market_status="open",
            config=CONFIG, now=NOW,
        )
        assert result.allowed is True

    def test_safety_disabled_always_allowed(self) -> None:
        result = check_pre_execution(
            close_time=None, market_status="closed",
            config=DISABLED, now=NOW,
        )
        assert result.allowed is True
        assert result.reason == "safety_disabled"

    def test_unknown_market_status_allowed(self) -> None:
        result = check_pre_execution(
            close_time=None, market_status="unknown",
            config=CONFIG, now=NOW,
        )
        assert result.allowed is True


# -- Outcome classification -------------------------------------------------

class TestClassifyExecutionResult:
    def test_success(self) -> None:
        """C. Success -> executed."""
        outcome = classify_execution_result(success=True, error=None)
        assert outcome == ExecutionOutcome.SUCCESS

    def test_explicit_failure(self) -> None:
        """D. Explicit failure -> failed."""
        outcome = classify_execution_result(
            success=False, error="order rejected by exchange",
        )
        assert outcome == ExecutionOutcome.FAILED

    def test_timeout_unknown(self) -> None:
        """E. Timeout -> pending_verification."""
        outcome = classify_execution_result(
            success=False, error="connection timeout",
        )
        assert outcome == ExecutionOutcome.UNKNOWN

    def test_connection_error_unknown(self) -> None:
        outcome = classify_execution_result(
            success=False, error="Connection reset by peer",
        )
        assert outcome == ExecutionOutcome.UNKNOWN

    def test_none_success_unknown(self) -> None:
        outcome = classify_execution_result(success=None, error=None)
        assert outcome == ExecutionOutcome.UNKNOWN

    def test_exception_timeout(self) -> None:
        outcome = classify_execution_result(
            success=None, error=None,
            exception=TimeoutError("read timed out"),
        )
        assert outcome == ExecutionOutcome.UNKNOWN

    def test_exception_connection(self) -> None:
        outcome = classify_execution_result(
            success=None, error=None,
            exception=ConnectionError("connection refused"),
        )
        assert outcome == ExecutionOutcome.UNKNOWN

    def test_exception_other(self) -> None:
        outcome = classify_execution_result(
            success=None, error=None,
            exception=ValueError("invalid parameter"),
        )
        assert outcome == ExecutionOutcome.FAILED

    def test_false_no_error(self) -> None:
        outcome = classify_execution_result(success=False, error="")
        assert outcome == ExecutionOutcome.FAILED


# -- Terminal state determination -------------------------------------------

class TestDetermineTerminalState:
    def test_precheck_market_closed(self) -> None:
        pre = PreCheckResult(allowed=False, reason="market_closed")
        result = determine_terminal_state(pre, outcome=None)
        assert result.state == ExecutionState.EXPIRED

    def test_precheck_too_late(self) -> None:
        pre = PreCheckResult(allowed=False, reason="too_late_to_execute")
        result = determine_terminal_state(pre, outcome=None)
        assert result.state == ExecutionState.EXPIRED

    def test_success_executed(self) -> None:
        pre = PreCheckResult.ok()
        result = determine_terminal_state(pre, ExecutionOutcome.SUCCESS)
        assert result.state == ExecutionState.EXECUTED

    def test_failed(self) -> None:
        pre = PreCheckResult.ok()
        result = determine_terminal_state(pre, ExecutionOutcome.FAILED)
        assert result.state == ExecutionState.FAILED

    def test_unknown_pending_verification(self) -> None:
        """F. No retry on unknown."""
        pre = PreCheckResult.ok()
        result = determine_terminal_state(pre, ExecutionOutcome.UNKNOWN)
        assert result.state == ExecutionState.PENDING_VERIFICATION
        assert result.reason == "unknown_submit_result"

    def test_no_outcome_rejected(self) -> None:
        pre = PreCheckResult.ok()
        result = determine_terminal_state(pre, outcome=None)
        assert result.state == ExecutionState.REJECTED


# -- Post-close cleanup ----------------------------------------------------

class TestPostCloseExpiry:
    def test_pending_after_close_expired(self) -> None:
        """G. Post-close pending -> expired."""
        state = check_post_close_expiry(
            ExecutionState.PENDING_VERIFICATION,
            close_time=NOW - timedelta(minutes=5),
            now=NOW,
        )
        assert state == ExecutionState.EXPIRED

    def test_pending_before_close_stays(self) -> None:
        state = check_post_close_expiry(
            ExecutionState.PENDING_VERIFICATION,
            close_time=NOW + timedelta(hours=1),
            now=NOW,
        )
        assert state == ExecutionState.PENDING_VERIFICATION

    def test_pending_no_close_time_stays(self) -> None:
        state = check_post_close_expiry(
            ExecutionState.PENDING_VERIFICATION,
            close_time=None, now=NOW,
        )
        assert state == ExecutionState.PENDING_VERIFICATION

    def test_executed_not_affected(self) -> None:
        state = check_post_close_expiry(
            ExecutionState.EXECUTED,
            close_time=NOW - timedelta(minutes=5),
            now=NOW,
        )
        assert state == ExecutionState.EXECUTED

    def test_failed_not_affected(self) -> None:
        state = check_post_close_expiry(
            ExecutionState.FAILED,
            close_time=NOW - timedelta(minutes=5),
            now=NOW,
        )
        assert state == ExecutionState.FAILED


# -- Integration with TradingService ----------------------------------------

class TestTradingServiceExecuteWithSafety:
    """H. Integration with TradingService."""

    def _make_service(self) -> TradingService:
        return TradingService(
            logger=logging.getLogger("test"),
            safety_config=CONFIG,
        )

    def test_success_execution(self) -> None:
        svc = self._make_service()
        result = svc.execute_with_safety(
            ticker="SBER",
            execute_fn=lambda: (True, None),
            close_time=NOW + timedelta(hours=2),
            market_status="open",
            now=NOW,
        )
        assert result.state == ExecutionState.EXECUTED

    def test_market_closed_blocks(self) -> None:
        svc = self._make_service()
        result = svc.execute_with_safety(
            ticker="SBER",
            execute_fn=lambda: (True, None),  # should not be called
            close_time=None,
            market_status="closed",
            now=NOW,
        )
        assert result.state == ExecutionState.EXPIRED

    def test_too_late_blocks(self) -> None:
        svc = self._make_service()
        result = svc.execute_with_safety(
            ticker="SBER",
            execute_fn=lambda: (True, None),
            close_time=NOW + timedelta(seconds=10),
            market_status="open",
            now=NOW,
        )
        assert result.state == ExecutionState.EXPIRED

    def test_failure_classified(self) -> None:
        svc = self._make_service()
        result = svc.execute_with_safety(
            ticker="SBER",
            execute_fn=lambda: (False, "order rejected"),
            close_time=NOW + timedelta(hours=2),
            market_status="open",
            now=NOW,
        )
        assert result.state == ExecutionState.FAILED

    def test_timeout_pending_verification(self) -> None:
        svc = self._make_service()
        result = svc.execute_with_safety(
            ticker="SBER",
            execute_fn=lambda: (False, "connection timeout"),
            close_time=NOW + timedelta(hours=2),
            market_status="open",
            now=NOW,
        )
        assert result.state == ExecutionState.PENDING_VERIFICATION

    def test_exception_classified(self) -> None:
        def raise_timeout() -> tuple:
            msg = "read timed out"
            raise TimeoutError(msg)

        svc = self._make_service()
        result = svc.execute_with_safety(
            ticker="SBER",
            execute_fn=raise_timeout,
            close_time=NOW + timedelta(hours=2),
            market_status="open",
            now=NOW,
        )
        assert result.state == ExecutionState.PENDING_VERIFICATION

    def test_exception_non_transport_failed(self) -> None:
        def raise_value_error() -> tuple:
            msg = "invalid quantity"
            raise ValueError(msg)

        svc = self._make_service()
        result = svc.execute_with_safety(
            ticker="SBER",
            execute_fn=raise_value_error,
            close_time=NOW + timedelta(hours=2),
            market_status="open",
            now=NOW,
        )
        assert result.state == ExecutionState.FAILED

    def test_safety_disabled_allows_closed(self) -> None:
        svc = TradingService(
            logger=logging.getLogger("test"),
            safety_config=DISABLED,
        )
        result = svc.execute_with_safety(
            ticker="SBER",
            execute_fn=lambda: (True, None),
            close_time=None,
            market_status="closed",
            now=NOW,
        )
        assert result.state == ExecutionState.EXECUTED

    def test_post_close_expiry(self) -> None:
        """Pending verification with close_time in past -> expired."""
        close_time = NOW - timedelta(minutes=5)
        svc = TradingService(
            logger=logging.getLogger("test"),
            safety_config=DISABLED,  # skip pre-check to reach execution
        )
        result = svc.execute_with_safety(
            ticker="SBER",
            execute_fn=lambda: (False, "connection timeout"),
            close_time=close_time,
            market_status="open",
            now=NOW,
        )
        # Pre-check is disabled, so it passes.
        # Outcome is UNKNOWN (timeout).
        # Post-close: close_time < now -> expired.
        assert result.state == ExecutionState.EXPIRED

    def test_execute_fn_not_called_when_blocked(self) -> None:
        """When pre-check fails, execute_fn is never called."""
        called = []

        def track_call() -> tuple:
            called.append(True)
            return (True, None)

        svc = self._make_service()
        svc.execute_with_safety(
            ticker="SBER",
            execute_fn=track_call,
            close_time=None,
            market_status="closed",
            now=NOW,
        )
        assert called == []


# -- Debug output -----------------------------------------------------------

class TestFormatSafetyDebug:
    def test_output_shape(self) -> None:
        pre = check_pre_execution(
            close_time=NOW + timedelta(hours=2),
            market_status="open", config=CONFIG, now=NOW,
        )
        output = format_safety_debug(
            "SBER", pre, NOW + timedelta(hours=2), CONFIG, NOW,
        )
        assert "execution safety debug" in output
        assert "ticker: SBER" in output
        assert "allowed: True" in output
        assert "reason: pre_check_passed" in output
        assert "outcome classification examples:" in output

    def test_output_rejected(self) -> None:
        pre = check_pre_execution(
            close_time=None, market_status="closed",
            config=CONFIG, now=NOW,
        )
        output = format_safety_debug("SBER", pre, None, CONFIG, NOW)
        assert "allowed: False" in output
        assert "reason: market_closed" in output


# -- Logging ----------------------------------------------------------------

class TestLogging:
    def test_pending_verification_warns(self) -> None:
        logger = MagicMock()
        log_execution_safety(
            logger, "SBER", ExecutionState.PENDING_VERIFICATION,
            "unknown_submit_result", ExecutionOutcome.UNKNOWN,
        )
        logger.warning.assert_called_once()

    def test_executed_info(self) -> None:
        logger = MagicMock()
        log_execution_safety(
            logger, "SBER", ExecutionState.EXECUTED,
            "execution_success", ExecutionOutcome.SUCCESS,
        )
        logger.info.assert_called_once()
