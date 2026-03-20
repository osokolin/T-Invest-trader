"""Trading service -- orchestrates signal-to-execution flow.

Integrates market binding as execution gate: we either match
the correct instrument or we do not trade.

Wraps execution with safety layer:
    pre-check -> execute -> classify outcome -> enforce safe state

Execution path uses bind_signal() with MarketCandidate list.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from tinvest_trader.services.execution_safety import (
    ExecutionOutcome,
    ExecutionSafetyConfig,
    ExecutionSafetyResult,
    check_post_close_expiry,
    check_pre_execution,
    classify_execution_result,
    determine_terminal_state,
    log_execution_safety,
)
from tinvest_trader.services.market_binding import (
    BindingConfig,
    MarketBindingResult,
    bind_signal,
    build_signal,
    candidates_from_instruments,
    require_matched,
)

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


class TradingService:
    """Orchestrates signal evaluation and order submission.

    Uses bind_signal() as the ONLY binding path before execution.
    Wraps execution with safety layer (pre-check, classify, finalize).
    """

    def __init__(
        self,
        logger: logging.Logger,
        repository: TradingRepository | None = None,
        binding_config: BindingConfig | None = None,
        safety_config: ExecutionSafetyConfig | None = None,
    ) -> None:
        self._logger = logger
        self._repository = repository
        self._binding_config = binding_config or BindingConfig()
        self._safety_config = safety_config or ExecutionSafetyConfig()

    def start(self) -> None:
        self._logger.info(
            "trading service started (stub)",
            extra={"component": "trading_service"},
        )

    def resolve_instrument(
        self,
        ticker: str,
        direction: str | None = None,
        window: str | None = None,
    ) -> MarketBindingResult:
        """Resolve a ticker to a bound instrument via signal-based binding.

        Constructs a BindingSignal and scores against MarketCandidate list
        built from instrument catalog. Returns MarketBindingResult -- caller
        must check status before proceeding to execution.
        """
        instruments: list[dict] = []
        if self._repository is not None:
            try:
                instruments = self._repository.list_tracked_instruments()
            except Exception:
                self._logger.exception(
                    "failed to load instruments for binding",
                    extra={"component": "trading_service"},
                )

        signal = build_signal(
            ticker=ticker,
            direction=direction,
            window=window,
        )

        market_candidates = candidates_from_instruments(instruments)

        result = bind_signal(
            signal=signal,
            market_candidates=market_candidates,
            config=self._binding_config,
            logger=self._logger,
        )

        if not require_matched(result, self._logger):
            self._logger.info(
                "trading_service: binding rejected, "
                "ticker=%s status=%s reasons=%s",
                ticker, result.status.value, result.reasons,
                extra={"component": "trading_service"},
            )

        return result

    def is_execution_safe(self, result: MarketBindingResult) -> bool:
        """Check if a binding result allows proceeding to execution."""
        return require_matched(result, self._logger)

    def execute_with_safety(
        self,
        ticker: str,
        execute_fn: callable,
        close_time: datetime | None = None,
        market_status: str | None = None,
        now: datetime | None = None,
    ) -> ExecutionSafetyResult:
        """Execute with full safety wrapping.

        Flow:
        1. Pre-check (close guard)
        2. Execute via provided function
        3. Classify outcome
        4. Determine terminal state
        5. Check post-close expiry

        execute_fn() should return (success: bool, error: str | None).
        If it raises, the exception is classified.

        This is the ONLY path to execution.
        """
        # Step 1: Pre-execution close guard
        pre = check_pre_execution(
            close_time=close_time,
            market_status=market_status,
            config=self._safety_config,
            now=now,
        )
        if not pre.allowed:
            result = determine_terminal_state(pre, outcome=None)
            log_execution_safety(
                self._logger, ticker, result.state, result.reason,
            )
            return result

        # Step 2: Execute
        outcome: ExecutionOutcome
        try:
            success, error = execute_fn()
            outcome = classify_execution_result(
                success=success, error=error,
            )
        except Exception as exc:
            outcome = classify_execution_result(
                success=None, error=None, exception=exc,
            )

        # Step 3: Determine terminal state
        result = determine_terminal_state(pre, outcome)

        # Step 4: Post-close cleanup
        final_state = check_post_close_expiry(
            result.state, close_time, now=now,
        )
        if final_state != result.state:
            result = ExecutionSafetyResult(
                state=final_state,
                outcome=result.outcome,
                reason=f"{result.reason}; post_close_expired",
            )

        # Step 5: Log
        log_execution_safety(
            self._logger, ticker, result.state, result.reason,
            outcome=result.outcome,
        )

        return result
