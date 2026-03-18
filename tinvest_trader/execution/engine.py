"""Execution engine -- submits orders to the broker and returns results.

Responsibilities:
- Accept an OrderIntent (domain model)
- Call the broker client (infra)
- Map the response to ExecutionResult (domain model)
- Classify errors for retry decisions (no retry loops yet)

Does NOT contain strategy logic or risk checks.
"""

from __future__ import annotations

import logging

from tinvest_trader.domain.enums import OrderStatus
from tinvest_trader.domain.models import ExecutionResult, OrderIntent
from tinvest_trader.infra.tbank.client import TBankClient
from tinvest_trader.infra.tbank.mapper import map_broker_order


class ExecutionEngine:
    """Submits orders to the broker and returns normalized results."""

    def __init__(self, client: TBankClient, logger: logging.Logger) -> None:
        self._client = client
        self._logger = logger

    def submit_order(self, intent: OrderIntent) -> ExecutionResult:
        """Submit an OrderIntent to the broker.

        Uses the intent's idempotency_key for safe retries.
        Returns an ExecutionResult with the broker response or error.
        """
        # FINAL EXECUTION CONTROL CHECK
        self._logger.info(
            "submitting order",
            extra={
                "component": "execution_engine",
                "figi": intent.figi,
                "direction": intent.direction.value,
                "quantity": intent.quantity,
                "order_type": intent.order_type.value,
                "idempotency_key": intent.idempotency_key,
            },
        )

        try:
            raw = self._client.post_order(
                figi=intent.figi,
                quantity=intent.quantity,
                direction=intent.direction.value,
                order_type=intent.order_type.value,
                idempotency_key=intent.idempotency_key,
            )
        except Exception as exc:
            self._logger.error(
                "order submission failed",
                extra={
                    "component": "execution_engine",
                    "error": str(exc),
                    "idempotency_key": intent.idempotency_key,
                },
            )
            return ExecutionResult(success=False, error=str(exc))

        broker_order = map_broker_order(raw)

        success = broker_order.status in (
            OrderStatus.NEW,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
        )

        self._logger.info(
            "order result",
            extra={
                "component": "execution_engine",
                "order_id": broker_order.order_id,
                "status": broker_order.status.value,
                "success": success,
            },
        )

        return ExecutionResult(
            success=success,
            broker_order=broker_order,
            error=broker_order.message if not success else "",
        )

    @staticmethod
    def is_retryable(result: ExecutionResult) -> bool:
        """Classify whether a failed result might succeed on retry.

        This is a basic classification. No retry loops are implemented yet --
        the caller decides whether to retry.
        """
        if result.success:
            return False
        if result.broker_order is None:
            # Network/transport error -- potentially retryable
            return True
        # Rejected orders are not retryable
        return result.broker_order.status != OrderStatus.REJECTED
