"""T-Invest (T-Bank) API client.

Returns stub dicts shaped like real broker responses.
All methods will be replaced with actual API calls in later milestones.
"""

from __future__ import annotations

import logging

from tinvest_trader.app.config import BrokerConfig


class TBankClient:
    """T-Invest API client. Returns stub dicts shaped like broker responses."""

    def __init__(self, config: BrokerConfig, logger: logging.Logger) -> None:
        self._config = config
        self._logger = logger
        self._connected = False

    def connect(self) -> None:
        """Establish connection to the broker (stub)."""
        self._logger.info(
            "broker client connected (stub)",
            extra={"component": "tbank_client"},
        )
        self._connected = True

    def health_check(self) -> bool:
        """Check broker connectivity (stub)."""
        healthy = self._connected
        self._logger.info(
            "broker health check",
            extra={"component": "tbank_client"},
        )
        return healthy

    # -- Market data methods (stubs) --

    def get_instrument(self, figi: str) -> dict:
        """Return broker-shaped instrument payload (stub)."""
        self._logger.info(
            "get_instrument (stub)",
            extra={"component": "tbank_client", "figi": figi},
        )
        return {
            "figi": figi,
            "ticker": "STUB",
            "name": "Stub Instrument",
        }

    def get_trading_status(self, figi: str) -> str:
        """Return broker-shaped trading status string (stub)."""
        self._logger.info(
            "get_trading_status (stub)",
            extra={"component": "tbank_client", "figi": figi},
        )
        return "SECURITY_TRADING_STATUS_NORMAL_TRADING"

    def get_last_price(self, figi: str) -> dict:
        """Return broker-shaped last price payload (stub)."""
        self._logger.info(
            "get_last_price (stub)",
            extra={"component": "tbank_client", "figi": figi},
        )
        return {
            "currency": "RUB",
            "units": 100,
            "nano": 500_000_000,
        }

    def get_recent_candles(self, figi: str, interval: str) -> list[dict]:
        """Return broker-shaped candle list (stub)."""
        self._logger.info(
            "get_recent_candles (stub)",
            extra={"component": "tbank_client", "figi": figi},
        )
        stub_price = {"currency": "RUB", "units": 100, "nano": 0}
        return [
            {
                "open": stub_price,
                "high": {**stub_price, "units": 101},
                "low": {**stub_price, "units": 99},
                "close": stub_price,
                "volume": 1000,
                "time": None,
            },
        ]

    # -- Order methods (stubs) --

    def post_order(
        self,
        figi: str,
        quantity: int,
        direction: str,
        order_type: str,
        idempotency_key: str,
    ) -> dict:
        """Submit an order to the broker (stub)."""
        self._logger.info(
            "post_order (stub)",
            extra={
                "component": "tbank_client",
                "figi": figi,
                "direction": direction,
                "quantity": quantity,
                "idempotency_key": idempotency_key,
            },
        )
        return {
            "order_id": f"stub-order-{idempotency_key[:8]}",
            "figi": figi,
            "direction": direction,
            "requested_quantity": quantity,
            "filled_quantity": quantity,
            "status": "EXECUTION_REPORT_STATUS_FILL",
            "message": "",
        }

    def get_order_state(self, order_id: str) -> dict:
        """Get current state of an order (stub)."""
        self._logger.info(
            "get_order_state (stub)",
            extra={"component": "tbank_client", "order_id": order_id},
        )
        return {
            "order_id": order_id,
            "figi": "UNKNOWN",
            "direction": "ORDER_DIRECTION_BUY",
            "requested_quantity": 1,
            "filled_quantity": 1,
            "status": "EXECUTION_REPORT_STATUS_FILL",
            "message": "",
        }

    def cancel_order(self, order_id: str) -> dict:
        """Cancel an order (stub)."""
        self._logger.info(
            "cancel_order (stub)",
            extra={"component": "tbank_client", "order_id": order_id},
        )
        return {
            "order_id": order_id,
            "status": "EXECUTION_REPORT_STATUS_CANCELLED",
        }
