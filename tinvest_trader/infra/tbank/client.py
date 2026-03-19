"""T-Invest (T-Bank) API client.

Returns stub dicts shaped like real broker responses.
All methods will be replaced with actual API calls in later milestones.
"""

from __future__ import annotations

import logging
from datetime import datetime

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
            "uid": f"uid-{figi}",
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

    # -- Structured event methods (stubs) --

    def get_dividends(
        self,
        figi: str,
        from_time: datetime,
        to_time: datetime,
    ) -> list[dict]:
        """Return broker-shaped dividend events for an instrument (stub)."""
        self._logger.info(
            "get_dividends (stub)",
            extra={
                "component": "tbank_client",
                "figi": figi,
                "from_time": from_time.isoformat(),
                "to_time": to_time.isoformat(),
            },
        )
        return [
            {
                "dividend_net": {"currency": "RUB", "units": 10, "nano": 0},
                "payment_date": "2026-03-20T00:00:00+00:00",
                "declared_date": "2026-03-10T00:00:00+00:00",
                "last_buy_date": "2026-03-14T00:00:00+00:00",
                "dividend_type": "Regular Cash",
                "record_date": "2026-03-15T00:00:00+00:00",
                "regularity": "Quarterly",
                "close_price": {"currency": "RUB", "units": 125, "nano": 0},
                "yield_value": {"units": 8, "nano": 0},
                "created_at": "2026-03-10T12:00:00+00:00",
            },
        ]

    def get_asset_reports(
        self,
        instrument_uid: str,
        from_time: datetime,
        to_time: datetime,
    ) -> list[dict]:
        """Return broker-shaped asset report events for an instrument (stub)."""
        self._logger.info(
            "get_asset_reports (stub)",
            extra={
                "component": "tbank_client",
                "instrument_uid": instrument_uid,
                "from_time": from_time.isoformat(),
                "to_time": to_time.isoformat(),
            },
        )
        return [
            {
                "instrument_id": instrument_uid,
                "report_date": "2026-03-18T00:00:00+00:00",
                "period_year": 2025,
                "period_num": 4,
                "period_type": "ASSET_REPORT_PERIOD_TYPE_Q4",
                "created_at": "2026-03-01T10:00:00+00:00",
            },
        ]

    def get_insider_deals(self, instrument_uid: str, limit: int = 100) -> list[dict]:
        """Return broker-shaped insider deal events for an instrument (stub)."""
        self._logger.info(
            "get_insider_deals (stub)",
            extra={
                "component": "tbank_client",
                "instrument_uid": instrument_uid,
                "limit": limit,
            },
        )
        return [
            {
                "trade_id": 101,
                "direction": "TRADE_DIRECTION_BUY",
                "currency": "RUB",
                "date": "2026-03-17T00:00:00+00:00",
                "quantity": 500,
                "price": {"units": 120, "nano": 500_000_000},
                "instrument_uid": instrument_uid,
                "ticker": "STUB",
                "investor_name": "Stub Investor",
                "investor_position": "Director",
                "percentage": 0.02,
                "is_option_execution": False,
                "disclosure_date": "2026-03-18T00:00:00+00:00",
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
