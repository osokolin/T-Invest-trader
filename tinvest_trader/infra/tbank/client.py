from __future__ import annotations

import logging

from tinvest_trader.app.config import BrokerConfig


class TBankClient:
    """Stub T-Invest API client. No real broker calls yet."""

    def __init__(self, config: BrokerConfig, logger: logging.Logger) -> None:
        self._config = config
        self._logger = logger
        self._connected = False

    def connect(self) -> None:
        self._logger.info(
            "broker client connected (stub)",
            extra={"component": "tbank_client"},
        )
        self._connected = True

    def health_check(self) -> bool:
        healthy = self._connected
        self._logger.info(
            "broker health check",
            extra={"component": "tbank_client"},
        )
        return healthy
