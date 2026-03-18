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
        return {"currency": "RUB", "units": 100, "nano": 500_000_000}

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
