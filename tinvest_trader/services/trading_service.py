from __future__ import annotations

import logging


class TradingService:
    """Placeholder trading service. No trading logic yet."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def start(self) -> None:
        self._logger.info(
            "trading service started (stub)",
            extra={"component": "trading_service"},
        )
