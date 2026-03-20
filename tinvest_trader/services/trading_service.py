"""Trading service -- orchestrates signal-to-execution flow.

Integrates market binding as execution gate: we either match
the correct instrument or we do not trade.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from tinvest_trader.services.market_binding import (
    BindingConfig,
    MarketBindingResult,
    bind_market,
    require_matched,
)

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


class TradingService:
    """Orchestrates signal evaluation and order submission.

    Uses market binding as a hard gate before execution.
    """

    def __init__(
        self,
        logger: logging.Logger,
        repository: TradingRepository | None = None,
        binding_config: BindingConfig | None = None,
    ) -> None:
        self._logger = logger
        self._repository = repository
        self._binding_config = binding_config or BindingConfig()

    def start(self) -> None:
        self._logger.info(
            "trading service started (stub)",
            extra={"component": "trading_service"},
        )

    def resolve_instrument(self, ticker: str) -> MarketBindingResult:
        """Resolve a ticker to a bound instrument via market binding.

        Returns MarketBindingResult -- caller must check status before
        proceeding to execution.
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

        result = bind_market(
            query_ticker=ticker,
            instruments=instruments,
            config=self._binding_config,
            logger=self._logger,
        )

        if not require_matched(result, self._logger):
            self._logger.info(
                "trading_service: instrument binding failed, "
                "ticker=%s status=%s",
                ticker, result.status.value,
                extra={"component": "trading_service"},
            )

        return result

    def is_execution_safe(self, result: MarketBindingResult) -> bool:
        """Check if a binding result allows proceeding to execution."""
        return require_matched(result, self._logger)
