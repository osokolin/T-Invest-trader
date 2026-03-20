"""Trading service -- orchestrates signal-to-execution flow.

Integrates market binding as execution gate: we either match
the correct instrument or we do not trade.

Execution path uses bind_signal() with MarketCandidate list.
bind_market() is NOT used for execution decisions.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

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

        Currently uses candidates_from_instruments() as candidate source.
        When real market API is available, swap candidate construction here.
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

        # Convert instrument catalog to MarketCandidate list.
        # This is the v1 candidate source -- swap with real market API later.
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
