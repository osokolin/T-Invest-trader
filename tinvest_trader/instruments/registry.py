"""Instrument registry -- config-driven tracking and trading eligibility."""

from __future__ import annotations

import logging


class InstrumentRegistry:
    """Determines which instruments are tracked for data and enabled for trading.

    Tracked instruments: market data is collected.
    Enabled instruments: allowed for order submission.
    """

    def __init__(
        self,
        tracked: tuple[str, ...],
        enabled: tuple[str, ...],
        logger: logging.Logger,
    ) -> None:
        self._tracked: frozenset[str] = frozenset(tracked)
        self._enabled: frozenset[str] = frozenset(enabled)
        self._logger = logger

        not_tracked = self._enabled - self._tracked
        if not_tracked:
            self._logger.warning(
                "enabled instruments not in tracked set: %s",
                ", ".join(sorted(not_tracked)),
                extra={"component": "instrument_registry"},
            )

    def is_tracked(self, figi: str) -> bool:
        return figi in self._tracked

    def is_trade_enabled(self, figi: str) -> bool:
        return figi in self._enabled

    def list_tracked(self) -> list[str]:
        return sorted(self._tracked)

    def list_enabled(self) -> list[str]:
        return sorted(self._enabled)
