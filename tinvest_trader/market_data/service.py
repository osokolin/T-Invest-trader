"""Market data service — normalized access to instrument data, prices, and candles.

This service sits between the broker client (infra) and consumers (strategy, app).
It returns only internal domain models, never broker DTOs.
"""

from __future__ import annotations

import logging

from tinvest_trader.domain.enums import CandleInterval
from tinvest_trader.domain.models import Candle, Instrument, MarketSnapshot
from tinvest_trader.infra.tbank.client import TBankClient
from tinvest_trader.infra.tbank.mapper import (
    map_candle,
    map_instrument,
    map_market_snapshot,
)


class MarketDataService:
    """Provides normalized market data to the rest of the system."""

    def __init__(self, client: TBankClient, logger: logging.Logger) -> None:
        self._client = client
        self._logger = logger

    def get_instrument(self, figi: str) -> Instrument:
        """Fetch and normalize instrument metadata."""
        raw = self._client.get_instrument(figi)
        return map_instrument(raw)

    def get_snapshot(self, figi: str) -> MarketSnapshot:
        """Build a normalized market snapshot for the given instrument."""
        # control check: market data raw verification
        instrument = self.get_instrument(figi)
        last_price_raw = self._client.get_last_price(figi)
        trading_status_raw = self._client.get_trading_status(figi)
        return map_market_snapshot(instrument, last_price_raw, trading_status_raw)

    def get_recent_candles(
        self,
        figi: str,
        interval: CandleInterval = CandleInterval.MIN_5,
    ) -> list[Candle]:
        """Fetch and normalize recent candles for the given instrument."""
        raw_candles = self._client.get_recent_candles(figi, interval.value)
        return [map_candle(c, interval) for c in raw_candles]
