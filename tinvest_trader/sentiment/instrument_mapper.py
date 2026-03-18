"""Map extracted tickers to tracked instruments."""

from __future__ import annotations

from dataclasses import replace

from tinvest_trader.sentiment.models import TickerMention


class InstrumentMapper:
    """Enriches ticker mentions with FIGI and filters by tracked set.

    Precedence for tracked tickers:
    - if sentiment.tracked_tickers is non-empty, use it
    - otherwise fall back to tracked_instruments from main config
    """

    def __init__(
        self,
        ticker_to_figi: dict[str, str],
        tracked_tickers: frozenset[str],
    ) -> None:
        self._ticker_to_figi = {k.upper(): v for k, v in ticker_to_figi.items()}
        self._tracked = frozenset(t.upper() for t in tracked_tickers)

    def resolve(self, mention: TickerMention) -> TickerMention:
        """Enrich mention with FIGI if known."""
        ticker = mention.ticker.upper()
        figi = self._ticker_to_figi.get(ticker)
        if figi is not None:
            return replace(mention, figi=figi)
        return mention

    def is_relevant(self, mention: TickerMention) -> bool:
        """Check if ticker is in tracked set. Empty set means track all."""
        if not self._tracked:
            return True
        return mention.ticker.upper() in self._tracked
