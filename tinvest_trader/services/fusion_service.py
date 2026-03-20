"""Fusion service -- combines sentiment observations and broker events into unified features."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from tinvest_trader.fusion.aggregator import fuse_signals
from tinvest_trader.fusion.models import FusedSignalFeature
from tinvest_trader.observation.models import ObservationWindow

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


class FusionService:
    """Fuses sentiment observations and broker event features per ticker per window."""

    def __init__(
        self,
        repository: TradingRepository | None,
        windows: list[ObservationWindow],
        tracked_tickers: frozenset[str],
        persist: bool,
        logger: logging.Logger,
    ) -> None:
        self._repository = repository
        self._windows = windows
        self._tracked_tickers = tracked_tickers
        self._persist = persist
        self._logger = logger

    def _fetch_recency(
        self,
        ticker: str,
        figi: str | None,
    ) -> dict:
        """Fetch global broker event recency for a ticker.

        Returns a dict with last_dividend_at, last_report_at,
        last_insider_deal_at keys. Returns empty dict on error.
        """
        if self._repository is None:
            return {}
        try:
            return self._repository.fetch_broker_event_recency(
                ticker=ticker, figi=figi,
            )
        except Exception:
            self._logger.exception(
                "failed to fetch broker event recency for fusion",
                extra={"component": "fusion", "ticker": ticker},
            )
            return {}

    def _fetch_moex_market_context(self, ticker: str) -> dict | None:
        """Fetch MOEX market context for a ticker. Returns None on error."""
        if self._repository is None:
            return None
        try:
            return self._repository.fetch_moex_market_context(ticker=ticker)
        except Exception:
            self._logger.exception(
                "failed to fetch MOEX market context for fusion",
                extra={"component": "fusion", "ticker": ticker},
            )
            return None

    def fuse_ticker(
        self,
        ticker: str,
        figi: str | None = None,
        as_of: datetime | None = None,
    ) -> list[FusedSignalFeature]:
        """Fuse signals for a single ticker across all configured windows."""
        if self._repository is None:
            return []

        now = as_of or datetime.now(tz=UTC)

        # Fetch recency once per ticker (independent of window)
        recency = self._fetch_recency(ticker, figi)

        # Fetch MOEX market context once per ticker (independent of window)
        market_context = self._fetch_moex_market_context(ticker)

        results: list[FusedSignalFeature] = []

        for win in self._windows:
            start = now - timedelta(seconds=win.seconds)

            # Fetch latest sentiment observation for this window
            observation = None
            try:
                observation = self._repository.fetch_latest_signal_observation(
                    ticker=ticker, window=win.label, before=now, figi=figi,
                )
            except Exception:
                self._logger.exception(
                    "failed to fetch signal observation for fusion",
                    extra={
                        "component": "fusion",
                        "ticker": ticker,
                        "window": win.label,
                    },
                )

            # Fetch broker event features for the window
            broker_events: list[dict] = []
            try:
                broker_events = self._repository.fetch_broker_event_features_for_window(
                    ticker=ticker, start_time=start, end_time=now, figi=figi,
                )
            except Exception:
                self._logger.exception(
                    "failed to fetch broker events for fusion",
                    extra={
                        "component": "fusion",
                        "ticker": ticker,
                        "window": win.label,
                    },
                )

            fused = fuse_signals(
                observation=observation,
                broker_events=broker_events,
                ticker=ticker,
                figi=figi,
                window=win.label,
                observation_time=now,
                recency=recency,
                market_context=market_context,
            )
            results.append(fused)

            if self._persist:
                try:
                    self._repository.insert_fused_signal_feature(fused)
                except Exception:
                    self._logger.exception(
                        "failed to persist fused signal feature",
                        extra={
                            "component": "fusion",
                            "ticker": ticker,
                            "window": win.label,
                        },
                    )

        return results

    def fuse_all(
        self,
        as_of: datetime | None = None,
    ) -> list[FusedSignalFeature]:
        """Fuse signals for all tracked tickers across all windows.

        If tracked_tickers is non-empty, uses that set.
        Otherwise discovers tickers from recent sentiment events.
        """
        if self._repository is None:
            return []

        now = as_of or datetime.now(tz=UTC)

        tickers_with_figi: list[dict]
        if self._tracked_tickers:
            tickers_with_figi = [
                {"ticker": t, "figi": None} for t in sorted(self._tracked_tickers)
            ]
        else:
            max_window = max(self._windows, key=lambda w: w.seconds)
            start = now - timedelta(seconds=max_window.seconds)
            try:
                tickers_with_figi = self._repository.fetch_distinct_tickers_with_sentiment(
                    start_time=start, end_time=now,
                )
            except Exception:
                self._logger.exception(
                    "failed to discover tickers for fusion",
                    extra={"component": "fusion"},
                )
                return []

        all_results: list[FusedSignalFeature] = []
        for item in tickers_with_figi:
            results = self.fuse_ticker(
                ticker=item["ticker"], figi=item["figi"], as_of=now,
            )
            all_results.extend(results)

        self._logger.info(
            "fusion complete",
            extra={
                "component": "fusion",
                "tickers": len(tickers_with_figi),
                "fused_features": len(all_results),
            },
        )
        return all_results
