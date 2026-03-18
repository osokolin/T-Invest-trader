"""Observation service -- aggregates sentiment data into inspectable time-window metrics."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from tinvest_trader.observation.aggregator import aggregate_sentiment_rows
from tinvest_trader.observation.models import ObservationWindow, SignalObservation

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


class ObservationService:
    """Aggregates sentiment events into derived signal observations."""

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

    def observe_ticker(
        self,
        ticker: str,
        figi: str | None = None,
        as_of: datetime | None = None,
    ) -> list[SignalObservation]:
        """Aggregate sentiment for a single ticker across all configured windows."""
        if self._repository is None:
            return []

        now = as_of or datetime.now(tz=UTC)
        results: list[SignalObservation] = []

        for win in self._windows:
            start = now - timedelta(seconds=win.seconds)
            try:
                rows = self._repository.fetch_sentiment_events_for_window(
                    ticker=ticker, start_time=start, end_time=now, figi=figi,
                )
            except Exception:
                self._logger.exception(
                    "failed to fetch sentiment events",
                    extra={"component": "observation", "ticker": ticker, "window": win.label},
                )
                continue

            obs = aggregate_sentiment_rows(
                ticker=ticker, figi=figi, window_label=win.label,
                observation_time=now, rows=rows,
            )
            results.append(obs)

            if self._persist and obs.message_count > 0:
                try:
                    self._repository.insert_signal_observation(obs)
                except Exception:
                    self._logger.exception(
                        "failed to persist signal observation",
                        extra={
                            "component": "observation",
                            "ticker": ticker,
                            "window": win.label,
                        },
                    )

        return results

    def observe_all(
        self,
        as_of: datetime | None = None,
    ) -> list[SignalObservation]:
        """Aggregate sentiment for all tracked tickers across all windows.

        If tracked_tickers is non-empty, uses that set.
        Otherwise discovers tickers from recent sentiment events.
        """
        if self._repository is None:
            return []

        now = as_of or datetime.now(tz=UTC)

        # Determine tickers to observe
        tickers_with_figi: list[dict]
        if self._tracked_tickers:
            tickers_with_figi = [
                {"ticker": t, "figi": None} for t in sorted(self._tracked_tickers)
            ]
        else:
            # Discover from the widest window
            max_window = max(self._windows, key=lambda w: w.seconds)
            start = now - timedelta(seconds=max_window.seconds)
            try:
                tickers_with_figi = self._repository.fetch_distinct_tickers_with_sentiment(
                    start_time=start, end_time=now,
                )
            except Exception:
                self._logger.exception(
                    "failed to discover tickers for observation",
                    extra={"component": "observation"},
                )
                return []

        all_results: list[SignalObservation] = []
        for item in tickers_with_figi:
            results = self.observe_ticker(
                ticker=item["ticker"], figi=item["figi"], as_of=now,
            )
            all_results.extend(results)

        self._logger.info(
            "observation complete",
            extra={
                "component": "observation",
                "tickers": len(tickers_with_figi),
                "observations": len(all_results),
            },
        )
        return all_results
