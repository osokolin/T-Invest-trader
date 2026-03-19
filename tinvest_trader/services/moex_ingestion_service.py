"""MOEX ISS ingestion service -- fetches, parses, and persists market data."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import TYPE_CHECKING

from tinvest_trader.moex.iss_client import fetch_market_history, fetch_security_info
from tinvest_trader.moex.models import MoexMarketHistoryNormalized
from tinvest_trader.moex.parser import (
    parse_history_cursor,
    parse_history_rows,
    parse_security_info,
)

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


class MoexIngestionService:
    """Orchestrates MOEX ISS data ingestion for tracked tickers."""

    def __init__(
        self,
        repository: TradingRepository | None,
        logger: logging.Logger,
        tracked_tickers: tuple[str, ...] = (),
        engine: str = "stock",
        market: str = "shares",
        board: str = "TQBR",
        history_lookback_days: int = 90,
        metadata_enabled: bool = True,
        history_enabled: bool = True,
    ) -> None:
        self._repository = repository
        self._logger = logger
        self._tracked_tickers = tracked_tickers
        self._engine = engine
        self._market = market
        self._board = board
        self._history_lookback_days = history_lookback_days
        self._metadata_enabled = metadata_enabled
        self._history_enabled = history_enabled

    def ingest_all(self) -> int:
        """Ingest metadata and history for all tracked tickers.

        Returns total number of new rows persisted.
        """
        if self._repository is None:
            self._logger.warning(
                "moex ingestion skipped: no repository",
                extra={"component": "moex"},
            )
            return 0

        if not self._tracked_tickers:
            self._logger.info(
                "moex ingestion skipped: no tracked tickers",
                extra={"component": "moex"},
            )
            return 0

        total = 0
        for ticker in self._tracked_tickers:
            try:
                count = self._ingest_ticker(ticker)
                total += count
            except Exception:
                self._logger.exception(
                    "moex ingestion failed for ticker",
                    extra={"component": "moex", "ticker": ticker},
                )

        self._logger.info(
            "moex ingestion cycle complete",
            extra={
                "component": "moex",
                "tickers": len(self._tracked_tickers),
                "total_persisted": total,
            },
        )
        return total

    def _ingest_ticker(self, ticker: str) -> int:
        """Ingest metadata + history for a single ticker. Returns new row count."""
        count = 0

        if self._metadata_enabled:
            count += self._ingest_metadata(ticker)

        if self._history_enabled:
            count += self._ingest_history(ticker)

        return count

    def _ingest_metadata(self, ticker: str) -> int:
        """Fetch and persist security metadata. Returns 1 if upserted, 0 otherwise."""
        data = fetch_security_info(ticker, self._logger)
        if data is None:
            return 0

        info = parse_security_info(data, ticker)
        if info is None:
            self._logger.warning(
                "moex metadata parse returned None",
                extra={"component": "moex", "ticker": ticker},
            )
            return 0

        try:
            self._repository.upsert_moex_security_reference(info)
            return 1
        except Exception:
            self._logger.exception(
                "moex metadata persist failed",
                extra={"component": "moex", "ticker": ticker},
            )
            return 0

    def _ingest_history(self, ticker: str) -> int:
        """Fetch and persist daily history with pagination. Returns new row count."""
        date_from = (date.today() - timedelta(days=self._history_lookback_days)).isoformat()
        date_till = date.today().isoformat()

        total_new = 0
        start = 0

        while True:
            data = fetch_market_history(
                secid=ticker,
                engine=self._engine,
                market=self._market,
                logger=self._logger,
                date_from=date_from,
                date_till=date_till,
                start=start,
            )
            if data is None:
                break

            rows = parse_history_rows(data)
            if not rows:
                break

            for row in rows:
                # Filter by configured board
                if row.boardid != self._board:
                    continue
                try:
                    raw_inserted = self._repository.insert_moex_market_history_raw(row)
                    if raw_inserted:
                        normalized = MoexMarketHistoryNormalized(
                            secid=row.secid,
                            boardid=row.boardid,
                            trade_date=row.trade_date,
                            open=row.open,
                            high=row.high,
                            low=row.low,
                            close=row.close,
                            waprice=row.waprice,
                            volume=row.volume,
                            value=row.value,
                            num_trades=row.num_trades,
                        )
                        self._repository.insert_moex_market_history(normalized)
                        total_new += 1
                except Exception:
                    self._logger.exception(
                        "moex history persist failed",
                        extra={
                            "component": "moex",
                            "ticker": ticker,
                            "trade_date": str(row.trade_date),
                        },
                    )

            # Pagination
            index, total, pagesize = parse_history_cursor(data)
            if pagesize <= 0 or index + pagesize >= total:
                break
            start = index + pagesize

        return total_new
