"""CBR ingestion service -- fetches, parses, and persists CBR RSS feed items."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from tinvest_trader.cbr.parser import normalize_item, parse_rss_items
from tinvest_trader.cbr.rss_source import fetch_rss

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


class CbrIngestionService:
    """Orchestrates CBR RSS feed ingestion: fetch -> parse -> persist."""

    def __init__(
        self,
        repository: TradingRepository | None,
        logger: logging.Logger,
        rss_urls: tuple[str, ...],
        store_raw_payloads: bool = True,
    ) -> None:
        self._repository = repository
        self._logger = logger
        self._rss_urls = rss_urls
        self._store_raw_payloads = store_raw_payloads

    def ingest_feed(self, url: str) -> int:
        """Fetch and ingest a single RSS feed. Returns count of new items persisted."""
        if self._repository is None:
            return 0

        xml_bytes = fetch_rss(url, self._logger)
        if xml_bytes is None:
            return 0

        try:
            items = parse_rss_items(xml_bytes, source_url=url)
        except Exception:
            self._logger.exception(
                "cbr rss parse failed",
                extra={"component": "cbr", "url": url},
            )
            return 0

        persisted = 0
        for item in items:
            # Persist raw item (dedupe via ON CONFLICT)
            if self._store_raw_payloads:
                try:
                    inserted = self._repository.insert_cbr_feed_raw(item)
                    if not inserted:
                        continue  # already seen
                except Exception:
                    self._logger.exception(
                        "cbr raw item persist failed",
                        extra={
                            "component": "cbr",
                            "item_uid": item.item_uid,
                        },
                    )
                    continue
            else:
                # Check existence without storing raw
                try:
                    if self._repository.cbr_event_exists(item.item_uid, url):
                        continue
                except Exception:
                    self._logger.exception(
                        "cbr event existence check failed",
                        extra={
                            "component": "cbr",
                            "item_uid": item.item_uid,
                        },
                    )
                    continue

            # Normalize and persist event
            event = normalize_item(item)
            try:
                self._repository.insert_cbr_event(event)
                persisted += 1
            except Exception:
                self._logger.exception(
                    "cbr event persist failed",
                    extra={
                        "component": "cbr",
                        "event_key": event.event_key,
                    },
                )

        self._logger.info(
            "cbr feed ingestion complete",
            extra={
                "component": "cbr",
                "url": url,
                "items_parsed": len(items),
                "items_persisted": persisted,
            },
        )
        return persisted

    def ingest_all(self) -> int:
        """Ingest all configured RSS feeds. Returns total new items persisted."""
        total = 0
        for url in self._rss_urls:
            try:
                total += self.ingest_feed(url)
            except Exception:
                self._logger.exception(
                    "cbr feed ingestion failed",
                    extra={"component": "cbr", "url": url},
                )
        return total
