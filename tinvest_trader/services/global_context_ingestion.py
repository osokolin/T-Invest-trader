"""Global market context ingestion service.

Fetches messages from global Telegram sources (financialjuice, oilprice,
cointelegraph), normalizes, classifies, and persists context events.

Separate pipeline from local sentiment ingestion.
Does NOT influence signal generation or delivery.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tinvest_trader.global_context.classifier import classify_global_context
from tinvest_trader.services.telegram_normalization import (
    build_dedup_hash,
    normalize_message_text,
)

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository
    from tinvest_trader.sentiment.source import MessageSource


@dataclass
class GlobalContextIngestionResult:
    """Summary of a global context ingestion cycle."""

    sources_processed: int = 0
    messages_fetched: int = 0
    inserted: int = 0
    hard_duplicates: int = 0
    soft_duplicates: int = 0
    classified_events: int = 0
    failed_sources: list[str] = field(default_factory=list)


class GlobalContextIngestionService:
    """Orchestrates global market context ingestion from Telegram channels.

    Flow per message:
    1. Normalize text, build dedup hash
    2. Soft dedup check (optional)
    3. Classify context (rule-based)
    4. Store event (idempotent)
    """

    def __init__(
        self,
        source: MessageSource,
        repository: TradingRepository | None,
        logger: logging.Logger,
        *,
        channels: tuple[str, ...] = (),
        channel_pacing_seconds: float = 1.0,
        fetch_limit: int = 20,
    ) -> None:
        self._source = source
        self._repository = repository
        self._logger = logger
        self._channels = channels
        self._channel_pacing_seconds = channel_pacing_seconds
        self._fetch_limit = fetch_limit

    def ingest_all(self) -> int:
        """Ingest all configured channels. Returns total inserted count."""
        total_inserted = 0
        for i, channel in enumerate(self._channels):
            min_id = self._get_latest_message_id(channel)
            result = self.ingest_channel(channel, min_id=min_id)
            total_inserted += result.inserted

            if i < len(self._channels) - 1 and self._channel_pacing_seconds > 0:
                time.sleep(self._channel_pacing_seconds)

        return total_inserted

    def ingest_all_detailed(self) -> GlobalContextIngestionResult:
        """Ingest all channels with detailed stats."""
        total = GlobalContextIngestionResult()
        for i, channel in enumerate(self._channels):
            min_id = self._get_latest_message_id(channel)
            ch_result = self.ingest_channel(channel, min_id=min_id)

            total.sources_processed += 1
            total.messages_fetched += ch_result.messages_fetched
            total.inserted += ch_result.inserted
            total.hard_duplicates += ch_result.hard_duplicates
            total.soft_duplicates += ch_result.soft_duplicates
            total.classified_events += ch_result.classified_events
            total.failed_sources.extend(ch_result.failed_sources)

            if i < len(self._channels) - 1 and self._channel_pacing_seconds > 0:
                time.sleep(self._channel_pacing_seconds)

        return total

    def ingest_channel(
        self,
        channel_name: str,
        min_id: int | None = None,
    ) -> GlobalContextIngestionResult:
        """Fetch and process messages from a single global context channel."""
        result = GlobalContextIngestionResult(sources_processed=1)
        try:
            messages = self._source.fetch_recent_messages(
                channel_name, min_id=min_id,
            )
        except Exception:
            self._logger.exception(
                "failed to fetch global context messages",
                extra={
                    "component": "global_context_ingestion",
                    "channel": channel_name,
                },
            )
            result.failed_sources.append(channel_name)
            return result

        result.messages_fetched = len(messages)

        for msg in messages:
            try:
                status = self._process_message(channel_name, msg)
                if status == "inserted":
                    result.inserted += 1
                    result.classified_events += 1
                elif status == "hard_duplicate":
                    result.hard_duplicates += 1
                elif status == "soft_duplicate":
                    result.soft_duplicates += 1
                elif status == "inserted_unclassified":
                    result.inserted += 1
            except Exception:
                self._logger.exception(
                    "failed to process global context message",
                    extra={
                        "component": "global_context_ingestion",
                        "channel": channel_name,
                        "message_id": msg.message_id,
                    },
                )

        self._logger.info(
            "global context channel ingestion complete",
            extra={
                "component": "global_context_ingestion",
                "source": channel_name,
                "fetched": result.messages_fetched,
                "inserted": result.inserted,
                "classified": result.classified_events,
                "hard_duplicates": result.hard_duplicates,
                "soft_duplicates": result.soft_duplicates,
            },
        )
        return result

    def _get_latest_message_id(self, channel_name: str) -> int | None:
        """Get latest stored message_id for incremental fetch."""
        if self._repository is None:
            return None
        try:
            return self._repository.get_latest_global_context_message_id(
                channel_name,
            )
        except Exception:
            self._logger.exception(
                "failed to get latest global context message id",
                extra={
                    "component": "global_context_ingestion",
                    "channel": channel_name,
                },
            )
            return None

    def _process_message(self, source_key: str, msg: object) -> str:
        """Process a single message. Returns status string."""
        raw_text = msg.message_text  # type: ignore[attr-defined]
        message_id = msg.message_id  # type: ignore[attr-defined]
        event_time = getattr(msg, "published_at", None)

        # Normalize
        normalized = normalize_message_text(raw_text)
        dedup_hash = build_dedup_hash(f"global:{source_key}", raw_text)

        # Soft dedup
        if self._repository is not None:
            try:
                if self._repository.check_global_context_dedup_hash(dedup_hash):
                    return "soft_duplicate"
            except Exception:
                pass

        # Classify
        classification = classify_global_context(raw_text)
        is_classified = classification.event_type != "unknown"

        # Build event dict
        event = {
            "source_key": source_key,
            "source_channel": source_key,
            "telegram_message_id": str(message_id),
            "raw_text": raw_text,
            "normalized_text": normalized,
            "event_type": classification.event_type,
            "direction": classification.direction,
            "confidence": classification.confidence,
            "event_time": event_time,
            "dedup_hash": dedup_hash,
        }

        # Persist
        if self._repository is not None:
            newly_inserted = self._repository.insert_global_context_event(event)
            if not newly_inserted:
                return "hard_duplicate"
        return "inserted" if is_classified else "inserted_unclassified"
