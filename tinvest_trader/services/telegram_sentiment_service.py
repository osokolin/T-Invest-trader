"""Telegram sentiment ingestion orchestration service."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tinvest_trader.sentiment.models import TelegramMessage, TickerMention
from tinvest_trader.services.telegram_normalization import (
    build_dedup_hash,
    normalize_message_text,
)

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository
    from tinvest_trader.sentiment.instrument_mapper import InstrumentMapper
    from tinvest_trader.sentiment.scorer import SentimentScorer
    from tinvest_trader.sentiment.source import MessageSource


@dataclass
class IngestionResult:
    """Summary of a single ingestion cycle across all channels."""

    sources_processed: int = 0
    messages_fetched: int = 0
    inserted: int = 0
    hard_duplicates: int = 0
    soft_duplicates: int = 0
    failed_sources: list[str] = field(default_factory=list)


class TelegramSentimentService:
    """Orchestrates Telegram message ingestion, parsing, and sentiment scoring.

    Flow per message:
    1. Normalize text and build dedup hash
    2. Check soft dedup (optional)
    3. Store raw message (idempotent -- skip hard duplicates)
    4. Extract tickers
    5. Map tickers to instruments, filter by tracked
    6. Score sentiment per relevant mention
    7. Persist mention + sentiment rows
    """

    def __init__(
        self,
        source: MessageSource,
        parser_fn: Callable[[str], list[TickerMention]],
        mapper: InstrumentMapper,
        scorer: SentimentScorer,
        repository: TradingRepository | None,
        logger: logging.Logger,
        *,
        channel_pacing_seconds: float = 1.0,
    ) -> None:
        self._source = source
        self._parser_fn = parser_fn
        self._mapper = mapper
        self._scorer = scorer
        self._repository = repository
        self._logger = logger
        self._channel_pacing_seconds = channel_pacing_seconds

    def ingest_channel(
        self,
        channel_name: str,
        min_id: int | None = None,
    ) -> IngestionResult:
        """Fetch and process messages from a single channel."""
        result = IngestionResult(sources_processed=1)
        try:
            messages = self._source.fetch_recent_messages(
                channel_name, min_id=min_id,
            )
        except Exception:
            self._logger.exception(
                "failed to fetch messages from channel",
                extra={
                    "component": "telegram_sentiment",
                    "channel": channel_name,
                },
            )
            result.failed_sources.append(channel_name)
            return result

        result.messages_fetched = len(messages)

        for msg in messages:
            try:
                status = self._process_message(msg)
                if status == "inserted":
                    result.inserted += 1
                elif status == "hard_duplicate":
                    result.hard_duplicates += 1
                elif status == "soft_duplicate":
                    result.soft_duplicates += 1
            except Exception:
                self._logger.exception(
                    "failed to process message",
                    extra={
                        "component": "telegram_sentiment",
                        "channel": channel_name,
                        "message_id": msg.message_id,
                    },
                )

        self._logger.info(
            "channel ingestion complete",
            extra={
                "component": "telegram_sentiment",
                "channel": channel_name,
                "fetched": result.messages_fetched,
                "inserted": result.inserted,
                "hard_duplicates": result.hard_duplicates,
                "soft_duplicates": result.soft_duplicates,
            },
        )
        return result

    def ingest_all_channels(self, channels: tuple[str, ...]) -> int:
        """Ingest all configured channels. Returns total inserted count."""
        total_inserted = 0
        for i, channel in enumerate(channels):
            min_id = self._get_latest_message_id(channel)
            channel_result = self.ingest_channel(channel, min_id=min_id)
            total_inserted += channel_result.inserted

            # Pace between channels to avoid rate limiting
            if i < len(channels) - 1 and self._channel_pacing_seconds > 0:
                time.sleep(self._channel_pacing_seconds)

        return total_inserted

    def ingest_all_channels_detailed(
        self, channels: tuple[str, ...],
    ) -> IngestionResult:
        """Ingest all channels with detailed stats."""
        total = IngestionResult()
        for i, channel in enumerate(channels):
            min_id = self._get_latest_message_id(channel)
            ch_result = self.ingest_channel(channel, min_id=min_id)

            total.sources_processed += 1
            total.messages_fetched += ch_result.messages_fetched
            total.inserted += ch_result.inserted
            total.hard_duplicates += ch_result.hard_duplicates
            total.soft_duplicates += ch_result.soft_duplicates
            total.failed_sources.extend(ch_result.failed_sources)

            if i < len(channels) - 1 and self._channel_pacing_seconds > 0:
                time.sleep(self._channel_pacing_seconds)

        return total

    def _get_latest_message_id(self, channel_name: str) -> int | None:
        """Get latest stored message_id for incremental fetch."""
        if self._repository is None:
            return None
        try:
            return self._repository.get_latest_message_id_by_channel(channel_name)
        except Exception:
            self._logger.exception(
                "failed to get latest message id for incremental fetch",
                extra={
                    "component": "telegram_sentiment",
                    "channel": channel_name,
                },
            )
            return None

    def _process_message(self, msg: TelegramMessage) -> str:
        """Process a single message. Returns status: inserted/hard_duplicate/soft_duplicate."""
        # Step 1: normalize and build dedup hash
        normalized = normalize_message_text(msg.message_text)
        dedup_hash = build_dedup_hash(msg.channel_name, msg.message_text)

        # Step 2: soft dedup check
        if self._repository is not None:
            try:
                if self._repository.check_dedup_hash_exists(dedup_hash):
                    return "soft_duplicate"
            except Exception:
                pass  # proceed on error

        # Step 3: store raw (idempotent)
        newly_inserted = self._store_raw(msg, normalized, dedup_hash)
        if not newly_inserted:
            return "hard_duplicate"

        # Step 3.5: macro tagging (shadow, non-blocking)
        self._tag_macro(msg, normalized)

        # Step 4: extract tickers
        mentions = self._parser_fn(msg.message_text)
        if not mentions:
            return "inserted"

        # Step 5: resolve and filter mentions
        resolved = [self._mapper.resolve(m) for m in mentions]
        relevant = [m for m in resolved if self._mapper.is_relevant(m)]

        # Step 6: score and persist
        if relevant:
            sentiment = self._scorer.score(msg.message_text)
            for mention in relevant:
                self._store_mention(msg, mention)
                self._store_sentiment(msg, mention, sentiment)

        return "inserted"

    def _store_raw(
        self,
        msg: TelegramMessage,
        normalized_text: str,
        dedup_hash: str,
    ) -> bool:
        """Store raw message. Returns True if newly inserted."""
        if self._repository is None:
            return True
        try:
            return self._repository.insert_telegram_message_raw(
                msg,
                normalized_text=normalized_text,
                dedup_hash=dedup_hash,
            )
        except Exception:
            self._logger.exception(
                "failed to persist raw message",
                extra={
                    "component": "telegram_sentiment",
                    "message_id": msg.message_id,
                },
            )
            return True

    def _store_mention(self, msg: TelegramMessage, mention: TickerMention) -> None:
        if self._repository is None:
            return
        try:
            self._repository.insert_telegram_message_mention(msg, mention)
        except Exception:
            self._logger.exception(
                "failed to persist mention",
                extra={
                    "component": "telegram_sentiment",
                    "ticker": mention.ticker,
                },
            )

    def _store_sentiment(
        self, msg: TelegramMessage, mention: TickerMention, result: object,
    ) -> None:
        if self._repository is None:
            return
        try:
            self._repository.insert_telegram_sentiment_event(msg, mention, result)
        except Exception:
            self._logger.exception(
                "failed to persist sentiment event",
                extra={
                    "component": "telegram_sentiment",
                    "ticker": mention.ticker,
                },
            )

    def _tag_macro(self, msg: TelegramMessage, normalized: str) -> None:
        """Tag message with macro context (shadow, non-blocking).

        Detects market themes (oil, gas, risk, etc.) and persists
        for later analysis. Never blocks ingestion.
        """
        if self._repository is None:
            return
        try:
            from tinvest_trader.services.macro_mapping import (
                get_affected_tickers,
                get_sectors,
            )
            from tinvest_trader.services.macro_tagging import tag_macro_message

            tags = tag_macro_message(normalized)
            if not tags:
                return

            sectors = get_sectors(tags)
            tickers = get_affected_tickers(tags)

            self._repository.insert_macro_message(
                source_message_id=msg.message_id,
                channel_name=msg.channel_name,
                tags=tags,
                sectors=sectors,
                affected_tickers=tickers,
                raw_text=normalized,
            )
        except Exception:
            self._logger.exception(
                "failed to tag macro message (non-blocking)",
                extra={
                    "component": "telegram_sentiment",
                    "channel": msg.channel_name,
                    "message_id": msg.message_id,
                },
            )
