"""Telegram sentiment ingestion orchestration service."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from tinvest_trader.sentiment.models import TelegramMessage, TickerMention

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository
    from tinvest_trader.sentiment.instrument_mapper import InstrumentMapper
    from tinvest_trader.sentiment.scorer import SentimentScorer
    from tinvest_trader.sentiment.source import MessageSource


class TelegramSentimentService:
    """Orchestrates Telegram message ingestion, parsing, and sentiment scoring.

    Flow per message:
    1. Store raw message (idempotent -- skip duplicates)
    2. Extract tickers
    3. Map tickers to instruments, filter by tracked
    4. Score sentiment per relevant mention
    5. Persist mention + sentiment rows
    """

    def __init__(
        self,
        source: MessageSource,
        parser_fn: Callable[[str], list[TickerMention]],
        mapper: InstrumentMapper,
        scorer: SentimentScorer,
        repository: TradingRepository | None,
        logger: logging.Logger,
    ) -> None:
        self._source = source
        self._parser_fn = parser_fn
        self._mapper = mapper
        self._scorer = scorer
        self._repository = repository
        self._logger = logger

    def ingest_channel(self, channel_name: str) -> int:
        """Fetch and process messages from a single channel. Returns processed count."""
        messages = self._source.fetch_recent_messages(channel_name)
        processed = 0

        for msg in messages:
            try:
                if self._process_message(msg):
                    processed += 1
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
                "total": len(messages),
                "processed": processed,
            },
        )
        return processed

    def ingest_all_channels(self, channels: tuple[str, ...]) -> int:
        """Ingest all configured channels. Returns total processed count."""
        total = 0
        for channel in channels:
            total += self.ingest_channel(channel)
        return total

    def _process_message(self, msg: TelegramMessage) -> bool:
        """Process a single message. Returns True if newly processed."""
        # Step 1: store raw (idempotent)
        newly_inserted = self._store_raw(msg)
        if not newly_inserted:
            return False  # already processed, skip

        # Step 2: extract tickers
        mentions = self._parser_fn(msg.message_text)
        if not mentions:
            return True  # stored raw, no tickers to process

        # Step 3: resolve and filter mentions
        resolved = [self._mapper.resolve(m) for m in mentions]
        relevant = [m for m in resolved if self._mapper.is_relevant(m)]

        # Step 4: score and persist
        if relevant:
            sentiment = self._scorer.score(msg.message_text)
            for mention in relevant:
                self._store_mention(msg, mention)
                self._store_sentiment(msg, mention, sentiment)

        return True

    def _store_raw(self, msg: TelegramMessage) -> bool:
        """Store raw message. Returns True if newly inserted."""
        if self._repository is None:
            return True  # no DB, treat as new
        try:
            return self._repository.insert_telegram_message_raw(msg)
        except Exception:
            self._logger.exception(
                "failed to persist raw message",
                extra={
                    "component": "telegram_sentiment",
                    "message_id": msg.message_id,
                },
            )
            return True  # proceed with processing on DB failure

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
