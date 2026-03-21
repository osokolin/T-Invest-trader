"""Message source abstraction for Telegram channel ingestion."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from tinvest_trader.sentiment.models import TelegramMessage


class MessageSource(Protocol):
    """Protocol for fetching messages from a channel."""

    def fetch_recent_messages(
        self,
        channel_name: str,
        min_id: int | None = None,
    ) -> list[TelegramMessage]: ...


class StubMessageSource:
    """Returns canned MarketTwits-style messages for testing."""

    def __init__(self, messages: list[TelegramMessage] | None = None) -> None:
        self._messages = messages

    def fetch_recent_messages(
        self,
        channel_name: str,
        min_id: int | None = None,
    ) -> list[TelegramMessage]:
        if self._messages is not None:
            return self._messages
        now = datetime.now(tz=UTC)
        return [
            TelegramMessage(
                channel_name=channel_name,
                message_id="1001",
                message_text="#SBER растет на фоне сильных квартальных результатов",
                published_at=now,
            ),
            TelegramMessage(
                channel_name=channel_name,
                message_id="1002",
                message_text="$GAZP падение после отмены дивидендов",
                published_at=now,
            ),
            TelegramMessage(
                channel_name=channel_name,
                message_id="1003",
                message_text="Рынок в боковике, #LKOH и #ROSN без изменений",
                published_at=now,
            ),
        ]
