"""Global market context domain models -- pure data structures."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class GlobalContextEvent:
    """A classified global market context event."""

    source_key: str            # e.g. "financialjuice", "oilprice"
    source_channel: str        # Telegram channel name
    telegram_message_id: str | None = None
    raw_text: str = ""
    normalized_text: str = ""
    event_type: str = "unknown"    # risk_sentiment, oil, crypto, macro
    direction: str = "unknown"     # positive, negative, neutral, unknown
    confidence: float = 0.0
    event_time: datetime | None = None
    dedup_hash: str = ""
