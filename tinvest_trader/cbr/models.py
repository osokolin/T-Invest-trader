"""CBR domain models -- raw feed items and normalized events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class CbrFeedItem:
    """A single raw item from a CBR RSS feed."""

    source_url: str
    item_uid: str
    title: str
    link: str
    published_at: datetime | None
    description: str
    payload_xml: str


@dataclass(frozen=True)
class CbrEvent:
    """A normalized CBR event derived from a feed item."""

    source_url: str
    event_type: str
    title: str
    published_at: datetime | None
    event_key: str
    url: str
    summary: str
