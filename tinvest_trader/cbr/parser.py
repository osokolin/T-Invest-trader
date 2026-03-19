"""CBR RSS feed parser -- converts XML bytes into CbrFeedItem/CbrEvent lists."""

from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from tinvest_trader.cbr.models import CbrEvent, CbrFeedItem

# Strip HTML tags for summary extraction
_TAG_RE = re.compile(r"<[^>]+>")


def _parse_pub_date(text: str | None) -> datetime | None:
    """Parse RFC 2822 pubDate into a timezone-aware datetime."""
    if not text or not text.strip():
        return None
    try:
        dt = parsedate_to_datetime(text.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except (ValueError, TypeError):
        return None


def _strip_html(text: str) -> str:
    """Remove HTML tags and unescape entities."""
    unescaped = html.unescape(text)
    return _TAG_RE.sub("", unescaped).strip()


def _classify_event_type(source_url: str) -> str:
    """Determine event_type from the feed URL."""
    lower = source_url.lower()
    if "rsspress" in lower:
        return "press_release"
    if "eventrss" in lower:
        return "event"
    if "rssnews" in lower:
        return "news"
    return "rss_item"


def parse_rss_items(xml_bytes: bytes, source_url: str) -> list[CbrFeedItem]:
    """Parse RSS XML into a list of CbrFeedItem objects."""
    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    if channel is None:
        return []

    items: list[CbrFeedItem] = []
    for item_el in channel.findall("item"):
        title = (item_el.findtext("title") or "").strip()
        link = (item_el.findtext("link") or "").strip()
        guid = (item_el.findtext("guid") or "").strip()
        description = (item_el.findtext("description") or "").strip()
        pub_date_text = (item_el.findtext("pubDate") or "").strip()

        # Use guid as item_uid; fall back to link
        item_uid = guid or link
        if not item_uid:
            continue

        published_at = _parse_pub_date(pub_date_text)

        # Serialize the item element back to XML for raw storage
        payload_xml = ET.tostring(item_el, encoding="unicode")

        items.append(CbrFeedItem(
            source_url=source_url,
            item_uid=item_uid,
            title=title,
            link=link,
            published_at=published_at,
            description=description,
            payload_xml=payload_xml,
        ))

    return items


def normalize_item(item: CbrFeedItem) -> CbrEvent:
    """Convert a raw CbrFeedItem into a normalized CbrEvent."""
    event_type = _classify_event_type(item.source_url)
    summary = _strip_html(item.description) if item.description else ""

    return CbrEvent(
        source_url=item.source_url,
        event_type=event_type,
        title=item.title,
        published_at=item.published_at,
        event_key=item.item_uid,
        url=item.link,
        summary=summary,
    )
