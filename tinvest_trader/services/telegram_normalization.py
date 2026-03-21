"""Telegram message text normalization and dedup helpers.

Simple, deterministic text normalization for deduplication.
No NLP. No ML. Just whitespace/unicode cleanup and hashing.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata

_WHITESPACE_RE = re.compile(r"\s+")
_URL_RE = re.compile(r"https?://\S+")


def normalize_message_text(text: str) -> str:
    """Normalize message text for comparison and dedup.

    - Strip surrounding whitespace
    - Normalize unicode (NFC)
    - Collapse repeated whitespace to single space
    - Lowercase
    """
    text = text.strip()
    text = unicodedata.normalize("NFC", text)
    text = _WHITESPACE_RE.sub(" ", text)
    return text.lower()


def strip_urls(text: str) -> str:
    """Remove URLs from text."""
    return _URL_RE.sub("", text).strip()


def build_dedup_hash(source: str, text: str) -> str:
    """Build dedup key from source + normalized text (without URLs).

    Returns hex SHA-256 prefix (16 chars) for compact storage.
    """
    normalized = normalize_message_text(strip_urls(text))
    raw = f"{source}:{normalized}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
