"""Ticker extraction from message text."""

from __future__ import annotations

import re

from tinvest_trader.sentiment.models import TickerMention

# Patterns for ticker extraction
_HASHTAG_RE = re.compile(r"#([A-Za-z]{2,6})\b")
_CASHTAG_RE = re.compile(r"\$([A-Za-z]{2,6})\b")


def extract_tickers(text: str) -> list[TickerMention]:
    """Extract ticker mentions from message text.

    Supports:
    - #SBER -> hashtag mention
    - $GAZP -> cashtag mention

    Returns deduplicated mentions, normalized to uppercase.
    """
    seen: set[str] = set()
    mentions: list[TickerMention] = []

    for match in _HASHTAG_RE.finditer(text):
        ticker = match.group(1).upper()
        if ticker not in seen:
            seen.add(ticker)
            mentions.append(TickerMention(ticker=ticker, mention_type="hashtag"))

    for match in _CASHTAG_RE.finditer(text):
        ticker = match.group(1).upper()
        if ticker not in seen:
            seen.add(ticker)
            mentions.append(TickerMention(ticker=ticker, mention_type="cashtag"))

    return mentions
