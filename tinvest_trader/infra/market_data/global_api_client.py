"""Global market data API client -- Yahoo Finance chart endpoint.

Fetches current prices for a small set of global instruments using
Yahoo Finance's public chart API (v8). Stdlib only, no extra dependencies.

Chosen source: Yahoo Finance chart API
Why: free, no API key, JSON response, covers all required instruments
(indices, oil, FX, volatility), stdlib urllib is sufficient.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError

_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
_TIMEOUT = 15
_USER_AGENT = "tinvest_trader/1.0"

# Default instrument set for v1
DEFAULT_SYMBOLS: dict[str, dict[str, str]] = {
    "^GSPC": {"name": "SPX", "category": "index"},
    "^NDX": {"name": "NDX", "category": "index"},
    "^VIX": {"name": "VIX", "category": "volatility"},
    "BZ=F": {"name": "BRENT", "category": "oil"},
    "DX-Y.NYB": {"name": "DXY", "category": "fx"},
}


@dataclass(frozen=True)
class MarketSnapshot:
    """Normalized market data point."""

    symbol: str
    category: str
    price: float
    change_pct: float | None
    source_time: datetime | None
    source_name: str = "yahoo_finance"


def fetch_instrument(
    yahoo_symbol: str,
    logger: logging.Logger,
    *,
    timeout: int = _TIMEOUT,
) -> dict | None:
    """Fetch raw chart data for a single Yahoo Finance symbol.

    Returns parsed JSON dict or None on failure.
    """
    url = f"{_BASE_URL}/{yahoo_symbol}?range=1d&interval=1d"
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        return json.loads(data)
    except (HTTPError, URLError, json.JSONDecodeError, OSError) as exc:
        logger.warning(
            "yahoo finance fetch failed",
            extra={
                "component": "global_market_data",
                "symbol": yahoo_symbol,
                "error": str(exc),
            },
        )
        return None


def normalize_response(
    yahoo_symbol: str,
    raw: dict,
    symbol_map: dict[str, dict[str, str]] | None = None,
) -> MarketSnapshot | None:
    """Normalize Yahoo Finance chart response into MarketSnapshot.

    Returns None if response is malformed or missing data.
    """
    smap = symbol_map or DEFAULT_SYMBOLS
    meta_info = smap.get(yahoo_symbol, {})
    display_symbol = meta_info.get("name", yahoo_symbol)
    category = meta_info.get("category", "unknown")

    try:
        result = raw["chart"]["result"][0]
        meta = result["meta"]
        price = float(meta["regularMarketPrice"])

        # Change percent
        change_pct = None
        prev_close = meta.get("chartPreviousClose") or meta.get(
            "previousClose",
        )
        if prev_close and float(prev_close) > 0:
            change_pct = round(
                (price - float(prev_close)) / float(prev_close) * 100, 2,
            )

        # Source time
        source_time = None
        ts = meta.get("regularMarketTime")
        if ts:
            source_time = datetime.fromtimestamp(int(ts), tz=UTC)

        return MarketSnapshot(
            symbol=display_symbol,
            category=category,
            price=round(price, 4),
            change_pct=change_pct,
            source_time=source_time,
        )
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def fetch_all_instruments(
    logger: logging.Logger,
    *,
    symbols: dict[str, dict[str, str]] | None = None,
    timeout: int = _TIMEOUT,
) -> list[MarketSnapshot]:
    """Fetch and normalize all configured instruments.

    Returns list of successfully fetched snapshots.
    Partial failures are logged and skipped.
    """
    smap = symbols or DEFAULT_SYMBOLS
    results: list[MarketSnapshot] = []

    for yahoo_symbol in smap:
        raw = fetch_instrument(yahoo_symbol, logger, timeout=timeout)
        if raw is None:
            continue
        snapshot = normalize_response(yahoo_symbol, raw, smap)
        if snapshot is not None:
            results.append(snapshot)

    return results
