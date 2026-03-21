"""Global market data sync service -- structured price ingestion.

Fetches current prices for global instruments (indices, oil, FX)
via Yahoo Finance chart API, normalizes, and persists to
global_market_snapshots table.

Separate pipeline from Telegram/news global context ingestion.
Ingestion + storage + reporting only -- NO signal influence.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tinvest_trader.infra.market_data.global_api_client import (
    DEFAULT_SYMBOLS,
    MarketSnapshot,
    fetch_all_instruments,
)

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass
class GlobalMarketDataSyncResult:
    """Summary of a sync cycle."""

    requested: int = 0
    received: int = 0
    inserted: int = 0
    failed: int = 0
    missing_symbols: list[str] = field(default_factory=list)


def sync_global_market_data(
    repository: TradingRepository | None,
    logger: logging.Logger,
    *,
    symbols: dict[str, dict[str, str]] | None = None,
    timeout: int = 15,
) -> GlobalMarketDataSyncResult:
    """Fetch, normalize, and persist global market data.

    Returns sync result summary.
    """
    smap = symbols or DEFAULT_SYMBOLS
    result = GlobalMarketDataSyncResult(requested=len(smap))

    # Fetch from API
    snapshots = fetch_all_instruments(logger, symbols=smap, timeout=timeout)
    result.received = len(snapshots)

    # Detect missing
    fetched_names = {s.symbol for s in snapshots}
    all_names = {info["name"] for info in smap.values()}
    result.missing_symbols = sorted(all_names - fetched_names)

    if not snapshots:
        logger.warning(
            "global market data sync: no data received",
            extra={"component": "global_market_data"},
        )
        return result

    # Persist
    if repository is None:
        logger.warning(
            "global market data sync: no repository, skipping persistence",
            extra={"component": "global_market_data"},
        )
        return result

    for snap in snapshots:
        ok = repository.insert_global_market_snapshot(
            _snapshot_to_dict(snap),
        )
        if ok:
            result.inserted += 1
        else:
            result.failed += 1

    logger.info(
        "global_market_data_sync",
        extra={
            "component": "global_market_data",
            "requested": result.requested,
            "received": result.received,
            "inserted": result.inserted,
            "failed": result.failed,
        },
    )

    return result


def _snapshot_to_dict(snap: MarketSnapshot) -> dict:
    """Convert MarketSnapshot to dict for repository insertion."""
    return {
        "symbol": snap.symbol,
        "category": snap.category,
        "price": snap.price,
        "change_pct": snap.change_pct,
        "source_time": snap.source_time,
        "source_name": snap.source_name,
    }


def build_global_market_data_report(
    repository: TradingRepository,
) -> str:
    """Build a human-readable report of latest global market data."""
    snapshots = repository.get_latest_global_market_snapshots()

    if not snapshots:
        return "global market data snapshot\n  no data yet"

    lines: list[str] = ["global market data snapshot", ""]

    # Group by category
    by_category: dict[str, list[dict]] = {}
    for snap in snapshots:
        cat = snap["category"]
        by_category.setdefault(cat, []).append(snap)

    category_order = ["index", "volatility", "oil", "fx"]
    category_labels = {
        "index": "indices",
        "volatility": "volatility",
        "oil": "oil",
        "fx": "fx",
    }

    for cat in category_order:
        items = by_category.get(cat)
        if not items:
            continue

        lines.append(f"{category_labels.get(cat, cat)}:")
        for item in items:
            price = item["price"]
            chg = item.get("change_pct")
            chg_str = f" ({chg:+.1f}%)" if chg is not None else ""
            ts = item.get("source_time")
            ts_str = ""
            if ts:
                ts_str = f"  [{ts:%H:%M UTC}]"
            lines.append(f"  {item['symbol']}: {price:.2f}{chg_str}{ts_str}")
        lines.append("")

    return "\n".join(lines).rstrip()
