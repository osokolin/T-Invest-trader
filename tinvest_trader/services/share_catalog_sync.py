"""Bulk share catalog sync -- downloads T-Bank share catalog and upserts locally."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository
    from tinvest_trader.infra.tbank.client import TBankClient


@dataclass
class SyncResult:
    """Summary of one catalog sync run."""

    synced: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0


def sync_share_catalog(
    repository: TradingRepository,
    client: TBankClient,
    logger: logging.Logger,
    *,
    limit: int = 0,
) -> SyncResult:
    """Fetch all shares from T-Bank and upsert into instrument_catalog.

    - Does NOT auto-track new instruments (tracked stays as-is in DB).
    - Enriches existing tracked rows with real FIGI/name/ISIN/UID.
    - Preserves placeholder protection logic via repository upsert.
    """
    result = SyncResult()

    shares = client.list_all_shares()
    if not shares:
        logger.warning(
            "share catalog sync: no shares returned from API",
            extra={"component": "share_catalog_sync"},
        )
        return result

    if limit > 0:
        shares = shares[:limit]

    result.synced = len(shares)

    for share in shares:
        ticker = share.get("ticker", "")
        if not ticker:
            result.skipped += 1
            continue

        try:
            outcome = repository.upsert_catalog_entry(
                ticker=ticker,
                figi=share.get("figi", ""),
                instrument_uid=share.get("uid", ""),
                name=share.get("name", ""),
                isin=share.get("isin", ""),
                lot=share.get("lot"),
                currency=share.get("currency"),
            )
            if outcome == "inserted":
                result.inserted += 1
            elif outcome == "updated":
                result.updated += 1
            else:
                result.skipped += 1
        except Exception:
            result.failed += 1
            logger.warning(
                "share catalog sync: upsert failed",
                extra={
                    "component": "share_catalog_sync",
                    "ticker": ticker,
                },
                exc_info=True,
            )

    logger.info(
        "share catalog sync complete",
        extra={
            "component": "share_catalog_sync",
            "synced": result.synced,
            "inserted": result.inserted,
            "updated": result.updated,
            "skipped": result.skipped,
            "failed": result.failed,
        },
    )
    return result
