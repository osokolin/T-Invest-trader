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

    selected_shares = _select_preferred_shares(shares)
    result.skipped += len(shares) - len(selected_shares)

    if limit > 0:
        selected_shares = selected_shares[:limit]

    result.synced = len(selected_shares)

    for share in selected_shares:
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


def _select_preferred_shares(shares: list[dict]) -> list[dict]:
    """Choose one deterministic, tradable instrument for each ticker."""
    selected: dict[str, dict] = {}
    for share in shares:
        ticker = str(share.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        current = selected.get(ticker)
        if current is None or _share_priority(share) > _share_priority(current):
            selected[ticker] = share
    return list(selected.values())


def _share_priority(share: dict) -> tuple[bool, bool, bool, bool, bool, bool]:
    return (
        bool(share.get("api_trade_available")),
        str(share.get("class_code") or "").upper() == "TQBR",
        str(share.get("trading_status") or "").upper()
        == "SECURITY_TRADING_STATUS_NORMAL_TRADING",
        str(share.get("real_exchange") or "").upper() == "REAL_EXCHANGE_MOEX",
        bool(share.get("buy_available")),
        bool(share.get("sell_available")),
    )
