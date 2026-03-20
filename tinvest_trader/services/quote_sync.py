"""Quote sync service -- bulk last-price ingestion from T-Bank API."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

from tinvest_trader.infra.storage.repository import TradingRepository
from tinvest_trader.infra.tbank.client import TBankClient

_PLACEHOLDER_FIGI_PREFIX = "TICKER:"


@dataclass
class QuoteSyncResult:
    """Summary of a quote sync cycle."""

    requested: int = 0
    received: int = 0
    inserted: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


def sync_quotes(
    *,
    client: TBankClient,
    repository: TradingRepository,
    logger: logging.Logger,
    limit: int = 0,
) -> QuoteSyncResult:
    """Fetch last prices for tracked instruments and persist to DB.

    Flow:
    1. List tracked instruments from DB
    2. Filter to those with real FIGI / instrument_uid
    3. Bulk-fetch last prices via T-Bank API
    4. Map prices back to instruments and persist
    5. Return summary
    """
    result = QuoteSyncResult()

    # 1. Get tracked instruments
    try:
        instruments = repository.list_tracked_instruments()
    except Exception:
        logger.exception(
            "quote sync: failed to list tracked instruments",
            extra={"component": "quote_sync"},
        )
        result.errors.append("failed to list tracked instruments")
        return result

    if not instruments:
        logger.info(
            "quote sync: no tracked instruments",
            extra={"component": "quote_sync"},
        )
        return result

    # 2. Filter to instruments with real FIGI (skip placeholders)
    eligible = []
    for inst in instruments:
        figi = inst.get("figi", "")
        if not figi or figi.startswith(_PLACEHOLDER_FIGI_PREFIX):
            result.skipped += 1
            continue
        eligible.append(inst)

    if limit > 0:
        eligible = eligible[:limit]

    if not eligible:
        logger.info(
            "quote sync: no eligible instruments (all placeholders)",
            extra={"component": "quote_sync", "total": len(instruments)},
        )
        return result

    # 3. Build instrument_id list (prefer instrument_uid, fallback to figi)
    uid_to_inst: dict[str, dict] = {}
    figi_to_inst: dict[str, dict] = {}
    instrument_ids: list[str] = []

    for inst in eligible:
        uid = (inst.get("instrument_uid") or "").strip()
        figi = inst["figi"]
        # Prefer uid for API call, fallback to figi
        api_id = uid if uid else figi
        instrument_ids.append(api_id)
        if uid:
            uid_to_inst[uid] = inst
        figi_to_inst[figi] = inst

    result.requested = len(instrument_ids)

    # 4. Bulk fetch
    try:
        raw_prices = client.get_last_prices(instrument_ids)
    except Exception:
        logger.exception(
            "quote sync: bulk fetch failed",
            extra={"component": "quote_sync", "requested": result.requested},
        )
        result.failed = result.requested
        result.errors.append("bulk fetch failed")
        return result

    result.received = len(raw_prices)

    # 5. Map prices to instruments and build quote records
    now = datetime.now(UTC)
    quotes_to_insert: list[dict] = []

    for price_item in raw_prices:
        price_uid = price_item.get("instrument_uid", "")
        price_val = price_item.get("price")
        source_time_str = price_item.get("source_time", "")

        # Resolve to our instrument
        inst = uid_to_inst.get(price_uid)
        if inst is None:
            # Try reverse lookup by checking all eligible
            for elig in eligible:
                elig_uid = (elig.get("instrument_uid") or "").strip()
                if elig_uid == price_uid:
                    inst = elig
                    break

        if inst is None:
            result.skipped += 1
            continue

        # Parse source_time if present
        source_time = _parse_timestamp(source_time_str)

        quotes_to_insert.append({
            "ticker": inst["ticker"],
            "figi": inst["figi"],
            "instrument_uid": price_uid,
            "price": price_val,
            "source_time": source_time,
            "fetched_at": now,
        })

    # 6. Persist
    if quotes_to_insert:
        try:
            inserted_count = repository.insert_market_quotes_bulk(quotes_to_insert)
            result.inserted = inserted_count
            result.failed = len(quotes_to_insert) - inserted_count
        except Exception:
            logger.exception(
                "quote sync: persist failed",
                extra={"component": "quote_sync"},
            )
            result.failed = len(quotes_to_insert)
            result.errors.append("persist failed")

    logger.info(
        "quote sync complete",
        extra={
            "component": "quote_sync",
            "requested": result.requested,
            "received": result.received,
            "inserted": result.inserted,
            "skipped": result.skipped,
            "failed": result.failed,
        },
    )
    return result


def _parse_timestamp(value: str) -> datetime | None:
    """Parse ISO timestamp from T-Bank API response."""
    if not value:
        return None
    try:
        # T-Bank uses ISO format like "2026-03-20T10:30:00.123Z"
        cleaned = value.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except (ValueError, TypeError):
        return None
