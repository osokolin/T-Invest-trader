"""Instrument enrichment -- fills missing fields in instrument_catalog."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository
    from tinvest_trader.infra.tbank.client import TBankClient


@dataclass
class EnrichmentResult:
    """Summary of one enrichment run."""

    processed: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


def _needs_enrichment(inst: dict) -> bool:
    """Return True if an instrument has missing or placeholder data."""
    figi = inst.get("figi") or ""
    if not figi or figi.startswith("TICKER:"):
        return True
    if not inst.get("instrument_uid"):
        return True
    return bool(not inst.get("name"))


def enrich_instruments(
    repository: TradingRepository,
    client: TBankClient,
    logger: logging.Logger,
    *,
    limit: int = 0,
) -> EnrichmentResult:
    """Enrich tracked instruments with data from T-Bank API.

    Fetches instrument metadata by ticker, then upserts missing fields
    (figi, instrument_uid, name, isin) into instrument_catalog.
    Already-complete instruments are skipped. API failures are logged
    but do not abort the run.
    """
    result = EnrichmentResult()

    tracked = repository.list_tracked_instruments()
    candidates = [inst for inst in tracked if _needs_enrichment(inst)]

    if limit > 0:
        candidates = candidates[:limit]

    for inst in candidates:
        ticker = inst["ticker"]
        result.processed += 1
        try:
            api_data = client.get_instrument_by_ticker(ticker)
        except Exception as exc:
            result.failed += 1
            msg = f"{ticker}: API lookup failed: {exc}"
            result.errors.append(msg)
            logger.warning(
                "instrument enrichment failed",
                extra={"component": "enrichment", "ticker": ticker, "error": str(exc)},
            )
            continue

        if not api_data or not api_data.get("figi"):
            result.skipped += 1
            logger.info(
                "instrument enrichment skipped: no API data",
                extra={"component": "enrichment", "ticker": ticker},
            )
            continue

        try:
            repository.ensure_instrument(
                ticker=ticker,
                figi=api_data.get("figi", ""),
                name=api_data.get("name", ""),
                isin=api_data.get("isin", ""),
                moex_secid=ticker,
                tracked=True,
            )
            # Also update instrument_uid via direct SQL if available
            uid = api_data.get("uid") or ""
            if uid:
                repository.update_instrument_uid(ticker=ticker, instrument_uid=uid)

            result.updated += 1
            logger.info(
                "instrument enriched",
                extra={
                    "component": "enrichment",
                    "ticker": ticker,
                    "figi": api_data.get("figi"),
                },
            )
        except Exception as exc:
            result.failed += 1
            msg = f"{ticker}: persist failed: {exc}"
            result.errors.append(msg)
            logger.warning(
                "instrument enrichment persist failed",
                extra={"component": "enrichment", "ticker": ticker, "error": str(exc)},
            )

    skipped_complete = len(tracked) - len(candidates)
    result.skipped += skipped_complete

    return result
