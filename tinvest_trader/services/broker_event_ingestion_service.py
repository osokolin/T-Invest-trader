"""Broker-side structured event ingestion orchestration."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from tinvest_trader.domain.models import BrokerEventRaw, Instrument
from tinvest_trader.infra.tbank.mapper import map_broker_event_feature

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository
    from tinvest_trader.infra.tbank.client import TBankClient


_SOURCE_METHODS = {
    "dividends": "GetDividends",
    "reports": "GetAssetReports",
    "insider_deals": "GetInsiderDeals",
}


@dataclass(frozen=True)
class _TrackedInstrumentRef:
    figi: str
    ticker: str | None
    instrument_uid: str | None


class BrokerEventIngestionService:
    """Fetches structured broker events, stores raw payloads, and normalizes features."""

    def __init__(
        self,
        client: TBankClient,
        repository: TradingRepository | None,
        logger: logging.Logger,
        account_id: str,
        tracked_figis: tuple[str, ...],
        event_types: tuple[str, ...],
        lookback_days_by_event_type: dict[str, int],
    ) -> None:
        self._client = client
        self._repository = repository
        self._logger = logger
        self._account_id = account_id
        self._tracked_figis = tracked_figis
        self._event_types = event_types
        self._lookback_days_by_event_type = dict(lookback_days_by_event_type)

    def ingest_all(self, as_of: datetime | None = None) -> int:
        """Run one full broker structured-event ingestion pass."""
        if not self._tracked_figis:
            self._logger.info(
                "broker event ingestion skipped: no tracked figis configured",
                extra={"component": "broker_events"},
            )
            return 0

        now = as_of or datetime.now(tz=UTC)
        tracked_instruments = self._resolve_tracked_instruments()
        total_processed = 0

        for event_type in self._event_types:
            if event_type not in _SOURCE_METHODS:
                self._logger.warning(
                    "skipping unsupported broker event type",
                    extra={"component": "broker_events", "event_type": event_type},
                )
                continue

            for instrument in tracked_instruments:
                try:
                    total_processed += self._ingest_event_type_for_instrument(
                        event_type=event_type,
                        instrument=instrument,
                        as_of=now,
                    )
                except Exception:
                    self._logger.exception(
                        "broker event ingestion failed for instrument",
                        extra={
                            "component": "broker_events",
                            "event_type": event_type,
                            "figi": instrument.figi,
                        },
                    )

        self._logger.info(
            "broker event ingestion complete",
            extra={
                "component": "broker_events",
                "tracked_figis": len(tracked_instruments),
                "event_types": list(self._event_types),
                "processed": total_processed,
            },
        )
        return total_processed

    def _resolve_tracked_instruments(self) -> list[_TrackedInstrumentRef]:
        resolved: list[_TrackedInstrumentRef] = []
        for figi in self._tracked_figis:
            try:
                raw_instrument = self._client.get_instrument(figi)
            except Exception:
                self._logger.exception(
                    "failed to resolve tracked instrument metadata",
                    extra={"component": "broker_events", "figi": figi},
                )
                raw_instrument = {}

            ticker = raw_instrument.get("ticker") or None
            instrument_uid = (
                raw_instrument.get("uid")
                or raw_instrument.get("instrument_uid")
            )

            # Fallback: look up ticker from instrument_catalog if API
            # returned no ticker (stub mode or transient failure).
            if not ticker and self._repository is not None:
                try:
                    ticker = self._repository.fetch_ticker_by_figi(figi)
                    if ticker:
                        self._logger.info(
                            "resolved ticker from instrument catalog",
                            extra={
                                "component": "broker_events",
                                "figi": figi,
                                "ticker": ticker,
                            },
                        )
                except Exception:
                    self._logger.exception(
                        "failed to look up ticker from instrument catalog",
                        extra={"component": "broker_events", "figi": figi},
                    )

            # Persist resolved metadata for future fallback lookups.
            if ticker and self._repository is not None:
                try:
                    self._repository.upsert_instrument(
                        inst=Instrument(figi=figi, ticker=ticker, name=""),
                        tracked=True,
                        enabled=False,
                        instrument_uid=instrument_uid,
                    )
                except Exception:
                    self._logger.exception(
                        "failed to cache instrument metadata",
                        extra={
                            "component": "broker_events",
                            "figi": figi,
                            "ticker": ticker,
                        },
                    )

            if not ticker:
                self._logger.warning(
                    "ticker resolution failed for tracked figi",
                    extra={"component": "broker_events", "figi": figi},
                )

            resolved.append(
                _TrackedInstrumentRef(
                    figi=figi,
                    ticker=ticker,
                    instrument_uid=instrument_uid,
                ),
            )
        return resolved

    def _ingest_event_type_for_instrument(
        self,
        event_type: str,
        instrument: _TrackedInstrumentRef,
        as_of: datetime,
    ) -> int:
        source_method = _SOURCE_METHODS[event_type]
        lookback_days = self._lookback_days_for_event_type(event_type)
        window_start = as_of - timedelta(days=max(1, lookback_days))
        raw_events = self._fetch_raw_events(event_type, instrument, window_start, as_of)
        processed = 0

        for raw_event in raw_events:
            feature = map_broker_event_feature(
                source_method=source_method,
                raw=raw_event,
                figi=instrument.figi,
                ticker=raw_event.get("ticker") or instrument.ticker,
                account_id=self._account_id,
            )
            if feature.event_time is not None and feature.event_time < window_start:
                continue

            if self._repository is None:
                processed += 1
                continue

            raw_record = BrokerEventRaw(
                account_id=self._account_id,
                source_method=feature.source_method,
                figi=feature.figi,
                ticker=feature.ticker,
                event_uid=feature.event_uid,
                event_time=feature.event_time,
                payload=raw_event,
            )
            raw_inserted = self._repository.insert_broker_event_raw(raw_record)
            feature_inserted = self._repository.insert_broker_event_feature(feature)
            if raw_inserted or feature_inserted:
                processed += 1

        self._logger.info(
            "broker event source ingestion complete",
            extra={
                "component": "broker_events",
                "event_type": event_type,
                "source_method": source_method,
                "figi": instrument.figi,
                "processed": processed,
                "fetched": len(raw_events),
            },
        )
        return processed

    def _lookback_days_for_event_type(self, event_type: str) -> int:
        return max(1, self._lookback_days_by_event_type.get(event_type, 1))

    def _fetch_raw_events(
        self,
        event_type: str,
        instrument: _TrackedInstrumentRef,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict]:
        if event_type == "dividends":
            return self._client.get_dividends(
                figi=instrument.figi,
                from_time=window_start,
                to_time=window_end,
            )
        if event_type == "reports":
            instrument_id = instrument.instrument_uid or instrument.figi
            return self._client.get_asset_reports(
                instrument_uid=instrument_id,
                from_time=window_start,
                to_time=window_end,
            )
        if event_type == "insider_deals":
            instrument_id = instrument.instrument_uid or instrument.figi
            return self._client.get_insider_deals(instrument_uid=instrument_id)
        raise ValueError(f"unsupported broker event type: {event_type}")
