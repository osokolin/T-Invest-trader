"""Trading repository -- audit trail write path with explicit SQL."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from tinvest_trader.cbr.models import CbrEvent, CbrFeedItem
from tinvest_trader.domain.models import (
    BrokerEventFeature,
    BrokerEventRaw,
    ExecutionResult,
    Instrument,
    MarketSnapshot,
    OrderIntent,
)
from tinvest_trader.fusion.models import FusedSignalFeature
from tinvest_trader.infra.storage.postgres import PostgresPool
from tinvest_trader.moex.models import (
    MoexHistoryRow,
    MoexMarketHistoryNormalized,
    MoexSecurityInfo,
)
from tinvest_trader.observation.models import SignalObservation
from tinvest_trader.sentiment.models import SentimentResult, TelegramMessage, TickerMention


class TradingRepository:
    """Write-path repository for the audit trail. Explicit SQL, no ORM."""

    def __init__(self, pool: PostgresPool, logger: logging.Logger) -> None:
        self._pool = pool
        self._logger = logger

    # -- Instrument catalog --

    def upsert_instrument(
        self,
        inst: Instrument,
        tracked: bool,
        enabled: bool,
        instrument_uid: str | None = None,
        lot: int | None = None,
        currency: str | None = None,
    ) -> None:
        """Upsert instrument using ticker as the canonical conflict key.

        When a bootstrap placeholder row exists (figi='TICKER:SBER'), this
        replaces the placeholder figi with the real one. Non-empty incoming
        fields always win over empty/placeholder values.
        """
        sql = """
            INSERT INTO instrument_catalog
                (figi, instrument_uid, ticker, name, lot, currency, tracked, enabled, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (ticker)
            DO UPDATE SET
                figi = CASE
                    WHEN EXCLUDED.figi != '' AND NOT EXCLUDED.figi LIKE 'TICKER:%%'
                    THEN EXCLUDED.figi
                    ELSE instrument_catalog.figi
                END,
                instrument_uid = COALESCE(
                    EXCLUDED.instrument_uid,
                    instrument_catalog.instrument_uid
                ),
                name = CASE
                    WHEN EXCLUDED.name != '' THEN EXCLUDED.name
                    ELSE instrument_catalog.name
                END,
                lot = COALESCE(EXCLUDED.lot, instrument_catalog.lot),
                currency = COALESCE(EXCLUDED.currency, instrument_catalog.currency),
                tracked = EXCLUDED.tracked,
                enabled = EXCLUDED.enabled,
                updated_at = now()
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (
                inst.figi, instrument_uid, inst.ticker.upper(), inst.name,
                lot, currency, tracked, enabled,
            ))
            conn.commit()

    def fetch_ticker_by_figi(self, figi: str) -> str | None:
        """Look up ticker from instrument_catalog by FIGI."""
        sql = """
            SELECT ticker FROM instrument_catalog
            WHERE figi = %s AND ticker IS NOT NULL AND ticker != ''
            LIMIT 1
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (figi,))
            row = cur.fetchone()
        return row[0] if row else None

    def list_tracked_instruments(self) -> list[dict]:
        """Return all instruments with tracked=True."""
        sql = """
            SELECT ticker, figi, instrument_uid, name, isin, moex_secid,
                   lot, currency, enabled, updated_at
            FROM instrument_catalog
            WHERE tracked = TRUE
            ORDER BY ticker
        """
        columns = (
            "ticker", "figi", "instrument_uid", "name", "isin", "moex_secid",
            "lot", "currency", "enabled", "updated_at",
        )
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql)
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    def list_all_instruments(self) -> list[dict]:
        """Return all instruments."""
        sql = """
            SELECT ticker, figi, instrument_uid, name, isin, moex_secid,
                   lot, currency, tracked, enabled, updated_at
            FROM instrument_catalog
            ORDER BY ticker
        """
        columns = (
            "ticker", "figi", "instrument_uid", "name", "isin", "moex_secid",
            "lot", "currency", "tracked", "enabled", "updated_at",
        )
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql)
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    def get_instrument_by_ticker(self, ticker: str) -> dict | None:
        """Look up instrument by ticker. Returns dict or None."""
        sql = """
            SELECT ticker, figi, instrument_uid, name, isin, moex_secid,
                   lot, currency, tracked, enabled, updated_at
            FROM instrument_catalog
            WHERE ticker = %s
            LIMIT 1
        """
        columns = (
            "ticker", "figi", "instrument_uid", "name", "isin", "moex_secid",
            "lot", "currency", "tracked", "enabled", "updated_at",
        )
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (ticker.upper(),))
            row = cur.fetchone()
        if row is None:
            return None
        return dict(zip(columns, row, strict=True))

    def set_tracked_status(self, ticker: str, tracked: bool) -> bool:
        """Set tracked status for a ticker. Returns True if row was updated."""
        sql = """
            UPDATE instrument_catalog
            SET tracked = %s, updated_at = now()
            WHERE ticker = %s
            RETURNING id
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (tracked, ticker.upper()))
            updated = cur.fetchone() is not None
            conn.commit()
        return updated

    def update_instrument_uid(self, ticker: str, instrument_uid: str) -> None:
        """Set instrument_uid for a ticker if not already set."""
        sql = """
            UPDATE instrument_catalog
            SET instrument_uid = %s, updated_at = now()
            WHERE ticker = %s
              AND (instrument_uid IS NULL OR instrument_uid = '')
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (instrument_uid, ticker.upper()))
            conn.commit()

    def ensure_instrument(
        self,
        ticker: str,
        tracked: bool = False,
        figi: str = "",
        name: str = "",
        isin: str = "",
        moex_secid: str = "",
    ) -> None:
        """Insert instrument if not exists, or update tracked status if exists.

        Uses ticker as the conflict key. If the instrument already exists,
        only upgrades tracked (to True if requested), replaces placeholder figi
        with real one, and fills metadata fields only when non-empty.
        """
        sql = """
            INSERT INTO instrument_catalog
                (figi, ticker, name, isin, moex_secid, tracked, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (ticker)
            DO UPDATE SET
                tracked = instrument_catalog.tracked OR EXCLUDED.tracked,
                figi = CASE
                    WHEN EXCLUDED.figi != '' AND NOT EXCLUDED.figi LIKE 'TICKER:%%'
                    THEN EXCLUDED.figi
                    ELSE instrument_catalog.figi
                END,
                name = CASE
                    WHEN EXCLUDED.name != '' THEN EXCLUDED.name
                    ELSE instrument_catalog.name
                END,
                moex_secid = CASE
                    WHEN EXCLUDED.moex_secid != '' THEN EXCLUDED.moex_secid
                    ELSE instrument_catalog.moex_secid
                END,
                isin = CASE
                    WHEN EXCLUDED.isin != '' THEN EXCLUDED.isin
                    ELSE instrument_catalog.isin
                END,
                updated_at = now()
        """
        effective_figi = figi if figi else f"TICKER:{ticker.upper()}"
        with self._pool.get_connection() as conn:
            conn.execute(sql, (
                effective_figi, ticker.upper(), name, isin,
                moex_secid if moex_secid else ticker.upper(), tracked,
            ))
            conn.commit()

    def upsert_catalog_entry(
        self,
        ticker: str,
        figi: str = "",
        instrument_uid: str = "",
        name: str = "",
        isin: str = "",
        lot: int | None = None,
        currency: str | None = None,
    ) -> str:
        """Upsert a catalog entry from bulk data. Never changes tracked flag.

        Returns 'inserted' if new row, 'updated' if existing row changed,
        'skipped' if existing row had no changes to apply.
        Uses same placeholder-safe CASE logic as ensure_instrument.
        """
        effective_figi = figi if figi else f"TICKER:{ticker.upper()}"
        sql = """
            INSERT INTO instrument_catalog
                (figi, instrument_uid, ticker, name, isin, lot, currency,
                 tracked, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE, now())
            ON CONFLICT (ticker)
            DO UPDATE SET
                figi = CASE
                    WHEN EXCLUDED.figi != '' AND NOT EXCLUDED.figi LIKE 'TICKER:%%'
                    THEN EXCLUDED.figi
                    ELSE instrument_catalog.figi
                END,
                instrument_uid = CASE
                    WHEN EXCLUDED.instrument_uid IS NOT NULL
                         AND EXCLUDED.instrument_uid != ''
                    THEN EXCLUDED.instrument_uid
                    ELSE instrument_catalog.instrument_uid
                END,
                name = CASE
                    WHEN EXCLUDED.name != '' THEN EXCLUDED.name
                    ELSE instrument_catalog.name
                END,
                isin = CASE
                    WHEN EXCLUDED.isin != '' THEN EXCLUDED.isin
                    ELSE instrument_catalog.isin
                END,
                lot = COALESCE(EXCLUDED.lot, instrument_catalog.lot),
                currency = COALESCE(EXCLUDED.currency, instrument_catalog.currency),
                updated_at = now()
            RETURNING (xmax = 0) AS inserted
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (
                effective_figi, instrument_uid or None, ticker.upper(),
                name, isin, lot, currency,
            ))
            row = cur.fetchone()
            conn.commit()
        if row is None:
            return "skipped"
        return "inserted" if row[0] else "updated"

    def count_tracked_instruments(self) -> int:
        """Return count of tracked instruments."""
        sql = "SELECT count(*) FROM instrument_catalog WHERE tracked = TRUE"
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql)
            row = cur.fetchone()
        return int(row[0]) if row else 0

    def bootstrap_tracked_instruments(self, tickers: tuple[str, ...]) -> int:
        """Seed instrument_catalog from env tickers if DB tracked set is empty.

        Returns number of instruments seeded. Does nothing if tracked rows exist.
        """
        if self.count_tracked_instruments() > 0:
            return 0
        seeded = 0
        for ticker in tickers:
            t = ticker.strip().upper()
            if not t:
                continue
            self.ensure_instrument(ticker=t, tracked=True)
            seeded += 1
        return seeded

    # -- Broker event fetch state --

    def get_fetch_state(self, figi: str, event_type: str) -> dict | None:
        """Return fetch state for (figi, event_type) or None."""
        sql = """
            SELECT last_checked_at, last_success_at, last_error_at, error_count
            FROM broker_event_fetch_state
            WHERE figi = %s AND event_type = %s
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (figi, event_type))
            row = cur.fetchone()
        if row is None:
            return None
        return {
            "last_checked_at": row[0],
            "last_success_at": row[1],
            "last_error_at": row[2],
            "error_count": row[3],
        }

    def get_all_fetch_states(self) -> list[dict]:
        """Return all fetch states as list of dicts."""
        sql = """
            SELECT figi, event_type, last_checked_at, last_success_at,
                   last_error_at, error_count
            FROM broker_event_fetch_state
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql)
            rows = cur.fetchall()
        cols = (
            "figi", "event_type", "last_checked_at",
            "last_success_at", "last_error_at", "error_count",
        )
        return [dict(zip(cols, row, strict=True)) for row in rows]

    def record_fetch_success(
        self, figi: str, event_type: str, now: datetime,
    ) -> None:
        """Record a successful fetch for (figi, event_type)."""
        sql = """
            INSERT INTO broker_event_fetch_state
                (figi, event_type, last_checked_at, last_success_at, error_count, updated_at)
            VALUES (%s, %s, %s, %s, 0, %s)
            ON CONFLICT (figi, event_type)
            DO UPDATE SET
                last_checked_at = EXCLUDED.last_checked_at,
                last_success_at = EXCLUDED.last_success_at,
                error_count = 0,
                updated_at = EXCLUDED.updated_at
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (figi, event_type, now, now, now))
            conn.commit()

    def record_fetch_failure(
        self, figi: str, event_type: str, now: datetime,
    ) -> None:
        """Record a failed fetch for (figi, event_type)."""
        sql = """
            INSERT INTO broker_event_fetch_state
                (figi, event_type, last_checked_at, last_error_at,
                 error_count, updated_at)
            VALUES (%s, %s, %s, %s, 1, %s)
            ON CONFLICT (figi, event_type)
            DO UPDATE SET
                last_checked_at = EXCLUDED.last_checked_at,
                last_error_at = EXCLUDED.last_error_at,
                error_count = broker_event_fetch_state.error_count + 1,
                updated_at = EXCLUDED.updated_at
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (figi, event_type, now, now, now))
            conn.commit()

    def list_broker_fetch_failures(
        self, *, min_error_count: int = 1, limit: int = 50,
    ) -> list[dict]:
        """Return fetch states with error_count >= min_error_count."""
        sql = """
            SELECT fs.figi, fs.event_type, fs.last_checked_at,
                   fs.last_success_at, fs.last_error_at, fs.error_count,
                   ic.ticker
            FROM broker_event_fetch_state fs
            LEFT JOIN instrument_catalog ic ON ic.figi = fs.figi
            WHERE fs.error_count >= %s
            ORDER BY fs.error_count DESC, fs.last_error_at DESC
            LIMIT %s
        """
        cols = (
            "figi", "event_type", "last_checked_at",
            "last_success_at", "last_error_at", "error_count", "ticker",
        )
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (min_error_count, limit))
            return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def list_broker_fetch_never_succeeded(
        self, *, limit: int = 50,
    ) -> list[dict]:
        """Return fetch states that have never succeeded."""
        sql = """
            SELECT fs.figi, fs.event_type, fs.last_checked_at,
                   fs.last_error_at, fs.error_count, ic.ticker
            FROM broker_event_fetch_state fs
            LEFT JOIN instrument_catalog ic ON ic.figi = fs.figi
            WHERE fs.last_success_at IS NULL
            ORDER BY fs.error_count DESC, fs.last_checked_at DESC
            LIMIT %s
        """
        cols = (
            "figi", "event_type", "last_checked_at",
            "last_error_at", "error_count", "ticker",
        )
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (limit,))
            return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def list_broker_fetch_stale(
        self, *, stale_seconds: int = 172800, limit: int = 50,
    ) -> list[dict]:
        """Return fetch states considered stale.

        Stale means: last_success_at is older than stale_seconds,
        or last_success_at is NULL and last_checked_at is older than stale_seconds.
        """
        sql = """
            SELECT fs.figi, fs.event_type, fs.last_checked_at,
                   fs.last_success_at, fs.last_error_at, fs.error_count,
                   ic.ticker
            FROM broker_event_fetch_state fs
            LEFT JOIN instrument_catalog ic ON ic.figi = fs.figi
            WHERE (
                fs.last_success_at IS NOT NULL
                AND fs.last_success_at < now() - make_interval(secs => %s)
            ) OR (
                fs.last_success_at IS NULL
                AND fs.last_checked_at IS NOT NULL
                AND fs.last_checked_at < now() - make_interval(secs => %s)
            )
            ORDER BY COALESCE(fs.last_success_at, fs.last_checked_at) ASC
            LIMIT %s
        """
        cols = (
            "figi", "event_type", "last_checked_at",
            "last_success_at", "last_error_at", "error_count", "ticker",
        )
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (stale_seconds, stale_seconds, limit))
            return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def get_broker_fetch_policy_summary(self) -> dict:
        """Return aggregate summary of broker fetch state."""
        sql = """
            SELECT
                count(*) AS total_states,
                count(*) FILTER (WHERE last_success_at IS NOT NULL) AS succeeded_ever,
                count(*) FILTER (WHERE last_success_at IS NULL) AS never_succeeded,
                count(*) FILTER (WHERE error_count > 0) AS recent_failures,
                max(error_count) AS max_error_count
            FROM broker_event_fetch_state
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql)
            row = cur.fetchone()
        if row is None:
            return {
                "total_states": 0,
                "succeeded_ever": 0,
                "never_succeeded": 0,
                "recent_failures": 0,
                "max_error_count": 0,
            }
        return {
            "total_states": row[0],
            "succeeded_ever": row[1],
            "never_succeeded": row[2],
            "recent_failures": row[3],
            "max_error_count": row[4] or 0,
        }

    # -- Signal predictions --

    def insert_signal_prediction(
        self,
        ticker: str,
        signal_type: str,
        price_at_signal: float | None,
        confidence: float | None = None,
        source: str = "fusion",
        features_json: dict | None = None,
        created_at: datetime | None = None,
        *,
        source_channel: str | None = None,
        source_message_id: str | None = None,
        source_message_db_id: int | None = None,
    ) -> int | None:
        """Insert a signal prediction. Returns the new row id."""
        import json as _json

        sql = """
            INSERT INTO signal_predictions
                (ticker, signal_type, confidence, source, features_json,
                 price_at_signal, created_at,
                 source_channel, source_message_id, source_message_db_id,
                 pipeline_stage)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'generated')
            RETURNING id
        """
        if created_at is None:
            created_at = datetime.now(UTC)
        features = (
            _json.dumps(features_json, default=str)
            if features_json
            else None
        )
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(
                    sql,
                    (
                        ticker, signal_type, confidence, source,
                        features, price_at_signal, created_at,
                        source_channel, source_message_id,
                        source_message_db_id,
                    ),
                ).fetchone()
                return row[0] if row else None
        except Exception:
            self._logger.exception(
                "failed to insert signal prediction",
                extra={"component": "postgres", "ticker": ticker},
            )
            return None

    def list_pending_predictions(
        self, before: datetime,
    ) -> list[dict]:
        """List unresolved predictions created before the given time."""
        sql = """
            SELECT id, ticker, signal_type, price_at_signal, created_at
            FROM signal_predictions
            WHERE resolved_at IS NULL
              AND created_at < %s
            ORDER BY created_at
        """
        with self._pool.connection() as conn:
            rows = conn.execute(sql, (before,)).fetchall()
        return [
            {
                "id": r[0],
                "ticker": r[1],
                "signal_type": r[2],
                "price_at_signal": float(r[3]) if r[3] is not None else None,
                "created_at": r[4],
            }
            for r in rows
        ]

    def resolve_prediction(
        self,
        prediction_id: int,
        price_at_outcome: float,
        return_pct: float,
        outcome_label: str,
        resolved_at: datetime,
    ) -> None:
        """Update a prediction with outcome data."""
        sql = """
            UPDATE signal_predictions
            SET price_at_outcome = %s,
                return_pct = %s,
                outcome_label = %s,
                resolved_at = %s
            WHERE id = %s
              AND resolved_at IS NULL
        """
        try:
            with self._pool.connection() as conn:
                conn.execute(
                    sql,
                    (
                        price_at_outcome, return_pct, outcome_label,
                        resolved_at, prediction_id,
                    ),
                )
        except Exception:
            self._logger.exception(
                "failed to resolve prediction",
                extra={
                    "component": "postgres",
                    "prediction_id": prediction_id,
                },
            )

    def get_signal_stats(self) -> dict:
        """Aggregate signal prediction statistics."""
        sql = """
            SELECT
                count(*) AS total,
                count(resolved_at) AS resolved,
                count(*) FILTER (WHERE outcome_label = 'win') AS wins,
                count(*) FILTER (WHERE outcome_label = 'loss') AS losses,
                count(*) FILTER (WHERE outcome_label = 'neutral') AS neutrals,
                avg(return_pct) FILTER (WHERE resolved_at IS NOT NULL)
                    AS avg_return
            FROM signal_predictions
        """
        with self._pool.connection() as conn:
            row = conn.execute(sql).fetchone()
        if not row:
            return {
                "total": 0, "resolved": 0, "wins": 0, "losses": 0,
                "neutrals": 0, "avg_return": None,
            }
        return {
            "total": row[0],
            "resolved": row[1],
            "wins": row[2],
            "losses": row[3],
            "neutrals": row[4],
            "avg_return": float(row[5]) if row[5] is not None else None,
        }

    def get_signal_stats_by_ticker(self) -> list[dict]:
        """Aggregate signal prediction statistics grouped by ticker."""
        sql = """
            SELECT
                ticker,
                count(*) AS total,
                count(resolved_at) AS resolved,
                count(*) FILTER (WHERE outcome_label = 'win') AS wins,
                avg(return_pct) FILTER (WHERE resolved_at IS NOT NULL)
                    AS avg_return
            FROM signal_predictions
            GROUP BY ticker
            ORDER BY total DESC
        """
        with self._pool.connection() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {
                "ticker": r[0],
                "total": r[1],
                "resolved": r[2],
                "wins": r[3],
                "avg_return": float(r[4]) if r[4] is not None else None,
            }
            for r in rows
        ]

    def get_signal_stats_by_type(self) -> list[dict]:
        """Aggregate signal prediction statistics grouped by signal_type."""
        sql = """
            SELECT
                signal_type,
                count(*) AS total,
                count(resolved_at) AS resolved,
                count(*) FILTER (WHERE outcome_label = 'win') AS wins,
                avg(return_pct) FILTER (WHERE resolved_at IS NOT NULL)
                    AS avg_return
            FROM signal_predictions
            GROUP BY signal_type
            ORDER BY signal_type
        """
        with self._pool.connection() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {
                "signal_type": r[0],
                "total": r[1],
                "resolved": r[2],
                "wins": r[3],
                "avg_return": float(r[4]) if r[4] is not None else None,
            }
            for r in rows
        ]

    # -- Source performance attribution --

    def get_signal_stats_by_source(self) -> list[dict]:
        """Aggregate signal stats grouped by source_channel."""
        sql = """
            SELECT
                source_channel,
                count(*) AS total,
                count(resolved_at) AS resolved,
                count(*) FILTER (WHERE outcome_label = 'win') AS wins,
                count(*) FILTER (WHERE outcome_label = 'loss') AS losses,
                count(*) FILTER (WHERE outcome_label = 'neutral') AS neutrals,
                avg(return_pct) FILTER (WHERE resolved_at IS NOT NULL)
                    AS avg_return
            FROM signal_predictions
            WHERE source_channel IS NOT NULL
            GROUP BY source_channel
            ORDER BY total DESC
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {
                "source_channel": r[0],
                "total": r[1],
                "resolved": r[2],
                "wins": r[3],
                "losses": r[4],
                "neutrals": r[5],
                "avg_return": float(r[6]) if r[6] is not None else None,
            }
            for r in rows
        ]

    def get_signal_stats_by_source_and_ticker(self) -> list[dict]:
        """Aggregate signal stats grouped by source_channel + ticker."""
        sql = """
            SELECT
                source_channel,
                ticker,
                count(*) AS total,
                count(resolved_at) AS resolved,
                count(*) FILTER (WHERE outcome_label = 'win') AS wins,
                count(*) FILTER (WHERE outcome_label = 'loss') AS losses,
                count(*) FILTER (WHERE outcome_label = 'neutral') AS neutrals,
                avg(return_pct) FILTER (WHERE resolved_at IS NOT NULL)
                    AS avg_return
            FROM signal_predictions
            WHERE source_channel IS NOT NULL
            GROUP BY source_channel, ticker
            ORDER BY source_channel, total DESC
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {
                "source_channel": r[0],
                "ticker": r[1],
                "total": r[2],
                "resolved": r[3],
                "wins": r[4],
                "losses": r[5],
                "neutrals": r[6],
                "avg_return": float(r[7]) if r[7] is not None else None,
            }
            for r in rows
        ]

    def get_signal_stats_by_source_and_type(self) -> list[dict]:
        """Aggregate signal stats grouped by source_channel + signal_type."""
        sql = """
            SELECT
                source_channel,
                signal_type,
                count(*) AS total,
                count(resolved_at) AS resolved,
                count(*) FILTER (WHERE outcome_label = 'win') AS wins,
                count(*) FILTER (WHERE outcome_label = 'loss') AS losses,
                count(*) FILTER (WHERE outcome_label = 'neutral') AS neutrals,
                avg(return_pct) FILTER (WHERE resolved_at IS NOT NULL)
                    AS avg_return
            FROM signal_predictions
            WHERE source_channel IS NOT NULL
            GROUP BY source_channel, signal_type
            ORDER BY source_channel, signal_type
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {
                "source_channel": r[0],
                "signal_type": r[1],
                "total": r[2],
                "resolved": r[3],
                "wins": r[4],
                "losses": r[5],
                "neutrals": r[6],
                "avg_return": float(r[7]) if r[7] is not None else None,
            }
            for r in rows
        ]

    # -- Source-aware weighting (shadow mode) --

    def get_unweighted_signals(self, limit: int = 500) -> list[dict]:
        """Fetch signals with source_channel but no source_weight yet."""
        sql = """
            SELECT id, source_channel, confidence
            FROM signal_predictions
            WHERE source_channel IS NOT NULL
              AND source_weight IS NULL
            ORDER BY created_at DESC
            LIMIT %s
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
        return [
            {
                "id": r[0],
                "source_channel": r[1],
                "confidence": float(r[2]) if r[2] is not None else None,
            }
            for r in rows
        ]

    def update_source_weight(
        self,
        signal_id: int,
        *,
        source_weight: float,
        weighted_confidence: float | None = None,
        weighted_severity: str | None = None,
    ) -> bool:
        """Persist shadow source weight fields for a signal."""
        sql = """
            UPDATE signal_predictions
            SET source_weight = %s,
                weighted_confidence = %s,
                weighted_severity = %s
            WHERE id = %s
        """
        try:
            with self._pool.get_connection() as conn:
                conn.execute(
                    sql,
                    (source_weight, weighted_confidence, weighted_severity,
                     signal_id),
                )
            return True
        except Exception:
            self._logger.exception(
                "failed to update source weight",
                extra={"component": "postgres", "signal_id": signal_id},
            )
            return False

    def get_source_weighting_baseline(self) -> dict:
        """Get baseline performance stats for all resolved signals."""
        sql = """
            SELECT
                count(*) AS total,
                count(resolved_at) AS resolved,
                count(*) FILTER (WHERE outcome_label = 'win') AS wins,
                count(*) FILTER (WHERE outcome_label = 'loss') AS losses,
                avg(return_pct) FILTER (WHERE resolved_at IS NOT NULL)
                    AS avg_return
            FROM signal_predictions
            WHERE resolved_at IS NOT NULL
        """
        with self._pool.get_connection() as conn:
            row = conn.execute(sql).fetchone()
        if not row:
            return {
                "total": 0, "resolved": 0, "wins": 0, "losses": 0,
                "avg_return": None,
            }
        return {
            "total": row[0],
            "resolved": row[1],
            "wins": row[2],
            "losses": row[3],
            "avg_return": float(row[4]) if row[4] is not None else None,
        }

    def get_weighted_performance(self, *, threshold: float = 0.6) -> dict:
        """Get performance stats for signals above weighted_confidence threshold."""
        sql = """
            SELECT
                count(*) AS total,
                count(resolved_at) AS resolved,
                count(*) FILTER (WHERE outcome_label = 'win') AS wins,
                count(*) FILTER (WHERE outcome_label = 'loss') AS losses,
                avg(return_pct) FILTER (WHERE resolved_at IS NOT NULL)
                    AS avg_return
            FROM signal_predictions
            WHERE resolved_at IS NOT NULL
              AND weighted_confidence IS NOT NULL
              AND weighted_confidence >= %s
        """
        with self._pool.get_connection() as conn:
            row = conn.execute(sql, (threshold,)).fetchone()
        if not row:
            return {
                "total": 0, "resolved": 0, "wins": 0, "losses": 0,
                "avg_return": None,
            }
        return {
            "total": row[0],
            "resolved": row[1],
            "wins": row[2],
            "losses": row[3],
            "avg_return": float(row[4]) if row[4] is not None else None,
        }

    def get_source_weights_snapshot(self) -> list[dict]:
        """Get current source weight distribution from stored signals."""
        sql = """
            SELECT
                source_channel,
                avg(source_weight) AS avg_weight,
                count(*) AS count,
                count(resolved_at) AS resolved
            FROM signal_predictions
            WHERE source_channel IS NOT NULL
              AND source_weight IS NOT NULL
            GROUP BY source_channel
            ORDER BY avg_weight DESC
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {
                "source_channel": r[0],
                "avg_weight": float(r[1]) if r[1] is not None else None,
                "count": r[2],
                "resolved": r[3],
            }
            for r in rows
        ]

    # -- Global market context --

    def insert_global_context_event(
        self,
        event: dict,
    ) -> bool:
        """Insert a global context event. Returns True if newly inserted.

        Uses ON CONFLICT DO NOTHING for hard dedup by (source_key, telegram_message_id).
        """
        import json as _json

        sql = """
            INSERT INTO global_market_context_events
                (source_key, source_channel, telegram_message_id,
                 raw_text, normalized_text, event_type, direction,
                 confidence, event_time, dedup_hash, metadata_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_key, telegram_message_id) DO NOTHING
            RETURNING id
        """
        metadata = event.get("metadata_json")
        metadata_str = (
            _json.dumps(metadata, default=str) if metadata else None
        )
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(
                    sql,
                    (
                        event["source_key"],
                        event["source_channel"],
                        event.get("telegram_message_id"),
                        event.get("raw_text", ""),
                        event.get("normalized_text", ""),
                        event.get("event_type", "unknown"),
                        event.get("direction", "unknown"),
                        event.get("confidence", 0.0),
                        event.get("event_time"),
                        event.get("dedup_hash"),
                        metadata_str,
                    ),
                ).fetchone()
                return row is not None
        except Exception:
            self._logger.exception(
                "failed to insert global context event",
                extra={
                    "component": "postgres",
                    "source_key": event.get("source_key"),
                },
            )
            return False

    def check_global_context_dedup_hash(self, dedup_hash: str) -> bool:
        """Check if a dedup hash already exists in global context events."""
        sql = """
            SELECT 1
            FROM global_market_context_events
            WHERE dedup_hash = %s
            LIMIT 1
        """
        with self._pool.get_connection() as conn:
            row = conn.execute(sql, (dedup_hash,)).fetchone()
        return row is not None

    def get_latest_global_context_message_id(
        self, source_key: str,
    ) -> int | None:
        """Get max telegram_message_id for incremental fetch."""
        sql = """
            SELECT max(telegram_message_id::bigint)
            FROM global_market_context_events
            WHERE source_key = %s
              AND telegram_message_id IS NOT NULL
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql, (source_key,)).fetchone()
            if row and row[0] is not None:
                return int(row[0])
        except Exception:
            self._logger.exception(
                "failed to get latest global context message id",
                extra={
                    "component": "postgres",
                    "source_key": source_key,
                },
            )
        return None

    def get_global_context_summary(self) -> list[dict]:
        """Get event counts grouped by event_type + direction."""
        sql = """
            SELECT event_type, direction, count(*) AS count
            FROM global_market_context_events
            GROUP BY event_type, direction
            ORDER BY event_type, direction
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {"event_type": r[0], "direction": r[1], "count": r[2]}
            for r in rows
        ]

    def get_recent_global_context_events(
        self, limit: int = 10,
    ) -> list[dict]:
        """Get most recent global context events."""
        sql = """
            SELECT source_key, event_type, direction,
                   confidence, event_time, raw_text
            FROM global_market_context_events
            ORDER BY fetched_at DESC
            LIMIT %s
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
        return [
            {
                "source_key": r[0],
                "event_type": r[1],
                "direction": r[2],
                "confidence": float(r[3]) if r[3] is not None else 0.0,
                "event_time": r[4],
                "raw_text": r[5],
            }
            for r in rows
        ]

    # -- Global context -> signal enrichment (shadow) --

    def get_global_context_for_enrichment(
        self, *, lookback_seconds: int = 900,
    ) -> list[dict]:
        """Get recent global context events for enrichment snapshot."""
        sql = """
            SELECT event_type, direction, confidence
            FROM global_market_context_events
            WHERE event_type != 'unknown'
              AND direction != 'unknown'
              AND fetched_at >= now() - make_interval(secs => %s)
            ORDER BY fetched_at DESC
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql, (lookback_seconds,)).fetchall()
        return [
            {
                "event_type": r[0],
                "direction": r[1],
                "confidence": float(r[2]) if r[2] is not None else 0.0,
            }
            for r in rows
        ]

    def get_unenriched_global_context_signals(
        self, *, limit: int = 500,
    ) -> list[dict]:
        """Get signals that have not been enriched with global context yet."""
        sql = """
            SELECT id, signal_type, confidence
            FROM signal_predictions
            WHERE global_alignment IS NULL
            ORDER BY created_at DESC
            LIMIT %s
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
        return [
            {
                "id": r[0],
                "signal_type": r[1],
                "confidence": float(r[2]) if r[2] is not None else None,
            }
            for r in rows
        ]

    def update_global_context_enrichment(
        self,
        signal_id: int,
        *,
        global_alignment: str,
        global_adjustment: float,
        global_adjusted_confidence: float | None,
        global_context_json: str | None = None,
    ) -> bool:
        """Store global context enrichment shadow fields on a signal."""
        sql = """
            UPDATE signal_predictions
            SET global_alignment = %s,
                global_adjustment = %s,
                global_adjusted_confidence = %s,
                global_context_json = %s
            WHERE id = %s
        """
        try:
            with self._pool.get_connection() as conn:
                conn.execute(
                    sql,
                    (
                        global_alignment,
                        global_adjustment,
                        global_adjusted_confidence,
                        global_context_json,
                        signal_id,
                    ),
                )
            return True
        except Exception:
            self._logger.exception(
                "failed to update global context enrichment",
                extra={
                    "component": "postgres",
                    "signal_id": signal_id,
                },
            )
            return False

    def get_global_alignment_performance(self) -> list[dict]:
        """Get performance stats grouped by global alignment."""
        sql = """
            SELECT
                global_alignment,
                count(*) AS total,
                count(resolved_at) AS resolved,
                count(*) FILTER (WHERE outcome_label = 'win') AS wins,
                count(*) FILTER (WHERE outcome_label = 'loss') AS losses,
                avg(return_pct) FILTER (WHERE resolved_at IS NOT NULL)
                    AS avg_return
            FROM signal_predictions
            WHERE global_alignment IS NOT NULL
              AND resolved_at IS NOT NULL
            GROUP BY global_alignment
            ORDER BY global_alignment
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {
                "alignment": r[0],
                "total": r[1],
                "resolved": r[2],
                "wins": r[3],
                "losses": r[4],
                "avg_return": float(r[5]) if r[5] is not None else None,
            }
            for r in rows
        ]

    def get_global_alignment_breakdown(self) -> list[dict]:
        """Get signal count breakdown by alignment (all signals, not just resolved)."""
        sql = """
            SELECT
                global_alignment,
                count(*) AS total
            FROM signal_predictions
            WHERE global_alignment IS NOT NULL
            GROUP BY global_alignment
            ORDER BY global_alignment
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {"alignment": r[0], "total": r[1]}
            for r in rows
        ]

    # -- Global market data snapshots --

    def insert_global_market_snapshot(self, snapshot: dict) -> bool:
        """Insert a global market data snapshot. Returns True on success."""
        sql = """
            INSERT INTO global_market_snapshots
                (symbol, category, price, change_pct,
                 source_time, source_name, metadata_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(
                    sql,
                    (
                        snapshot["symbol"],
                        snapshot.get("category", "unknown"),
                        snapshot["price"],
                        snapshot.get("change_pct"),
                        snapshot.get("source_time"),
                        snapshot.get("source_name", "yahoo_finance"),
                        None,
                    ),
                ).fetchone()
                return row is not None
        except Exception:
            self._logger.exception(
                "failed to insert global market snapshot",
                extra={
                    "component": "postgres",
                    "symbol": snapshot.get("symbol"),
                },
            )
            return False

    def get_latest_global_market_snapshots(self) -> list[dict]:
        """Get most recent snapshot per symbol."""
        sql = """
            SELECT DISTINCT ON (symbol)
                symbol, category, price, change_pct,
                source_time, fetched_at, source_name
            FROM global_market_snapshots
            ORDER BY symbol, fetched_at DESC
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {
                "symbol": r[0],
                "category": r[1],
                "price": float(r[2]) if r[2] is not None else None,
                "change_pct": float(r[3]) if r[3] is not None else None,
                "source_time": r[4],
                "fetched_at": r[5],
                "source_name": r[6],
            }
            for r in rows
        ]

    def get_global_market_snapshot_history(
        self, *, symbol: str | None = None, limit: int = 50,
    ) -> list[dict]:
        """Get recent global market snapshot history."""
        if symbol:
            sql = """
                SELECT symbol, category, price, change_pct,
                       source_time, fetched_at, source_name
                FROM global_market_snapshots
                WHERE symbol = %s
                ORDER BY fetched_at DESC
                LIMIT %s
            """
            params: tuple = (symbol, limit)
        else:
            sql = """
                SELECT symbol, category, price, change_pct,
                       source_time, fetched_at, source_name
                FROM global_market_snapshots
                ORDER BY fetched_at DESC
                LIMIT %s
            """
            params = (limit,)
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            {
                "symbol": r[0],
                "category": r[1],
                "price": float(r[2]) if r[2] is not None else None,
                "change_pct": float(r[3]) if r[3] is not None else None,
                "source_time": r[4],
                "fetched_at": r[5],
                "source_name": r[6],
            }
            for r in rows
        ]

    # -- Signal delivery --

    def list_undelivered_signals(
        self, limit: int = 50,
    ) -> list[dict]:
        """List resolved signals that have not been delivered yet."""
        sql = """
            SELECT id, ticker, signal_type, confidence, source,
                   price_at_signal, created_at, source_channel,
                   return_pct, outcome_label
            FROM signal_predictions
            WHERE resolved_at IS NOT NULL
              AND delivered_at IS NULL
            ORDER BY created_at
            LIMIT %s
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
        return [
            {
                "id": r[0],
                "ticker": r[1],
                "signal_type": r[2],
                "confidence": float(r[3]) if r[3] is not None else None,
                "source": r[4],
                "price_at_signal": float(r[5]) if r[5] is not None else None,
                "created_at": r[6],
                "source_channel": r[7],
                "return_pct": float(r[8]) if r[8] is not None else None,
                "outcome_label": r[9],
            }
            for r in rows
        ]

    def mark_signal_delivered(
        self, prediction_id: int, delivered_at: datetime | None = None,
    ) -> bool:
        """Mark a signal as delivered. Returns True if updated."""
        if delivered_at is None:
            delivered_at = datetime.now(UTC)
        sql = """
            UPDATE signal_predictions
            SET delivered_at = %s
            WHERE id = %s AND delivered_at IS NULL
        """
        try:
            with self._pool.get_connection() as conn:
                cur = conn.execute(sql, (delivered_at, prediction_id))
                return cur.rowcount > 0
        except Exception:
            self._logger.exception(
                "failed to mark signal delivered",
                extra={
                    "component": "postgres",
                    "prediction_id": prediction_id,
                },
            )
            return False

    # -- Pipeline stage tracking --

    def update_signal_stage(
        self,
        prediction_id: int,
        stage: str,
        reason: str | None = None,
    ) -> bool:
        """Update the pipeline stage (and optional rejection reason) for a signal."""
        sql = """
            UPDATE signal_predictions
            SET pipeline_stage = %s,
                rejection_reason = %s
            WHERE id = %s
        """
        try:
            with self._pool.get_connection() as conn:
                cur = conn.execute(sql, (stage, reason, prediction_id))
                return cur.rowcount > 0
        except Exception:
            self._logger.exception(
                "failed to update signal stage",
                extra={
                    "component": "postgres",
                    "prediction_id": prediction_id,
                    "stage": stage,
                },
            )
            return False

    def get_divergence_stats(self) -> dict:
        """Aggregate signal counts by pipeline stage."""
        sql = """
            SELECT
                count(*) AS total,
                count(*) FILTER (
                    WHERE pipeline_stage = 'generated') AS generated,
                count(*) FILTER (
                    WHERE pipeline_stage = 'rejected_calibration'
                ) AS rejected_calibration,
                count(*) FILTER (
                    WHERE pipeline_stage = 'rejected_binding'
                ) AS rejected_binding,
                count(*) FILTER (
                    WHERE pipeline_stage = 'rejected_safety'
                ) AS rejected_safety,
                count(*) FILTER (
                    WHERE pipeline_stage = 'delivered') AS delivered,
                count(*) FILTER (
                    WHERE pipeline_stage IS NULL) AS untracked
            FROM signal_predictions
        """
        with self._pool.get_connection() as conn:
            row = conn.execute(sql).fetchone()
        if not row:
            return {}
        return {
            "total": row[0],
            "generated": row[1],
            "rejected_calibration": row[2],
            "rejected_binding": row[3],
            "rejected_safety": row[4],
            "delivered": row[5],
            "untracked": row[6],
        }

    def get_divergence_stats_by_stage(self) -> list[dict]:
        """Per-stage stats with win rate and avg return."""
        sql = """
            SELECT
                pipeline_stage,
                count(*) AS total,
                count(resolved_at) AS resolved,
                count(*) FILTER (WHERE outcome_label = 'win') AS wins,
                count(*) FILTER (WHERE outcome_label = 'loss') AS losses,
                count(*) FILTER (WHERE outcome_label = 'neutral') AS neutrals,
                avg(return_pct) FILTER (WHERE resolved_at IS NOT NULL) AS avg_return
            FROM signal_predictions
            WHERE pipeline_stage IS NOT NULL
            GROUP BY pipeline_stage
            ORDER BY pipeline_stage
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {
                "stage": r[0],
                "total": r[1],
                "resolved": r[2],
                "wins": r[3],
                "losses": r[4],
                "neutrals": r[5],
                "avg_return": float(r[6]) if r[6] is not None else None,
            }
            for r in rows
        ]

    def get_rejected_signals(
        self, stage: str, limit: int = 20,
    ) -> list[dict]:
        """List rejected signals for a specific stage."""
        sql = """
            SELECT id, ticker, signal_type, confidence,
                   rejection_reason, created_at,
                   return_pct, outcome_label
            FROM signal_predictions
            WHERE pipeline_stage = %s
            ORDER BY created_at DESC
            LIMIT %s
        """
        with self._pool.get_connection() as conn:
            rows = conn.execute(sql, (stage, limit)).fetchall()
        return [
            {
                "id": r[0],
                "ticker": r[1],
                "signal_type": r[2],
                "confidence": float(r[3]) if r[3] is not None else None,
                "rejection_reason": r[4],
                "created_at": r[5],
                "return_pct": float(r[6]) if r[6] is not None else None,
                "outcome_label": r[7],
            }
            for r in rows
        ]

    # -- Signal lookup (single prediction) --

    def get_signal_prediction(self, prediction_id: int) -> dict | None:
        """Fetch a single signal prediction by id."""
        sql = """
            SELECT id, ticker, signal_type, confidence, source,
                   price_at_signal, created_at, source_channel,
                   return_pct, outcome_label
            FROM signal_predictions
            WHERE id = %s
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql, (prediction_id,)).fetchone()
        except Exception:
            self._logger.exception(
                "failed to fetch signal prediction",
                extra={
                    "component": "postgres",
                    "prediction_id": prediction_id,
                },
            )
            return None
        if row is None:
            return None
        return {
            "id": row[0],
            "ticker": row[1],
            "signal_type": row[2],
            "confidence": float(row[3]) if row[3] is not None else None,
            "source": row[4],
            "price_at_signal": float(row[5]) if row[5] is not None else None,
            "created_at": row[6],
            "source_channel": row[7],
            "return_pct": float(row[8]) if row[8] is not None else None,
            "outcome_label": row[9],
        }

    # -- AI analysis cache --

    def get_cached_ai_analysis(self, signal_id: int) -> dict | None:
        """Fetch cached AI analysis for a signal."""
        sql = """
            SELECT analysis_text, model, created_at
            FROM signal_ai_analyses
            WHERE signal_id = %s
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql, (signal_id,)).fetchone()
        except Exception:
            self._logger.exception(
                "failed to fetch ai analysis cache",
                extra={
                    "component": "postgres",
                    "signal_id": signal_id,
                },
            )
            return None
        if row is None:
            return None
        return {
            "analysis_text": row[0],
            "model": row[1],
            "created_at": row[2],
        }

    def insert_ai_analysis(
        self,
        signal_id: int,
        analysis_text: str,
        model: str,
    ) -> bool:
        """Insert or ignore AI analysis cache entry."""
        sql = """
            INSERT INTO signal_ai_analyses (signal_id, analysis_text, model)
            VALUES (%s, %s, %s)
            ON CONFLICT (signal_id) DO NOTHING
        """
        try:
            with self._pool.get_connection() as conn:
                conn.execute(sql, (signal_id, analysis_text, model))
            return True
        except Exception:
            self._logger.exception(
                "failed to insert ai analysis",
                extra={
                    "component": "postgres",
                    "signal_id": signal_id,
                },
            )
            return False

    def update_signal_ai_structured_fields(
        self,
        signal_id: int,
        *,
        ai_confidence: str = "UNKNOWN",
        ai_actionability: str = "UNKNOWN",
        ai_bias: str = "unknown",
        system_severity: str = "",
        divergence_bucket: str = "unknown",
    ) -> bool:
        """Update structured AI divergence fields on an existing analysis row."""
        sql = """
            UPDATE signal_ai_analyses
            SET ai_confidence = %s,
                ai_actionability = %s,
                ai_bias = %s,
                system_severity = %s,
                divergence_bucket = %s
            WHERE signal_id = %s
        """
        try:
            with self._pool.get_connection() as conn:
                conn.execute(
                    sql,
                    (
                        ai_confidence,
                        ai_actionability,
                        ai_bias,
                        system_severity,
                        divergence_bucket,
                        signal_id,
                    ),
                )
            return True
        except Exception:
            self._logger.exception(
                "failed to update ai structured fields",
                extra={
                    "component": "postgres",
                    "signal_id": signal_id,
                },
            )
            return False

    def get_ai_snapshot(self, signal_id: int) -> dict | None:
        """Fetch AI structured fields for a signal (lightweight, no text)."""
        sql = """
            SELECT ai_confidence, ai_actionability
            FROM signal_ai_analyses
            WHERE signal_id = %s
              AND ai_confidence IS NOT NULL
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql, (signal_id,)).fetchone()
        except Exception:
            self._logger.exception(
                "failed to fetch ai snapshot",
                extra={"component": "postgres", "signal_id": signal_id},
            )
            return None
        if row is None:
            return None
        return {
            "ai_confidence": row[0],
            "ai_actionability": row[1],
        }

    def get_ai_divergence_stats(self) -> dict:
        """Get aggregate AI divergence stats."""
        sql = """
            SELECT
                COUNT(*) AS total_analyzed,
                COUNT(divergence_bucket) AS total_with_bucket
            FROM signal_ai_analyses
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql).fetchone()
        except Exception:
            self._logger.exception(
                "failed to fetch ai divergence stats",
                extra={"component": "postgres"},
            )
            return {}
        if row is None:
            return {}
        return {
            "total_analyzed": row[0],
            "total_with_bucket": row[1],
        }

    def get_ai_divergence_stats_by_bucket(self) -> list[dict]:
        """Get per-bucket divergence stats with outcome data."""
        sql = """
            SELECT
                a.divergence_bucket AS bucket,
                COUNT(*) AS total,
                COUNT(sp.outcome_label) AS resolved,
                COUNT(*) FILTER (WHERE sp.outcome_label = 'win') AS wins,
                COUNT(*) FILTER (WHERE sp.outcome_label = 'loss') AS losses,
                COUNT(*) FILTER (WHERE sp.outcome_label = 'neutral') AS neutrals,
                AVG(sp.return_pct) FILTER (WHERE sp.outcome_label IS NOT NULL)
                    AS avg_return
            FROM signal_ai_analyses a
            JOIN signal_predictions sp ON sp.id = a.signal_id
            WHERE a.divergence_bucket IS NOT NULL
            GROUP BY a.divergence_bucket
            ORDER BY total DESC
        """
        try:
            with self._pool.get_connection() as conn:
                rows = conn.execute(sql).fetchall()
        except Exception:
            self._logger.exception(
                "failed to fetch ai divergence stats by bucket",
                extra={"component": "postgres"},
            )
            return []
        return [
            {
                "bucket": row[0],
                "total": row[1],
                "resolved": row[2],
                "wins": row[3],
                "losses": row[4],
                "neutrals": row[5],
                "avg_return": float(row[6]) if row[6] is not None else None,
            }
            for row in rows
        ]

    def get_ai_divergence_examples(
        self,
        bucket: str,
        limit: int = 5,
    ) -> list[dict]:
        """Get example signals for a specific divergence bucket."""
        sql = """
            SELECT
                sp.id, sp.ticker, sp.signal_type, sp.confidence,
                sp.outcome_label, sp.return_pct,
                a.ai_confidence, a.system_severity, a.divergence_bucket
            FROM signal_ai_analyses a
            JOIN signal_predictions sp ON sp.id = a.signal_id
            WHERE a.divergence_bucket = %s
            ORDER BY a.created_at DESC
            LIMIT %s
        """
        try:
            with self._pool.get_connection() as conn:
                rows = conn.execute(sql, (bucket, limit)).fetchall()
        except Exception:
            self._logger.exception(
                "failed to fetch ai divergence examples",
                extra={"component": "postgres", "bucket": bucket},
            )
            return []
        return [
            {
                "id": row[0],
                "ticker": row[1],
                "signal_type": row[2],
                "confidence": float(row[3]) if row[3] is not None else None,
                "outcome_label": row[4],
                "return_pct": float(row[5]) if row[5] is not None else None,
                "ai_confidence": row[6],
                "system_severity": row[7],
                "divergence_bucket": row[8],
            }
            for row in rows
        ]

    # -- AI shadow gating --

    def update_signal_ai_gate(
        self,
        signal_id: int,
        decision: str,
        reason: str,
    ) -> bool:
        """Persist shadow AI gating decision on a signal prediction."""
        sql = """
            UPDATE signal_predictions
            SET ai_gate_decision = %s,
                ai_gate_reason = %s
            WHERE id = %s
        """
        try:
            with self._pool.get_connection() as conn:
                conn.execute(sql, (decision, reason, signal_id))
            return True
        except Exception:
            self._logger.exception(
                "failed to update ai gate decision",
                extra={
                    "component": "postgres",
                    "signal_id": signal_id,
                },
            )
            return False

    def get_ai_gating_stats(self) -> dict:
        """Get aggregate AI gating decision counts."""
        sql = """
            SELECT
                COUNT(*) AS total_signals,
                COUNT(ai_gate_decision) AS total_with_gate,
                COUNT(*) FILTER (WHERE ai_gate_decision = 'BLOCK') AS blocked,
                COUNT(*) FILTER (WHERE ai_gate_decision = 'CAUTION') AS caution,
                COUNT(*) FILTER (WHERE ai_gate_decision = 'ALLOW') AS allow
            FROM signal_predictions
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql).fetchone()
        except Exception:
            self._logger.exception(
                "failed to fetch ai gating stats",
                extra={"component": "postgres"},
            )
            return {}
        if row is None:
            return {}
        return {
            "total_signals": row[0],
            "total_with_gate": row[1],
            "blocked": row[2],
            "caution": row[3],
            "allow": row[4],
        }

    def get_ai_gating_performance(self) -> dict:
        """Get outcome stats for baseline, AI-filtered, and blocked groups."""
        sql = """
            SELECT
                'baseline' AS group_label,
                COUNT(*) AS total,
                COUNT(outcome_label) AS resolved,
                COUNT(*) FILTER (WHERE outcome_label = 'win') AS wins,
                COUNT(*) FILTER (WHERE outcome_label = 'loss') AS losses,
                COUNT(*) FILTER (WHERE outcome_label = 'neutral') AS neutrals,
                AVG(return_pct) FILTER (WHERE outcome_label IS NOT NULL)
                    AS avg_return
            FROM signal_predictions
            WHERE ai_gate_decision IS NOT NULL
            UNION ALL
            SELECT
                'ai_filtered',
                COUNT(*),
                COUNT(outcome_label),
                COUNT(*) FILTER (WHERE outcome_label = 'win'),
                COUNT(*) FILTER (WHERE outcome_label = 'loss'),
                COUNT(*) FILTER (WHERE outcome_label = 'neutral'),
                AVG(return_pct) FILTER (WHERE outcome_label IS NOT NULL)
            FROM signal_predictions
            WHERE ai_gate_decision IS NOT NULL
              AND ai_gate_decision != 'BLOCK'
            UNION ALL
            SELECT
                'blocked',
                COUNT(*),
                COUNT(outcome_label),
                COUNT(*) FILTER (WHERE outcome_label = 'win'),
                COUNT(*) FILTER (WHERE outcome_label = 'loss'),
                COUNT(*) FILTER (WHERE outcome_label = 'neutral'),
                AVG(return_pct) FILTER (WHERE outcome_label IS NOT NULL)
            FROM signal_predictions
            WHERE ai_gate_decision = 'BLOCK'
        """
        try:
            with self._pool.get_connection() as conn:
                rows = conn.execute(sql).fetchall()
        except Exception:
            self._logger.exception(
                "failed to fetch ai gating performance",
                extra={"component": "postgres"},
            )
            return {}
        result: dict = {}
        for row in rows:
            result[row[0]] = {
                "total": row[1],
                "resolved": row[2],
                "wins": row[3],
                "losses": row[4],
                "neutrals": row[5],
                "avg_return": float(row[6]) if row[6] is not None else None,
            }
        return result

    # -- Bot command helpers --

    def list_recent_signals(self, limit: int = 5) -> list[dict]:
        """List most recent signals for bot /last_signals command."""
        sql = """
            SELECT id, ticker, signal_type, confidence,
                   pipeline_stage, created_at
            FROM signal_predictions
            ORDER BY created_at DESC
            LIMIT %s
        """
        try:
            with self._pool.get_connection() as conn:
                rows = conn.execute(sql, (min(limit, 10),)).fetchall()
        except Exception:
            self._logger.exception(
                "failed to list recent signals",
                extra={"component": "postgres"},
            )
            return []
        return [
            {
                "id": r[0],
                "ticker": r[1],
                "signal_type": r[2],
                "confidence": float(r[3]) if r[3] is not None else None,
                "pipeline_stage": r[4],
                "created_at": r[5],
            }
            for r in rows
        ]

    def get_signal_detail(self, prediction_id: int) -> dict | None:
        """Fetch detailed signal info for bot /signal command."""
        sql = """
            SELECT id, ticker, signal_type, confidence, source,
                   price_at_signal, created_at, source_channel,
                   return_pct, outcome_label, pipeline_stage,
                   rejection_reason, delivered_at
            FROM signal_predictions
            WHERE id = %s
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql, (prediction_id,)).fetchone()
        except Exception:
            self._logger.exception(
                "failed to fetch signal detail",
                extra={
                    "component": "postgres",
                    "prediction_id": prediction_id,
                },
            )
            return None
        if row is None:
            return None
        return {
            "id": row[0],
            "ticker": row[1],
            "signal_type": row[2],
            "confidence": float(row[3]) if row[3] is not None else None,
            "source": row[4],
            "price_at_signal": float(row[5]) if row[5] is not None else None,
            "created_at": row[6],
            "source_channel": row[7],
            "return_pct": float(row[8]) if row[8] is not None else None,
            "outcome_label": row[9],
            "pipeline_stage": row[10],
            "rejection_reason": row[11],
            "delivered_at": row[12],
        }

    # -- Market quotes (T-Bank last prices) --

    def insert_market_quote(
        self,
        ticker: str,
        figi: str,
        price: float,
        instrument_uid: str = "",
        source_time: datetime | None = None,
        fetched_at: datetime | None = None,
    ) -> int | None:
        """Insert a single market quote. Returns the new row id."""
        sql = """
            INSERT INTO market_quotes
                (ticker, figi, instrument_uid, price, source_time, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        if fetched_at is None:
            fetched_at = datetime.now(UTC)
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(
                    sql,
                    (ticker, figi, instrument_uid or "", price,
                     source_time, fetched_at),
                ).fetchone()
                return row[0] if row else None
        except Exception:
            self._logger.exception(
                "failed to insert market quote",
                extra={"component": "postgres", "ticker": ticker, "figi": figi},
            )
            return None

    def insert_market_quotes_bulk(
        self, quotes: list[dict],
    ) -> int:
        """Bulk-insert market quotes. Returns count of inserted rows."""
        if not quotes:
            return 0
        sql = """
            INSERT INTO market_quotes
                (ticker, figi, instrument_uid, price, source_time, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        now = datetime.now(UTC)
        inserted = 0
        try:
            with self._pool.get_connection() as conn:
                for q in quotes:
                    conn.execute(sql, (
                        q["ticker"],
                        q["figi"],
                        q.get("instrument_uid", ""),
                        q["price"],
                        q.get("source_time"),
                        q.get("fetched_at", now),
                    ))
                    inserted += 1
        except Exception:
            self._logger.exception(
                "failed to bulk insert market quotes",
                extra={"component": "postgres", "attempted": len(quotes)},
            )
        return inserted

    def get_latest_quote_by_ticker(self, ticker: str) -> dict | None:
        """Return the most recent quote for a ticker."""
        sql = """
            SELECT id, ticker, figi, instrument_uid, price, source_time, fetched_at
            FROM market_quotes
            WHERE ticker = %s
            ORDER BY fetched_at DESC
            LIMIT 1
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql, (ticker,)).fetchone()
        except Exception:
            self._logger.exception(
                "failed to get latest quote by ticker",
                extra={"component": "postgres", "ticker": ticker},
            )
            return None
        if not row:
            return None
        return {
            "id": row[0],
            "ticker": row[1],
            "figi": row[2],
            "instrument_uid": row[3],
            "price": float(row[4]),
            "source_time": row[5],
            "fetched_at": row[6],
        }

    def get_latest_quote_by_figi(self, figi: str) -> dict | None:
        """Return the most recent quote for a FIGI."""
        sql = """
            SELECT id, ticker, figi, instrument_uid, price, source_time, fetched_at
            FROM market_quotes
            WHERE figi = %s
            ORDER BY fetched_at DESC
            LIMIT 1
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql, (figi,)).fetchone()
        except Exception:
            self._logger.exception(
                "failed to get latest quote by figi",
                extra={"component": "postgres", "figi": figi},
            )
            return None
        if not row:
            return None
        return {
            "id": row[0],
            "ticker": row[1],
            "figi": row[2],
            "instrument_uid": row[3],
            "price": float(row[4]),
            "source_time": row[5],
            "fetched_at": row[6],
        }

    def get_first_quote_after(
        self, ticker: str, after: datetime,
    ) -> dict | None:
        """Return the earliest quote for ticker with source_time >= after."""
        sql = """
            SELECT price, source_time
            FROM market_quotes
            WHERE ticker = %s AND source_time >= %s
            ORDER BY source_time ASC
            LIMIT 1
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql, (ticker, after)).fetchone()
        except Exception:
            self._logger.exception(
                "failed to get first quote after timestamp",
                extra={"component": "postgres", "ticker": ticker},
            )
            return None
        if not row:
            return None
        return {"price": float(row[0]), "source_time": row[1]}

    def get_latest_quote_before(
        self, ticker: str, before: datetime,
    ) -> dict | None:
        """Return the latest quote for ticker with source_time < before."""
        sql = """
            SELECT price, source_time
            FROM market_quotes
            WHERE ticker = %s AND source_time < %s
            ORDER BY source_time DESC
            LIMIT 1
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql, (ticker, before)).fetchone()
        except Exception:
            self._logger.exception(
                "failed to get latest quote before timestamp",
                extra={"component": "postgres", "ticker": ticker},
            )
            return None
        if not row:
            return None
        return {"price": float(row[0]), "source_time": row[1]}

    # -- Market data --

    def insert_market_snapshot(self, snap: MarketSnapshot) -> None:
        sql = """
            INSERT INTO market_snapshots
                (figi, ticker, last_price, currency, trading_status, snapshot_time)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (
                snap.instrument.figi,
                snap.instrument.ticker,
                snap.last_price.as_float,
                snap.last_price.currency,
                snap.trading_status.value,
                snap.time,
            ))
            conn.commit()

    # -- Orders --

    def insert_order_intent(self, intent: OrderIntent, account_id: str = "") -> None:
        sql = """
            INSERT INTO order_intents
                (account_id, figi, direction, quantity, order_type,
                 limit_price, idempotency_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
        """
        limit_price = intent.limit_price.as_float if intent.limit_price else None
        with self._pool.get_connection() as conn:
            conn.execute(sql, (
                account_id,
                intent.figi,
                intent.direction.value,
                intent.quantity,
                intent.order_type.value,
                limit_price,
                intent.idempotency_key,
            ))
            conn.commit()

    def insert_execution_event(
        self,
        intent: OrderIntent,
        result: ExecutionResult,
        account_id: str = "",
        event_type: str = "submission",
        raw_payload: dict | None = None,
    ) -> None:
        bo = result.broker_order
        sql = """
            INSERT INTO execution_events
                (account_id, event_type, idempotency_key, success, order_id,
                 figi, direction, quantity, filled_quantity, status, error, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (
                account_id,
                event_type,
                intent.idempotency_key,
                result.success,
                bo.order_id if bo else None,
                bo.figi if bo else intent.figi,
                bo.direction.value if bo else intent.direction.value,
                bo.quantity if bo else intent.quantity,
                bo.filled_quantity if bo else None,
                bo.status.value if bo else None,
                result.error,
                json.dumps(raw_payload) if raw_payload else None,
            ))
            conn.commit()

    # -- Broker sync foundation --

    def insert_broker_operation(
        self,
        account_id: str,
        operation_type: str,
        figi: str | None = None,
        quantity: int | None = None,
        price: float | None = None,
        currency: str | None = None,
        broker_date: datetime | None = None,
        broker_operation_id: str | None = None,
        raw_payload: dict | None = None,
    ) -> None:
        sql = """
            INSERT INTO broker_operations
                (account_id, broker_operation_id, operation_type,
                 figi, quantity, price, currency, broker_date, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (
                account_id, broker_operation_id, operation_type,
                figi, quantity, price, currency, broker_date,
                json.dumps(raw_payload) if raw_payload else None,
            ))
            conn.commit()

    def insert_portfolio_snapshot(
        self,
        account_id: str,
        snapshot_time: datetime,
        total_value: float | None = None,
        currency: str = "RUB",
    ) -> None:
        sql = """
            INSERT INTO portfolio_snapshots
                (account_id, total_value, currency, snapshot_time)
            VALUES (%s, %s, %s, %s)
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (account_id, total_value, currency, snapshot_time))
            conn.commit()

    def insert_position_snapshot(
        self,
        account_id: str,
        figi: str,
        quantity: int,
        snapshot_time: datetime,
        average_price: float | None = None,
    ) -> None:
        sql = """
            INSERT INTO position_snapshots
                (account_id, figi, quantity, average_price, snapshot_time)
            VALUES (%s, %s, %s, %s, %s)
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (account_id, figi, quantity, average_price, snapshot_time))
            conn.commit()

    # -- Broker-side structured events --

    def insert_broker_event_raw(self, event: BrokerEventRaw) -> bool:
        sql = """
            INSERT INTO broker_event_raw
                (account_id, source_method, figi, ticker, event_uid, event_time, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (account_id, source_method, event_uid) DO NOTHING
            RETURNING id
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (
                event.account_id,
                event.source_method,
                event.figi,
                event.ticker,
                event.event_uid,
                event.event_time,
                json.dumps(event.payload),
            ))
            inserted = cur.fetchone() is not None
            conn.commit()
        return inserted

    def insert_broker_event_feature(self, event: BrokerEventFeature) -> bool:
        sql = """
            INSERT INTO broker_event_features
                (account_id, source_method, figi, ticker, event_uid, event_time,
                 event_type, event_direction, event_value, currency)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (account_id, source_method, event_uid) DO UPDATE
                SET ticker = EXCLUDED.ticker
                WHERE EXCLUDED.ticker IS NOT NULL
                  AND (broker_event_features.ticker IS NULL
                       OR broker_event_features.ticker = 'STUB')
            RETURNING id
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (
                event.account_id,
                event.source_method,
                event.figi,
                event.ticker,
                event.event_uid,
                event.event_time,
                event.event_type,
                event.event_direction,
                event.event_value,
                event.currency,
            ))
            inserted = cur.fetchone() is not None
            conn.commit()
        return inserted

    def fetch_latest_broker_event_time(
        self,
        source_method: str,
        figi: str | None = None,
        account_id: str = "",
    ) -> datetime | None:
        if figi:
            sql = """
                SELECT max(event_time)
                FROM broker_event_features
                WHERE account_id = %s AND source_method = %s AND figi = %s
            """
            params = (account_id, source_method, figi)
        else:
            sql = """
                SELECT max(event_time)
                FROM broker_event_features
                WHERE account_id = %s AND source_method = %s
            """
            params = (account_id, source_method)

        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, params)
            row = cur.fetchone()
        if row is None:
            return None
        return row[0]

    # -- Telegram sentiment ingestion --

    def insert_telegram_message_raw(
        self,
        msg: TelegramMessage,
        normalized_text: str | None = None,
        dedup_hash: str | None = None,
    ) -> bool:
        """Insert raw Telegram message. Returns True if newly inserted, False if duplicate."""
        sql = """
            INSERT INTO telegram_messages_raw
                (channel_name, message_id, published_at, message_text,
                 source_payload, normalized_text, dedup_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (channel_name, message_id) DO NOTHING
            RETURNING id
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (
                msg.channel_name,
                msg.message_id,
                msg.published_at,
                msg.message_text,
                json.dumps(msg.source_payload) if msg.source_payload else None,
                normalized_text,
                dedup_hash,
            ))
            inserted = cur.fetchone() is not None
            conn.commit()
        return inserted

    def check_dedup_hash_exists(self, dedup_hash: str) -> bool:
        """Check if a dedup hash already exists in telegram_messages_raw."""
        sql = """
            SELECT 1 FROM telegram_messages_raw
            WHERE dedup_hash = %s
            LIMIT 1
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql, (dedup_hash,)).fetchone()
            return row is not None
        except Exception:
            self._logger.exception(
                "failed to check dedup hash",
                extra={"component": "postgres"},
            )
            return False

    def get_latest_message_id_by_channel(self, channel_name: str) -> int | None:
        """Return the highest numeric message_id for a channel, or None."""
        sql = """
            SELECT max(message_id::bigint)
            FROM telegram_messages_raw
            WHERE channel_name = %s
        """
        try:
            with self._pool.get_connection() as conn:
                row = conn.execute(sql, (channel_name,)).fetchone()
            if row and row[0] is not None:
                return int(row[0])
        except Exception:
            self._logger.exception(
                "failed to get latest message id",
                extra={"component": "postgres", "channel": channel_name},
            )
        return None

    def get_telegram_ingest_status(self) -> list[dict]:
        """Return per-channel ingestion stats."""
        sql = """
            SELECT
                channel_name,
                count(*) AS total_messages,
                max(published_at) AS latest_published,
                max(recorded_at) AS latest_recorded,
                max(message_id::bigint) AS max_message_id
            FROM telegram_messages_raw
            GROUP BY channel_name
            ORDER BY channel_name
        """
        try:
            with self._pool.get_connection() as conn:
                rows = conn.execute(sql).fetchall()
            return [
                {
                    "channel": r[0],
                    "total_messages": r[1],
                    "latest_published": r[2],
                    "latest_recorded": r[3],
                    "max_message_id": int(r[4]) if r[4] is not None else None,
                }
                for r in rows
            ]
        except Exception:
            self._logger.exception(
                "failed to get telegram ingest status",
                extra={"component": "postgres"},
            )
            return []

    def insert_telegram_message_mention(
        self,
        msg: TelegramMessage,
        mention: TickerMention,
    ) -> None:
        sql = """
            INSERT INTO telegram_message_mentions
                (channel_name, message_id, published_at, figi, ticker,
                 mention_type, confidence)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (
                msg.channel_name,
                msg.message_id,
                msg.published_at,
                mention.figi,
                mention.ticker.upper(),
                mention.mention_type,
                mention.confidence,
            ))
            conn.commit()

    def insert_telegram_sentiment_event(
        self,
        msg: TelegramMessage,
        mention: TickerMention,
        result: SentimentResult,
    ) -> None:
        sql = """
            INSERT INTO telegram_sentiment_events
                (channel_name, message_id, published_at, figi, ticker,
                 model_name, label, score_positive, score_negative,
                 score_neutral, scored_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (
                msg.channel_name,
                msg.message_id,
                msg.published_at,
                mention.figi,
                mention.ticker.upper(),
                result.model_name,
                result.label,
                result.score_positive,
                result.score_negative,
                result.score_neutral,
                result.scored_at,
            ))
            conn.commit()

    # -- Observation / aggregation --

    def fetch_sentiment_events_for_window(
        self,
        ticker: str,
        start_time: datetime,
        end_time: datetime,
        figi: str | None = None,
    ) -> list[dict]:
        """Fetch sentiment event rows for a ticker within a time window.

        Returns list of dicts with keys: label, score_positive, score_negative, score_neutral.
        """
        if figi:
            sql = """
                SELECT label, score_positive, score_negative, score_neutral
                FROM telegram_sentiment_events
                WHERE figi = %s AND scored_at >= %s AND scored_at < %s
                ORDER BY scored_at
            """
            params = (figi, start_time, end_time)
        else:
            sql = """
                SELECT label, score_positive, score_negative, score_neutral
                FROM telegram_sentiment_events
                WHERE ticker = %s AND scored_at >= %s AND scored_at < %s
                ORDER BY scored_at
            """
            params = (ticker.upper(), start_time, end_time)

        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, params)
            columns = ("label", "score_positive", "score_negative", "score_neutral")
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    def fetch_distinct_tickers_with_sentiment(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict]:
        """Fetch distinct ticker/figi pairs that have sentiment events in the window."""
        sql = """
            SELECT DISTINCT ticker, figi
            FROM telegram_sentiment_events
            WHERE scored_at >= %s AND scored_at < %s
            ORDER BY ticker
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (start_time, end_time))
            return [{"ticker": row[0], "figi": row[1]} for row in cur.fetchall()]

    def insert_signal_observation(self, obs: SignalObservation) -> None:
        sql = """
            INSERT INTO signal_observations
                (ticker, figi, "window", observation_time, message_count,
                 positive_count, negative_count, neutral_count,
                 positive_score_avg, negative_score_avg, neutral_score_avg,
                 sentiment_balance)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (
                obs.ticker,
                obs.figi,
                obs.window,
                obs.observation_time,
                obs.message_count,
                obs.positive_count,
                obs.negative_count,
                obs.neutral_count,
                obs.positive_score_avg,
                obs.negative_score_avg,
                obs.neutral_score_avg,
                obs.sentiment_balance,
            ))
            conn.commit()

    def fetch_operational_summary(self) -> dict[str, int]:
        """Fetch simple operational row counts for key tables."""
        sql = """
            SELECT
                (SELECT count(*) FROM telegram_messages_raw),
                (SELECT count(*) FROM telegram_message_mentions),
                (SELECT count(*) FROM telegram_sentiment_events),
                (SELECT count(*) FROM signal_observations),
                (SELECT count(*) FROM market_snapshots)
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql)
            row = cur.fetchone()
        if row is None:
            return {
                "telegram_messages_raw": 0,
                "telegram_message_mentions": 0,
                "telegram_sentiment_events": 0,
                "signal_observations": 0,
                "market_snapshots": 0,
            }
        return {
            "telegram_messages_raw": int(row[0]),
            "telegram_message_mentions": int(row[1]),
            "telegram_sentiment_events": int(row[2]),
            "signal_observations": int(row[3]),
            "market_snapshots": int(row[4]),
        }

    # -- Signal fusion --

    def fetch_broker_event_features_for_window(
        self,
        ticker: str,
        start_time: datetime,
        end_time: datetime,
        figi: str | None = None,
    ) -> list[dict]:
        """Fetch broker event feature rows for a ticker within a time window."""
        if figi:
            sql = """
                SELECT source_method, event_type, event_direction,
                       event_value, currency, event_time
                FROM broker_event_features
                WHERE figi = %s AND event_time >= %s AND event_time < %s
                ORDER BY event_time
            """
            params = (figi, start_time, end_time)
        else:
            sql = """
                SELECT source_method, event_type, event_direction,
                       event_value, currency, event_time
                FROM broker_event_features
                WHERE ticker = %s AND event_time >= %s AND event_time < %s
                ORDER BY event_time
            """
            params = (ticker.upper(), start_time, end_time)

        columns = (
            "source_method", "event_type", "event_direction",
            "event_value", "currency", "event_time",
        )
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, params)
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    def fetch_latest_signal_observation(
        self,
        ticker: str,
        window: str,
        before: datetime,
        figi: str | None = None,
    ) -> SignalObservation | None:
        """Fetch the most recent signal observation for a ticker/window."""
        if figi:
            sql = """
                SELECT ticker, figi, "window", observation_time,
                       message_count, positive_count, negative_count, neutral_count,
                       positive_score_avg, negative_score_avg, neutral_score_avg,
                       sentiment_balance
                FROM signal_observations
                WHERE figi = %s AND "window" = %s AND observation_time <= %s
                ORDER BY observation_time DESC
                LIMIT 1
            """
            params = (figi, window, before)
        else:
            sql = """
                SELECT ticker, figi, "window", observation_time,
                       message_count, positive_count, negative_count, neutral_count,
                       positive_score_avg, negative_score_avg, neutral_score_avg,
                       sentiment_balance
                FROM signal_observations
                WHERE ticker = %s AND "window" = %s AND observation_time <= %s
                ORDER BY observation_time DESC
                LIMIT 1
            """
            params = (ticker.upper(), window, before)

        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, params)
            row = cur.fetchone()

        if row is None:
            return None

        return SignalObservation(
            ticker=row[0],
            figi=row[1],
            window=row[2],
            observation_time=row[3],
            message_count=int(row[4]),
            positive_count=int(row[5]),
            negative_count=int(row[6]),
            neutral_count=int(row[7]),
            positive_score_avg=float(row[8]) if row[8] is not None else None,
            negative_score_avg=float(row[9]) if row[9] is not None else None,
            neutral_score_avg=float(row[10]) if row[10] is not None else None,
            sentiment_balance=float(row[11]) if row[11] is not None else None,
        )

    def fetch_broker_event_recency(
        self,
        ticker: str,
        figi: str | None = None,
    ) -> dict:
        """Fetch the latest event timestamp per source_method for a ticker.

        Returns a dict with keys: last_dividend_at, last_report_at,
        last_insider_deal_at (each datetime | None).
        """
        if figi:
            sql = """
                SELECT source_method, MAX(event_time) AS latest
                FROM broker_event_features
                WHERE figi = %s
                GROUP BY source_method
            """
            params = (figi,)
        else:
            sql = """
                SELECT source_method, MAX(event_time) AS latest
                FROM broker_event_features
                WHERE ticker = %s
                GROUP BY source_method
            """
            params = (ticker.upper(),)

        result: dict = {
            "last_dividend_at": None,
            "last_report_at": None,
            "last_insider_deal_at": None,
        }
        method_map = {
            "GetDividends": "last_dividend_at",
            "GetAssetReports": "last_report_at",
            "GetInsiderDeals": "last_insider_deal_at",
        }
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, params)
            for row in cur.fetchall():
                key = method_map.get(row[0])
                if key:
                    result[key] = row[1]

        return result

    def insert_fused_signal_feature(self, feature: FusedSignalFeature) -> None:
        sql = """
            INSERT INTO fused_signal_features
                (ticker, figi, "window", observation_time,
                 sentiment_message_count, sentiment_positive_count,
                 sentiment_negative_count, sentiment_neutral_count,
                 sentiment_positive_avg, sentiment_negative_avg,
                 sentiment_neutral_avg, sentiment_balance,
                 broker_dividends_count, broker_reports_count,
                 broker_insider_deals_count, broker_total_event_count,
                 broker_latest_dividend_value, broker_latest_dividend_currency,
                 broker_latest_report_time, broker_latest_insider_deal_time,
                 last_dividend_at, last_report_at, last_insider_deal_at,
                 days_since_last_dividend, days_since_last_report,
                 days_since_last_insider_deal,
                 moex_latest_close, moex_latest_volume, moex_latest_numtrades,
                 moex_last_trade_date, moex_days_since_last_trade,
                 moex_price_change_1d_pct, moex_range_pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s)
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (
                feature.ticker,
                feature.figi,
                feature.window,
                feature.observation_time,
                feature.sentiment_message_count,
                feature.sentiment_positive_count,
                feature.sentiment_negative_count,
                feature.sentiment_neutral_count,
                feature.sentiment_positive_avg,
                feature.sentiment_negative_avg,
                feature.sentiment_neutral_avg,
                feature.sentiment_balance,
                feature.broker_dividends_count,
                feature.broker_reports_count,
                feature.broker_insider_deals_count,
                feature.broker_total_event_count,
                feature.broker_latest_dividend_value,
                feature.broker_latest_dividend_currency,
                feature.broker_latest_report_time,
                feature.broker_latest_insider_deal_time,
                feature.last_dividend_at,
                feature.last_report_at,
                feature.last_insider_deal_at,
                feature.days_since_last_dividend,
                feature.days_since_last_report,
                feature.days_since_last_insider_deal,
                feature.moex_latest_close,
                feature.moex_latest_volume,
                feature.moex_latest_numtrades,
                feature.moex_last_trade_date,
                feature.moex_days_since_last_trade,
                feature.moex_price_change_1d_pct,
                feature.moex_range_pct,
            ))
            conn.commit()

    def fetch_moex_market_context(self, ticker: str) -> dict | None:
        """Fetch latest + previous MOEX market history rows for a ticker.

        Returns dict with keys: latest (dict|None), previous_close (float|None).
        Returns None if no data available.
        """
        sql = """
            SELECT trade_date, open, high, low, close, volume, num_trades
            FROM moex_market_history
            WHERE secid = %s
            ORDER BY trade_date DESC
            LIMIT 2
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (ticker.upper(),))
            rows = cur.fetchall()

        if not rows:
            return None

        columns = ("trade_date", "open", "high", "low", "close", "volume", "num_trades")
        latest = dict(zip(columns, rows[0], strict=True))
        previous_close = None
        if len(rows) > 1:
            prev = dict(zip(columns, rows[1], strict=True))
            previous_close = prev.get("close")

        return {"latest": latest, "previous_close": previous_close}

    # -- CBR events --

    def insert_cbr_feed_raw(self, item: CbrFeedItem) -> bool:
        """Insert a raw CBR feed item. Returns True if inserted, False if duplicate."""
        sql = """
            INSERT INTO cbr_feed_raw
                (source_url, source_type, item_uid, published_at, payload)
            VALUES (%s, 'rss', %s, %s, %s)
            ON CONFLICT (source_url, item_uid) DO NOTHING
            RETURNING id
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (
                item.source_url,
                item.item_uid,
                item.published_at,
                item.payload_xml,
            ))
            row = cur.fetchone()
            conn.commit()
        return row is not None

    def insert_cbr_event(self, event: CbrEvent) -> bool:
        """Insert a normalized CBR event. Returns True if inserted, False if duplicate."""
        sql = """
            INSERT INTO cbr_events
                (source_url, event_type, title, published_at, event_key, url, summary)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_url, event_key) DO NOTHING
            RETURNING id
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (
                event.source_url,
                event.event_type,
                event.title,
                event.published_at,
                event.event_key,
                event.url,
                event.summary,
            ))
            row = cur.fetchone()
            conn.commit()
        return row is not None

    def cbr_event_exists(self, event_key: str, source_url: str) -> bool:
        """Check if a CBR event already exists."""
        sql = """
            SELECT 1 FROM cbr_events
            WHERE source_url = %s AND event_key = %s
            LIMIT 1
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (source_url, event_key))
            return cur.fetchone() is not None

    # -- MOEX ISS market data --

    def upsert_moex_security_reference(self, info: MoexSecurityInfo) -> bool:
        """Upsert MOEX security metadata. Returns True if inserted/updated."""
        sql = """
            INSERT INTO moex_security_reference
                (secid, name, short_name, isin, reg_number, list_level,
                 issuer, issue_size, "group", primary_boardid, raw_description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (secid) DO UPDATE SET
                name = EXCLUDED.name,
                short_name = EXCLUDED.short_name,
                isin = EXCLUDED.isin,
                reg_number = EXCLUDED.reg_number,
                list_level = EXCLUDED.list_level,
                issuer = EXCLUDED.issuer,
                issue_size = EXCLUDED.issue_size,
                "group" = EXCLUDED."group",
                primary_boardid = EXCLUDED.primary_boardid,
                raw_description = EXCLUDED.raw_description,
                recorded_at = now()
            RETURNING id
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (
                info.secid,
                info.name,
                info.short_name,
                info.isin,
                info.reg_number,
                info.list_level,
                info.issuer,
                info.issue_size,
                info.group,
                info.primary_boardid,
                json.dumps(info.raw_description),
            ))
            row = cur.fetchone()
            conn.commit()
        return row is not None

    def insert_moex_market_history_raw(self, row: MoexHistoryRow) -> bool:
        """Insert raw MOEX history row. Returns True if inserted, False if duplicate."""
        sql = """
            INSERT INTO moex_market_history_raw
                (secid, boardid, trade_date, open, high, low, close,
                 legal_close, waprice, volume, value, num_trades)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (secid, boardid, trade_date) DO NOTHING
            RETURNING id
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (
                row.secid,
                row.boardid,
                row.trade_date,
                row.open,
                row.high,
                row.low,
                row.close,
                row.legal_close,
                row.waprice,
                row.volume,
                row.value,
                row.num_trades,
            ))
            inserted = cur.fetchone() is not None
            conn.commit()
        return inserted

    def insert_moex_market_history(self, row: MoexMarketHistoryNormalized) -> bool:
        """Insert normalized MOEX history row. Returns True if inserted, False if duplicate."""
        sql = """
            INSERT INTO moex_market_history
                (secid, boardid, trade_date, open, high, low, close,
                 waprice, volume, value, num_trades)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (secid, boardid, trade_date) DO NOTHING
            RETURNING id
        """
        with self._pool.get_connection() as conn:
            cur = conn.execute(sql, (
                row.secid,
                row.boardid,
                row.trade_date,
                row.open,
                row.high,
                row.low,
                row.close,
                row.waprice,
                row.volume,
                row.value,
                row.num_trades,
            ))
            inserted = cur.fetchone() is not None
            conn.commit()
        return inserted
