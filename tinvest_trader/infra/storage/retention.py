"""Explicit, opt-in retention for dense Fusion/quote history only.

Never remove signals, trades, outcomes, raw events, or MOEX replay inputs.
References in signals and prices needed to resolve predictions are preserved.
"""

from __future__ import annotations

import logging
import time

import psycopg
from psycopg import sql

from tinvest_trader.app.config import AppConfig

TABLES = {
    "fused_signal_features": ("recorded_at", "idx_fused_retention_recorded_at"),
    "market_quotes": ("fetched_at", "idx_quotes_retention_fetched_at"),
}
LOCK_KEY = 2026091003


def _protected_ids(conn, config: AppConfig) -> dict[str, list[int]]:
    # JSON references have no FK; preserve them explicitly, including resolved signals.
    refs = conn.execute("""
        SELECT DISTINCT features_json->>'fused_feature_id' FROM signal_predictions
        WHERE features_json->>'fused_feature_id' IS NOT NULL
    """).fetchall()
    fused = []
    for (value,) in refs:
        if str(value).isdigit() and 0 < int(value) < 2**63:
            fused.append(int(value))

    # Use the same source-time bounds as signal_outcome. Keep input prices even
    # for resolved predictions for traceability, plus latest catalog FIGI quotes.
    quotes = conn.execute("""
        SELECT q.id FROM signal_predictions s
        CROSS JOIN LATERAL (
            SELECT id FROM market_quotes
            WHERE ticker = s.ticker AND source_time < s.created_at
            ORDER BY source_time DESC LIMIT 1
        ) q
        UNION
        SELECT q.id FROM signal_predictions s
        CROSS JOIN LATERAL (
            SELECT id FROM market_quotes
            WHERE ticker = s.ticker
              AND source_time >= s.created_at + %s * interval '1 second'
              AND source_time <= s.created_at + %s * interval '1 second'
            ORDER BY source_time ASC LIMIT 1
        ) q
        UNION
        SELECT q.id FROM instrument_catalog c
        CROSS JOIN LATERAL (
            SELECT id FROM market_quotes WHERE figi = c.figi
            ORDER BY fetched_at DESC LIMIT 1
        ) q
    """, (
        max(0, config.signal_resolution.eval_window_seconds),
        max(0, config.signal_resolution.eval_window_seconds)
        + max(0, config.signal_resolution.max_quote_delay_seconds),
    )).fetchall()
    return {"fused_signal_features": fused, "market_quotes": [row[0] for row in quotes]}


def retention_cycle(config: AppConfig, *, apply: bool = False) -> dict:
    """Preview by default; opt-in writes are bounded and independently committed.

    A session advisory lock prevents concurrent CLI and background maintenance.
    No COPY/TRUNCATE/VACUUM FULL, unbounded deletes, or automatic DDL here.
    """
    if not config.database.postgres_dsn:
        raise ValueError("database is not configured")
    cfg = config.storage_retention
    if apply and not cfg.enabled:
        raise ValueError("enable TINVEST_STORAGE_RETENTION_ENABLED before applying retention")
    result: dict = {"applied": apply, "months": cfg.months, "tables": {}}
    with psycopg.connect(
        config.database.postgres_dsn, connect_timeout=3, autocommit=True,
        options="-c statement_timeout=5000 -c lock_timeout=1000",
        application_name="tinvest-storage-retention",
    ) as conn:
        locked = conn.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,)).fetchone()[0]
        if not locked:
            return {**result, "skipped": "maintenance already running"}
        try:
            deadline = time.monotonic() + 20
            cutoff = conn.execute(
                "SELECT now() - make_interval(months => %s)", (cfg.months,),
            ).fetchone()[0]
            result["cutoff"] = cutoff.isoformat()
            protected = _protected_ids(conn, config)
            for table, (time_column, index_name) in TABLES.items():
                # A BRIN index is required before periodic deletion on large history.
                # Build it CONCURRENTLY in an explicit maintenance step, not startup.
                ready = conn.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_index WHERE indexrelid = to_regclass(%s)
                        AND indrelid = to_regclass(%s) AND indisvalid AND indisready
                    )
                """, (index_name, table)).fetchone()[0]
                summary = {"protected_ids": len(protected[table]), "deleted": 0,
                           "index_ready": ready}
                result["tables"][table] = summary
                if not ready:
                    summary["skipped"] = "retention index missing or invalid"
                    continue
                query = sql.SQL("""
                    WITH batch AS MATERIALIZED (
                        SELECT t.id FROM {table} t
                        WHERE t.{time_column} < %s
                          AND NOT EXISTS (
                              SELECT 1 FROM unnest(%s::bigint[]) p(id) WHERE p.id = t.id
                          )
                        LIMIT %s FOR UPDATE OF t SKIP LOCKED
                    )
                    DELETE FROM {table} t USING batch WHERE t.id = batch.id
                """).format(table=sql.Identifier(table), time_column=sql.Identifier(time_column))
                if not apply:
                    # A preview is bounded too, not a multi-million-row COUNT scan.
                    preview = sql.SQL("""
                        SELECT id FROM {table} t WHERE t.{time_column} < %s
                        AND NOT EXISTS (
                            SELECT 1 FROM unnest(%s::bigint[]) p(id) WHERE p.id = t.id
                        ) LIMIT %s
                    """).format(
                        table=sql.Identifier(table), time_column=sql.Identifier(time_column),
                    )
                    summary["next_batch_candidates"] = len(conn.execute(
                        preview, (cutoff, protected[table], cfg.batch_size),
                    ).fetchall())
                    continue
                for _ in range(cfg.batches_per_cycle):
                    if time.monotonic() >= deadline:
                        summary["budget_exhausted"] = True
                        break
                    with conn.transaction():
                        deleted = conn.execute(
                            query, (cutoff, protected[table], cfg.batch_size),
                        ).rowcount
                        conn.execute("""
                            INSERT INTO storage_retention_state
                                (table_name, cutoff, checked_at, deleted_rows_total)
                            VALUES (%s, %s, now(), %s)
                            ON CONFLICT (table_name) DO UPDATE SET
                                cutoff = EXCLUDED.cutoff, checked_at = EXCLUDED.checked_at,
                                deleted_rows_total = storage_retention_state.deleted_rows_total
                                    + EXCLUDED.deleted_rows_total
                        """, (table, cutoff, deleted))
                    summary["deleted"] += deleted
                    if deleted < cfg.batch_size:
                        break
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
    return result


def run_retention(config: AppConfig, logger: logging.Logger) -> None:
    """Runner callback; errors do not affect other pipelines."""
    try:
        result = retention_cycle(config, apply=True)
        incomplete = result.get("skipped") or any(
            item.get("skipped") or item.get("budget_exhausted")
            for item in result["tables"].values()
        )
        log = logger.warning if incomplete else logger.info
        log("storage retention cycle finished", extra={"retention": result})
    except Exception:
        logger.exception("storage retention cycle failed")
