"""Bounded storage probes, independent of ingestion and its connection pool."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from tinvest_trader.app.config import AppConfig

ERROR_PATH = Path("/tmp/tinvest_storage_error.json")


def record_storage_error(error: Exception) -> None:
    """Keep failures visible even when a repository caller catches the exception.

    Do not store exception messages: connection errors can contain credentials.
    Failure to write the marker must never replace the original DB exception.
    """
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=ERROR_PATH.parent, delete=False) as out:
            temporary = Path(out.name)
            json.dump({"at": datetime.now(UTC).isoformat(), "type": type(error).__name__}, out)
        temporary.replace(ERROR_PATH)
    except OSError:
        pass
    finally:
        if temporary is not None:
            with contextlib.suppress(OSError):
                temporary.unlink(missing_ok=True)


def market_session_expected(config: AppConfig, now: datetime, grace_seconds: int) -> bool:
    """Fresh candles are expected only inside the configured weekday session.

    This is a freshness heuristic, not an exchange holiday calendar.
    """
    local = now.astimezone(UTC) + timedelta(hours=3)
    activity = config.market_activity
    start = local.replace(
        hour=activity.session_start_hour_moscow,
        minute=activity.session_start_minute_moscow, second=0, microsecond=0,
    )
    end = local.replace(
        hour=activity.session_end_hour_moscow,
        minute=activity.session_end_minute_moscow, second=0, microsecond=0,
    )
    return local.weekday() < 5 and start + timedelta(seconds=grace_seconds) <= local < end


def inspect_storage(
    config: AppConfig, *, persist: bool = False, now: datetime | None = None,
) -> dict:
    """CLI is read-only; Docker opts into a one-row committed write probe.

    Catalog estimates and indexed latest-row reads avoid full scans on large
    tables. All queries are local to Postgres; no broker/container startup.
    """
    now = now or datetime.now(UTC)
    health = config.storage_health
    report: dict = {"checked_at": now.isoformat(), "status": "ok", "issues": [], "sources": {}}

    def issue(message: str, *, warning: bool = False) -> None:
        report["issues"].append(message)
        if not warning or report["status"] == "ok":
            report["status"] = "warning" if warning else "critical"

    try:
        disk = os.statvfs(health.disk_path)
        total = disk.f_blocks * disk.f_frsize
        free = disk.f_bavail * disk.f_frsize
        percent = free / total * 100
        report["disk"] = {"path": health.disk_path, "free_bytes": free,
                          "total_bytes": total, "free_percent": round(percent, 2)}
        if percent <= health.critical_free_percent or free < health.min_free_bytes:
            issue("database filesystem low on free space")
        elif percent <= health.warning_free_percent:
            issue("database filesystem free-space warning", warning=True)
    except (OSError, ZeroDivisionError):
        issue("database filesystem unavailable (check read-only volume mount)")

    try:
        error = json.loads(ERROR_PATH.read_text())
        age = (now - datetime.fromisoformat(error["at"])).total_seconds()
        if age < health.recent_error_seconds:
            # Do not let a later successful SELECT hide a recent write/pool failure.
            issue("recent application database failure")
            report["last_error"] = error
    except FileNotFoundError:
        pass
    except (OSError, ValueError, KeyError, TypeError):
        issue("cannot read application database error marker")

    if not config.database.postgres_dsn:
        issue("database is not configured")
        return report

    try:
        with psycopg.connect(
            config.database.postgres_dsn, connect_timeout=3,
            options="-c statement_timeout=2000 -c lock_timeout=1000",
            application_name="tinvest-storage-health",
        ) as conn:
            conn.execute(
                "SET TRANSACTION READ ONLY" if not persist else "SET TRANSACTION READ WRITE",
            )
            report["database_bytes"] = conn.execute(
                "SELECT pg_database_size(current_database())",
            ).fetchone()[0]
            rows = conn.execute("""
                SELECT c.relname, pg_relation_size(c.oid), pg_indexes_size(c.oid),
                       pg_total_relation_size(c.oid), c.reltuples::bigint
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public' AND c.relkind = 'r'
                ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 10
            """).fetchall()
            report["tables"] = [dict(zip(
                ("table", "heap_bytes", "index_bytes", "total_bytes", "estimated_rows"), row,
                strict=True,
            )) for row in rows]

            bg = config.background
            sources = [
                ("fusion",
                 "SELECT recorded_at FROM fused_signal_features ORDER BY id DESC LIMIT 1",
                 bg.enabled and bg.run_fusion and config.fusion.enabled and config.fusion.persist,
                 max(health.freshness_seconds, bg.fusion_interval_seconds * 3)),
                ("quotes", "SELECT fetched_at FROM market_quotes ORDER BY id DESC LIMIT 1",
                 bg.enabled and bg.run_quote_sync and config.quote_sync.enabled,
                 max(health.freshness_seconds, config.quote_sync.poll_interval_seconds * 3)),
                ("activity",
                 "SELECT max(candle_time) FROM (SELECT candle_time"
                 " FROM market_activity_observations"
                 " ORDER BY id DESC LIMIT 1000) recent",
                 bg.enabled and bg.run_market_activity and config.market_activity.enabled,
                 max(health.freshness_seconds, config.market_activity.poll_interval_seconds * 3)),
            ]
            for name, sql, enabled, threshold in sources:
                if not enabled:
                    continue
                expected = name == "fusion" or market_session_expected(config, now, threshold)
                row = conn.execute(sql).fetchone()
                latest = row[0] if row else None
                age = (now - latest).total_seconds() if latest else None
                report["sources"][name] = {
                    "latest": latest.isoformat() if latest else None,
                    "age_seconds": age, "threshold_seconds": threshold, "expected": expected,
                }
                if expected and (age is None or age > threshold or age < -60):
                    issue(f"{name} persistence is missing or stale")

            if persist:
                conn.execute("""
                    INSERT INTO storage_health_snapshot (id, checked_at, report)
                    VALUES (1, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        checked_at = EXCLUDED.checked_at, report = EXCLUDED.report
                """, (now, Jsonb(report)))
        report["write_probe"] = "committed" if persist else "not_requested"
    except Exception as exc:
        # Do not return a partial-success report or leak DSNs from psycopg.
        issue(f"database probe failed ({type(exc).__name__})")
        report["write_probe"] = "failed" if persist else "not_requested"
    return report
