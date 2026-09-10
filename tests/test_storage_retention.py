"""Offline safety tests for destructive, explicitly enabled maintenance."""

import logging
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from tinvest_trader import cli
from tinvest_trader.app import container as container_module
from tinvest_trader.app.config import (
    AppConfig,
    BackgroundConfig,
    DatabaseConfig,
    StorageRetentionConfig,
    load_config,
)
from tinvest_trader.infra.storage import retention
from tinvest_trader.services.background_runner import BackgroundRunner


@pytest.fixture
def db(monkeypatch):
    conn = MagicMock()

    def execute(query, params=None):
        text = str(query)
        cursor = MagicMock()
        if "pg_try_advisory_lock" in text or "SELECT EXISTS" in text:
            cursor.fetchone.return_value = (True,)
        elif "make_interval" in text:
            cursor.fetchone.return_value = (datetime(2026, 6, 10, tzinfo=UTC),)
        elif "features_json" in text:
            cursor.fetchall.return_value = [("123",), ("not-an-id",), (None,), (str(2**64),)]
        elif "CROSS JOIN LATERAL" in text:
            cursor.fetchall.return_value = [(1,), (2,)]
        elif "DELETE" in text:
            cursor.rowcount = 7
        else:
            cursor.fetchall.return_value = [(10,)]
        return cursor

    conn.execute.side_effect = execute
    connect = MagicMock()
    connect.return_value.__enter__.return_value = conn
    monkeypatch.setattr(retention.psycopg, "connect", connect)
    config = AppConfig(
        database=DatabaseConfig(postgres_dsn="postgresql://test"),
        storage_retention=StorageRetentionConfig(enabled=True),
    )
    return config, conn, connect


def test_config_is_opt_in_with_three_calendar_months(monkeypatch):
    assert not StorageRetentionConfig().enabled
    monkeypatch.setenv("TINVEST_STORAGE_RETENTION_ENABLED", "true")
    monkeypatch.setenv("TINVEST_STORAGE_RETENTION_MONTHS", "6")
    monkeypatch.setenv("TINVEST_STORAGE_RETENTION_INTERVAL_SECONDS", "7200")
    monkeypatch.setenv("TINVEST_STORAGE_RETENTION_BATCH_SIZE", "2000")
    monkeypatch.setenv("TINVEST_STORAGE_RETENTION_BATCHES_PER_CYCLE", "3")
    assert load_config().storage_retention == StorageRetentionConfig(True, 6, 7200, 2000, 3)


@pytest.mark.parametrize("values", [
    {"months": 2}, {"poll_interval_seconds": 0}, {"batch_size": 0},
    {"batch_size": 50001}, {"batches_per_cycle": 11},
])
def test_reject_unsafe_retention_settings(values):
    with pytest.raises(ValueError):
        StorageRetentionConfig(**values)


def test_preview_never_deletes_or_starts_application(db, monkeypatch, capsys):
    config, conn, _ = db
    monkeypatch.setattr(cli, "load_config", lambda: config)
    builder = MagicMock(side_effect=AssertionError("must not build container"))
    monkeypatch.setattr(cli, "build_container", builder)
    assert cli.main(["storage-retention"]) == 0
    assert '"applied": false' in capsys.readouterr().out
    statements = "\n".join(str(call.args[0]) for call in conn.execute.call_args_list)
    assert "DELETE" not in statements
    assert "INSERT" not in statements
    assert "count(*)" not in statements
    builder.assert_not_called()


def test_apply_requires_flag(db):
    config, _, connect = db
    config = replace(config, storage_retention=StorageRetentionConfig())
    with pytest.raises(ValueError, match="enable TINVEST_STORAGE_RETENTION_ENABLED"):
        retention.retention_cycle(config, apply=True)
    connect.assert_not_called()


def test_retention_preserves_references_and_quote_resolution_inputs(db):
    config, conn, _ = db
    refs = retention._protected_ids(conn, config)
    assert refs == {"fused_signal_features": [123], "market_quotes": [1, 2]}
    query, params = conn.execute.call_args.args
    assert "resolved_at IS NULL" not in query  # resolved signals remain auditable too
    assert "source_time < s.created_at" in query
    assert "source_time >= s.created_at" in query
    assert "source_time <= s.created_at" in query
    assert "instrument_catalog" in query
    assert params == (300, 1200)


def test_bounded_deletes_only_allowlisted_tables_and_atomically_record_totals(db):
    config, conn, _ = db
    result = retention.retention_cycle(config, apply=True)
    assert result["cutoff"] == "2026-06-10T00:00:00+00:00"
    assert result["tables"]["fused_signal_features"]["deleted"] == 7
    assert result["tables"]["market_quotes"]["deleted"] == 7
    deletes = [c for c in conn.execute.call_args_list if "DELETE" in str(c.args[0])]
    assert len(deletes) == 2
    for delete, expected_ids in zip(deletes, ([123], [1, 2]), strict=True):
        query, params = delete.args
        assert "SKIP LOCKED" in str(query)
        assert "LIMIT %s" in str(query)
        assert "NOT EXISTS" in str(query)
        assert params[1] == expected_ids
        assert params[2] == config.storage_retention.batch_size
    assert conn.transaction.call_count == 2
    statements = "\n".join(str(c.args[0]) for c in conn.execute.call_args_list)
    assert "TRUNCATE" not in statements
    assert "VACUUM" not in statements
    assert "DROP" not in statements
    assert "storage_retention_state.deleted_rows_total" in statements
    assert list(retention.TABLES) == ["fused_signal_features", "market_quotes"]


def test_missing_index_prevents_deletion(db):
    config, conn, _ = db
    execute = conn.execute.side_effect

    def missing_index(query, params=None):
        cursor = execute(query, params)
        if "SELECT EXISTS" in str(query):
            cursor.fetchone.return_value = (False,)
        return cursor

    conn.execute.side_effect = missing_index
    result = retention.retention_cycle(config, apply=True)
    assert not any("DELETE" in str(c.args[0]) for c in conn.execute.call_args_list)
    assert all(not t["index_ready"] for t in result["tables"].values())


def test_lock_contention_skips_without_writes(db):
    config, conn, _ = db
    conn.execute.side_effect = None
    conn.execute.return_value.fetchone.return_value = (False,)
    result = retention.retention_cycle(config, apply=True)
    assert result["skipped"] == "maintenance already running"
    assert conn.execute.call_count == 1


def test_commit_failure_propagates_and_releases_lock(db):
    config, conn, _ = db
    conn.transaction.return_value.__exit__.side_effect = RuntimeError("disk full")
    with pytest.raises(RuntimeError, match="disk full"):
        retention.retention_cycle(config, apply=True)
    assert "pg_advisory_unlock" in conn.execute.call_args.args[0]


def test_time_budget_prevents_long_maintenance(db, monkeypatch):
    config, conn, _ = db
    timer = iter([0, 30, 30])
    monkeypatch.setattr(retention.time, "monotonic", lambda: next(timer))
    result = retention.retention_cycle(config, apply=True)
    assert all(t["budget_exhausted"] for t in result["tables"].values())
    assert not any("DELETE" in str(c.args[0]) for c in conn.execute.call_args_list)


def test_runner_delays_maintenance_and_continues_after_failure(monkeypatch):
    clock = [0.0]
    callback = MagicMock(side_effect=RuntimeError("DB failed"))
    runner = BackgroundRunner(
        BackgroundConfig(enabled=True), logging.getLogger("test"),
        storage_retention_fn=callback, storage_retention_interval_seconds=3600,
        time_fn=lambda: clock[0],
    )
    waits = []

    def wait(timeout):
        waits.append(timeout)
        clock[0] += timeout
        if len(waits) >= 2:
            runner._stop_event.set()

    monkeypatch.setattr(runner._stop_event, "wait", wait)
    runner._run_loop()
    assert waits == [3600, 3600]
    callback.assert_called_once()


def test_disabled_runner_never_starts_retention():
    callback = MagicMock()
    runner = BackgroundRunner(
        BackgroundConfig(enabled=False), logging.getLogger("test"),
        storage_retention_fn=callback,
    )
    runner.start()
    assert runner._thread is None
    callback.assert_not_called()


@pytest.mark.parametrize("database", [False, True])
def test_container_only_wires_retention_when_database_configured(monkeypatch, database):
    monkeypatch.setattr(container_module, "PostgresPool", MagicMock())
    config = AppConfig(
        database=DatabaseConfig(postgres_dsn="postgresql://test" if database else ""),
        background=BackgroundConfig(enabled=True),
        storage_retention=StorageRetentionConfig(enabled=True),
    )
    container = container_module.build_container(config)
    assert (container.background_runner._storage_retention_fn is not None) == database


def test_container_retention_is_off_by_default(monkeypatch):
    monkeypatch.setattr(container_module, "PostgresPool", MagicMock())
    container = container_module.build_container(AppConfig(
        database=DatabaseConfig(postgres_dsn="postgresql://test"),
        background=BackgroundConfig(enabled=True),
    ))
    assert container.background_runner._storage_retention_fn is None
