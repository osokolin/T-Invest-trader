"""Storage incident regressions, offline: no broker, real DB, or long sleeps."""

import json
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tinvest_trader import cli, healthcheck
from tinvest_trader.app.config import (
    AppConfig,
    BackgroundConfig,
    DatabaseConfig,
    FusionConfig,
    MarketActivityConfig,
    QuoteSyncConfig,
    StorageHealthConfig,
    load_config,
)
from tinvest_trader.infra.storage import health, postgres
from tinvest_trader.infra.storage.repository import TradingRepository

NOW = datetime(2026, 9, 10, 12, tzinfo=UTC)


@pytest.fixture
def probes(monkeypatch, tmp_path):
    monkeypatch.setattr(health, "ERROR_PATH", tmp_path / "error.json")
    disk = SimpleNamespace(f_blocks=100, f_frsize=1024**3, f_bavail=50)
    monkeypatch.setattr(health.os, "statvfs", lambda _: disk)
    connection = MagicMock()

    def execute(sql, params=None):
        cursor = MagicMock()
        if "pg_database_size" in sql:
            cursor.fetchone.return_value = (20 * 1024**3,)
        elif "pg_class" in sql:
            cursor.fetchall.return_value = [("fused_signal_features", 10, 20, 30, 73_000_000)]
        else:
            cursor.fetchone.return_value = (NOW - timedelta(seconds=60),)
        return cursor

    connection.execute.side_effect = execute
    connect = MagicMock()
    connect.return_value.__enter__.return_value = connection
    monkeypatch.setattr(health.psycopg, "connect", connect)
    config = AppConfig(
        database=DatabaseConfig(postgres_dsn="postgresql://test"),
        background=BackgroundConfig(enabled=True, fusion_interval_seconds=60),
        fusion=FusionConfig(enabled=True),
        quote_sync=QuoteSyncConfig(enabled=True),
        market_activity=MarketActivityConfig(enabled=True),
        storage_health=StorageHealthConfig(enabled=True),
    )
    return config, connection, connect, disk


def test_config_defaults_and_parsing(monkeypatch):
    assert not StorageHealthConfig().enabled
    monkeypatch.setenv("TINVEST_STORAGE_HEALTH_ENABLED", "true")
    monkeypatch.setenv("TINVEST_STORAGE_HEALTH_DISK_PATH", "/mounted-pgdata")
    monkeypatch.setenv("TINVEST_STORAGE_HEALTH_WARNING_FREE_PERCENT", "25")
    monkeypatch.setenv("TINVEST_STORAGE_HEALTH_CRITICAL_FREE_PERCENT", "12")
    monkeypatch.setenv("TINVEST_STORAGE_HEALTH_MIN_FREE_BYTES", "4096")
    monkeypatch.setenv("TINVEST_STORAGE_HEALTH_FRESHNESS_SECONDS", "1200")
    monkeypatch.setenv("TINVEST_STORAGE_HEALTH_RECENT_ERROR_SECONDS", "600")
    cfg = load_config().storage_health
    assert cfg == StorageHealthConfig(True, "/mounted-pgdata", 25, 12, 4096, 1200, 600)


@pytest.mark.parametrize("values", [
    {"critical_free_percent": 20}, {"warning_free_percent": 101},
    {"min_free_bytes": -1}, {"freshness_seconds": 0}, {"recent_error_seconds": 0},
])
def test_invalid_config(values):
    with pytest.raises(ValueError):
        StorageHealthConfig(**values)


def test_inspection_is_read_only_bounded_and_reports_estimates(probes):
    cfg, conn, connect, _ = probes
    result = health.inspect_storage(cfg, now=NOW)
    assert result["status"] == "ok"
    assert result["write_probe"] == "not_requested"
    assert result["tables"][0]["estimated_rows"] == 73_000_000
    sql = "\n".join(call.args[0] for call in conn.execute.call_args_list)
    assert "SET TRANSACTION READ ONLY" in sql
    assert "ORDER BY id DESC LIMIT" in sql
    assert "count(*)" not in sql
    assert "INSERT" not in sql
    assert "DELETE" not in sql
    assert connect.call_args.kwargs["connect_timeout"] == 3
    assert "statement_timeout=2000" in connect.call_args.kwargs["options"]
    assert set(result["sources"]) == {"fusion", "quotes", "activity"}


def test_persist_uses_one_committed_snapshot(probes):
    cfg, conn, connect, _ = probes
    result = health.inspect_storage(cfg, persist=True, now=NOW)
    assert result["write_probe"] == "committed"
    sql, params = conn.execute.call_args.args
    assert "INSERT INTO storage_health_snapshot" in sql
    assert "ON CONFLICT (id) DO UPDATE" in sql
    assert params[0] == NOW
    assert params[1].obj["status"] == "ok"
    connect.return_value.__exit__.assert_called_once()


@pytest.mark.parametrize(("free", "status"), [(19, "warning"), (10, "critical"), (1, "critical")])
def test_disk_thresholds(probes, free, status):
    cfg, _, _, disk = probes
    disk.f_bavail = free
    assert health.inspect_storage(cfg, now=NOW)["status"] == status


def test_absolute_free_bytes_limit(probes):
    cfg, _, _, disk = probes
    disk.f_frsize = 1024
    assert health.inspect_storage(cfg, now=NOW)["status"] == "critical"


def test_missing_mount_is_not_silently_measured_on_app_filesystem(probes, monkeypatch):
    cfg, _, _, _ = probes
    monkeypatch.setattr(health.os, "statvfs", MagicMock(side_effect=FileNotFoundError))
    assert health.inspect_storage(cfg, now=NOW)["status"] == "critical"


@pytest.mark.parametrize("commit_failure", [False, True])
def test_probe_errors_fail_closed_without_secrets(probes, commit_failure):
    cfg, conn, connect, _ = probes
    error = RuntimeError("postgresql://user:SECRET@host")
    if commit_failure:
        connect.return_value.__exit__.side_effect = error
    else:
        conn.execute.side_effect = error
    result = health.inspect_storage(cfg, persist=True, now=NOW)
    assert result["status"] == "critical"
    assert result["write_probe"] == "failed"
    assert "SECRET" not in json.dumps(result)


def test_disabled_pipelines_do_not_raise_staleness(probes):
    cfg, _, _, _ = probes
    cfg = replace(cfg, background=BackgroundConfig(enabled=False))
    result = health.inspect_storage(cfg, now=NOW)
    assert result["status"] == "ok"
    assert result["sources"] == {}


def test_missing_and_stale_data_are_not_healthy(probes):
    cfg, conn, _, _ = probes
    execute = conn.execute.side_effect

    def stale(sql, params=None):
        cursor = execute(sql, params)
        if "ORDER BY id DESC" in sql:
            cursor.fetchone.return_value = None
        return cursor

    conn.execute.side_effect = stale
    result = health.inspect_storage(cfg, now=NOW)
    assert result["status"] == "critical"
    assert len(result["issues"]) == 3


def test_slow_fusion_interval_has_matching_grace(probes):
    cfg, _, _, _ = probes
    cfg = replace(cfg, background=replace(cfg.background, fusion_interval_seconds=3600))
    result = health.inspect_storage(cfg, now=NOW + timedelta(minutes=30))
    assert result["sources"]["fusion"]["threshold_seconds"] == 10800
    assert "fusion persistence is missing or stale" not in result["issues"]


def test_no_market_gap_alert_at_night_or_weekend(probes):
    cfg, _, _, _ = probes
    cfg = replace(cfg, fusion=FusionConfig(enabled=False))
    for now in [NOW.replace(hour=0), NOW + timedelta(days=2)]:
        result = health.inspect_storage(cfg, now=now)
        assert result["status"] == "ok"
        assert not result["sources"]["activity"]["expected"]


def test_recent_failure_survives_successful_probe_then_expires(probes):
    cfg, _, _, _ = probes
    health.ERROR_PATH.write_text(json.dumps({"at": NOW.isoformat(), "type": "DiskFull"}))
    assert health.inspect_storage(cfg, now=NOW)["status"] == "critical"
    assert health.inspect_storage(cfg, now=NOW + timedelta(seconds=301))["status"] == "ok"


def test_error_marker_is_atomic_sanitized_and_best_effort(probes, monkeypatch):
    health.record_storage_error(ValueError("SECRET"))
    payload = json.loads(health.ERROR_PATH.read_text())
    assert payload["type"] == "ValueError"
    assert "SECRET" not in health.ERROR_PATH.read_text()
    assert len(list(health.ERROR_PATH.parent.iterdir())) == 1
    monkeypatch.setattr(health.tempfile, "NamedTemporaryFile", MagicMock(side_effect=OSError))
    health.record_storage_error(ValueError("still does not raise"))


@pytest.mark.parametrize("stage", ["acquire", "execute", "commit"])
def test_pool_marks_errors_before_repository_can_swallow_them(monkeypatch, stage):
    raw_pool = MagicMock()
    conn = raw_pool.connection.return_value.__enter__.return_value
    error = RuntimeError("disk full")
    if stage == "acquire":
        raw_pool.connection.return_value.__enter__.side_effect = error
    elif stage == "commit":
        raw_pool.connection.return_value.__exit__.side_effect = error
    else:
        conn.execute.side_effect = error
    monkeypatch.setattr(postgres, "ConnectionPool", MagicMock(return_value=raw_pool))
    marker = MagicMock()
    monkeypatch.setattr(postgres, "record_storage_error", marker)
    pool = postgres.PostgresPool(DatabaseConfig(), logging.getLogger("test"))
    with (
        pytest.raises(RuntimeError, match="disk full"),
        pool.get_connection() as connection,
    ):
        connection.execute("test")
    marker.assert_called_once_with(error)


@pytest.mark.parametrize("stage", ["statement", "commit"])
def test_quote_batch_does_not_count_rolled_back_rows(stage):
    pool = MagicMock()
    conn = pool.get_connection.return_value.__enter__.return_value
    if stage == "statement":
        conn.execute.side_effect = [None, RuntimeError("disk full")]
    else:
        pool.get_connection.return_value.__exit__.side_effect = RuntimeError("disk full")
    repo = TradingRepository(pool, logging.getLogger("test"))
    quotes = [{"ticker": "SBER", "figi": "figi", "price": 100}] * 2
    assert repo.insert_market_quotes_bulk(quotes) == 0


def test_cli_does_not_initialize_container_or_write_probe(probes, monkeypatch, capsys):
    cfg, conn, _, _ = probes
    monkeypatch.setattr(cli, "load_config", lambda: cfg)
    builder = MagicMock(side_effect=AssertionError("must not build container"))
    monkeypatch.setattr(cli, "build_container", builder)
    cli.main(["storage-health"])
    assert "estimated_rows" in capsys.readouterr().out
    builder.assert_not_called()
    assert not any("INSERT" in c.args[0] for c in conn.execute.call_args_list)


@pytest.mark.parametrize(("status", "exit_code"), [("ok", 0), ("warning", 0), ("critical", 1)])
def test_docker_probe_is_independent_of_runner_pool(
    probes, monkeypatch, tmp_path, status, exit_code,
):
    cfg, _, _, _ = probes
    monkeypatch.setattr(healthcheck, "load_config", lambda: cfg)
    heartbeat = tmp_path / "heartbeat"
    heartbeat.touch()
    monkeypatch.setattr(healthcheck, "HEARTBEAT_PATH", heartbeat)
    probe = MagicMock(return_value={"status": status, "issues": ["test"]})
    monkeypatch.setattr(healthcheck, "inspect_storage", probe)
    assert healthcheck.main() == exit_code
    probe.assert_called_once_with(cfg, persist=True)


def test_missing_heartbeat_fails_even_when_db_is_healthy(probes, monkeypatch, tmp_path):
    cfg, _, _, _ = probes
    monkeypatch.setattr(healthcheck, "load_config", lambda: cfg)
    monkeypatch.setattr(healthcheck, "HEARTBEAT_PATH", tmp_path / "missing")
    monkeypatch.setattr(healthcheck, "inspect_storage", lambda *a, **k: {"status": "ok"})
    assert healthcheck.main() == 1


def test_disabled_storage_and_runner_require_no_files_or_db(monkeypatch):
    monkeypatch.setattr(healthcheck, "load_config", AppConfig)
    probe = MagicMock(side_effect=AssertionError("should not probe"))
    monkeypatch.setattr(healthcheck, "inspect_storage", probe)
    assert healthcheck.main() == 0
    probe.assert_not_called()
