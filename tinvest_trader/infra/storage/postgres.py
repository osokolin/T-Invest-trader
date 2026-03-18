"""Postgres connection pool wrapper."""

from __future__ import annotations

import logging
from pathlib import Path

from psycopg_pool import ConnectionPool

from tinvest_trader.app.config import DatabaseConfig

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class PostgresPool:
    """Thin wrapper around psycopg_pool.ConnectionPool."""

    def __init__(self, config: DatabaseConfig, logger: logging.Logger) -> None:
        self._logger = logger
        self._pool = ConnectionPool(
            conninfo=config.postgres_dsn,
            min_size=config.pool_min_size,
            max_size=config.pool_max_size,
        )

    def get_connection(self):
        """Return a context-managed connection from the pool."""
        return self._pool.connection()

    def initialize_schema(self) -> None:
        """Run schema.sql to create tables if they don't exist."""
        sql = SCHEMA_PATH.read_text()
        with self._pool.connection() as conn:
            conn.execute(sql)
            conn.commit()
        self._logger.info("database schema initialized", extra={"component": "postgres"})

    def health_check(self) -> bool:
        """Verify connectivity with SELECT 1."""
        try:
            with self._pool.connection() as conn:
                conn.execute("SELECT 1")
            return True
        except Exception:
            self._logger.exception("database health check failed")
            return False

    def close(self) -> None:
        """Shut down the connection pool."""
        self._pool.close()
        self._logger.info("database pool closed", extra={"component": "postgres"})
