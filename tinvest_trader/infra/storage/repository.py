"""Trading repository -- audit trail write path with explicit SQL."""

from __future__ import annotations

import json
import logging
from datetime import datetime

from tinvest_trader.domain.models import (
    ExecutionResult,
    Instrument,
    MarketSnapshot,
    OrderIntent,
)
from tinvest_trader.infra.storage.postgres import PostgresPool


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
        sql = """
            INSERT INTO instrument_catalog
                (figi, instrument_uid, ticker, name, lot, currency, tracked, enabled, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (figi)
            DO UPDATE SET ticker = EXCLUDED.ticker, name = EXCLUDED.name,
                          instrument_uid = EXCLUDED.instrument_uid,
                          lot = EXCLUDED.lot, currency = EXCLUDED.currency,
                          tracked = EXCLUDED.tracked, enabled = EXCLUDED.enabled,
                          updated_at = now()
        """
        with self._pool.get_connection() as conn:
            conn.execute(sql, (
                inst.figi, instrument_uid, inst.ticker, inst.name,
                lot, currency, tracked, enabled,
            ))
            conn.commit()

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
