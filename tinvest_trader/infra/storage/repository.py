"""Trading repository -- audit trail write path with explicit SQL."""

from __future__ import annotations

import json
import logging
from datetime import datetime

from tinvest_trader.domain.models import (
    BrokerEventFeature,
    BrokerEventRaw,
    ExecutionResult,
    Instrument,
    MarketSnapshot,
    OrderIntent,
)
from tinvest_trader.infra.storage.postgres import PostgresPool
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

    def insert_telegram_message_raw(self, msg: TelegramMessage) -> bool:
        """Insert raw Telegram message. Returns True if newly inserted, False if duplicate."""
        sql = """
            INSERT INTO telegram_messages_raw
                (channel_name, message_id, published_at, message_text, source_payload)
            VALUES (%s, %s, %s, %s, %s)
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
            ))
            inserted = cur.fetchone() is not None
            conn.commit()
        return inserted

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
