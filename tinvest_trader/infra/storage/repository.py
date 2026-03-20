"""Trading repository -- audit trail write path with explicit SQL."""

from __future__ import annotations

import json
import logging
from datetime import datetime

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
        only updates tracked (to True if requested) and metadata if non-empty.
        """
        sql = """
            INSERT INTO instrument_catalog
                (figi, ticker, name, isin, moex_secid, tracked, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (ticker)
            DO UPDATE SET
                tracked = instrument_catalog.tracked OR EXCLUDED.tracked,
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
