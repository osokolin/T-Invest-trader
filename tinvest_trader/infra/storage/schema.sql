-- Milestone 3: audit trail / statistics schema
-- All tables are append-only or upsert-only.
-- No foreign keys: each table is independently queryable.
-- Prices use NUMERIC(20, 9) to match broker nano precision.

-- Instrument catalog: reference data, upserted from broker
CREATE TABLE IF NOT EXISTS instrument_catalog (
    id              BIGSERIAL PRIMARY KEY,
    figi            TEXT NOT NULL UNIQUE,
    instrument_uid  TEXT,
    ticker          TEXT NOT NULL,
    name            TEXT NOT NULL DEFAULT '',
    lot             INTEGER,
    currency        TEXT,
    tracked         BOOLEAN NOT NULL DEFAULT FALSE,
    enabled         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Market snapshots: one row per (instrument, fetch time)
CREATE TABLE IF NOT EXISTS market_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    figi            TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    last_price      NUMERIC(20, 9) NOT NULL,
    currency        TEXT NOT NULL DEFAULT 'RUB',
    trading_status  TEXT NOT NULL,
    snapshot_time   TIMESTAMPTZ NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_market_snapshots_figi_time
    ON market_snapshots (figi, snapshot_time DESC);

-- Order intents: what we decided to send
CREATE TABLE IF NOT EXISTS order_intents (
    id              BIGSERIAL PRIMARY KEY,
    account_id      TEXT NOT NULL DEFAULT '',
    figi            TEXT NOT NULL,
    direction       TEXT NOT NULL,
    quantity        INTEGER NOT NULL,
    order_type      TEXT NOT NULL,
    limit_price     NUMERIC(20, 9),
    idempotency_key TEXT NOT NULL UNIQUE,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_order_intents_figi_time
    ON order_intents (figi, recorded_at DESC);

-- Execution events: append-only broker responses
CREATE TABLE IF NOT EXISTS execution_events (
    id              BIGSERIAL PRIMARY KEY,
    account_id      TEXT NOT NULL DEFAULT '',
    event_type      TEXT NOT NULL DEFAULT 'submission',
    idempotency_key TEXT NOT NULL,
    success         BOOLEAN NOT NULL,
    order_id        TEXT,
    figi            TEXT,
    direction       TEXT,
    quantity        INTEGER,
    filled_quantity INTEGER,
    status          TEXT,
    error           TEXT NOT NULL DEFAULT '',
    raw_payload     JSONB,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_execution_events_key
    ON execution_events (idempotency_key);
CREATE INDEX IF NOT EXISTS idx_execution_events_figi_time
    ON execution_events (figi, recorded_at DESC);

-- Broker operations: raw broker-reported operations (future reconciliation)
CREATE TABLE IF NOT EXISTS broker_operations (
    id                  BIGSERIAL PRIMARY KEY,
    account_id          TEXT NOT NULL DEFAULT '',
    broker_operation_id TEXT,
    operation_type      TEXT NOT NULL,
    figi                TEXT,
    quantity            INTEGER,
    price               NUMERIC(20, 9),
    currency            TEXT,
    broker_date         TIMESTAMPTZ,
    raw_payload         JSONB,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_broker_operations_figi_time
    ON broker_operations (figi, recorded_at DESC);

-- Portfolio snapshots: periodic full portfolio state
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    account_id      TEXT NOT NULL DEFAULT '',
    total_value     NUMERIC(20, 9),
    currency        TEXT NOT NULL DEFAULT 'RUB',
    snapshot_time   TIMESTAMPTZ NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_account_time
    ON portfolio_snapshots (account_id, snapshot_time DESC);

-- Position snapshots: per-instrument position at a point in time
CREATE TABLE IF NOT EXISTS position_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    account_id      TEXT NOT NULL DEFAULT '',
    figi            TEXT NOT NULL,
    quantity        INTEGER NOT NULL,
    average_price   NUMERIC(20, 9),
    snapshot_time   TIMESTAMPTZ NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_position_snapshots_account_figi_time
    ON position_snapshots (account_id, figi, snapshot_time DESC);

-- ============================================================
-- Milestone 3.5: Telegram sentiment ingestion
-- ============================================================

-- Raw Telegram messages: stored before any analysis
CREATE TABLE IF NOT EXISTS telegram_messages_raw (
    id              BIGSERIAL PRIMARY KEY,
    channel_name    TEXT NOT NULL,
    message_id      TEXT NOT NULL,
    published_at    TIMESTAMPTZ,
    message_text    TEXT NOT NULL,
    source_payload  JSONB,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (channel_name, message_id)
);
CREATE INDEX IF NOT EXISTS idx_telegram_raw_channel_time
    ON telegram_messages_raw (channel_name, published_at DESC);

-- Ticker mentions extracted from messages
CREATE TABLE IF NOT EXISTS telegram_message_mentions (
    id              BIGSERIAL PRIMARY KEY,
    channel_name    TEXT NOT NULL,
    message_id      TEXT NOT NULL,
    published_at    TIMESTAMPTZ,
    figi            TEXT,
    ticker          TEXT NOT NULL,
    mention_type    TEXT NOT NULL,
    confidence      NUMERIC(8, 6),
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_telegram_mentions_ticker_time
    ON telegram_message_mentions (ticker, recorded_at DESC);

-- Sentiment scoring results per mention
CREATE TABLE IF NOT EXISTS telegram_sentiment_events (
    id              BIGSERIAL PRIMARY KEY,
    channel_name    TEXT NOT NULL,
    message_id      TEXT NOT NULL,
    published_at    TIMESTAMPTZ,
    figi            TEXT,
    ticker          TEXT,
    model_name      TEXT NOT NULL,
    label           TEXT NOT NULL,
    score_positive  NUMERIC(8, 6),
    score_negative  NUMERIC(8, 6),
    score_neutral   NUMERIC(8, 6),
    scored_at       TIMESTAMPTZ NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_telegram_sentiment_figi_time
    ON telegram_sentiment_events (figi, scored_at DESC);
CREATE INDEX IF NOT EXISTS idx_telegram_sentiment_ticker_time
    ON telegram_sentiment_events (ticker, scored_at DESC);

-- ============================================================
-- Milestone 4: Signal observation and aggregation
-- ============================================================

-- Derived sentiment observations per ticker per time window
CREATE TABLE IF NOT EXISTS signal_observations (
    id                  BIGSERIAL PRIMARY KEY,
    ticker              TEXT NOT NULL,
    figi                TEXT,
    "window"            TEXT NOT NULL,
    observation_time    TIMESTAMPTZ NOT NULL,
    message_count       INTEGER NOT NULL,
    positive_count      INTEGER NOT NULL,
    negative_count      INTEGER NOT NULL,
    neutral_count       INTEGER NOT NULL,
    positive_score_avg  NUMERIC(8, 6),
    negative_score_avg  NUMERIC(8, 6),
    neutral_score_avg   NUMERIC(8, 6),
    sentiment_balance   NUMERIC(8, 6),
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_signal_obs_ticker_time
    ON signal_observations (ticker, observation_time DESC);
CREATE INDEX IF NOT EXISTS idx_signal_obs_figi_time
    ON signal_observations (figi, observation_time DESC);
CREATE INDEX IF NOT EXISTS idx_signal_obs_window_time
    ON signal_observations ("window", observation_time DESC);

-- ============================================================
-- Milestone 6: Broker-side structured event ingestion
-- ============================================================

CREATE TABLE IF NOT EXISTS broker_event_raw (
    id              BIGSERIAL PRIMARY KEY,
    account_id      TEXT NOT NULL DEFAULT '',
    source_method   TEXT NOT NULL,
    figi            TEXT,
    ticker          TEXT,
    event_uid       TEXT NOT NULL,
    event_time      TIMESTAMPTZ,
    payload         JSONB NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_broker_event_raw_unique
    ON broker_event_raw (account_id, source_method, event_uid);
CREATE INDEX IF NOT EXISTS idx_broker_event_raw_figi_time
    ON broker_event_raw (figi, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_broker_event_raw_ticker_time
    ON broker_event_raw (ticker, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_broker_event_raw_source_time
    ON broker_event_raw (source_method, event_time DESC);

CREATE TABLE IF NOT EXISTS broker_event_features (
    id              BIGSERIAL PRIMARY KEY,
    account_id      TEXT NOT NULL DEFAULT '',
    source_method   TEXT NOT NULL,
    figi            TEXT,
    ticker          TEXT,
    event_uid       TEXT NOT NULL,
    event_time      TIMESTAMPTZ,
    event_type      TEXT NOT NULL,
    event_direction TEXT,
    event_value     NUMERIC(20, 9),
    currency        TEXT,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_broker_event_features_unique
    ON broker_event_features (account_id, source_method, event_uid);
CREATE INDEX IF NOT EXISTS idx_broker_event_features_figi_time
    ON broker_event_features (figi, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_broker_event_features_ticker_time
    ON broker_event_features (ticker, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_broker_event_features_source_time
    ON broker_event_features (source_method, event_time DESC);

-- ============================================================
-- Milestone 7: Signal Fusion Layer
-- ============================================================

-- Fused per-ticker per-window feature rows combining sentiment
-- observations and broker event features into a single queryable set.
CREATE TABLE IF NOT EXISTS fused_signal_features (
    id                      BIGSERIAL PRIMARY KEY,
    ticker                  TEXT NOT NULL,
    figi                    TEXT,
    "window"                TEXT NOT NULL,
    observation_time        TIMESTAMPTZ NOT NULL,

    -- Sentiment features (from signal_observations)
    sentiment_message_count     INTEGER,
    sentiment_positive_count    INTEGER,
    sentiment_negative_count    INTEGER,
    sentiment_neutral_count     INTEGER,
    sentiment_positive_avg      NUMERIC(8, 6),
    sentiment_negative_avg      NUMERIC(8, 6),
    sentiment_neutral_avg       NUMERIC(8, 6),
    sentiment_balance           NUMERIC(8, 6),

    -- Broker event features (aggregated counts per source)
    broker_dividends_count      INTEGER NOT NULL DEFAULT 0,
    broker_reports_count        INTEGER NOT NULL DEFAULT 0,
    broker_insider_deals_count  INTEGER NOT NULL DEFAULT 0,
    broker_total_event_count    INTEGER NOT NULL DEFAULT 0,

    -- Latest broker event value per source (nullable)
    broker_latest_dividend_value    NUMERIC(20, 9),
    broker_latest_dividend_currency TEXT,
    broker_latest_report_time       TIMESTAMPTZ,
    broker_latest_insider_deal_time TIMESTAMPTZ,

    -- Broker event recency (global latest per ticker, independent of window)
    last_dividend_at                TIMESTAMPTZ,
    last_report_at                  TIMESTAMPTZ,
    last_insider_deal_at            TIMESTAMPTZ,
    days_since_last_dividend        NUMERIC(10, 2),
    days_since_last_report          NUMERIC(10, 2),
    days_since_last_insider_deal    NUMERIC(10, 2),

    recorded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fused_signal_ticker_time
    ON fused_signal_features (ticker, observation_time DESC);
CREATE INDEX IF NOT EXISTS idx_fused_signal_figi_time
    ON fused_signal_features (figi, observation_time DESC);
CREATE INDEX IF NOT EXISTS idx_fused_signal_window_time
    ON fused_signal_features ("window", observation_time DESC);

-- ============================================================
-- Milestone 8: CBR (Bank of Russia) event ingestion
-- ============================================================

-- Raw CBR feed items: stored before normalization
CREATE TABLE IF NOT EXISTS cbr_feed_raw (
    id              BIGSERIAL PRIMARY KEY,
    source_url      TEXT NOT NULL,
    source_type     TEXT NOT NULL DEFAULT 'rss',
    item_uid        TEXT NOT NULL,
    published_at    TIMESTAMPTZ,
    payload         TEXT NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cbr_feed_raw_unique
    ON cbr_feed_raw (source_url, item_uid);
CREATE INDEX IF NOT EXISTS idx_cbr_feed_raw_published
    ON cbr_feed_raw (published_at DESC);

-- Normalized CBR events
CREATE TABLE IF NOT EXISTS cbr_events (
    id              BIGSERIAL PRIMARY KEY,
    source_url      TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    title           TEXT NOT NULL,
    published_at    TIMESTAMPTZ,
    event_key       TEXT NOT NULL,
    url             TEXT NOT NULL DEFAULT '',
    summary         TEXT NOT NULL DEFAULT '',
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cbr_events_unique
    ON cbr_events (source_url, event_key);
CREATE INDEX IF NOT EXISTS idx_cbr_events_published
    ON cbr_events (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_cbr_events_source_published
    ON cbr_events (source_url, published_at DESC);
