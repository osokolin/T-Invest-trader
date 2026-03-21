-- Milestone 3: audit trail / statistics schema
-- All tables are append-only or upsert-only.
-- No foreign keys: each table is independently queryable.
-- Prices use NUMERIC(20, 9) to match broker nano precision.

-- Instrument catalog: canonical registry for tracked instruments
CREATE TABLE IF NOT EXISTS instrument_catalog (
    id              BIGSERIAL PRIMARY KEY,
    figi            TEXT NOT NULL UNIQUE,
    instrument_uid  TEXT,
    ticker          TEXT NOT NULL,
    name            TEXT NOT NULL DEFAULT '',
    isin            TEXT NOT NULL DEFAULT '',
    moex_secid      TEXT NOT NULL DEFAULT '',
    lot             INTEGER,
    currency        TEXT,
    tracked         BOOLEAN NOT NULL DEFAULT FALSE,
    enabled         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Additive migration: add columns that may be missing on existing deployments
ALTER TABLE instrument_catalog ADD COLUMN IF NOT EXISTS isin TEXT NOT NULL DEFAULT '';
ALTER TABLE instrument_catalog ADD COLUMN IF NOT EXISTS moex_secid TEXT NOT NULL DEFAULT '';
CREATE UNIQUE INDEX IF NOT EXISTS idx_instrument_catalog_ticker
    ON instrument_catalog (ticker);

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
    normalized_text TEXT,
    dedup_hash      TEXT,
    UNIQUE (channel_name, message_id)
);
CREATE INDEX IF NOT EXISTS idx_telegram_raw_channel_time
    ON telegram_messages_raw (channel_name, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_telegram_raw_channel_msgid
    ON telegram_messages_raw (channel_name, message_id DESC);
CREATE INDEX IF NOT EXISTS idx_telegram_raw_dedup_hash
    ON telegram_messages_raw (dedup_hash) WHERE dedup_hash IS NOT NULL;

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

    -- MOEX market context (latest available data from moex_market_history)
    moex_latest_close               NUMERIC(20, 9),
    moex_latest_volume              BIGINT,
    moex_latest_numtrades           INTEGER,
    moex_last_trade_date            DATE,
    moex_days_since_last_trade      NUMERIC(10, 2),
    moex_price_change_1d_pct        NUMERIC(10, 4),
    moex_range_pct                  NUMERIC(10, 4),

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

-- ============================================================
-- Milestone 9: MOEX ISS market data ingestion
-- ============================================================

-- Security reference metadata from ISS /securities/{secid}.json
CREATE TABLE IF NOT EXISTS moex_security_reference (
    id              BIGSERIAL PRIMARY KEY,
    secid           TEXT NOT NULL,
    name            TEXT NOT NULL DEFAULT '',
    short_name      TEXT NOT NULL DEFAULT '',
    isin            TEXT NOT NULL DEFAULT '',
    reg_number      TEXT NOT NULL DEFAULT '',
    list_level      INTEGER,
    issuer          TEXT NOT NULL DEFAULT '',
    issue_size      BIGINT,
    "group"         TEXT NOT NULL DEFAULT '',
    primary_boardid TEXT NOT NULL DEFAULT '',
    raw_description JSONB,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_moex_security_ref_secid
    ON moex_security_reference (secid);
CREATE INDEX IF NOT EXISTS idx_moex_security_ref_isin
    ON moex_security_reference (isin);

-- Raw daily market history rows from ISS history endpoint
CREATE TABLE IF NOT EXISTS moex_market_history_raw (
    id              BIGSERIAL PRIMARY KEY,
    secid           TEXT NOT NULL,
    boardid         TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    open            NUMERIC(20, 9),
    high            NUMERIC(20, 9),
    low             NUMERIC(20, 9),
    close           NUMERIC(20, 9),
    legal_close     NUMERIC(20, 9),
    waprice         NUMERIC(20, 9),
    volume          BIGINT,
    value           NUMERIC(20, 4),
    num_trades      INTEGER,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_moex_history_raw_unique
    ON moex_market_history_raw (secid, boardid, trade_date);
CREATE INDEX IF NOT EXISTS idx_moex_history_raw_secid_date
    ON moex_market_history_raw (secid, trade_date DESC);

-- Normalized daily market history (board-filtered, cleaned)
CREATE TABLE IF NOT EXISTS moex_market_history (
    id              BIGSERIAL PRIMARY KEY,
    secid           TEXT NOT NULL,
    boardid         TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    open            NUMERIC(20, 9),
    high            NUMERIC(20, 9),
    low             NUMERIC(20, 9),
    close           NUMERIC(20, 9),
    waprice         NUMERIC(20, 9),
    volume          BIGINT,
    value           NUMERIC(20, 4),
    num_trades      INTEGER,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_moex_history_unique
    ON moex_market_history (secid, boardid, trade_date);
CREATE INDEX IF NOT EXISTS idx_moex_history_secid_date
    ON moex_market_history (secid, trade_date DESC);

-- ============================================================
-- Milestone 10: Broker event fetch policy state
-- ============================================================

-- Tracks when each (figi, event_type) was last fetched and its outcome.
-- Used by the fetch policy to implement TTL-based selective fetching.
CREATE TABLE IF NOT EXISTS broker_event_fetch_state (
    id              BIGSERIAL PRIMARY KEY,
    figi            TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    last_checked_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_error_at   TIMESTAMPTZ,
    error_count     INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_broker_event_fetch_state_unique
    ON broker_event_fetch_state (figi, event_type);

-- ============================================================
-- Milestone 11: Signal prediction tracking + calibration
-- ============================================================

-- Each row is a prediction: "ticker will go up/down".
-- Outcome fields are filled later by the resolution service.
CREATE TABLE IF NOT EXISTS signal_predictions (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    signal_type     TEXT NOT NULL,          -- up, down
    confidence      NUMERIC(8, 6),
    source          TEXT NOT NULL DEFAULT 'fusion',
    features_json   JSONB,
    price_at_signal NUMERIC(20, 9),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Outcome (filled by resolution service)
    price_at_outcome NUMERIC(20, 9),
    return_pct       NUMERIC(12, 6),
    outcome_label    TEXT,                  -- win, loss, neutral
    resolved_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_signal_predictions_ticker
    ON signal_predictions (ticker);
CREATE INDEX IF NOT EXISTS idx_signal_predictions_created
    ON signal_predictions (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_predictions_pending
    ON signal_predictions (created_at) WHERE resolved_at IS NULL;

-- Source attribution columns (nullable for backward compatibility).
-- source_channel: Telegram channel name (e.g. markettwits, banksta)
-- source_message_id: original Telegram message_id
-- source_message_db_id: FK-style pointer to telegram_messages_raw.id
ALTER TABLE signal_predictions ADD COLUMN IF NOT EXISTS source_channel TEXT;
ALTER TABLE signal_predictions ADD COLUMN IF NOT EXISTS source_message_id TEXT;
ALTER TABLE signal_predictions ADD COLUMN IF NOT EXISTS source_message_db_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_signal_predictions_source_channel
    ON signal_predictions (source_channel) WHERE source_channel IS NOT NULL;

-- Signal delivery tracking (nullable for backward compatibility).
ALTER TABLE signal_predictions ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ;

-- Pipeline stage tracking (nullable for backward compatibility).
-- pipeline_stage: generated, rejected_calibration, rejected_binding, rejected_safety, delivered
-- rejection_reason: low_confidence, negative_ev, type_disabled, etc.
ALTER TABLE signal_predictions ADD COLUMN IF NOT EXISTS pipeline_stage TEXT;
ALTER TABLE signal_predictions ADD COLUMN IF NOT EXISTS rejection_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_signal_predictions_pipeline_stage
    ON signal_predictions (pipeline_stage) WHERE pipeline_stage IS NOT NULL;

-- AI shadow gating columns (nullable for backward compatibility).
-- ai_gate_decision: ALLOW, CAUTION, BLOCK (shadow-mode only, never affects execution)
-- ai_gate_reason: short explanation of the gating decision
ALTER TABLE signal_predictions ADD COLUMN IF NOT EXISTS ai_gate_decision TEXT;
ALTER TABLE signal_predictions ADD COLUMN IF NOT EXISTS ai_gate_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_signal_predictions_ai_gate
    ON signal_predictions (ai_gate_decision) WHERE ai_gate_decision IS NOT NULL;

-- AI analysis cache (one analysis per signal, avoids duplicate API calls).
CREATE TABLE IF NOT EXISTS signal_ai_analyses (
    id          BIGSERIAL PRIMARY KEY,
    signal_id   BIGINT NOT NULL,
    analysis_text TEXT NOT NULL,
    model       TEXT NOT NULL DEFAULT 'claude-sonnet-4-20250514',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (signal_id)
);

-- AI divergence tracking: structured fields parsed from AI output
-- plus system severity snapshot at analysis time.
ALTER TABLE signal_ai_analyses ADD COLUMN IF NOT EXISTS
    ai_confidence   TEXT;              -- LOW, MEDIUM, HIGH (parsed)
ALTER TABLE signal_ai_analyses ADD COLUMN IF NOT EXISTS
    ai_actionability TEXT;             -- CONSIDER, WATCH, WEAK, CAUTION, UNKNOWN
ALTER TABLE signal_ai_analyses ADD COLUMN IF NOT EXISTS
    ai_bias         TEXT;              -- bullish, bearish, neutral, unknown
ALTER TABLE signal_ai_analyses ADD COLUMN IF NOT EXISTS
    system_severity TEXT;              -- HIGH, MEDIUM, LOW (snapshot)
ALTER TABLE signal_ai_analyses ADD COLUMN IF NOT EXISTS
    divergence_bucket TEXT;            -- agree_strong, agree_weak, ai_more_bullish, ai_more_bearish, uncertain, unknown

CREATE INDEX IF NOT EXISTS idx_signal_ai_divergence_bucket
    ON signal_ai_analyses (divergence_bucket) WHERE divergence_bucket IS NOT NULL;

-- ============================================================
-- Milestone 12: T-Bank market quote ingestion (last prices)
-- ============================================================

-- Stores periodic last-price snapshots from T-Bank GetLastPrices bulk API.
-- One row per (figi, fetched_at) -- append-only for time-series analysis.
CREATE TABLE IF NOT EXISTS market_quotes (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    figi            TEXT NOT NULL,
    instrument_uid  TEXT NOT NULL DEFAULT '',
    price           NUMERIC(20, 9) NOT NULL,
    source_time     TIMESTAMPTZ,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_market_quotes_ticker_time
    ON market_quotes (ticker, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_quotes_figi_time
    ON market_quotes (figi, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_quotes_ticker_source_time
    ON market_quotes (ticker, source_time ASC);
CREATE INDEX IF NOT EXISTS idx_market_quotes_figi_source_time
    ON market_quotes (figi, source_time ASC);
