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
