-- Run explicitly with psql, OUTSIDE a transaction, before enabling retention.
-- BRIN indexes are small and avoid a large btree build on a nearly full VPS.
-- If a build was interrupted, inspect/drop its INVALID index before retrying.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fused_retention_recorded_at
    ON fused_signal_features USING brin (recorded_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quotes_retention_fetched_at
    ON market_quotes USING brin (fetched_at);
