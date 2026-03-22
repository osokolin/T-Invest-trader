-- ============================================================
-- GRAFANA DASHBOARD QUERIES REFERENCE
-- ============================================================
-- All queries used in Grafana dashboards, for standalone debugging.
-- Each query is labeled with its dashboard and panel name.
-- Time filters use fixed intervals (replace with $__timeFilter in Grafana).

-- ============================================================
-- DASHBOARD 1: OPERATOR OVERVIEW
-- ============================================================

-- [stat] Signals Generated (24h)
SELECT count(*) AS signals_generated
FROM signal_predictions
WHERE created_at >= now() - interval '24 hours';

-- [stat] Signals Delivered (24h)
SELECT count(*) AS signals_delivered
FROM signal_predictions
WHERE pipeline_stage = 'delivered'
  AND created_at >= now() - interval '24 hours';

-- [stat] Win Rate (7d, delivered)
SELECT round(avg(CASE WHEN outcome_label = 'win' THEN 1.0 ELSE 0.0 END)::numeric, 3) AS win_rate
FROM signal_predictions
WHERE resolved_at IS NOT NULL
  AND pipeline_stage = 'delivered'
  AND created_at >= now() - interval '7 days';

-- [stat] Win Rate (24h, delivered)
SELECT round(avg(CASE WHEN outcome_label = 'win' THEN 1.0 ELSE 0.0 END)::numeric, 3) AS win_rate
FROM signal_predictions
WHERE resolved_at IS NOT NULL
  AND pipeline_stage = 'delivered'
  AND created_at >= now() - interval '24 hours';

-- [stat] Avg Return (7d, delivered)
SELECT round(avg(return_pct)::numeric, 5) AS avg_return
FROM signal_predictions
WHERE resolved_at IS NOT NULL
  AND pipeline_stage = 'delivered'
  AND created_at >= now() - interval '7 days';

-- [stat] EV (all time, delivered)
SELECT round(avg(return_pct)::numeric, 5) AS ev
FROM signal_predictions
WHERE resolved_at IS NOT NULL
  AND pipeline_stage = 'delivered';

-- [table] Top Sources by EV
SELECT source_channel,
       count(*) AS signals,
       count(*) FILTER (WHERE resolved_at IS NOT NULL) AS resolved,
       round(avg(CASE WHEN outcome_label = 'win' THEN 1.0 ELSE 0.0 END)
             FILTER (WHERE resolved_at IS NOT NULL)::numeric, 3) AS win_rate,
       round(avg(return_pct)
             FILTER (WHERE resolved_at IS NOT NULL)::numeric, 5) AS ev
FROM signal_predictions
WHERE pipeline_stage = 'delivered'
  AND source_channel IS NOT NULL
GROUP BY source_channel
HAVING count(*) FILTER (WHERE resolved_at IS NOT NULL) >= 5
ORDER BY ev DESC;

-- [table] Top Tickers by EV
SELECT ticker,
       count(*) AS signals,
       count(*) FILTER (WHERE resolved_at IS NOT NULL) AS resolved,
       round(avg(CASE WHEN outcome_label = 'win' THEN 1.0 ELSE 0.0 END)
             FILTER (WHERE resolved_at IS NOT NULL)::numeric, 3) AS win_rate,
       round(avg(return_pct)
             FILTER (WHERE resolved_at IS NOT NULL)::numeric, 5) AS ev
FROM signal_predictions
WHERE pipeline_stage = 'delivered'
GROUP BY ticker
HAVING count(*) FILTER (WHERE resolved_at IS NOT NULL) >= 5
ORDER BY ev DESC;

-- [table] AI Gating Impact (shadow)
SELECT ai_gate_decision,
       count(*) AS signals,
       round(avg(CASE WHEN outcome_label = 'win' THEN 1.0 ELSE 0.0 END)::numeric, 3) AS win_rate,
       round(avg(return_pct)::numeric, 5) AS ev
FROM signal_predictions
WHERE resolved_at IS NOT NULL
  AND ai_gate_decision IS NOT NULL
  AND pipeline_stage = 'delivered'
GROUP BY ai_gate_decision
ORDER BY ai_gate_decision;

-- [table] Source Weight Tiers (shadow)
SELECT CASE
         WHEN source_weight IS NULL THEN 'no_weight'
         WHEN source_weight >= 1.1 THEN 'strong (>=1.1)'
         WHEN source_weight >= 1.0 THEN 'neutral (1.0-1.09)'
         ELSE 'weak (<1.0)'
       END AS weight_tier,
       count(*) AS signals,
       round(avg(CASE WHEN outcome_label = 'win' THEN 1.0 ELSE 0.0 END)::numeric, 3) AS win_rate,
       round(avg(return_pct)::numeric, 5) AS ev
FROM signal_predictions
WHERE resolved_at IS NOT NULL
  AND pipeline_stage = 'delivered'
GROUP BY 1
ORDER BY 1;

-- [table] Global Alignment Impact (shadow)
SELECT COALESCE(global_alignment, 'not_enriched') AS alignment,
       count(*) AS signals,
       round(avg(CASE WHEN outcome_label = 'win' THEN 1.0 ELSE 0.0 END)::numeric, 3) AS win_rate,
       round(avg(return_pct)::numeric, 5) AS ev
FROM signal_predictions
WHERE resolved_at IS NOT NULL
  AND pipeline_stage = 'delivered'
GROUP BY 1
ORDER BY 1;


-- ============================================================
-- DASHBOARD 2: SIGNAL PIPELINE FUNNEL
-- ============================================================

-- [bargauge] Signals by Pipeline Stage (7d)
SELECT COALESCE(pipeline_stage, 'unknown') AS stage,
       count(*) AS count
FROM signal_predictions
WHERE created_at >= now() - interval '7 days'
GROUP BY 1
ORDER BY count DESC;

-- [table] Pipeline Stage + Rejection Reason (7d)
SELECT COALESCE(pipeline_stage, 'unknown') AS stage,
       COALESCE(rejection_reason, '-') AS reason,
       count(*) AS count
FROM signal_predictions
WHERE created_at >= now() - interval '7 days'
GROUP BY 1, 2
ORDER BY count DESC
LIMIT 20;

-- [table] Stage Conversion + Performance (7d)
WITH totals AS (
  SELECT count(*) AS total
  FROM signal_predictions
  WHERE created_at >= now() - interval '7 days'
),
by_stage AS (
  SELECT COALESCE(pipeline_stage, 'unknown') AS stage,
         count(*) AS cnt
  FROM signal_predictions
  WHERE created_at >= now() - interval '7 days'
  GROUP BY 1
)
SELECT s.stage,
       s.cnt AS signals,
       round(s.cnt * 100.0 / NULLIF(t.total, 0), 1) AS pct_of_total,
       round(avg(CASE WHEN sp.outcome_label = 'win' THEN 1.0 ELSE 0.0 END)
             FILTER (WHERE sp.resolved_at IS NOT NULL), 3) AS win_rate,
       round(avg(sp.return_pct)
             FILTER (WHERE sp.resolved_at IS NOT NULL), 5) AS ev,
       count(*) FILTER (WHERE sp.resolved_at IS NOT NULL) AS resolved
FROM by_stage s
CROSS JOIN totals t
JOIN signal_predictions sp
  ON COALESCE(sp.pipeline_stage, 'unknown') = s.stage
  AND sp.created_at >= now() - interval '7 days'
GROUP BY s.stage, s.cnt, t.total
ORDER BY s.cnt DESC;

-- [timeseries] Signals by Stage Over Time (use $__timeFilter in Grafana)
SELECT date_trunc('day', created_at) AS time,
       COALESCE(pipeline_stage, 'unknown') AS stage,
       count(*) AS value
FROM signal_predictions
WHERE created_at >= now() - interval '7 days'
GROUP BY 1, 2
ORDER BY 1;


-- ============================================================
-- DASHBOARD 3: ANALYTICS (AI / SOURCE / GLOBAL)
-- ============================================================

-- [table] Source Performance (all time)
SELECT source_channel,
       count(*) AS total,
       count(*) FILTER (WHERE pipeline_stage = 'delivered') AS delivered,
       count(*) FILTER (WHERE resolved_at IS NOT NULL) AS resolved,
       round(avg(CASE WHEN outcome_label = 'win' THEN 1.0 ELSE 0.0 END)
             FILTER (WHERE resolved_at IS NOT NULL)::numeric, 3) AS win_rate,
       round(avg(return_pct)
             FILTER (WHERE resolved_at IS NOT NULL)::numeric, 5) AS avg_return,
       round(avg(return_pct)
             FILTER (WHERE resolved_at IS NOT NULL AND pipeline_stage = 'delivered')::numeric, 5) AS ev,
       round(avg(source_weight)
             FILTER (WHERE source_weight IS NOT NULL)::numeric, 3) AS avg_source_weight
FROM signal_predictions
WHERE source_channel IS NOT NULL
GROUP BY source_channel
HAVING count(*) >= 3
ORDER BY ev DESC NULLS LAST;

-- [bargauge] AI Divergence Buckets (delivered)
SELECT COALESCE(a.divergence_bucket, 'no_analysis') AS bucket,
       count(*) AS count
FROM signal_predictions sp
LEFT JOIN signal_ai_analyses a ON a.signal_id = sp.id
WHERE sp.pipeline_stage = 'delivered'
GROUP BY 1
ORDER BY count DESC;

-- [table] Performance by AI Divergence Bucket
SELECT COALESCE(a.divergence_bucket, 'no_analysis') AS bucket,
       count(*) AS signals,
       round(avg(CASE WHEN sp.outcome_label = 'win' THEN 1.0 ELSE 0.0 END)::numeric, 3) AS win_rate,
       round(avg(sp.return_pct)::numeric, 5) AS ev
FROM signal_predictions sp
LEFT JOIN signal_ai_analyses a ON a.signal_id = sp.id
WHERE sp.resolved_at IS NOT NULL
  AND sp.pipeline_stage = 'delivered'
GROUP BY 1
HAVING count(*) >= 3
ORDER BY ev DESC;

-- [table] AI Gating: Baseline vs Filtered
SELECT COALESCE(ai_gate_decision, 'no_gate') AS gate_decision,
       count(*) AS signals,
       count(*) FILTER (WHERE resolved_at IS NOT NULL) AS resolved,
       round(avg(CASE WHEN outcome_label = 'win' THEN 1.0 ELSE 0.0 END)
             FILTER (WHERE resolved_at IS NOT NULL)::numeric, 3) AS win_rate,
       round(avg(return_pct)
             FILTER (WHERE resolved_at IS NOT NULL)::numeric, 5) AS ev,
       round(avg(confidence)
             FILTER (WHERE resolved_at IS NOT NULL)::numeric, 4) AS avg_confidence
FROM signal_predictions
WHERE pipeline_stage = 'delivered'
GROUP BY 1
ORDER BY 1;

-- [table] Global Alignment Performance (delivered)
SELECT COALESCE(global_alignment, 'not_enriched') AS alignment,
       count(*) AS signals,
       count(*) FILTER (WHERE resolved_at IS NOT NULL) AS resolved,
       round(avg(CASE WHEN outcome_label = 'win' THEN 1.0 ELSE 0.0 END)
             FILTER (WHERE resolved_at IS NOT NULL)::numeric, 3) AS win_rate,
       round(avg(return_pct)
             FILTER (WHERE resolved_at IS NOT NULL)::numeric, 5) AS ev,
       round(avg(global_adjustment)
             FILTER (WHERE global_adjustment IS NOT NULL)::numeric, 4) AS avg_adjustment
FROM signal_predictions
WHERE pipeline_stage = 'delivered'
GROUP BY 1
ORDER BY 1;


-- ============================================================
-- DASHBOARD 4: DATA & INFRASTRUCTURE HEALTH
-- ============================================================

-- [table] Telegram Messages by Channel
SELECT channel_name,
       count(*) AS total_messages,
       count(*) FILTER (WHERE recorded_at >= now() - interval '24 hours') AS messages_24h,
       count(*) FILTER (WHERE recorded_at >= now() - interval '1 hour') AS messages_1h,
       max(recorded_at) AS latest_message
FROM telegram_messages_raw
GROUP BY channel_name
ORDER BY messages_24h DESC;

-- [table] Telegram Channel Freshness
SELECT channel_name,
       max(recorded_at) AS latest,
       extract(epoch FROM (now() - max(recorded_at)))::int AS age_seconds
FROM telegram_messages_raw
GROUP BY channel_name
ORDER BY age_seconds DESC;

-- [stat] Quotes Fetched (24h)
SELECT count(*) AS quotes_24h
FROM market_quotes
WHERE fetched_at >= now() - interval '24 hours';

-- [stat] Latest Quote Age
SELECT extract(epoch FROM (now() - max(fetched_at)))::int AS age_seconds
FROM market_quotes;

-- [stat] Active Tickers (24h)
SELECT count(DISTINCT ticker) AS active_tickers
FROM market_quotes
WHERE fetched_at >= now() - interval '24 hours';

-- [table] Global Context Events by Source/Type
SELECT source_key,
       event_type,
       direction,
       count(*) AS total,
       count(*) FILTER (WHERE fetched_at >= now() - interval '24 hours') AS events_24h
FROM global_market_context_events
GROUP BY source_key, event_type, direction
ORDER BY events_24h DESC;

-- [table] Global Context Source Freshness
SELECT source_key,
       max(fetched_at) AS latest,
       extract(epoch FROM (now() - max(fetched_at)))::int AS age_seconds,
       count(*) FILTER (WHERE fetched_at >= now() - interval '1 hour') AS events_last_hour
FROM global_market_context_events
GROUP BY source_key
ORDER BY age_seconds DESC;

-- [table] Latest Global Market Snapshots
SELECT DISTINCT ON (symbol)
       symbol,
       category,
       price,
       change_pct,
       source_time,
       fetched_at,
       extract(epoch FROM (now() - fetched_at))::int AS age_seconds
FROM global_market_snapshots
ORDER BY symbol, fetched_at DESC;

-- [table] Broker Fetch State by Event Type
SELECT event_type,
       count(*) AS tracked_pairs,
       count(*) FILTER (WHERE last_checked_at >= now() - interval '24 hours') AS checked_24h,
       count(*) FILTER (WHERE error_count > 0) AS with_errors,
       sum(error_count) AS total_errors,
       max(last_checked_at) AS latest_check
FROM broker_event_fetch_state
GROUP BY event_type
ORDER BY event_type;

-- [table] Broker Fetch Errors (top 20)
SELECT figi, event_type, error_count,
       last_error_at, last_success_at, last_checked_at
FROM broker_event_fetch_state
WHERE error_count > 0
ORDER BY error_count DESC
LIMIT 20;

-- [stat] AI Analyses (total)
SELECT count(*) AS total_analyses FROM signal_ai_analyses;

-- [stat] AI Analyses (24h)
SELECT count(*) AS analyses_24h
FROM signal_ai_analyses
WHERE created_at >= now() - interval '24 hours';

-- [stat] AI Coverage (delivered)
SELECT round(count(a.id)::numeric / NULLIF(count(sp.id), 0), 3) AS coverage
FROM signal_predictions sp
LEFT JOIN signal_ai_analyses a ON a.signal_id = sp.id
WHERE sp.pipeline_stage = 'delivered';

-- [stat] Signals Pending Resolution
SELECT count(*) AS pending
FROM signal_predictions
WHERE pipeline_stage = 'delivered'
  AND resolved_at IS NULL;
