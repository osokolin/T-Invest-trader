# Alerting v1 Reference

Operator alerting for system health degradation.
Alerts are evaluated periodically by the background runner,
with cooldown/dedup to keep volume low and meaningful.

## Alert Catalog

### Signal Pipeline

| Key | Severity | Condition | Default Threshold |
|-----|----------|-----------|-------------------|
| `signal_gap` | warning | No new signals generated | 120 minutes |
| `pending_signals_high` | warning | Too many unresolved delivered signals | 50 signals |

### Data / Ingestion

| Key | Severity | Condition | Default Threshold |
|-----|----------|-----------|-------------------|
| `telegram_gap` | warning | No Telegram messages ingested | 60 minutes |
| `quote_gap` | warning | No market quotes fetched | 30 minutes |
| `global_context_gap` | info | No global context events fetched | 60 minutes |

### Analytics

| Key | Severity | Condition | Default Threshold |
|-----|----------|-----------|-------------------|
| `win_rate_low` | critical | 7d delivered win rate below threshold | 30% (min 10 resolved) |

## Configuration

All settings via environment variables (default OFF):

```
TINVEST_ALERTING_ENABLED=true
TINVEST_ALERTING_CHECK_INTERVAL_SECONDS=300
TINVEST_ALERTING_COOLDOWN_SECONDS=3600
TINVEST_ALERTING_SIGNAL_GAP_MINUTES=120
TINVEST_ALERTING_TELEGRAM_GAP_MINUTES=60
TINVEST_ALERTING_QUOTE_GAP_MINUTES=30
TINVEST_ALERTING_GLOBAL_CONTEXT_GAP_MINUTES=60
TINVEST_ALERTING_PENDING_SIGNALS_MAX=50
TINVEST_ALERTING_WIN_RATE_MIN=0.3
TINVEST_ALERTING_WIN_RATE_LOOKBACK_DAYS=7
TINVEST_ALERTING_WIN_RATE_MIN_RESOLVED=10
```

Background runner flag: `TINVEST_BACKGROUND_RUN_ALERTING=true`

## CLI

```bash
# Evaluate alerts (dry run, no persistence, no sending)
python -m tinvest_trader.cli check-alerts --dry-run

# Evaluate and persist (no Telegram sending)
python -m tinvest_trader.cli check-alerts

# Evaluate, persist, and send via Telegram
python -m tinvest_trader.cli check-alerts --send
```

## Delivery

Alerts are sent to the same Telegram chat as signal delivery,
using the existing `TINVEST_TELEGRAM_BOT_TOKEN` / `TINVEST_TELEGRAM_CHAT_ID`.

## Cooldown / Dedup

Each alert key has an independent cooldown timer (default 1 hour).
A fired alert is persisted in `alert_events` table regardless of
whether Telegram delivery succeeded. The cooldown prevents the same
alert from firing again within the cooldown window.

## Database

Table: `alert_events`

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| alert_key | TEXT | Alert identifier (e.g. `signal_gap`) |
| alert_category | TEXT | Category (signal_pipeline, data_ingestion, analytics) |
| severity | TEXT | critical, warning, info |
| title | TEXT | Short human-readable title |
| message | TEXT | Detailed message body |
| sent | BOOLEAN | Whether Telegram delivery succeeded |
| fired_at | TIMESTAMPTZ | When the alert was fired |

## Grafana Panel (optional)

To add an alert history panel to Grafana, use this query:

```sql
SELECT fired_at AS time,
       alert_key,
       severity,
       title,
       CASE WHEN sent THEN 'sent' ELSE 'not_sent' END AS status
FROM alert_events
WHERE fired_at >= now() - interval '7 days'
ORDER BY fired_at DESC
LIMIT 50;
```
