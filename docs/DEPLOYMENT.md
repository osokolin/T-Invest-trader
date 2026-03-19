# Deployment Runbook

Target server: `5e2ef2c9b5d1.vps.myjino.ru`
User: `t_bot`
Port: `49353`

## Prerequisites

On the VPS:
- Docker and Docker Compose installed
- Git installed
- SSH access configured

## Connect to VPS

```
ssh -p 49353 t_bot@5e2ef2c9b5d1.vps.myjino.ru
```

## Initial Setup

### 1. Clone the repository

```
cd ~
git clone https://github.com/osokolin/T-Invest-trader.git
cd T-Invest-trader
```

### 2. Create environment file

```
cp .env.example .env
```

Edit `.env` and fill in real values:
- `TINVEST_TOKEN` -- your T-Bank API token
- `TINVEST_ACCOUNT_ID` -- your account ID
- `POSTGRES_PASSWORD` -- set a strong password
- Update `TINVEST_POSTGRES_DSN` to match the password
- `TINVEST_ENVIRONMENT` -- set to `sandbox` or `production`

### 3. Start the stack

```
docker compose up -d
```

### 4. Verify services are running

```
docker compose ps
```

Expected: both `postgres` (healthy) and `app` services running.

### 5. Check app logs

```
docker compose logs -f app
```

Look for:
- `tinvest_trader starting`
- `database connected and schema ready`
- `tinvest_trader started successfully`

### 6. Check postgres is accessible

```
docker compose exec postgres psql -U tinvest -d tinvest -c "SELECT 1"
```

## Updating

```
cd ~/T-Invest-trader
git fetch origin
git checkout main
git reset --hard origin/main
docker compose up -d --build
```

## Useful Commands

| Command | Description |
|---------|-------------|
| `docker compose ps` | Show service status |
| `docker compose logs -f app` | Follow app logs |
| `docker compose logs -f postgres` | Follow DB logs |
| `docker compose restart app` | Restart app only |
| `docker compose down` | Stop all services |
| `docker compose exec app bash` | Shell into app container |
| `docker compose exec postgres psql -U tinvest -d tinvest` | Open psql |

## SQL Inspection Queries

Connect to postgres:
```
docker compose exec postgres psql -U tinvest -d tinvest
```

### Latest Telegram messages
```sql
SELECT channel_name, message_id, published_at, left(message_text, 80) AS text_preview
FROM telegram_messages_raw
ORDER BY recorded_at DESC
LIMIT 20;
```

### Latest ticker mentions
```sql
SELECT ticker, figi, mention_type, channel_name, message_id, recorded_at
FROM telegram_message_mentions
ORDER BY recorded_at DESC
LIMIT 20;
```

### Latest sentiment events
```sql
SELECT ticker, label, score_positive, score_negative, score_neutral, model_name, scored_at
FROM telegram_sentiment_events
ORDER BY recorded_at DESC
LIMIT 20;
```

### Latest signal observations
```sql
SELECT ticker, window, observation_time, message_count,
       positive_count, negative_count, neutral_count, sentiment_balance
FROM signal_observations
ORDER BY recorded_at DESC
LIMIT 20;
```

### Latest market snapshots
```sql
SELECT figi, ticker, last_price, trading_status, snapshot_time
FROM market_snapshots
ORDER BY recorded_at DESC
LIMIT 20;
```

### Sentiment summary by ticker (last hour)
```sql
SELECT ticker,
       count(*) AS total,
       count(*) FILTER (WHERE label = 'positive') AS pos,
       count(*) FILTER (WHERE label = 'negative') AS neg,
       count(*) FILTER (WHERE label = 'neutral') AS neu
FROM telegram_sentiment_events
WHERE scored_at > now() - interval '1 hour'
GROUP BY ticker
ORDER BY total DESC;
```

### Table row counts
```sql
SELECT 'telegram_messages_raw' AS tbl, count(*) FROM telegram_messages_raw
UNION ALL SELECT 'telegram_message_mentions', count(*) FROM telegram_message_mentions
UNION ALL SELECT 'telegram_sentiment_events', count(*) FROM telegram_sentiment_events
UNION ALL SELECT 'signal_observations', count(*) FROM signal_observations
UNION ALL SELECT 'market_snapshots', count(*) FROM market_snapshots
UNION ALL SELECT 'order_intents', count(*) FROM order_intents
UNION ALL SELECT 'execution_events', count(*) FROM execution_events;
```

## Troubleshooting

### App behavior
The app starts, runs health checks, and then blocks waiting for SIGINT/SIGTERM.
It stays alive as a long-running process suitable for `restart: unless-stopped`.
Future milestones will replace the idle wait with a real trading/event loop.

### Database connection refused
Check that postgres container is healthy:
```
docker compose ps postgres
docker compose logs postgres
```

Verify DSN in `.env` matches postgres credentials.

### Permission denied on VPS
Ensure `t_bot` user has docker group membership:
```
groups t_bot
```

## Future Autodeploy Plan

Required GitHub Secrets for automated deployment:
- `DEPLOY_HOST` -- `5e2ef2c9b5d1.vps.myjino.ru`
- `DEPLOY_PORT` -- `49353`
- `DEPLOY_USER` -- `t_bot`
- `DEPLOY_SSH_KEY` -- private SSH key for VPS access

Workflow: on merge to main, SSH into VPS, fetch the latest `origin/main`,
hard-reset the working tree to that revision, then rebuild and restart.
Enable only after first successful manual deployment.
