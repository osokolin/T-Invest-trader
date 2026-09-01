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
With Grafana enabled in `docker-compose.yml`, expected services are `postgres`, `app`, and `grafana`.

### 5. Check app logs

```
docker compose logs -f app
```

Look for:
- `tinvest_trader starting`
- `database connected and schema ready`
- `tinvest_trader started successfully`

## Grafana Access

Grafana is exposed on port `3000` by default:

```
http://<your-vps-host>:3000
```

Admin credentials come from `.env`:
- `GRAFANA_ADMIN_USER`
- `GRAFANA_ADMIN_PASSWORD`

Defaults in `.env.example` are `admin` / `admin`. Change them before exposing Grafana publicly.
The deployed stack also syncs Grafana admin credentials from `.env` on container startup:
- `GRAFANA_ADMIN_PASSWORD` is reset from config on every start
- `GRAFANA_ADMIN_USER` is applied automatically when the current login is still `admin` or already matches the configured login

If you intentionally change the admin login later, restart Grafana after updating `.env`:

```
docker compose up -d grafana
```

## Grafana Verification

After `docker compose up -d --build`, check Grafana:

```
docker compose ps grafana
docker compose logs grafana
```

On first startup, Grafana should automatically provision:
- a PostgreSQL datasource named `Postgres`
- a dashboard folder named `T-Invest Trader`

Start with these operational dashboards:

- `Operator Overview` -- signal throughput, outcomes, and the latest paper portfolio
- `Paper Trading` -- virtual positions, realized PnL, and source/ticker attribution
- `Paper Tariff Comparison` -- counterfactual net PnL under T-Bank cost profiles
- `Medium-Term Paper Strategy` -- staircase, ATR, and hybrid virtual portfolios
- `Medium-Term Historical Replay` -- stored-history strategy/benchmark comparison
- `Market Activity Monitor` -- T-Bank candle volume and price spikes; observational only
- `Data Freshness & Pipeline Health` -- ingestion freshness and source errors
- `Signal Lifecycle` -- generation, filtering, delivery, and outcomes

Use these drill-down dashboards when investigating a source or pipeline stage:

- `Telegram Sentiment`, `Sentiment Observations`, `Fusion Inputs & Features`
- `Broker Events`, `CBR Events`, `MOEX Market History`
- `Combined Market Context`, `Pipeline Debugging · Raw Data Flow`
- `Signal Research · Sources, AI & Global Context`, `Macro Context Impact`

Every dashboard includes a `T-Invest dashboards` dropdown that preserves the
selected time range while navigating between views.

## Market Activity Monitor

The market activity monitor uses T-Bank minute candles for tracked instruments.
It writes candle observations and explainable volume/price spikes to Postgres;
it does not create signals, virtual positions, orders, or broker requests.

Enable it in `.env` only after the application revision with this module is
deployed:

```
TINVEST_MARKET_ACTIVITY_ENABLED=true
TINVEST_MARKET_ACTIVITY_POLL_INTERVAL_SECONDS=60
TINVEST_MARKET_ACTIVITY_CANDLE_INTERVAL=CANDLE_INTERVAL_1_MIN
TINVEST_MARKET_ACTIVITY_LOOKBACK_MINUTES=60
TINVEST_MARKET_ACTIVITY_BASELINE_CANDLES=20
TINVEST_MARKET_ACTIVITY_VOLUME_SPIKE_MULTIPLIER=3.0
TINVEST_MARKET_ACTIVITY_PRICE_CHANGE_SPIKE_PCT=0.01
TINVEST_MARKET_ACTIVITY_SESSION_FILTER_ENABLED=true
TINVEST_MARKET_ACTIVITY_SESSION_START_HOUR_MOSCOW=9
TINVEST_MARKET_ACTIVITY_SESSION_START_MINUTE_MOSCOW=50
TINVEST_MARKET_ACTIVITY_SESSION_END_HOUR_MOSCOW=18
TINVEST_MARKET_ACTIVITY_SESSION_END_MINUTE_MOSCOW=50
TINVEST_BACKGROUND_RUN_MARKET_ACTIVITY=true
```

The initial cycle backfills the requested candle lookback. Repeated cycles are
idempotent and only add unseen candle observations or spikes. Candle
observations remain complete for audit, while spike creation is restricted to
weekdays in the configured Moscow session by default.

To compare momentum and reversion after each spike without affecting signals
or trading, enable the local outcome resolver:

```
TINVEST_MARKET_ACTIVITY_OUTCOMES_ENABLED=true
TINVEST_MARKET_ACTIVITY_OUTCOMES_POLL_INTERVAL_SECONDS=60
TINVEST_MARKET_ACTIVITY_OUTCOMES_HORIZONS_MINUTES=5,15,60
TINVEST_MARKET_ACTIVITY_OUTCOMES_NEUTRAL_THRESHOLD_PCT=0.0005
TINVEST_MARKET_ACTIVITY_OUTCOMES_EOD_ENABLED=true
TINVEST_MARKET_ACTIVITY_OUTCOMES_EOD_HOUR_MOSCOW=23
TINVEST_MARKET_ACTIVITY_OUTCOMES_EOD_MINUTE_MOSCOW=50
TINVEST_MARKET_ACTIVITY_OUTCOMES_LOOKBACK_DAYS=30
TINVEST_MARKET_ACTIVITY_OUTCOMES_MAX_PRICE_DELAY_MINUTES=30
TINVEST_BACKGROUND_RUN_MARKET_ACTIVITY_OUTCOMES=true
```

The resolver reads only stored market-activity candles. Every configured
horizon has an independent backlog so a future `60m` or EOD target cannot
delay elapsed `5m` or `15m` outcomes. The bounded price delay tolerates sparse
minute candles without selecting an arbitrary much-later price. Inspect aggregate results in the
`Market Activity Monitor` dashboard or run:

```
python -m tinvest_trader.cli market-activity-outcomes
```

## Activity Paper Strategy

The activity paper strategy runs equal-capital momentum and reversion
experiments from new market-activity spikes. An optional third experiment
observes pure-volume spikes and enters at the first following closed candle
only when that candle confirms a configured minimum price direction. It stores
virtual positions and explainable enter/skip decisions only. It has no broker
client, order, or execution dependency.

An independent `volume-confirmed-v2` arm applies stricter score, volume-ratio,
confirmation, cooldown, and Moscow-calendar daily-entry gates. Run it alongside
v1 so historical v1 behavior and results remain an honest control group.

The configured horizon must also be enabled in the market-activity outcome
resolver. The confirmed-volume arm enters at the confirmation close and uses
the existing spike-horizon outcome price; its return is calculated from that
actual virtual entry price. Enable the experiment with conservative defaults:

```
TINVEST_ACTIVITY_PAPER_ENABLED=true
TINVEST_ACTIVITY_PAPER_POLL_INTERVAL_SECONDS=60
TINVEST_ACTIVITY_PAPER_MOMENTUM_NAME=activity-momentum-v1
TINVEST_ACTIVITY_PAPER_REVERSION_NAME=activity-reversion-v1
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_ENABLED=false
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_NAME=activity-volume-confirmed-v1
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMATION_MIN_MOVE_PCT=0.002
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMATION_MAX_DELAY_MINUTES=3
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_ENABLED=false
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_NAME=activity-volume-confirmed-v2
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MIN_SCORE=80
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MIN_VOLUME_RATIO=5
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MIN_MOVE_PCT=0.004
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MAX_DELAY_MINUTES=2
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_COOLDOWN_MINUTES=120
TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MAX_ENTRIES_PER_DAY=20
TINVEST_ACTIVITY_PAPER_HORIZON=15m
TINVEST_ACTIVITY_PAPER_INITIAL_CASH=1000000
TINVEST_ACTIVITY_PAPER_POSITION_FRACTION=0.02
TINVEST_ACTIVITY_PAPER_MAX_OPEN_POSITIONS=10
TINVEST_ACTIVITY_PAPER_MAX_OPEN_PER_TICKER=1
TINVEST_ACTIVITY_PAPER_COMMISSION_RATE=0.0005
TINVEST_ACTIVITY_PAPER_SLIPPAGE_RATE=0.0005
TINVEST_ACTIVITY_PAPER_MIN_SCORE=45
TINVEST_ACTIVITY_PAPER_ALLOWED_SEVERITIES=medium,high
TINVEST_ACTIVITY_PAPER_ALLOWED_SPIKE_TYPES=volume_price,price_momentum
TINVEST_ACTIVITY_PAPER_COOLDOWN_MINUTES=30
TINVEST_ACTIVITY_PAPER_MAX_CANDIDATE_AGE_MINUTES=10
TINVEST_ACTIVITY_PAPER_UNRESOLVED_EXPIRY_MINUTES=180
TINVEST_BACKGROUND_RUN_ACTIVITY_PAPER_STRATEGY=true
```

Open virtual positions that still have no valid outcome after the expiry
window move to `expired`. They release paper capacity without recording a
synthetic exit price, return, or PnL.

For signal outcome quotes, reject stale broker timestamps and refresh the
share catalog before the first runtime after upgrading:

```
TINVEST_QUOTE_SYNC_MAX_SOURCE_AGE_SECONDS=604800
docker compose exec -T app python -m tinvest_trader.cli sync-share-catalog
docker compose exec -T app python -m tinvest_trader.cli sync-quotes
```

Catalog sync selects one active, API-tradable `TQBR` instrument per ticker so
historical duplicate listings cannot replace current MOEX identifiers.

Inspect all enabled arms with the `Activity Paper Strategy` Grafana dashboard or:

```
python -m tinvest_trader.cli activity-paper-stats
```

## Medium-Term Paper Strategy

The medium-term experiment is a daily, long-only A/B/C comparison built only
from stored MOEX history. It never submits broker orders or creates broker stop
orders. A completed day produces a trend/breakout/volume decision, and an
eligible signal enters virtually at the next available daily open.

The three isolated portfolios compare:

- `staircase`: initial 2% stop, then +1 percentage point for every +2% gain
- `atr`: initial and trailing stop based on two average true ranges
- `hybrid`: ATR-aware initial stop, then breakeven and ATR trailing after +3%

All arms use the same conservative defaults: 0.5% virtual equity risk per
position, 20% maximum allocation, five concurrent positions, modeled round-trip
commission/slippage, and a 63-session maximum holding period. A gap below the
virtual stop exits at the next stored open rather than assuming the stop price.

Enable sufficient MOEX history before enabling the experiment:

```dotenv
TINVEST_MOEX_ENABLED=true
TINVEST_MOEX_HISTORY_ENABLED=true
TINVEST_MOEX_HISTORY_LOOKBACK_DAYS=1825
TINVEST_BACKGROUND_RUN_MOEX=true

TINVEST_MEDIUM_TERM_PAPER_ENABLED=true
TINVEST_MEDIUM_TERM_PAPER_POLL_INTERVAL_SECONDS=3600
TINVEST_MEDIUM_TERM_PAPER_TRACKED_TICKERS=SBER,GAZP,LKOH
TINVEST_MEDIUM_TERM_PAPER_RISK_PER_POSITION=0.005
TINVEST_MEDIUM_TERM_PAPER_MAX_POSITION_FRACTION=0.20
TINVEST_MEDIUM_TERM_PAPER_MAX_OPEN_POSITIONS=5
TINVEST_MEDIUM_TERM_PAPER_INITIAL_STOP_PCT=0.02
TINVEST_MEDIUM_TERM_PAPER_MAX_HOLDING_SESSIONS=63
TINVEST_BACKGROUND_RUN_MEDIUM_TERM_PAPER_STRATEGY=true
```

An empty `TINVEST_MEDIUM_TERM_PAPER_TRACKED_TICKERS` falls back to the tracked
instrument catalog. The initial MOEX backfill may take multiple cycles; verify
at least 51 complete daily bars per ticker before judging signal frequency.
Dividends are not credited to virtual PnL in this first research version.

Inspect the three arms in the `Medium-Term Paper Strategy` Grafana dashboard or:

```bash
docker compose exec -T app python -m tinvest_trader.cli medium-term-paper-stats
```

Run a named historical replay only after the required MOEX range is present:

```bash
docker compose exec -T app python -m tinvest_trader.cli medium-term-replay \
  --start 2021-01-01 \
  --end 2026-01-01 \
  --tickers SBER,GAZP,LKOH \
  --name medium-term-five-year-v1
```

Replay names are immutable. Use a new name when changing dates, tickers, costs,
or strategy settings. The replay performs no network calls and writes only
`medium_term_replay_*` research tables. Grafana compares daily net-liquidation
equity and mark-to-market drawdown with an equal-weight buy-and-hold benchmark.
The initial version does not credit dividends or adjust corporate actions, and
a replay over today's tracked ticker set has survivorship bias. Treat it as a
screening experiment rather than evidence for real-money execution.

To confirm the datasource is connected:
1. Log in to Grafana
2. Open `Connections` -> `Data sources`
3. Open `Postgres`
4. Verify it reports a successful connection

If Grafana is exposed on a public VPS, consider:
- restricting port `3000` via firewall
- placing Grafana behind a reverse proxy with HTTPS
- changing the default admin password immediately

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

## Shadow Paper Portfolio

The paper portfolio measures new delivered signals as virtual positions. It
does not submit broker orders and does not use the execution engine.

Enable it in `.env` after the signal, quote, and outcome pipelines are healthy:

```dotenv
TINVEST_PAPER_PORTFOLIO_ENABLED=true
TINVEST_PAPER_PORTFOLIO_NAME=shadow-v1
TINVEST_PAPER_PORTFOLIO_INITIAL_CASH=1000000
TINVEST_PAPER_PORTFOLIO_POSITION_FRACTION=0.10
TINVEST_PAPER_PORTFOLIO_MAX_OPEN_POSITIONS=5
TINVEST_PAPER_PORTFOLIO_COMMISSION_RATE=0.0005
TINVEST_PAPER_PORTFOLIO_SLIPPAGE_RATE=0.0005
TINVEST_PAPER_PORTFOLIO_UNRESOLVED_EXPIRY_MINUTES=180
TINVEST_BACKGROUND_RUN_PAPER_PORTFOLIO=true
```

Compare the same closed virtual positions under configurable T-Bank cost
profiles without changing stored trades:

```bash
python -m tinvest_trader.cli paper-tariff-comparison --days 30
```

The report and `Paper Tariff Comparison` Grafana dashboard separate broker
commission, slippage, and monthly subscription cost. Paid and fee-waived
Trader/Premium scenarios are shown independently. CLI assumptions are
configurable with `TINVEST_PAPER_TARIFF_*`; the provisioned dashboard states
its embedded assumptions explicitly. Update both when the broker's tariff
terms change. Subscription cost is charged once per active Moscow calendar
month in the combined account view.

Signal outcomes use the first quote near the configured evaluation target.
Keep the quote window bounded so a quote arriving days later cannot resolve an
old signal:

```dotenv
TINVEST_SIGNAL_RESOLUTION_EVAL_WINDOW_SECONDS=300
TINVEST_SIGNAL_RESOLUTION_MAX_QUOTE_DELAY_SECONDS=900
```

Paper positions whose signals remain unresolved past the expiry are marked
`expired`; they do not contribute synthetic PnL.

Use a new `TINVEST_PAPER_PORTFOLIO_NAME` to start an independent experiment.
The first cycle stores the portfolio start time, so historical signals are not
included. Inspect realized PnL and virtual exposure with:

```bash
docker compose exec -T app python -m tinvest_trader.cli paper-portfolio-stats
```

## Useful Commands

| Command | Description |
|---------|-------------|
| `docker compose ps` | Show service status |
| `docker compose logs -f app` | Follow app logs |
| `docker compose logs -f postgres` | Follow DB logs |
| `docker compose logs -f grafana` | Follow Grafana logs |
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
UNION ALL SELECT 'broker_event_raw', count(*) FROM broker_event_raw
UNION ALL SELECT 'broker_event_features', count(*) FROM broker_event_features
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
