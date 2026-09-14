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

### Stricter Momentum and Confirmed-Volume Entries

After deploying the code, opt in using
`TINVEST_ACTIVITY_PAPER_STRICT_ENTRIES_ENABLED=true` and recreate the app
container. Default is `false`, preserving existing entry rules. This flag only
affects new momentum and volume-confirmed-v2 entries; reversion, original
confirmed-volume v1, and the resolution of existing positions retain their rules.
The v2 arm must still be enabled separately. Do not re-enable a disabled v1 arm.

The strict profile uses the following initial research parameters:

```
TINVEST_ACTIVITY_PAPER_STRICT_ENTRIES_ENABLED=true
TINVEST_ACTIVITY_PAPER_STRICT_MIN_SCORE=80
TINVEST_ACTIVITY_PAPER_STRICT_MIN_VOLUME_RATIO=10
TINVEST_ACTIVITY_PAPER_STRICT_MAX_SPIKE_MOVE_PCT=0.02
TINVEST_ACTIVITY_PAPER_STRICT_MIN_CONFIRMATION_MOVE_PCT=0.004
TINVEST_ACTIVITY_PAPER_STRICT_MAX_CONFIRMATION_MOVE_PCT=0.01
TINVEST_ACTIVITY_PAPER_STRICT_MIN_COST_MULTIPLE=2
TINVEST_ACTIVITY_PAPER_STRICT_CONFIRMATION_MAX_DELAY_MINUTES=2
TINVEST_ACTIVITY_PAPER_STRICT_MAX_ENTRY_AGE_MINUTES=2
TINVEST_ACTIVITY_PAPER_STRICT_COOLDOWN_MINUTES=180
TINVEST_ACTIVITY_PAPER_STRICT_MAX_ENTRIES_PER_DAY=5
```

Both arms require high severity and a non-flat spike with volume ratio >= 10.
Momentum also requires a `volume_price` spike. Spikes larger than 2% are skipped.
The first subsequent stored minute candle must continue the spike direction by
0.4%-1%, and that observed move must cover twice the modeled round-trip commission
and slippage. This is a filter on an already observed move, not a forecast of
remaining profit. Confirmation must arrive within two candle minutes; entries
use its close price and close time, only once that minute is complete. Entries
older than two minutes, from a previous Moscow date, or at/after the existing
outcome horizon are skipped. Non-minute intervals are rejected in this profile.
Use a minute-based outcome horizon such as `15m`; `eod` is not supported by the
strict-entry profile because confirmation must precede a known exit deadline.
The existing v2 thresholds remain binding when they are more restrictive.

Each targeted portfolio gets at most five entries per Moscow calendar day and
a three-hour cooldown per ticker, including entries before a same-day restart.
Disabling the flag restores the original entry rules. Skipped candidates are
terminal decisions and are not replayed by toggling the flag.

Strict entries record `strict_eligible` in `activity_paper_decisions`. The CLI and
the Grafana **Long / Short by Entry Policy** panel separate them from legacy
entries; rows missing an entry decision are shown as `unknown`. Grafana uses the
selected entry-time range for this comparison; CLI shows all-time cohorts.
Changing thresholds later within the strict profile requires a separate
portfolio name (or a recorded rollout date) for a clean comparison.

The existing activity arms already simulate both directions: `up` is long and
`down` is short. Momentum/confirmed-volume follow the observed direction;
reversion opposes it. These are virtual directional experiments, with modeled
commission/slippage but no borrow availability check or overnight borrowing fee.
Use the directional breakdown to evaluate a short-only hypothesis before adding
another portfolio. Neither thresholds nor one profitable day establish an edge.

### Causal Reversion v2 (Virtual Only)

`reversion-v2` is an opt-in execution experiment, not a new signal strategy.
It reuses the reversion-v1 direction, quality gates, sizing, cooldown and exposure
limits. It has a separate portfolio and never rewrites v1 positions or PnL.
The strict-entry flag still applies only to momentum and confirmed-volume v2.

After deploying the code and initializing the additive schema, configure:

```dotenv
TINVEST_ACTIVITY_PAPER_REVERSION_V2_ENABLED=true
TINVEST_ACTIVITY_PAPER_REVERSION_V2_NAME=activity-reversion-v2
TINVEST_ACTIVITY_PAPER_REVERSION_V2_QUOTE_WAIT_SECONDS=120
TINVEST_ACTIVITY_PAPER_REVERSION_V2_MAX_QUOTE_AGE_SECONDS=30
```

The default is disabled. Existing activity-paper/background prerequisites still
apply, and quote ingestion must be running (`TINVEST_QUOTE_SYNC_ENABLED=true`
and `TINVEST_BACKGROUND_RUN_QUOTE_SYNC=true`, or an equivalent quote producer).
This milestone does not increase quote polling frequency or modify live trading.
Use a minute horizon such as `TINVEST_ACTIVITY_PAPER_HORIZON=15m`; v2 rejects `eod`.
Recreate the app container after changing its environment.

The execution contract is:

1. A qualified spike from a closed one-minute candle creates a durable request in
   `activity_paper_execution`, reserving virtual cash and position slots. The
   database timestamps the decision; incomplete or non-minute candles are rejected.
2. Entry uses the first stored quote whose source time is after the decision and
   whose reception time is within the request deadline. Quote age at reception
   must not exceed the configured maximum. Future/invalid prices cannot fill.
3. The quote reception timestamp becomes `entry_time`. The older source timestamp
   and eventual worker-processing timestamp are stored separately for diagnosis.
4. Exit is scheduled from this entry time plus the portfolio horizon. It uses the
   same bounded fresh-quote rule after that target, not the spike outcome table.
5. Missing entry quotes cancel the request without a position; missing exit quotes
   mark the virtual position expired with unknown PnL. There is no candle fallback.

Pending requests survive restart. Quotes received inside the stored window can be
processed later; quotes outside that window cannot rescue a timed-out request.
Disabling v2 pauses its processing, including existing requests/positions, without
touching the other portfolios. Re-enabling resumes persisted requests. Timeouts,
cost rates and exit deadlines are snapshotted, so config changes do not rewrite
existing requests. A new experiment name is recommended when changing parameters.

Inspect `python -m tinvest_trader.cli activity-paper-stats`: when enabled, it includes
v2 plus an all-time v1/v2 report of reservations, cancellations, fill latency, entry
and exit turnover, costs, gross PnL and net PnL. Grafana's existing **Activity Paper
Strategy** dashboard includes **Execution Audit**, **Entry Cohort / Turnover and
Costs**, and **Causal Requests / Reservations and Timeouts** panels. Filter both
portfolios over the same observation period; their accepted trades need not match.
Entry-cohort results include eventual outcomes, not only exits inside the range.

These are last-price simulations, not guaranteed bid/ask executions. Costs remain
the configured commission and slippage model; lot rounding, borrow availability,
borrowing fees and market depth are not modeled. Do not interpret simulated PnL
as achievable live returns. Audit prices and timestamps are copied into permanent
paper records and do not depend on retained quote rows or a broker connection.

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
TINVEST_MOEX_CORPORATE_ACTIONS_ENABLED=true
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
Forward paper positions remain price-return experiments and do not credit
dividends. Historical replay separately includes persisted dividend income.

Inspect the three arms in the `Medium-Term Paper Strategy` Grafana dashboard or:

```bash
docker compose exec -T app python -m tinvest_trader.cli medium-term-paper-stats
```

Run a named historical replay only after the required MOEX range is present:

```dotenv
TINVEST_BROKER_EVENTS_DIVIDENDS_LOOKBACK_DAYS=3650
```

The broker-event tracked FIGI set must cover every replay ticker. Run a
broker-event ingestion pass after increasing the lookback so historical
`GetDividends` rows exist before the immutable replay is created.

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
equity and mark-to-market drawdown with an equal-weight total-return benchmark.
Replay adjusts OHLCV history with persisted MOEX split ratios, credits persisted
RUB `GetDividends` events to eligible strategy positions, and reinvests those
dividends in the equal-weight total-return benchmark. It performs no network
calls, so run MOEX and broker-event ingestion before creating an immutable run.
Other corporate actions, taxes, delistings, ticker migrations, and currency
conversion are not modeled. A replay over today's tracked ticker set still has
survivorship bias; treat it as screening, not evidence for real-money execution.

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

### Disk Capacity and Persistence Health

Docker logs are rotated at 20 MB per file, three files per service (approximately
60 MB each). The setting applies to recreated containers, not existing ones.
Before rollout, export any incident logs that must be preserved to external
storage. `docker compose up -d --build` recreates services whose logging settings
changed, including Postgres; plan for a brief interruption. Named volumes remain
intact. Never use `down -v`, volume pruning, or log-file truncation to recover space.

Enable the independent storage check after deploying the schema and code:

```dotenv
TINVEST_STORAGE_HEALTH_ENABLED=true
TINVEST_STORAGE_HEALTH_DISK_PATH=/var/lib/tinvest-postgres
TINVEST_STORAGE_HEALTH_WARNING_FREE_PERCENT=20
TINVEST_STORAGE_HEALTH_CRITICAL_FREE_PERCENT=10
TINVEST_STORAGE_HEALTH_MIN_FREE_BYTES=2147483648
TINVEST_STORAGE_HEALTH_FRESHNESS_SECONDS=900
TINVEST_STORAGE_HEALTH_RECENT_ERROR_SECONDS=300
```

Default is off; disabling the flag restores the heartbeat-only Docker check.
The app receives a **read-only** mount of `pgdata` to measure the actual database
filesystem, including when Docker volumes live on a different disk. No database
files are read or modified through this mount. A missing mount is an error, not
a silent fallback to the app filesystem.

Every 60 seconds Docker starts a separate short-lived probe. It uses a separate
Postgres connection with connect/statement/lock timeouts, so a stuck runner or
exhausted application pool cannot stop the probe. It checks disk headroom,
recent application DB errors, enabled pipeline freshness, and commits an upsert
to the single-row `storage_health_snapshot` table. No market API calls or trading
operations are performed. A successful SELECT does not clear a recent application
DB failure. Error markers keep only a timestamp and exception class, not secrets.

Warnings start at 20% free. At 10% free, less than 2 GiB available, failed probes,
recent DB errors, or stale expected pipelines, the check fails. Docker reports
`unhealthy` after three failed checks; **restart: unless-stopped does not restart
an unhealthy running process**. This adds visibility, not automated recovery or
Telegram notifications, and it does not pause or change trading decisions.

Fusion freshness is checked whenever its persistent background task is enabled.
Quotes/activity use configured Moscow weekday session hours plus a grace period;
the check is not a holiday calendar. Quiet/no-tracked-instrument configurations
may report missing data intentionally. Quotes measure successful fetching, not
broker price freshness; activity checks candle time from the last 1000 inserts.
Thresholds are at least three configured polling intervals. A caught batch error
now reports zero inserted quotes because the entire transaction rolled back.

Inspect without initializing the application container or making any writes:

```bash
docker compose exec -T app python -m tinvest_trader.cli storage-health
docker inspect --format '{{json .State.Health}}' "$(docker compose ps -q app)"
```

`storage-health` runs even when the periodic flag is off; exit code 1 means
critical, 0 means OK or warning. Missing DB configuration is reported explicitly.
The `Data Freshness & Pipeline Health` dashboard includes live storage status,
largest tables, and last persisted-data freshness. A snapshot older than three
minutes is shown as stale, never as a previous green result. If Postgres is
unreachable, inspect Docker health output; Grafana cannot query a failed database.

The report uses physical relation sizes and **planner row estimates**, not
expensive full-table counts or resettable activity counters. A post-restart
`n_live_tup` counter is not sufficient evidence of bloat. Tables and indexes
continue growing with retained history: the monitor does not delete or compact
anything. Trade history, outcomes, raw inputs, and replay data remain unchanged.

When headroom falls, first take and verify a backup on another disk, then choose
an explicit archival/retention policy or expand storage. Do not run `VACUUM FULL`
on a large live table without adequate temporary disk space and a maintenance
window. Ordinary VACUUM reuses space internally but usually does not shrink
files. Fusion polling frequency controls future growth, but changing it also
changes sampling and must be a separately recorded experiment/config decision.

### Three-Month Dense History Retention

Retention applies **only** to `fused_signal_features.recorded_at` and
`market_quotes.fetched_at`, using three calendar months rather than 90 days.
Fusion rows referenced by saved signals survive. Entry-binding and outcome-window
quotes for all stored predictions survive, as do latest catalog FIGI quotes.
Protected records can therefore be older than the cutoff.

Trades, signals, outcomes, raw source events, activity candles/spikes, MOEX daily
history, corporate actions and replay data are not deleted. Three months of
daily prices is insufficient for medium-term warmup and historical replay.
Do not run new historical signal jobs during the initial cleanup.

First take and validate an external backup, preferably with a test restore.
Transferring a full DB dump off the VPS requires the owner's explicit consent:
it may contain private messages and account data. `.local-backups/` is gitignored;
use restrictive permissions. Build the small retention indexes explicitly:

```bash
docker compose exec -T postgres psql -U tinvest -d tinvest -v ON_ERROR_STOP=1 \
  < deploy/storage-retention-indexes.sql
docker compose exec -T app python -m tinvest_trader.cli storage-retention
```

The index script uses CONCURRENTLY and must not run inside a transaction.
Interrupted invalid indexes need inspection/repair before retrying: IF NOT EXISTS
does not repair them. Retention refuses to delete without valid, ready indexes.
The CLI previews a bounded batch by default, without starting any pipelines.

After backup and initial maintenance, enable and recreate app:

```dotenv
TINVEST_STORAGE_RETENTION_ENABLED=true
TINVEST_STORAGE_RETENTION_MONTHS=3
TINVEST_STORAGE_RETENTION_INTERVAL_SECONDS=3600
TINVEST_STORAGE_RETENTION_BATCH_SIZE=10000
TINVEST_STORAGE_RETENTION_BATCHES_PER_CYCLE=5
```

Default is off. With background execution enabled, the first run waits one
interval after startup. Each cycle deletes at most five 10,000-row batches per
table, stopping further batches after a 20-second soft budget; each statement
has a 5-second timeout. A session advisory lock prevents overlapping CLI/runner
maintenance. Each batch commits its deletion and counters atomically. Failures
are logged and other pipelines continue. Missing indexes and exhausted budgets
are warnings, not successful deletion. Disabling the flag stops future deletion,
but cannot restore data already removed.

`storage-retention --apply` runs one explicit bounded cycle and also requires
the flag. Grafana shows `storage_retention_state` cutoff, last run and cumulative
deletion counts. Missing last_run means no cycle has committed. Monitor backlog
rather than increasing limits until maintenance stalls ingestion.

The initial multi-million-row backlog needs a separate controlled maintenance
window. Ordinary DELETE frees reusable database pages, not necessarily filesystem
space. Compact smaller tables first and verify headroom for the surviving Fusion
heap, rebuilt indexes, temporary files, WAL and a safety reserve before any
`VACUUM FULL`. Never truncate tables or change stored financial outcomes.

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
