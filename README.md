# T-Invest-trader

Automated trading signal platform for the Moscow Exchange, built on the T-Bank (Tinkoff Invest) API. Ingests multi-source market data, generates and enriches signals with AI analysis, delivers them via Telegram, and tracks outcomes with full observability.

## Overview

T-Invest-trader solves the problem of turning noisy market signals into structured, measurable trading decisions. It combines:

- **Multi-source ingestion** -- Telegram channels, broker events, MOEX data, CBR macro, global market indices
- **AI-powered enrichment** -- Claude-based signal analysis with structured confidence scoring
- **Shadow experimentation** -- source weighting, AI gating, and global context alignment measured in parallel without affecting execution
- **Full observability** -- Grafana dashboards, automated alerting, daily operator digest

The system is designed for transparency: every signal decision is traceable, every rejection is explained, and every shadow experiment is measurable.

## Architecture

The system follows a strict five-layer pipeline:

```
INGESTION -> ENRICHMENT -> DECISION -> DELIVERY -> ANALYTICS
```

### Ingestion

Fetches and normalizes raw data from external sources into dedicated tables. Each source has its own pipeline and failure boundary.

- Telegram sentiment (MarketTwits, other channels via Telethon)
- Broker events (dividends, reports, insider deals via T-Bank API)
- MOEX ISS (security metadata, daily market history)
- CBR (Central Bank RSS feeds for macro events)
- Global context (financialjuice, oilprice, cointelegraph via Telegram)
- Global market data (S&P 500, NASDAQ, VIX, Brent, DXY via Yahoo Finance)
- Quote sync (last prices for tracked instruments via T-Bank API)

### Enrichment

Adds derived fields to signals. All enrichment is additive -- it never modifies the original signal. Shadow enrichment is stored but does not affect decisions.

- AI signal analysis (Claude API -- confidence, actionability, bias)
- Source attribution (per-channel historical win rate and EV)
- Source weighting (shadow -- confidence multiplier based on source performance)
- Global context alignment (shadow -- aligned/against/neutral classification)
- AI gating (shadow -- ALLOW/CAUTION/BLOCK decision)

### Decision

The only layer that gates signal flow. Three sequential checks:

- **Market binding** -- resolves ticker to instrument (FIGI). Rejects ambiguous or missing matches.
- **Calibration** -- filters by confidence threshold, per-ticker win rate, EV, signal type.
- **Execution safety** -- checks market status and time to close.

### Delivery

Pure transport via Telegram Bot API. Formats signals with severity indicators, inline action buttons, and AI analysis snapshots. No business logic.

### Analytics

Read-only measurement layer. Resolves signal outcomes against market quotes, computes performance metrics, generates reports and digests.

## Core Features

### Ingestion

| Source | Data | Interval |
|--------|------|----------|
| Telegram channels | Sentiment per ticker mention | 5 min |
| T-Bank broker API | Dividends, reports, insider deals | 30 min |
| MOEX ISS | Security metadata, 90-day price history | 1 hour |
| CBR RSS | Central Bank macro events | 1 hour |
| Global Telegram | Risk sentiment, oil, crypto context | 2 min |
| Yahoo Finance | S&P 500, NASDAQ, VIX, Brent, DXY prices | 5 min |
| T-Bank quotes | Last prices for tracked instruments | 1 min |

### Enrichment

- **AI analysis** -- on-demand Claude API call per signal. Returns structured fields: confidence (LOW/MEDIUM/HIGH), actionability (CONSIDER/WATCH/WEAK), bias assessment. Cached to avoid duplicate calls.
- **Source attribution** -- historical win rate and EV per Telegram source channel.
- **Source weighting** (shadow) -- multiplicative confidence adjustment based on source track record. Range [0.5, 1.5].
- **Global context alignment** (shadow) -- classifies signal direction vs. recent global context events. Adjusts confidence +0.05 (aligned) or -0.10 (against).
- **AI gating** (shadow) -- deterministic ALLOW/CAUTION/BLOCK decision from AI analysis fields.

### Decision Pipeline

| Gate | Rejects When | Rejection Reason |
|------|-------------|-----------------|
| Market binding | No instrument match, ambiguous candidates | `no_match`, `ambiguous`, `placeholder_figi` |
| Calibration | Low confidence, negative EV, type disabled | `low_confidence`, `negative_ev`, `type_disabled` |
| Execution safety | Market closed, too close to close | `market_closed`, `too_late_to_execute` |

### Delivery

Signals are sent via Telegram Bot API with:

- Severity classification (HIGH / MEDIUM / LOW)
- Inline keyboard buttons for interactive exploration
- AI agreement snapshot (when available)
- Ticker and signal-type performance stats

### Observability

- **4 Grafana dashboards** covering operator overview, pipeline funnel, analytics, and infrastructure health
- **Automated alerting** with per-key cooldown and dedup (signal gaps, win rate drops, data ingestion delays)
- **Daily digest** -- once-per-day Telegram summary of 24h performance, top sources/tickers, shadow experiment results

## Signal Lifecycle

```
1. CREATED      Signal prediction generated (ticker, direction, confidence, source)
2. ENRICHED     Shadow fields computed (source weight, global alignment, AI gating)
3. FILTERED     Decision gates applied:
                  -> rejected_calibration (low confidence, negative EV, etc.)
                  -> rejected_binding (no instrument match)
                  -> rejected_safety (market closed)
4. DELIVERED    Formatted and sent to operator via Telegram
5. RESOLVED     Outcome determined from market quotes (win / loss / neutral + return %)
6. ANALYZED     Performance aggregated in dashboards, reports, and digest
```

Every signal carries its full history: `pipeline_stage`, `rejection_reason`, `outcome_label`, `return_pct`, and all shadow fields.

## Shadow Experiments

Shadow experiments compute and store alternative decision signals **without affecting execution**. They exist to answer specific questions before enabling new features.

### Source Weighting

**Question:** Do historically better-performing sources deserve higher confidence?

Computes a `source_weight` multiplier (0.5--1.5) based on the source channel's historical win rate and EV. Stores `weighted_confidence = confidence * source_weight` alongside the original.

### AI Gating

**Question:** Would blocking signals that AI flags as low-confidence improve outcomes?

Deterministic rules: BLOCK if AI confidence is LOW or actionability is WEAK. ALLOW if AI confidence is HIGH and actionability is CONSIDER. CAUTION for everything else.

### Global Context Alignment

**Question:** Does trading with the macro trend improve win rates?

Classifies each signal as aligned, against, or neutral relative to recent global context events (risk sentiment, oil prices, crypto). Computes adjusted confidence.

**All shadow data is visible in Grafana and the daily digest.** When an experiment shows consistent positive impact, it can be promoted to an active decision gate.

## Telegram Bot

### Commands

| Command | Description |
|---------|-------------|
| `/last_signals [N]` | Show N most recent signals (default 5) |
| `/signal <id>` | Detailed view of a specific signal |
| `/ai <id>` | Request or show AI analysis for a signal |
| `/stats` | Overall system statistics (win rate, avg return, top source) |
| `/help` | List available commands |

### Interactive Buttons

Each delivered signal includes inline buttons:

- **Details** -- full signal metadata and outcome
- **AI** -- trigger on-demand Claude analysis (cached)
- **Stats** -- ticker and source performance stats

## Configuration

All features are controlled via environment variables. Every feature defaults to OFF and is toggled with `TINVEST_<FEATURE>_ENABLED=true`.

### Categories

| Category | Key Variables | Purpose |
|----------|--------------|---------|
| Database | `POSTGRES_DSN` | PostgreSQL connection |
| Broker | `TINVEST_BROKER_TOKEN`, `TINVEST_ACCOUNT_ID` | T-Bank API access |
| Telegram ingestion | `TINVEST_SENTIMENT_ENABLED`, `TINVEST_SENTIMENT_CHANNELS` | Source channel list |
| Global context | `TINVEST_GLOBAL_CONTEXT_ENABLED`, `TINVEST_GLOBAL_CONTEXT_CHANNELS` | Global signal sources |
| Global market data | `TINVEST_GLOBAL_MARKET_DATA_ENABLED` | Index/commodity prices |
| Signal delivery | `TINVEST_SIGNAL_DELIVERY_ENABLED`, `TINVEST_BOT_TOKEN`, `TINVEST_CHAT_ID` | Telegram bot |
| AI analysis | `TINVEST_ANTHROPIC_API_KEY`, `TINVEST_AI_MODEL` | Claude API |
| Calibration | `TINVEST_SIGNAL_CALIBRATION_ENABLED`, `TINVEST_MIN_CONFIDENCE` | Decision thresholds |
| Alerting | `TINVEST_ALERTING_ENABLED` | Operator alerts |
| Daily digest | `TINVEST_DAILY_DIGEST_ENABLED`, `TINVEST_DAILY_DIGEST_HOUR` | Daily summary |
| Background | `TINVEST_BACKGROUND_ENABLED` | Background runner |

## Running the System

### Docker (production)

```bash
# Configure
cp .env.example .env  # edit with your credentials

# Start all services
docker compose up -d

# Check logs
docker compose logs app --tail=50
```

Three containers: `postgres` (database), `app` (signal platform), `grafana` (dashboards on port 3000).

### Background Runner

The app runs a single background thread that schedules all periodic tasks: ingestion, delivery, alerting, and digest. Each task has its own interval and failure boundary -- one failing task does not crash the loop.

### CLI

The CLI provides operator commands for inspection, debugging, and one-shot operations:

```bash
# System status
docker compose exec app python -m tinvest_trader.cli status
docker compose exec app python -m tinvest_trader.cli db-summary

# Signal analysis
docker compose exec app python -m tinvest_trader.cli signal-stats
docker compose exec app python -m tinvest_trader.cli signal-divergence-report
docker compose exec app python -m tinvest_trader.cli telegram-source-report

# Shadow experiment reports
docker compose exec app python -m tinvest_trader.cli source-weighting-report
docker compose exec app python -m tinvest_trader.cli ai-gating-report
docker compose exec app python -m tinvest_trader.cli ai-divergence-report

# Manual operations
docker compose exec app python -m tinvest_trader.cli deliver-signals
docker compose exec app python -m tinvest_trader.cli send-daily-digest --send
docker compose exec app python -m tinvest_trader.cli check-alerts --dry-run
```

## Observability

### Grafana Dashboards

4 dashboards organized by purpose. See [docs/DASHBOARDS.md](docs/DASHBOARDS.md) for detailed panel descriptions and usage guide.

| Dashboard | Purpose |
|-----------|---------|
| Operator Overview | Daily operational health at a glance |
| Signal Pipeline Funnel | Where signals are lost and why |
| Analytics: AI / Source / Global | Shadow experiment performance |
| Data & Infra Health | Ingestion freshness and system health |

#### Operator Overview

![Operator Overview](docs/images/operator_overview.png)

#### Signal Pipeline Funnel

![Signal Pipeline Funnel](docs/images/signal_pipeline_funnel.png)

#### Analytics

![Analytics](docs/images/analytics.png)

#### Data & Infrastructure Health

![Data & Infrastructure Health](docs/images/data_infra_health.png)

### Alerting

Automated health checks with Telegram delivery and per-key cooldown:

- **signal_gap** -- no new signals for N minutes
- **pending_signals_high** -- too many unresolved signals
- **win_rate_low** -- 7-day win rate below threshold
- **telegram_gap** -- Telegram ingestion stale
- **quote_gap** -- quote sync stale
- **global_context_gap** -- global context ingestion stale

### Daily Digest

Once-per-day Telegram summary (default 20:00 UTC):

- Signal counts and delivery rate
- Win rate and average return
- Rejection breakdown (calibration, binding, safety)
- Top sources and tickers by EV
- AI agreement rate
- Shadow experiment deltas (weighting, gating, alignment)
- Best and worst signal of the day

## Architecture Guardrails

The system enforces strict architectural boundaries documented in [SYSTEM_GUARDRAILS.md](SYSTEM_GUARDRAILS.md). Key principles:

- **Pipeline boundaries** -- each layer has explicit allowed and forbidden operations
- **Shadow-first rule** -- new features affecting signal quality must start in shadow mode
- **Feature flag discipline** -- every feature toggleable via env var, default OFF
- **Deterministic logic** -- all scoring and gating is deterministic given same inputs
- **Observability-first** -- if you cannot measure impact, do not ship
- **Failure isolation** -- one failing component must not crash the system
