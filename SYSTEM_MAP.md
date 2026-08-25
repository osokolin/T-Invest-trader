# SYSTEM_MAP.md

## Purpose

System structure and module boundaries.

---

## High-Level Flow

Market Data → Strategy → Risk → Execution → Portfolio → Reconciliation → Journal

---

## Modules

- app/
- domain/
- market_data/
- strategy/
- risk/
- execution/
- portfolio/
- infra/
- services/
- tests/

---

## Key Rules

- broker state is source of truth
- strategy is pure
- risk is authoritative
- execution is idempotent-aware

---

## Runtime Flow

1. Collect data
2. Generate signal
3. Apply risk
4. Execute order
5. Update state
6. Reconcile

## Shadow Market Activity Flow

1. T-Bank closed candles → `MarketActivityService`
2. Observations → `market_activity_observations`
3. Explainable spikes → `market_activity_spikes`
4. Stored future candles → `MarketActivityOutcomeService`
5. Momentum/reversion results → `market_activity_spike_outcomes`
6. Read-only inspection → CLI and Grafana

Optional shadow experiment after step 5:

7. Qualified fresh spike → `ActivityPaperStrategyService`
8. Momentum/reversion or confirmed-volume decisions → `activity_paper_decisions`
9. Virtual positions and net PnL → `activity_paper_positions`
10. A/B/C comparison → CLI and Grafana

The shadow activity flow is isolated from signal generation, paper portfolio,
execution, and order placement.

Outcome horizons are scheduled independently so a future long or session-close
target cannot starve already elapsed short horizons.

---

## Safety Critical Areas

- order placement
- retry logic
- reconciliation
- restart behavior

---

## Final Principle

Reliable execution > complex strategy
