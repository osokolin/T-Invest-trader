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
8. Momentum/reversion or confirmed-volume v1/v2 decisions → `activity_paper_decisions`
9. Virtual positions and net PnL → `activity_paper_positions`
10. A/B/C/D comparison → CLI and Grafana

The shadow activity flow is isolated from signal generation, paper portfolio,
execution, and order placement.

Outcome horizons are scheduled independently so a future long or session-close
target cannot starve already elapsed short horizons.
Sparse candles use a bounded configurable delay; positions that still cannot be
resolved expire as virtual-only records without synthetic returns.

## Medium-Term Paper Flow

1. MOEX daily history → `moex_market_history`
2. Completed trend/breakout/volume setup → `MediumTermPaperStrategyService`
3. Next available daily open → virtual entry in `medium_term_paper_positions`
4. Staircase, ATR, or hybrid stop update → `medium_term_stop_history`
5. Skip/enter rationale → `medium_term_paper_decisions`
6. A/B/C performance → CLI and Grafana

This flow reads persisted market data and writes dedicated virtual research
records only. It has no broker client, execution engine, order-intent, or
reconciliation path.

---

## Safety Critical Areas

- order placement
- retry logic
- reconciliation
- restart behavior

---

## Final Principle

Reliable execution > complex strategy
