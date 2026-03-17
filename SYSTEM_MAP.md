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

---

## Safety Critical Areas

- order placement
- retry logic
- reconciliation
- restart behavior

---

## Final Principle

Reliable execution > complex strategy
