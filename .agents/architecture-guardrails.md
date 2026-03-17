# Architecture Guardrails

This file defines non-negotiable architecture rules for `tinvest_trader`.

All agents must read this before proposing, implementing, reviewing, or committing changes.

## 1. Core dependency direction

The dependency direction must remain:

App / Services
→ Domain workflows
→ Infra adapters / Persistence
→ External broker APIs / Database

A more practical module reading is:

`app/` → `services/` → `strategy/`, `risk/`, `execution/`, `portfolio/` → `infra/`

`domain/` should be reusable by the inner modules and should not depend on infra or app.

## 2. Module responsibilities

### `app/`
- bootstrap only
- config loading
- dependency wiring
- runtime startup/shutdown
- no business logic

### `strategy/`
- signal generation only
- no broker SDK calls
- no persistence calls
- deterministic and testable

### `risk/`
- risk checks and limits
- authoritative approve/reject/adjust decisions
- no broker transport details

### `execution/`
- order lifecycle logic
- idempotency handling
- retry policy
- reconciliation orchestration
- no presentation logic

### `portfolio/`
- local working state
- persistence-facing state operations
- no direct broker protocol logic

### `infra/`
- T-Invest API client wrappers
- DTO mapping
- storage adapters
- logging plumbing
- no trading policy logic

### `domain/`
- enums, models, events, typed records
- no infra dependencies

## 3. Thin-surface rule

Keep surfaces thin.

- CLI or app entrypoints must not contain business logic.
- Presentation/formatting must not leak into execution or risk logic.
- Avoid turning services into giant god-objects.

## 4. Broker integration rule

All direct T-Invest API access must live in `infra/tbank/*` or another clearly designated adapter boundary.

Not allowed:
- broker SDK calls inside `strategy/`
- broker SDK calls inside `risk/`
- broker SDK calls directly from app bootstrap

## 5. State authority rule

Source of truth priority:
1. broker order/position state
2. reconciled local persisted state
3. in-memory runtime cache

On conflict, broker truth wins.

## 6. Reconciliation rule

Reconciliation is mandatory after:
- restart
- ambiguous timeout during order placement/cancel
- partial failure during execution
- detected local/broker mismatch

Do not resume trading blindly when state confidence is low.

## 7. Idempotency rule

Order placement paths must remain idempotency-aware.

- every order should have a stable client-side identity
- retries must be explicit and safe
- duplicate submission risk is a blocking issue

## 8. Fail-closed rule

For market data and broker state:
- stale data must fail closed
- malformed data must fail closed
- unavailable critical data must fail closed
- no fabricated fallback values
- no hidden unsafe downgrade

## 9. New-layer rule

Do not add a new architectural layer unless there is a concrete boundary problem that existing modules cannot solve cleanly.

## 10. Review rule

Changes must be flagged if they do any of the following:
- broker calls outside infra/adapters
- strategy depending on broker DTOs
- risk logic scattered across unrelated modules
- execution silently ignoring unknown broker states
- persistence mixed into unrelated business logic
- restart behavior without reconciliation
- duplicate-order risk introduced by retries
