# Shared Project Context

Project: tinvest_trader

## Product boundaries
- This project is an automated trading system built around T-Invest API.
- MVP rollout path is: paper trading -> sandbox -> guarded production.
- Early work should prioritize market data, risk, execution correctness, reconciliation, and observability.
- Do not expand into multi-strategy or multi-asset complexity too early.

## Architecture expectations
- `PROJECT_CONTEXT.md` and `SYSTEM_MAP.md` are the main architectural references.
- `app/` composes the system and starts runtime loops.
- `domain/` stays pure.
- `strategy/` stays broker-agnostic and deterministic.
- `risk/` is authoritative.
- `execution/` owns broker order lifecycle behavior.
- `portfolio/` stores local working state.
- `infra/` owns broker protocol details, storage adapters, and logging.
- Broker state is the source of truth; local state is a cached working model.

## Safety expectations
- No silent continuation when execution state is uncertain.
- No unsafe retry that may duplicate orders.
- Every order path must remain idempotency-aware.
- Fail closed on stale or malformed market/broker data.
- Do not bypass reconciliation after restart or ambiguous broker responses.
- Do not weaken risk checks for convenience.

## Quality expectations
- Prefer the smallest safe step first.
- Prefer explicit models and state transitions.
- Keep logs and docs aligned with behavior.
- Keep changes easy to review.
- Add tests for risky paths.

## Default work priority
1. correctness
2. recoverability
3. auditability
4. operator safety
5. strategy sophistication

## Verification guidance
Prefer project-level verification commands when they exist. If they do not exist yet, use the smallest honest command set needed for the changed area and report exactly what was run.
