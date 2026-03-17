# Agent Roles

This repository uses a compact multi-agent Codex workflow designed for a trading system MVP.

## Active agents
- Planner — chooses the smallest useful next step.
- Architect — protects module boundaries and prevents design drift.
- Implementer — makes the approved change and nothing else.
- Tester — runs verification and reports exact results.
- Reviewer — reviews correctness, risk controls, and maintainability.
- Committer — creates a commit only when all checks are green.

## Why this set
This is the smallest loop that still gives:
- scoped planning
- architecture protection
- implementation discipline
- real verification
- review before commit

We intentionally do **not** keep separate Security or Gates agents for the MVP. Their responsibilities are folded into Architect, Reviewer, Tester, and Committer to reduce orchestration overhead.

## Source of truth context
Agents must treat these repository files as primary context:
- `PROJECT_CONTEXT.md`
- `SYSTEM_MAP.md`
- `.agents/shared-context.md`
- `.agents/architecture-guardrails.md`

## Standard operating loop
1. Planner proposes exactly one small milestone.
2. Human approves or adjusts scope.
3. Architect checks architecture fit before code changes.
4. Implementer makes the change.
5. Tester runs the smallest appropriate verification.
6. Reviewer reviews the delta.
7. If issues are found, run a fix pass.
8. Committer commits only when the loop is green.

## Core rules
1. Never work directly on `main` or `master`.
2. Prefer one milestone per pass.
3. Keep changes reviewable and small.
4. Keep broker integration inside adapters/infra only.
5. Keep strategy broker-agnostic.
6. Keep risk authoritative.
7. Treat broker state as source of truth.
8. Fail closed on uncertain execution state.
9. Do not commit unless tests and review are green.
10. Update docs when behavior or boundaries change.

## Suggested branch prefixes
- `feature/*`
- `fix/*`
- `refactor/*`
- `docs/*`
- `codex/*`

## Operating principle
A simple strategy on a reliable execution stack is better than a complex strategy on a fragile one.
