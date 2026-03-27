---
name: operator-incident-triage
description: Triage operator-facing incidents in T-Invest-trader. Use when alerts, daily digest, status output, read-only bot commands, or operational CLI reports are wrong, noisy, missing, or inconsistent with the actual system state.
---

# Operator Incident Triage

Debug operator-facing behavior without mutating the trading pipeline under the
cover of ops work.

## Workflow

1. Classify the incident.
   Decide whether it is an alerting issue, digest issue, status-reporting
   issue, or read-only command issue.
2. Confirm the underlying system state before changing operator code.
   Many ops incidents are symptoms of ingestion, decision, or delivery lag
   rather than bugs in reporting itself.
3. Read operator code in this order:
   `tinvest_trader/services/alerting.py`,
   `tinvest_trader/services/daily_digest.py`,
   `tinvest_trader/services/bot_commands.py`,
   `tinvest_trader/cli.py`.
4. Preserve read-only semantics.
   Reporting and bot commands must not quietly mutate trading behavior.
5. Make degraded data explicit.
   Prefer a partial but honest report over a clean-looking false summary.
6. Verify in dry-run mode before live operator sends.

## Guardrails

- Keep operator code human-facing and read-only.
- Preserve alert cooldown and dedup behavior.
- Do not fix pipeline bugs by hardcoding operator output.
- Keep partial-failure reporting explicit.

## Verification

```bash
./.venv/bin/pytest -q tests/test_alerting.py tests/test_daily_digest.py tests/test_bot_commands.py tests/test_cli.py
python -m tinvest_trader.cli status
python -m tinvest_trader.cli check-alerts --dry-run
python -m tinvest_trader.cli send-daily-digest --dry-run
```

Use `--send` only after dry-run output matches the expected operator view.
