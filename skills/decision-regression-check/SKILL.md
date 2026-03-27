---
name: decision-regression-check
description: Check deterministic signal-creation behavior in T-Invest-trader. Use when signal counts change unexpectedly, gating or calibration rules are modified, cooldown or dedup behavior shifts, or a code change may alter which candidates become pending signals.
---

# Decision Regression Check

Protect the boundary between enrichment and signal creation.

## Workflow

1. Start from the observed regression.
   Examples: fewer signals than expected, too many signals, changed rejection
   reasons, or duplicate pending signals.
2. Trace the decision path before editing.
   Read `tinvest_trader/services/signal_generation.py` first, then open only
   the gating or safety modules that participate in the failing path.
3. Separate threshold changes from dedup changes.
   A threshold bug and a cooldown bug often look similar in counts but require
   different fixes.
4. Preserve fail-closed behavior.
   When mandatory inputs are missing or ambiguous, prefer no signal.
5. Preserve explainability.
   If you change insertion or rejection behavior, keep reason taxonomy readable
   in logs, counters, and tests.
6. Verify with narrow regression tests and, if available, a dry-run generation
   pass.

## Key Files

- `tinvest_trader/services/signal_generation.py`
- `tinvest_trader/services/signal_calibration.py`
- `tinvest_trader/services/ai_gating.py`
- `tinvest_trader/services/signal_divergence.py`
- `tinvest_trader/services/market_binding.py`
- `tinvest_trader/services/execution_safety.py`

## Guardrails

- Keep decision logic deterministic for the same inputs.
- Keep re-runs idempotent through explicit dedup and cooldown rules.
- Do not move transport or operator concerns into decision code.
- Do not let shadow logic silently mutate production decisions.

## Verification

```bash
./.venv/bin/pytest -q tests/test_signal_generation.py tests/test_signal_calibration.py tests/test_ai_gating.py tests/test_execution_safety.py tests/test_signal_global_context.py
python -m tinvest_trader.cli generate-signals --dry-run --limit 50 --lookback-minutes 30
```

Run the CLI dry-run only when the database has representative fused features.
