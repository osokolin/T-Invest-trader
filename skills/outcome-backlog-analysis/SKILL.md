---
name: outcome-backlog-analysis
description: Analyze unresolved-signal backlog and outcome-quality behavior in T-Invest-trader. Use when pending signals accumulate, win rate changes unexpectedly, resolution logic is modified, or you need to verify that outcomes remain reproducible from local quote history.
---

# Outcome Backlog Analysis

Investigate outcome quality without introducing live external dependencies.

## Workflow

1. Measure the symptom first.
   Determine whether the issue is backlog growth, quote coverage, classification
   drift, or reporting inconsistency.
2. Trace resolution from stored signals to local quotes.
   Start with `tinvest_trader/services/signal_outcome.py` and verify that the
   issue can be explained from repository state alone.
3. Treat missing evidence as unresolved, not as implicit loss or win.
4. Keep analytics separate from outcome truth.
   Divergence and learning reports may explain behavior, but they should not
   overwrite resolved status.
5. Verify reproducibility by re-running only local-data workflows.

## Key Files

- `tinvest_trader/services/signal_outcome.py`
- `tinvest_trader/services/signal_divergence.py`
- `tinvest_trader/services/ai_divergence.py`
- `tinvest_trader/services/ai_gating_report.py`

## Guardrails

- Use only stored local data for resolution.
- Preserve idempotency of repeated resolution runs.
- Keep learning metrics separate from live decision mutation.
- Prefer leaving a signal unresolved over guessing from incomplete evidence.

## Verification

```bash
./.venv/bin/pytest -q tests/test_signal_outcome.py tests/test_signal_divergence.py
python -m tinvest_trader.cli signal-stats
python -m tinvest_trader.cli signal-divergence-report
```

Use the CLI reports when the database contains representative signal history.
