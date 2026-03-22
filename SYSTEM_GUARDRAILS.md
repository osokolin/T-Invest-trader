# SYSTEM_GUARDRAILS.md

Architecture guardrails for T-Invest-trader.
These rules prevent architectural drift as the system grows.

---

## 1. System Overview

The system is a multi-layer trading signal platform:

```
INGESTION -> ENRICHMENT -> DECISION -> DELIVERY -> ANALYTICS
```

**Ingestion** fetches and stores raw data from external sources (Telegram channels, broker API, MOEX, CBR, global market APIs). Each source has its own pipeline. Output: raw rows in dedicated tables.

**Enrichment** adds derived fields to signals: AI analysis, source weighting, global context alignment. All enrichment is additive -- it never modifies the original signal meaning. Shadow enrichment fields are stored on `signal_predictions` but do not affect decisions.

**Decision** pipeline determines whether a signal is delivered: binding (ticker resolution), calibration (confidence thresholds, EV filters), safety checks. This is the only layer that gates signal flow.

**Delivery** formats and sends signals via Telegram Bot API. It is a pure transport layer -- no business logic, no DB writes, no decisions.

**Analytics** provides read-only visibility: CLI reports, Grafana dashboards, retrospective analysis. Never writes operational data.

---

## 2. Pipeline Boundaries

Each layer has strict allowed/forbidden behavior.

### Ingestion
- **Allowed**: fetch external data, normalize, deduplicate, persist raw records
- **Forbidden**: decision logic, signal scoring, delivery, modifying other tables

### Enrichment
- **Allowed**: compute derived fields, store shadow columns on `signal_predictions`
- **Forbidden**: changing original `confidence`, `signal_type`, or `pipeline_stage`; blocking or filtering signals; calling delivery

### Decision
- **Allowed**: binding, calibration, safety checks, setting `pipeline_stage` and `rejection_reason`
- **Forbidden**: calling external APIs (AI, Telegram, market data), writing to non-signal tables

### Delivery
- **Allowed**: formatting messages, sending via Telegram Bot API, updating `delivered_at`
- **Forbidden**: business logic, DB writes (except `delivered_at`), signal filtering

### Analytics
- **Allowed**: read-only queries, aggregation, reporting
- **Forbidden**: any writes, any side effects

---

## 3. Single Source of Truth

`signal_predictions` is the canonical signal entity.

Rules:
- All enrichment fields (AI, source weight, global alignment) live as columns on `signal_predictions`
- No parallel signal representations in other tables
- All decision logic reads from and writes to `signal_predictions`
- Outcome tracking (`outcome_label`, `return_pct`, `resolved_at`) lives on `signal_predictions`

Context tables (`global_market_context_events`, `global_market_snapshots`, `signal_ai_analyses`) are reference data -- they inform enrichment but are not signal entities.

---

## 4. Shadow-First Rule

All new features that affect signal quality assessment MUST follow this progression:

```
1. Shadow mode    -- compute and store, no execution impact
2. Measurement    -- CLI report + Grafana panel to evaluate
3. Controlled rollout -- feature flag, gradual enable
```

Current shadow features:
- **AI gating**: `ai_gate_decision` (ALLOW/CAUTION/BLOCK) -- shadow only
- **Source weighting**: `source_weight`, `weighted_confidence` -- shadow only
- **Global alignment**: `global_alignment`, `global_adjusted_confidence` -- shadow only

A feature graduates from shadow when measurement shows clear improvement over baseline with sufficient sample size.

---

## 5. Feature Flag Discipline

Every new feature must be toggleable via environment variable.

Rules:
- Default: **OFF** (disabled)
- Naming: `TINVEST_<FEATURE>_ENABLED` (e.g. `TINVEST_GLOBAL_CONTEXT_ENABLED`)
- Safe rollback: disabling the flag must restore previous behavior with zero side effects
- No conditional logic scattered across unrelated modules -- feature checks belong in the service or container wiring

Examples:
```
TINVEST_GLOBAL_CONTEXT_ENABLED=false
TINVEST_GLOBAL_MARKET_DATA_ENABLED=false
TINVEST_BACKGROUND_RUN_GLOBAL_CONTEXT=false
```

---

## 6. Deterministic & Explainable Logic

Rules:
- All scoring, calibration, and gating logic must be deterministic given the same inputs
- AI analysis is informational -- it never directly controls signal flow
- Every decision must be traceable: `pipeline_stage`, `rejection_reason`, `ai_gate_decision` are always recorded
- No hidden thresholds or magic numbers -- all constants live in config dataclasses

---

## 7. Observability-First

Every feature must ship with:
- **CLI report** (`python -m tinvest_trader.cli <command>`) for on-demand inspection
- **Grafana panel** or query for continuous monitoring
- **Baseline comparison** -- ability to compare experiment vs control (e.g. "aligned" vs "not_enriched")

If you cannot measure a feature's impact, do not ship it.

---

## 8. No Duplication of Logic

Rules:
- One entry point per domain: severity in `signal_severity.py`, calibration in its module, AI in its module
- Reuse existing services -- do not copy-paste logic into new files
- Shared utilities (normalization, dedup hashing) live in dedicated modules and are imported
- If two modules need the same logic, extract it -- do not fork it

---

## 9. Data Contracts

Each field has a defined meaning. Changing semantics is a breaking change.

Key contracts:
- `confidence` is in `[0, 1]` -- probability-like score
- `return_pct` is signed decimal -- positive = profit, negative = loss
- `outcome_label` is one of: `win`, `loss`, `neutral`
- `pipeline_stage` is one of: `generated`, `rejected_calibration`, `rejected_binding`, `rejected_safety`, `delivered`
- Shadow adjustments are **additive** to confidence (e.g. `global_adjusted_confidence = confidence + adjustment`)
- Shadow weights are **multiplicative** to confidence (e.g. `weighted_confidence = confidence * source_weight`)
- `NULL` means "not computed yet" -- never use NULL to mean "zero" or "neutral"

---

## 10. Transport Layer Rule

Telegram (and any future UI):
- Read-only consumer of formatted data
- No business logic
- No DB writes (except delivery timestamp)
- No signal filtering or scoring
- Formatting lives in the delivery service, not in the transport

---

## 11. Context vs Signal Separation

**Signal** = base prediction (`ticker`, `signal_type`, `confidence`, `source`)
**Context** = supplementary data (AI analysis, source stats, global market state)

Rules:
- Context enriches signal but does NOT replace it
- Original signal fields are immutable after creation
- Shadow fields store context-derived adjustments alongside the signal
- Context can be missing -- signal must work without it

---

## 12. Failure & Fallback Rules

Rules:
- Missing enrichment data = neutral behavior (no adjustment, no block)
- One failing ingestion source must not crash the background loop
- DB write failures in non-critical paths: log and continue
- External API failures: retry is acceptable, but never block signal delivery
- Default shadow values: `NULL` (not computed), never fake values

---

## PR Checklist

Before merging any feature PR, verify:

```
- [ ] Follows pipeline boundaries (ingestion/enrichment/decision/delivery/analytics)
- [ ] Shadow mode first (if affects signal quality)
- [ ] Has feature flag (default OFF)
- [ ] Has CLI report command
- [ ] Has Grafana query or panel
- [ ] No business logic in delivery/transport layer
- [ ] No duplication of existing service logic
- [ ] Handles missing data gracefully (NULL-safe)
- [ ] Uses existing tables or adds columns additively (no destructive migrations)
- [ ] All new env vars documented in config dataclass
- [ ] Tests cover happy path + NULL/missing data + failure modes
```
