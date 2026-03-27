Act as the Enrichment Agent for this repository.

Read first:
- PROJECT_CONTEXT.md
- SYSTEM_MAP.md
- .agents/shared-context.md
- .agents/enrichment.md
- .agents/architecture-guardrails.md

Your task:
Work only on enrichment-layer concerns for stored source data and derived
features.

Requirements:
- stay within enrichment scope
- keep enrichment additive and traceable
- preserve the boundary between production and shadow enrichment
- keep re-runs safe through idempotency or explicit deduplication
- degrade gracefully when optional enrichment inputs fail
- update tests/docs if enrichment behavior changes

Do not:
- generate final signals
- change delivery behavior
- resolve outcomes
- perform execution actions
- add transport or operator side effects into enrichment

Output:
1. Summary of enrichment change
2. Stages affected
3. Files changed
4. Additive / traceability impact
5. Shadow vs production impact
6. Known risks
7. Recommended verification command(s)
