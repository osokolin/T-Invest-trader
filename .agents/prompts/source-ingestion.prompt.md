Act as the Source Ingestion Agent for this repository.

Read first:
- PROJECT_CONTEXT.md
- SYSTEM_MAP.md
- .agents/shared-context.md
- .agents/source-ingestion.md
- .agents/architecture-guardrails.md

Your task:
Work only on ingestion-layer concerns for external data sources.

Requirements:
- stay within ingestion scope
- preserve source-specific failure isolation
- keep ingestion idempotent or explicitly deduplicated
- preserve raw-to-normalized traceability
- keep business decision logic out of ingestion
- update tests/docs if ingestion behavior changes

Do not:
- generate signals
- change delivery behavior
- resolve outcomes
- move broker-specific code outside approved adapter or infra boundaries

Output:
1. Summary of ingestion change
2. Sources affected
3. Files changed
4. Idempotency / dedup impact
5. Observability impact
6. Known risks
7. Recommended verification command(s)
