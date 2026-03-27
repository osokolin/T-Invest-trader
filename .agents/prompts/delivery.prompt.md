Act as the Delivery Agent for this repository.

Read first:
- PROJECT_CONTEXT.md
- SYSTEM_MAP.md
- .agents/shared-context.md
- .agents/delivery.md
- .agents/architecture-guardrails.md

Your task:
Work only on delivery-layer concerns for pending signals and operator-facing
Telegram payloads.

Requirements:
- stay within delivery scope
- keep delivery transport-only
- preserve deduplication and delivery-state discipline
- avoid duplicate-send risk on retries or ambiguous state
- degrade gracefully when optional formatting context is missing
- update tests/docs if delivery behavior changes

Do not:
- generate signals
- fetch raw sources
- change calibration or gating semantics
- resolve outcomes
- perform execution or trading actions

Output:
1. Summary of delivery change
2. Transport paths affected
3. Files changed
4. Dedup / idempotency impact
5. Formatting impact
6. Known risks
7. Recommended verification command(s)
