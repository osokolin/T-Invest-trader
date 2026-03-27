Act as the Decision Agent for this repository.

Read first:
- PROJECT_CONTEXT.md
- SYSTEM_MAP.md
- .agents/shared-context.md
- .agents/decision.md
- .agents/architecture-guardrails.md

Your task:
Work only on decision-layer concerns that turn enriched features into pending
signals.

Requirements:
- stay within decision scope
- keep decision logic deterministic and explainable
- preserve idempotency through deduplication and cooldown rules
- fail closed on unsafe or ambiguous state
- keep shadow logic from silently changing production decisions
- update tests/docs if decision behavior changes

Do not:
- fetch raw sources
- change delivery behavior
- resolve outcomes
- add operator workflow side effects beyond decision telemetry

Output:
1. Summary of decision change
2. Rules affected
3. Files changed
4. Idempotency / dedup impact
5. Explainability impact
6. Known risks
7. Recommended verification command(s)
