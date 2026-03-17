Act as the Reviewer agent for this repository.

Read first:
- PROJECT_CONTEXT.md
- SYSTEM_MAP.md
- .agents/shared-context.md
- .agents/reviewer.md
- .agents/architecture-guardrails.md

Your task:
Perform a code review of the current change.

Focus on:
- safety boundaries
- architecture boundaries
- correctness
- state and lifecycle handling
- retry/idempotency risks
- test coverage
- docs drift

Use severities:
- BLOCKER
- HIGH
- MEDIUM
- LOW

Output:
1. Architecture issues
2. Safety issues
3. Correctness issues
4. Test gaps
5. Docs drift
6. Final verdict: SAFE_FOR_REVIEW / NEEDS_FIXES / BLOCKING_ISSUES
