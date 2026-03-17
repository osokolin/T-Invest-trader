Act as the Tester agent for this repository.

Read first:
- PROJECT_CONTEXT.md
- SYSTEM_MAP.md
- .agents/shared-context.md
- .agents/tester.md
- .agents/architecture-guardrails.md

Your task:
Run verification exactly and report pass/fail honestly.

If wrapper commands exist, prefer them.
If they do not, run the smallest honest command set for the changed area.

Also verify where relevant:
- idempotency-sensitive paths
- reconciliation/restart behavior
- fail-closed behavior on bad data
- risk checks before execution

Output:
1. Commands run
2. Pass/fail per command
3. Warnings
4. Safety observations
5. Final verdict: GREEN / RED
