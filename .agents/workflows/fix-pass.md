# Workflow: fix-pass

Use when review or testing found issues.

1. Reviewer and/or Tester lists blockers and non-blockers.
2. Planner reduces blockers into one small fix pass.
3. Architect checks whether the fix preserves boundaries.
4. Implementer fixes only the approved blockers.
5. Tester reruns relevant verification.
6. Reviewer rechecks the affected area.
7. Committer commits only if the result is green.
