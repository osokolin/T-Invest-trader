# Codex Operating Loop

This file defines the default working loop for Codex in this repository.

## Loop

### 1. Plan
Use the Planner agent to propose exactly one small milestone.

Expected output:
- milestone title
- why now
- scope
- acceptance criteria
- likely files
- tests
- risks
- safety check

### 2. Architecture check
Use the Architect agent before implementation when the change touches module boundaries, execution flow, state handling, or new files.

Expected output:
- architecture fit
- boundary violations
- state/lifecycle concerns
- recommended corrections
- verdict

### 3. Implement
Use the Implementer agent to make the smallest correct change set.

Expected output:
- summary of changes
- files changed
- tests added/updated
- known limitations
- tester commands

### 4. Verify
Use the Tester agent to run the smallest honest verification set.

Expected output:
- commands run
- pass/fail per command
- warnings
- safety observations
- verdict

### 5. Review
Use the Reviewer agent to review the delta.

Expected output:
- architecture issues
- safety issues
- correctness issues
- test gaps
- docs drift
- verdict

### 6. Fix pass if needed
If Tester or Reviewer is red, do not broaden scope.
Run a small fix pass through:
- Planner
- Architect
- Implementer
- Tester
- Reviewer

### 7. Commit
Use the Committer agent only when:
- milestone is complete
- Tester is GREEN
- Reviewer is SAFE_FOR_REVIEW
- docs are updated if needed
- branch is not main/master

## Default rules
- one milestone at a time
- small diffs beat ambitious diffs
- correctness beats speed
- reconciliation beats assumptions
- broker truth beats local cache
- no silent unsafe fallback

## Suggested human prompt template

```text
Use the Codex Operating Loop for this repository.
First act as Planner and propose one minimal next milestone.
After approval, act as Architect for architecture fit.
Then act as Implementer for the approved scope only.
Then act as Tester and run the smallest honest verification set.
Then act as Reviewer and list blocking/non-blocking issues.
If green, act as Committer and create a clean commit.
Use PROJECT_CONTEXT.md, SYSTEM_MAP.md, and .agents/* as binding context.
```

## Suggested compact prompt template

```text
Follow .agents/codex-operating-loop.md.
Use Planner -> Architect -> Implementer -> Tester -> Reviewer -> Committer.
One milestone only. Keep scope small. Preserve architecture guardrails.
```
