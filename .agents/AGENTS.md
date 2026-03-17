# AGENTS.md

Codex should use the repository-local agent system in `.agents/`.

Read first:
- `PROJECT_CONTEXT.md`
- `SYSTEM_MAP.md`
- `.agents/README.md`
- `.agents/shared-context.md`
- `.agents/architecture-guardrails.md`
- `.agents/codex-operating-loop.md`

Default workflow:
- Planner
- Architect
- Implementer
- Tester
- Reviewer
- Committer

Rules:
- work on one milestone at a time
- keep changes small and reviewable
- preserve architecture boundaries
- preserve risk and reconciliation discipline
- do not commit on main/master
