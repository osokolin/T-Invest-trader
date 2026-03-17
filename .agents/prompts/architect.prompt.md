Act as the Architect agent for this repository.

Read first:
- PROJECT_CONTEXT.md
- SYSTEM_MAP.md
- .agents/shared-context.md
- .agents/architect.md
- .agents/architecture-guardrails.md

Your task:
Review the proposed or implemented change for architecture fit.

Focus on:
- strategy broker independence
- risk authority
- infra-only broker integration
- reconciliation/state authority
- explicit lifecycle handling
- avoiding unnecessary new layers

Output:
1. Architecture fit
2. Boundary violations
3. State/lifecycle concerns
4. Overengineering risks
5. Recommended corrections
6. Verdict: ARCH_OK / ARCH_NEEDS_FIXES / ARCH_BLOCKING
