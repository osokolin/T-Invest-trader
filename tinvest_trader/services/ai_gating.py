"""AI-assisted signal gating -- SHADOW MODE only.

Produces ALLOW / CAUTION / BLOCK decisions based on AI analysis fields.
These decisions are persisted for measurement but NEVER affect real
execution or delivery. The goal is to evaluate whether AI gating
would improve outcomes if enabled in the future.

No ML. No adaptive rules. No execution impact. Pure shadow evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass

# -- Decision constants --

GATE_ALLOW = "ALLOW"
GATE_CAUTION = "CAUTION"
GATE_BLOCK = "BLOCK"


@dataclass(frozen=True)
class AIGateDecision:
    """Shadow gating decision for a signal."""

    decision: str  # ALLOW, CAUTION, BLOCK
    reason: str


def decide_ai_gate(
    ai_confidence: str,
    ai_actionability: str,
    divergence_bucket: str = "",
) -> AIGateDecision:
    """Compute shadow gating decision from AI structured fields.

    Rules (deterministic, checked in priority order):

    BLOCK:
    - ai_confidence == LOW
    - ai_actionability == WEAK
    - divergence_bucket == ai_more_bearish

    ALLOW:
    - ai_confidence == HIGH AND ai_actionability == CONSIDER

    CAUTION:
    - everything else (conservative default)
    """
    conf = (ai_confidence or "").upper()
    action = (ai_actionability or "").upper()
    bucket = (divergence_bucket or "").lower()

    # -- BLOCK rules (any one triggers) --
    if conf == "LOW":
        return AIGateDecision(GATE_BLOCK, "ai_confidence=LOW")

    if action == "WEAK":
        return AIGateDecision(GATE_BLOCK, "ai_actionability=WEAK")

    if bucket == "ai_more_bearish":
        return AIGateDecision(GATE_BLOCK, "divergence=ai_more_bearish")

    # -- ALLOW (all conditions must hold) --
    if conf == "HIGH" and action == "CONSIDER":
        return AIGateDecision(GATE_ALLOW, "high_confidence+consider")

    # -- CAUTION (conservative default) --
    reason_parts: list[str] = []
    if conf == "MEDIUM":
        reason_parts.append("ai_confidence=MEDIUM")
    if action in ("WATCH", "CAUTION"):
        reason_parts.append(f"ai_actionability={action}")
    if conf == "UNKNOWN" or action == "UNKNOWN":
        reason_parts.append("incomplete_ai_data")

    return AIGateDecision(
        GATE_CAUTION,
        ", ".join(reason_parts) if reason_parts else "default_caution",
    )
