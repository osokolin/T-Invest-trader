"""Prompt construction for AI research reports."""

from __future__ import annotations

import json

from tinvest_trader.services.ai_research.schemas import ResearchSnapshot

SYSTEM_INSTRUCTIONS = """\
You are a trading research assistant, not a financial advisor.
Use only the provided snapshot.
Do not invent prices, news, indicators, events, volumes, or market data.
If data is missing, explicitly say it is missing.
Return JSON only with these keys:
- bull_case
- bear_case
- skeptic_notes
- risk_notes
- final_summary
- confidence

confidence must be a number from 0.0 to 1.0, or null if the snapshot has
insufficient data.
"""


def build_research_prompt(snapshot: ResearchSnapshot) -> str:
    """Build a JSON-only research prompt from a curated snapshot."""
    snapshot_json = json.dumps(snapshot.to_dict(), ensure_ascii=False, indent=2)
    return (
        f"{SYSTEM_INSTRUCTIONS}\n"
        "Produce:\n"
        "1. Bull case\n"
        "2. Bear case\n"
        "3. Skeptic / why not trade\n"
        "4. Risk notes\n"
        "5. Final short summary\n"
        "6. Confidence from 0.0 to 1.0, or null if insufficient data\n\n"
        f"SNAPSHOT_JSON:\n{snapshot_json}"
    )
