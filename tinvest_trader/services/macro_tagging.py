"""Macro message tagging — keyword-based classification.

Detects macro/commodity themes in Telegram messages and returns
a list of market tags (e.g. oil, gas, risk). Simple, deterministic,
no ML.

SHADOW ONLY — no impact on signal generation or execution.
"""

from __future__ import annotations

# Tag → (required keywords).  A message matches a tag if ANY keyword is found.
# Prefer precision over recall: missing a tag is better than a false positive.
_TAG_KEYWORDS: dict[str, tuple[str, ...]] = {
    "oil": (
        "нефть", "нефти", "нефтян", "brent", "wti", "crude oil",
        "opec", "опек",
    ),
    "gas": (
        "газ ", "газа ", "газу ", "газом ", "газпром",
        "natural gas", "природный газ",
    ),
    "lng": (
        "спг", "lng", "новатэк", "сжиженн",
    ),
    "risk": (
        "кризис", "обвал", "крах", "паник", "дефолт",
        "crisis", "crash", "collapse", "default risk",
        "recession", "рецессия",
    ),
    "geopolitics": (
        "санкции", "sanctions", "геополитик", "geopolitic",
        "эмбарго", "embargo", "война", "war ",
    ),
    "inflation": (
        "инфляция", "инфляци", "inflation", "cpi ", "ипц ",
    ),
    "rates": (
        "ключевую ставку", "ключевая ставка", "ключевой ставк",
        "ставка цб", "ставку цб",
        "процентная ставка", "процентную ставку",
        "key rate", "fed rate", "ставка фрс",
    ),
    "macro": (
        "ввп ", "gdp ", "безработиц", "unemployment",
        "pmi ", "промышленн", "industrial",
    ),
}


def tag_macro_message(text: str) -> list[str]:
    """Extract macro tags from message text.

    Returns a sorted list of unique tags found in the text.
    Uses simple keyword matching on lowercased text.
    """
    if not text:
        return []

    lower = text.lower()
    tags: list[str] = []

    for tag, keywords in _TAG_KEYWORDS.items():
        for kw in keywords:
            if kw in lower:
                tags.append(tag)
                break  # one match per tag is enough

    tags.sort()
    return tags
