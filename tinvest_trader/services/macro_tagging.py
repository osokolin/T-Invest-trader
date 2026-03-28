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
    "coal": (
        "уголь", "угольн", "угля ", "coal ",
    ),
    "gold": (
        "золот", "gold ", "золота ", "золоту ",
    ),
    "risk": (
        "кризис", "обвал", "крах", "паник", "дефолт",
        "crisis", "crash", "collapse", "default risk",
        "recession", "рецессия",
    ),
    "geopolitics": (
        "санкции", "sanctions", "геополитик", "geopolitic",
        "эмбарго", "embargo", "война", "war ",
        "военн", "авианос", "минобороны", "дрон",
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
    "budget": (
        "бюджет", "дефицит бюджет", "расход бюджет",
        "fiscal", "госдолг",
    ),
    "crypto": (
        "биткоин", "bitcoin", "btc ", "крипто", "crypto",
        "эфириум", "ethereum",
    ),
    "realestate": (
        "ипотек", "ипотечн", "недвижимост", "девелопер",
        "жилищн", "mortgage", "real estate",
    ),
    "china_us": (
        "торговая война", "trade war", "пошлин", "тариф",
        "tariff",
    ),
    "deposits": (
        "ставки по вклад", "ставки по депозит", "вкладчик",
        "депозитн",
    ),
    "economy": (
        "экономик", "бизнес-климат", "деловая активност",
        "экономическ",
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
