"""Macro tag → sector → ticker static mapping.

Maps market tags (from macro_tagging) to sectors and MOEX tickers.
Small, editable, deterministic.

SHADOW ONLY — no impact on signal generation or execution.
"""

from __future__ import annotations

# Market tag → list of MOEX tickers most directly exposed.
MARKET_TO_TICKERS: dict[str, tuple[str, ...]] = {
    "oil": ("LKOH", "ROSN", "TATN", "SNGS", "SIBN"),
    "gas": ("GAZP",),
    "lng": ("NVTK",),
    "coal": ("MTLR", "RASP"),
    "gold": ("PLZL", "UGLD", "SELG"),
    "macro": ("SBER", "VTBR", "GAZP", "LKOH"),
    "risk": ("SBER", "VTBR", "MOEX"),
    "geopolitics": ("SBER", "GAZP", "LKOH", "ROSN", "VTBR"),
    "inflation": ("SBER", "VTBR"),
    "rates": ("SBER", "VTBR", "MOEX", "CBOM"),
    "budget": ("SBER", "VTBR"),
    "crypto": (),
    "realestate": ("LSRG", "SMLT", "PIKK"),
    "china_us": ("SBER", "LKOH", "GAZP"),
    "deposits": ("SBER", "VTBR", "CBOM", "BSPB"),
    "economy": ("SBER", "VTBR", "MOEX"),
}

# Market tag → broad sector classification.
MARKET_TO_SECTOR: dict[str, str] = {
    "oil": "energy",
    "gas": "gas",
    "lng": "gas",
    "coal": "energy",
    "gold": "commodities",
    "macro": "broad",
    "risk": "broad",
    "geopolitics": "broad",
    "inflation": "macro",
    "rates": "financials",
    "budget": "macro",
    "crypto": "crypto",
    "realestate": "realestate",
    "china_us": "broad",
    "deposits": "financials",
    "economy": "macro",
}


def get_affected_tickers(tags: list[str]) -> list[str]:
    """Return unique sorted tickers affected by given macro tags."""
    tickers: set[str] = set()
    for tag in tags:
        tickers.update(MARKET_TO_TICKERS.get(tag, ()))
    return sorted(tickers)


def get_sectors(tags: list[str]) -> list[str]:
    """Return unique sorted sectors for given macro tags."""
    sectors: set[str] = set()
    for tag in tags:
        sector = MARKET_TO_SECTOR.get(tag)
        if sector:
            sectors.add(sector)
    return sorted(sectors)
