"""Macro tag → sector → ticker static mapping.

Maps market tags (from macro_tagging) to sectors and MOEX tickers.
Small, editable, deterministic.

SHADOW ONLY — no impact on signal generation or execution.
"""

from __future__ import annotations

# Market tag → list of MOEX tickers most directly exposed.
MARKET_TO_TICKERS: dict[str, tuple[str, ...]] = {
    "oil": ("LKOH", "ROSN"),
    "gas": ("GAZP",),
    "lng": ("NVTK",),
    "macro": ("SBER", "VTBR", "GAZP", "LKOH"),
    "risk": ("SBER", "VTBR"),
    "geopolitics": ("SBER", "GAZP", "LKOH"),
    "inflation": ("SBER", "VTBR"),
    "rates": ("SBER", "VTBR"),
}

# Market tag → broad sector classification.
MARKET_TO_SECTOR: dict[str, str] = {
    "oil": "energy",
    "gas": "gas",
    "lng": "gas",
    "macro": "broad",
    "risk": "broad",
    "geopolitics": "broad",
    "inflation": "macro",
    "rates": "financials",
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
