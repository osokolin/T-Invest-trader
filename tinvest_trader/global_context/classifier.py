"""Rule-based global context classifier.

Simple keyword matching into broad event classes.
No NLP. No ML. Deterministic and explainable.
"""

from __future__ import annotations

from dataclasses import dataclass

# -- Classification rules --

_RISK_POSITIVE = (
    "risk-on", "risk on", "stocks rally", "futures higher",
    "futures up", "markets rise", "markets rally", "equities rally",
    "s&p higher", "s&p up", "nasdaq higher", "nasdaq up",
    "dow higher", "dow up", "bull", "bullish",
    "stocks surge", "market surge", "rally continues",
    "appetite for risk", "risk appetite",
)

_RISK_NEGATIVE = (
    "risk-off", "risk off", "selloff", "sell-off", "sell off",
    "futures lower", "futures down", "markets fall", "markets drop",
    "stocks tumble", "stocks fall", "stocks drop", "crash",
    "s&p lower", "s&p down", "nasdaq lower", "nasdaq down",
    "dow lower", "dow down", "bear", "bearish",
    "panic", "capitulation", "correction", "recession fears",
)

_OIL_POSITIVE = (
    "oil rises", "oil up", "oil higher", "oil rallies", "oil surges",
    "brent up", "brent higher", "brent rises", "brent rallies",
    "wti up", "wti higher", "wti rises",
    "crude up", "crude higher", "crude rises", "crude rallies",
    "opec cut", "opec+ cut", "oil supply cut",
)

_OIL_NEGATIVE = (
    "oil falls", "oil down", "oil lower", "oil drops", "oil tumbles",
    "brent down", "brent lower", "brent falls", "brent drops",
    "wti down", "wti lower", "wti falls",
    "crude down", "crude lower", "crude falls", "crude drops",
    "opec increase", "oil supply increase", "oil glut",
)

_CRYPTO_POSITIVE = (
    "bitcoin rallies", "bitcoin surges", "bitcoin up", "bitcoin higher",
    "btc rallies", "btc surges", "btc up", "btc higher",
    "crypto rally", "crypto surge", "crypto up",
    "ethereum up", "eth up", "eth higher", "eth rallies",
)

_CRYPTO_NEGATIVE = (
    "bitcoin dumps", "bitcoin falls", "bitcoin down", "bitcoin lower",
    "btc dumps", "btc falls", "btc down", "btc lower",
    "crypto crash", "crypto dump", "crypto down",
    "ethereum down", "eth down", "eth lower", "eth falls",
)

_MACRO_POSITIVE = (
    "rate cut", "fed cuts", "fed dovish", "easing",
    "inflation falls", "inflation lower", "cpi lower",
    "jobs strong", "employment strong", "gdp growth",
)

_MACRO_NEGATIVE = (
    "rate hike", "fed hikes", "fed hawkish", "tightening",
    "inflation rises", "inflation higher", "cpi higher",
    "jobs weak", "employment weak", "gdp contraction",
    "default risk", "debt ceiling",
)


@dataclass(frozen=True)
class ClassificationResult:
    """Result of rule-based global context classification."""

    event_type: str     # risk_sentiment, oil, crypto, macro, unknown
    direction: str      # positive, negative, neutral, unknown
    confidence: float   # 0.0-0.7


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    """Check if lowercased text contains any of the keywords."""
    return any(kw in text for kw in keywords)


def classify_global_context(text: str) -> ClassificationResult:
    """Classify text into a global context event type and direction.

    Simple keyword matching. Returns first strong match.
    Deterministic, no side effects.
    """
    lower = text.lower()

    # Oil (check before risk_sentiment since "oil" is specific)
    if _contains_any(lower, _OIL_POSITIVE):
        return ClassificationResult("oil", "positive", 0.7)
    if _contains_any(lower, _OIL_NEGATIVE):
        return ClassificationResult("oil", "negative", 0.7)

    # Crypto
    if _contains_any(lower, _CRYPTO_POSITIVE):
        return ClassificationResult("crypto", "positive", 0.7)
    if _contains_any(lower, _CRYPTO_NEGATIVE):
        return ClassificationResult("crypto", "negative", 0.7)

    # Macro
    if _contains_any(lower, _MACRO_POSITIVE):
        return ClassificationResult("macro", "positive", 0.5)
    if _contains_any(lower, _MACRO_NEGATIVE):
        return ClassificationResult("macro", "negative", 0.5)

    # Risk sentiment (broad, checked last)
    if _contains_any(lower, _RISK_POSITIVE):
        return ClassificationResult("risk_sentiment", "positive", 0.5)
    if _contains_any(lower, _RISK_NEGATIVE):
        return ClassificationResult("risk_sentiment", "negative", 0.5)

    return ClassificationResult("unknown", "unknown", 0.0)
