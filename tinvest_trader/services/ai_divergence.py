"""AI vs System divergence tracking -- measures agreement and predictive value.

Compares system severity (deterministic rules) with AI confidence
(parsed from Claude output). Tracks divergence buckets and their
outcomes to measure whether AI adds real edge.

No ML. No adaptive tuning. No execution gating. Pure measurement.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


# -- Parsing helpers --

_CONFIDENCE_MAP: dict[str, str] = {
    "НИЗКАЯ": "LOW",
    "СРЕДНЯЯ": "MEDIUM",
    "ВЫСОКАЯ": "HIGH",
}

_ACTIONABILITY_MAP: dict[str, str] = {
    "стоит рассматривать": "CONSIDER",
    "только наблюдать": "WATCH",
    "сигнал слабый": "WEAK",
    "нужна осторожность": "CAUTION",
}

_CONFIDENCE_RE = re.compile(
    r"Уверенность ИИ:\s*(НИЗКАЯ|СРЕДНЯЯ|ВЫСОКАЯ)",
    re.IGNORECASE,
)

_ACTIONABILITY_RE = re.compile(
    r"Применимость:\s*(.+)",
    re.IGNORECASE,
)

_BIAS_RE = re.compile(
    r"(?:Быки|Медведи):\s*(.+)",
    re.IGNORECASE,
)


def parse_ai_confidence(text: str) -> str:
    """Extract AI confidence level from analysis text. Returns LOW/MEDIUM/HIGH/UNKNOWN."""
    match = _CONFIDENCE_RE.search(text)
    if not match:
        return "UNKNOWN"
    raw = match.group(1).upper()
    return _CONFIDENCE_MAP.get(raw, "UNKNOWN")


def parse_ai_actionability(text: str) -> str:
    """Extract actionability verdict. Returns CONSIDER/WATCH/WEAK/CAUTION/UNKNOWN."""
    match = _ACTIONABILITY_RE.search(text)
    if not match:
        return "UNKNOWN"
    raw = match.group(1).strip().rstrip(".").lower()
    # Check if raw text starts with any known pattern
    for pattern, label in _ACTIONABILITY_MAP.items():
        if raw.startswith(pattern):
            return label
    return "UNKNOWN"


def parse_ai_bias(text: str) -> str:
    """Infer bias from Быки/Медведи balance. Conservative: returns unknown if unclear."""
    bull_match = re.search(r"Быки:\s*(.+)", text, re.IGNORECASE)
    bear_match = re.search(r"Медведи:\s*(.+)", text, re.IGNORECASE)
    if not bull_match or not bear_match:
        return "unknown"

    bull_text = bull_match.group(1).lower()
    bear_text = bear_match.group(1).lower()

    # Simple heuristic: if bull side mentions negatives or bear side is
    # clearly stronger, bias is bearish, and vice versa.
    bull_neg = any(w in bull_text for w in ("нет ", "отсутств", "слаб", "недостаточ"))
    bear_neg = any(w in bear_text for w in ("нет ", "отсутств", "слаб", "недостаточ"))

    if bull_neg and not bear_neg:
        return "bearish"
    if bear_neg and not bull_neg:
        return "bullish"
    return "neutral"


# -- Divergence classification --

BUCKET_AGREE_STRONG = "agree_strong"
BUCKET_AGREE_WEAK = "agree_weak"
BUCKET_AI_MORE_BULLISH = "ai_more_bullish"
BUCKET_AI_MORE_BEARISH = "ai_more_bearish"
BUCKET_UNCERTAIN = "uncertain"
BUCKET_UNKNOWN = "unknown"

ALL_BUCKETS = (
    BUCKET_AGREE_STRONG,
    BUCKET_AGREE_WEAK,
    BUCKET_AI_MORE_BULLISH,
    BUCKET_AI_MORE_BEARISH,
    BUCKET_UNCERTAIN,
    BUCKET_UNKNOWN,
)

_SEVERITY_RANK = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}


def classify_ai_divergence(
    system_severity: str,
    ai_confidence: str,
    ai_actionability: str = "UNKNOWN",
) -> str:
    """Classify divergence between system severity and AI confidence.

    Returns one of the BUCKET_* constants.
    """
    sys_rank = _SEVERITY_RANK.get(system_severity.upper(), 0)
    ai_rank = _SEVERITY_RANK.get(ai_confidence.upper(), 0)

    if sys_rank == 0 or ai_rank == 0:
        return BUCKET_UNKNOWN

    # Actionability WATCH/WEAK downgrades AI effective rank
    if ai_actionability in ("WATCH", "WEAK") and ai_rank >= 2:
        ai_rank -= 1

    diff = ai_rank - sys_rank

    if diff == 0:
        # Same level
        if sys_rank >= 2:
            return BUCKET_AGREE_STRONG
        return BUCKET_AGREE_WEAK
    if diff >= 1:
        return BUCKET_AI_MORE_BULLISH
    if diff <= -1:
        return BUCKET_AI_MORE_BEARISH

    return BUCKET_UNCERTAIN


# -- Report dataclasses --


@dataclass(frozen=True)
class BucketStats:
    """Performance stats for a single divergence bucket."""

    bucket: str
    total: int = 0
    resolved: int = 0
    wins: int = 0
    losses: int = 0
    neutrals: int = 0
    avg_return: float | None = None

    @property
    def win_rate(self) -> float | None:
        if self.resolved == 0:
            return None
        return self.wins / self.resolved

    @property
    def ev(self) -> float | None:
        if self.win_rate is None or self.avg_return is None:
            return None
        return self.win_rate * self.avg_return


@dataclass(frozen=True)
class AIDivergenceReport:
    """AI vs System divergence report."""

    total_analyzed: int = 0
    total_with_bucket: int = 0
    agreement_count: int = 0
    by_bucket: list[BucketStats] = field(default_factory=list)

    @property
    def agreement_rate(self) -> float | None:
        if self.total_with_bucket == 0:
            return None
        return self.agreement_count / self.total_with_bucket


# -- Report builder --


def build_ai_divergence_report(
    repository: TradingRepository,
    *,
    min_resolved: int = 0,
) -> AIDivergenceReport:
    """Build an AI divergence report from the database."""
    stats = repository.get_ai_divergence_stats()
    if not stats:
        return AIDivergenceReport()

    bucket_rows = repository.get_ai_divergence_stats_by_bucket()
    by_bucket = [
        BucketStats(
            bucket=row["bucket"],
            total=row["total"],
            resolved=row["resolved"],
            wins=row["wins"],
            losses=row["losses"],
            neutrals=row["neutrals"],
            avg_return=row.get("avg_return"),
        )
        for row in bucket_rows
        if row["resolved"] >= min_resolved
    ]

    agreement = sum(
        b.total for b in by_bucket
        if b.bucket in (BUCKET_AGREE_STRONG, BUCKET_AGREE_WEAK)
    )

    return AIDivergenceReport(
        total_analyzed=stats.get("total_analyzed", 0),
        total_with_bucket=stats.get("total_with_bucket", 0),
        agreement_count=agreement,
        by_bucket=by_bucket,
    )


# -- Report formatter --


def _pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.0%}"


def _ret(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.4f}"


def _ev_str(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.4f}"


def format_ai_divergence_report(report: AIDivergenceReport) -> str:
    """Format the AI divergence report for CLI output."""
    lines: list[str] = []

    lines.append("AI vs System divergence")
    lines.append(f"  total_analyzed: {report.total_analyzed}")
    lines.append(f"  with_bucket: {report.total_with_bucket}")
    lines.append(f"  agreement_rate: {_pct(report.agreement_rate)}")
    lines.append("")

    if not report.by_bucket:
        lines.append("  no bucket data yet")
        return "\n".join(lines)

    lines.append("Buckets:")
    for b in report.by_bucket:
        wr = _pct(b.win_rate)
        avg_r = _ret(b.avg_return)
        ev = _ev_str(b.ev)
        lines.append(
            f"  {b.bucket:<20} "
            f"count={b.total:<5} "
            f"win_rate={wr:<6} "
            f"avg_return={avg_r:<10} "
            f"EV={ev}"
            + (f"  (n={b.resolved})" if b.resolved > 0 else ""),
        )

    # -- Insights --
    bucket_map = {b.bucket: b for b in report.by_bucket}
    insights: list[str] = []

    agree = bucket_map.get(BUCKET_AGREE_STRONG)
    bearish = bucket_map.get(BUCKET_AI_MORE_BEARISH)
    bullish = bucket_map.get(BUCKET_AI_MORE_BULLISH)

    if (
        agree is not None
        and bearish is not None
        and agree.win_rate is not None
        and bearish.win_rate is not None
        and agree.resolved >= 3
        and bearish.resolved >= 3
        and agree.win_rate > bearish.win_rate + 0.05
    ):
        insights.append(
            f"  ! AI disagreement (bearish) has lower win rate "
            f"({bearish.win_rate:.0%}) than agreement ({agree.win_rate:.0%})",
        )

    if (
        bullish is not None
        and agree is not None
        and bullish.ev is not None
        and agree.ev is not None
        and bullish.resolved >= 3
        and bullish.ev > agree.ev
    ):
        insights.append(
            f"  ! AI bullish divergence has higher EV "
            f"({bullish.ev:+.4f}) than agreement ({agree.ev:+.4f})",
        )

    weak = bucket_map.get(BUCKET_AGREE_WEAK)
    if (
        weak is not None
        and weak.win_rate is not None
        and weak.resolved >= 3
        and weak.win_rate < 0.45
    ):
        insights.append(
            f"  ! agree_weak bucket underperforms (win_rate={weak.win_rate:.0%})",
        )

    if insights:
        lines.append("")
        lines.append("Insights:")
        lines.extend(insights)

    return "\n".join(lines)
