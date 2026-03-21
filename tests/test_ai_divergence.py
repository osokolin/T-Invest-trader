"""Tests for AI vs System divergence tracking."""

from __future__ import annotations

from unittest.mock import MagicMock

from tinvest_trader.services.ai_divergence import (
    BUCKET_AGREE_STRONG,
    BUCKET_AGREE_WEAK,
    BUCKET_AI_MORE_BEARISH,
    BUCKET_AI_MORE_BULLISH,
    BUCKET_UNKNOWN,
    AIDivergenceReport,
    build_ai_divergence_report,
    classify_ai_divergence,
    format_ai_divergence_report,
    parse_ai_actionability,
    parse_ai_bias,
    parse_ai_confidence,
)

# -- A. AI confidence parsing --


class TestParseAiConfidence:
    def test_low(self) -> None:
        text = "Уверенность ИИ: НИЗКАЯ"
        assert parse_ai_confidence(text) == "LOW"

    def test_medium(self) -> None:
        text = "Уверенность ИИ: СРЕДНЯЯ"
        assert parse_ai_confidence(text) == "MEDIUM"

    def test_high(self) -> None:
        text = "Уверенность ИИ: ВЫСОКАЯ"
        assert parse_ai_confidence(text) == "HIGH"

    def test_embedded_in_text(self) -> None:
        text = (
            "Итог: some text\n"
            "Быки: good\n"
            "Медведи: bad\n"
            "Риски: risk\n"
            "Применимость: стоит рассматривать\n"
            "Уверенность ИИ: СРЕДНЯЯ"
        )
        assert parse_ai_confidence(text) == "MEDIUM"

    def test_missing(self) -> None:
        assert parse_ai_confidence("no confidence here") == "UNKNOWN"

    def test_empty(self) -> None:
        assert parse_ai_confidence("") == "UNKNOWN"

    def test_case_insensitive(self) -> None:
        text = "уверенность ии: высокая"
        assert parse_ai_confidence(text) == "HIGH"


# -- B. Actionability parsing --


class TestParseAiActionability:
    def test_consider(self) -> None:
        text = "Применимость: стоит рассматривать для входа"
        assert parse_ai_actionability(text) == "CONSIDER"

    def test_watch(self) -> None:
        text = "Применимость: только наблюдать"
        assert parse_ai_actionability(text) == "WATCH"

    def test_weak(self) -> None:
        text = "Применимость: сигнал слабый, лучше подождать"
        assert parse_ai_actionability(text) == "WEAK"

    def test_caution(self) -> None:
        text = "Применимость: нужна осторожность"
        assert parse_ai_actionability(text) == "CAUTION"

    def test_unknown_phrasing(self) -> None:
        text = "Применимость: непонятно что делать"
        assert parse_ai_actionability(text) == "UNKNOWN"

    def test_missing(self) -> None:
        assert parse_ai_actionability("no field here") == "UNKNOWN"

    def test_empty(self) -> None:
        assert parse_ai_actionability("") == "UNKNOWN"

    def test_trailing_period(self) -> None:
        text = "Применимость: только наблюдать."
        assert parse_ai_actionability(text) == "WATCH"


# -- B2. Bias parsing --


class TestParseAiBias:
    def test_neutral_default(self) -> None:
        text = "Быки: momentum is good\nМедведи: risk of decline"
        assert parse_ai_bias(text) == "neutral"

    def test_bearish_when_bull_weak(self) -> None:
        text = "Быки: нет явных факторов роста\nМедведи: сильное давление"
        assert parse_ai_bias(text) == "bearish"

    def test_bullish_when_bear_weak(self) -> None:
        text = "Быки: strong momentum\nМедведи: нет явных рисков"
        assert parse_ai_bias(text) == "bullish"

    def test_missing_fields(self) -> None:
        assert parse_ai_bias("just some text") == "unknown"


# -- C. Divergence classification --


class TestClassifyAiDivergence:
    def test_high_high_agree_strong(self) -> None:
        assert classify_ai_divergence("HIGH", "HIGH") == BUCKET_AGREE_STRONG

    def test_medium_medium_agree_strong(self) -> None:
        assert classify_ai_divergence("MEDIUM", "MEDIUM") == BUCKET_AGREE_STRONG

    def test_low_low_agree_weak(self) -> None:
        assert classify_ai_divergence("LOW", "LOW") == BUCKET_AGREE_WEAK

    def test_high_system_low_ai(self) -> None:
        result = classify_ai_divergence("HIGH", "LOW")
        assert result == BUCKET_AI_MORE_BEARISH

    def test_low_system_high_ai(self) -> None:
        result = classify_ai_divergence("LOW", "HIGH")
        assert result == BUCKET_AI_MORE_BULLISH

    def test_medium_system_high_ai(self) -> None:
        result = classify_ai_divergence("MEDIUM", "HIGH")
        assert result == BUCKET_AI_MORE_BULLISH

    def test_medium_system_low_ai(self) -> None:
        result = classify_ai_divergence("MEDIUM", "LOW")
        assert result == BUCKET_AI_MORE_BEARISH

    def test_watch_downgrades_ai(self) -> None:
        # MEDIUM AI + WATCH -> effective LOW, vs MEDIUM system -> bearish
        result = classify_ai_divergence("MEDIUM", "MEDIUM", "WATCH")
        assert result == BUCKET_AI_MORE_BEARISH

    def test_weak_downgrades_ai(self) -> None:
        result = classify_ai_divergence("MEDIUM", "MEDIUM", "WEAK")
        assert result == BUCKET_AI_MORE_BEARISH

    def test_consider_no_downgrade(self) -> None:
        result = classify_ai_divergence("MEDIUM", "MEDIUM", "CONSIDER")
        assert result == BUCKET_AGREE_STRONG

    def test_unknown_system(self) -> None:
        assert classify_ai_divergence("", "HIGH") == BUCKET_UNKNOWN

    def test_unknown_ai(self) -> None:
        assert classify_ai_divergence("HIGH", "UNKNOWN") == BUCKET_UNKNOWN

    def test_both_unknown(self) -> None:
        assert classify_ai_divergence("", "") == BUCKET_UNKNOWN


# -- D. NULL-safe behavior --


class TestNullSafety:
    def test_parse_none_like_text(self) -> None:
        assert parse_ai_confidence("None") == "UNKNOWN"
        assert parse_ai_actionability("None") == "UNKNOWN"

    def test_classify_with_defaults(self) -> None:
        # UNKNOWN defaults should produce UNKNOWN bucket
        result = classify_ai_divergence("HIGH", "UNKNOWN", "UNKNOWN")
        assert result == BUCKET_UNKNOWN


# -- E. Report building + aggregation --


def _make_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_ai_divergence_stats.return_value = {
        "total_analyzed": 100,
        "total_with_bucket": 80,
    }
    repo.get_ai_divergence_stats_by_bucket.return_value = [
        {
            "bucket": "agree_strong",
            "total": 40,
            "resolved": 30,
            "wins": 20,
            "losses": 8,
            "neutrals": 2,
            "avg_return": 0.018,
        },
        {
            "bucket": "ai_more_bearish",
            "total": 20,
            "resolved": 15,
            "wins": 6,
            "losses": 7,
            "neutrals": 2,
            "avg_return": -0.005,
        },
        {
            "bucket": "ai_more_bullish",
            "total": 10,
            "resolved": 8,
            "wins": 5,
            "losses": 2,
            "neutrals": 1,
            "avg_return": 0.012,
        },
        {
            "bucket": "agree_weak",
            "total": 10,
            "resolved": 7,
            "wins": 3,
            "losses": 3,
            "neutrals": 1,
            "avg_return": 0.001,
        },
    ]
    return repo


class TestBuildAiDivergenceReport:
    def test_builds_report(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        assert report.total_analyzed == 100
        assert report.total_with_bucket == 80
        assert len(report.by_bucket) == 4

    def test_agreement_count(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        # agree_strong (40) + agree_weak (10)
        assert report.agreement_count == 50

    def test_agreement_rate(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        # 50 / 80
        assert report.agreement_rate is not None
        assert abs(report.agreement_rate - 0.625) < 0.01

    def test_empty_stats(self) -> None:
        repo = MagicMock()
        repo.get_ai_divergence_stats.return_value = {}
        report = build_ai_divergence_report(repo)
        assert report.total_analyzed == 0

    def test_min_resolved_filter(self) -> None:
        repo = _make_repo()
        report = build_ai_divergence_report(repo, min_resolved=10)
        # Only agree_strong (30) and ai_more_bearish (15) pass
        assert len(report.by_bucket) == 2

    def test_bucket_win_rate(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        agree = next(b for b in report.by_bucket if b.bucket == "agree_strong")
        assert agree.win_rate is not None
        assert abs(agree.win_rate - 20 / 30) < 0.01

    def test_bucket_ev(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        agree = next(b for b in report.by_bucket if b.bucket == "agree_strong")
        assert agree.ev is not None
        expected_ev = (20 / 30) * 0.018
        assert abs(agree.ev - expected_ev) < 0.001


# -- F. CLI output shape --


class TestFormatAiDivergenceReport:
    def test_contains_header(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        output = format_ai_divergence_report(report)
        assert "AI vs System divergence" in output

    def test_contains_total(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        output = format_ai_divergence_report(report)
        assert "total_analyzed: 100" in output

    def test_contains_agreement_rate(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        output = format_ai_divergence_report(report)
        assert "agreement_rate:" in output

    def test_contains_buckets(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        output = format_ai_divergence_report(report)
        assert "agree_strong" in output
        assert "ai_more_bearish" in output

    def test_contains_win_rate(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        output = format_ai_divergence_report(report)
        assert "win_rate=" in output

    def test_empty_report(self) -> None:
        report = AIDivergenceReport()
        output = format_ai_divergence_report(report)
        assert "no bucket data yet" in output

    def test_contains_ev(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        output = format_ai_divergence_report(report)
        assert "EV=" in output


# -- G. Insight detection --


class TestInsightDetection:
    def test_bearish_lower_win_rate_insight(self) -> None:
        report = build_ai_divergence_report(_make_repo())
        output = format_ai_divergence_report(report)
        # agree_strong win_rate=67%, ai_more_bearish=40%
        # 67% > 40% + 5% -> insight should fire
        assert "AI disagreement" in output

    def test_no_insight_on_empty(self) -> None:
        report = AIDivergenceReport()
        output = format_ai_divergence_report(report)
        assert "Insights" not in output

    def test_weak_underperforms_insight(self) -> None:
        repo = MagicMock()
        repo.get_ai_divergence_stats.return_value = {
            "total_analyzed": 50,
            "total_with_bucket": 50,
        }
        repo.get_ai_divergence_stats_by_bucket.return_value = [
            {
                "bucket": "agree_weak",
                "total": 20,
                "resolved": 15,
                "wins": 5,
                "losses": 8,
                "neutrals": 2,
                "avg_return": -0.01,
            },
        ]
        report = build_ai_divergence_report(repo)
        output = format_ai_divergence_report(report)
        assert "agree_weak" in output
        assert "underperforms" in output
