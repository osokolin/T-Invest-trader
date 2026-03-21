"""Tests for global context -> signal enrichment (shadow mode)."""

from __future__ import annotations

from unittest.mock import MagicMock

from tinvest_trader.services.signal_global_context import (
    AGAINST,
    ALIGNED,
    NEUTRAL,
    UNKNOWN,
    AlignmentPerformance,
    GlobalContextImpactReport,
    apply_global_context_enrichment,
    build_global_context_impact_report,
    classify_global_alignment,
    compute_global_adjusted_confidence,
    compute_global_adjustment,
    format_global_context_impact_report,
    get_recent_global_context,
)

# ================================================================
# A. Context aggregation
# ================================================================


class TestGetRecentGlobalContext:
    """Test get_recent_global_context aggregation."""

    def test_empty_returns_all_unknown(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = []

        result = get_recent_global_context(repo)

        assert result == {
            "risk_sentiment": "unknown",
            "oil": "unknown",
            "crypto": "unknown",
        }

    def test_single_positive_risk(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = [
            {"event_type": "risk_sentiment", "direction": "positive", "confidence": 0.7},
        ]

        result = get_recent_global_context(repo)

        assert result["risk_sentiment"] == "positive"
        assert result["oil"] == "unknown"

    def test_majority_vote(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = [
            {"event_type": "oil", "direction": "positive", "confidence": 0.7},
            {"event_type": "oil", "direction": "positive", "confidence": 0.5},
            {"event_type": "oil", "direction": "negative", "confidence": 0.7},
        ]

        result = get_recent_global_context(repo)

        assert result["oil"] == "positive"

    def test_tie_returns_neutral(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = [
            {"event_type": "crypto", "direction": "positive", "confidence": 0.7},
            {"event_type": "crypto", "direction": "negative", "confidence": 0.7},
        ]

        result = get_recent_global_context(repo)

        assert result["crypto"] == "neutral"

    def test_ignores_unknown_events(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = [
            {"event_type": "unknown", "direction": "positive", "confidence": 0.5},
            {"event_type": "risk_sentiment", "direction": "unknown", "confidence": 0.5},
        ]

        result = get_recent_global_context(repo)

        assert result["risk_sentiment"] == "unknown"

    def test_macro_events_not_in_alignment(self):
        """macro events are stored but not used for alignment in v1."""
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = [
            {"event_type": "macro", "direction": "positive", "confidence": 0.7},
        ]

        result = get_recent_global_context(repo)

        assert "macro" not in result
        assert result["risk_sentiment"] == "unknown"

    def test_lookback_passed_to_repo(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = []

        get_recent_global_context(repo, lookback_seconds=600)

        repo.get_global_context_for_enrichment.assert_called_once_with(
            lookback_seconds=600,
        )


# ================================================================
# B. Alignment classification
# ================================================================


class TestClassifyGlobalAlignment:
    """Test classify_global_alignment."""

    def test_up_signal_all_positive_aligned(self):
        ctx = {"risk_sentiment": "positive", "oil": "positive", "crypto": "positive"}
        assert classify_global_alignment("up", ctx) == ALIGNED

    def test_up_signal_all_negative_against(self):
        ctx = {"risk_sentiment": "negative", "oil": "negative", "crypto": "negative"}
        assert classify_global_alignment("up", ctx) == AGAINST

    def test_down_signal_all_negative_aligned(self):
        ctx = {"risk_sentiment": "negative", "oil": "negative", "crypto": "negative"}
        assert classify_global_alignment("down", ctx) == ALIGNED

    def test_down_signal_all_positive_against(self):
        ctx = {"risk_sentiment": "positive", "oil": "positive", "crypto": "positive"}
        assert classify_global_alignment("down", ctx) == AGAINST

    def test_mixed_signals_neutral(self):
        ctx = {"risk_sentiment": "positive", "oil": "negative", "crypto": "unknown"}
        assert classify_global_alignment("up", ctx) == NEUTRAL

    def test_all_unknown_returns_unknown(self):
        ctx = {"risk_sentiment": "unknown", "oil": "unknown", "crypto": "unknown"}
        assert classify_global_alignment("up", ctx) == UNKNOWN

    def test_empty_context_returns_unknown(self):
        assert classify_global_alignment("up", {}) == UNKNOWN

    def test_none_direction_returns_unknown(self):
        ctx = {"risk_sentiment": "positive", "oil": "positive", "crypto": "positive"}
        assert classify_global_alignment(None, ctx) == UNKNOWN

    def test_invalid_direction_returns_unknown(self):
        ctx = {"risk_sentiment": "positive"}
        assert classify_global_alignment("sideways", ctx) == UNKNOWN

    def test_case_insensitive_direction(self):
        ctx = {"risk_sentiment": "positive", "oil": "positive", "crypto": "unknown"}
        assert classify_global_alignment("UP", ctx) == ALIGNED

    def test_partial_context_one_positive(self):
        ctx = {"risk_sentiment": "positive", "oil": "unknown", "crypto": "unknown"}
        assert classify_global_alignment("up", ctx) == ALIGNED

    def test_neutral_context_direction_ignored(self):
        """Neutral context directions don't count as supporting or opposing."""
        ctx = {"risk_sentiment": "neutral", "oil": "neutral", "crypto": "neutral"}
        assert classify_global_alignment("up", ctx) == UNKNOWN

    def test_two_vs_one_aligned(self):
        ctx = {"risk_sentiment": "positive", "oil": "positive", "crypto": "negative"}
        assert classify_global_alignment("up", ctx) == ALIGNED

    def test_one_vs_two_against(self):
        ctx = {"risk_sentiment": "positive", "oil": "negative", "crypto": "negative"}
        assert classify_global_alignment("up", ctx) == AGAINST


# ================================================================
# C. Adjustment calculation
# ================================================================


class TestComputeGlobalAdjustment:
    """Test compute_global_adjustment."""

    def test_aligned(self):
        assert compute_global_adjustment(ALIGNED) == 0.05

    def test_against(self):
        assert compute_global_adjustment(AGAINST) == -0.10

    def test_neutral(self):
        assert compute_global_adjustment(NEUTRAL) == 0.0

    def test_unknown(self):
        assert compute_global_adjustment(UNKNOWN) == 0.0

    def test_invalid_defaults_zero(self):
        assert compute_global_adjustment("bogus") == 0.0


class TestComputeGlobalAdjustedConfidence:
    """Test compute_global_adjusted_confidence."""

    def test_positive_adjustment(self):
        result = compute_global_adjusted_confidence(0.60, 0.05)
        assert abs(result - 0.65) < 1e-6

    def test_negative_adjustment(self):
        result = compute_global_adjusted_confidence(0.60, -0.10)
        assert abs(result - 0.50) < 1e-6

    def test_clamp_at_one(self):
        result = compute_global_adjusted_confidence(0.98, 0.05)
        assert result == 1.0

    def test_clamp_at_zero(self):
        result = compute_global_adjusted_confidence(0.05, -0.10)
        assert result == 0.0

    def test_none_confidence(self):
        result = compute_global_adjusted_confidence(None, 0.05)
        assert result is None

    def test_zero_adjustment(self):
        result = compute_global_adjusted_confidence(0.55, 0.0)
        assert abs(result - 0.55) < 1e-6


# ================================================================
# D. NULL-safe behavior
# ================================================================


class TestNullSafety:
    """Test that NULL/missing values are handled gracefully."""

    def test_none_signal_direction(self):
        assert classify_global_alignment(None, {"risk_sentiment": "positive"}) == UNKNOWN

    def test_none_context(self):
        assert classify_global_alignment("up", None) == UNKNOWN

    def test_none_confidence_adjustment(self):
        assert compute_global_adjusted_confidence(None, 0.05) is None

    def test_enrichment_with_none_confidence(self):
        """Signals with NULL confidence should still get alignment."""
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = [
            {"event_type": "risk_sentiment", "direction": "positive", "confidence": 0.7},
        ]
        repo.get_unenriched_global_context_signals.return_value = [
            {"id": 1, "signal_type": "up", "confidence": None},
        ]
        repo.update_global_context_enrichment.return_value = True

        enriched = apply_global_context_enrichment(
            repo, MagicMock(),
        )

        assert enriched == 1
        call_kwargs = repo.update_global_context_enrichment.call_args
        assert call_kwargs[1]["global_alignment"] == ALIGNED
        assert call_kwargs[1]["global_adjusted_confidence"] is None


# ================================================================
# E. Batch enrichment
# ================================================================


class TestApplyGlobalContextEnrichment:
    """Test apply_global_context_enrichment batch processor."""

    def test_no_signals_returns_zero(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = []
        repo.get_unenriched_global_context_signals.return_value = []

        result = apply_global_context_enrichment(repo, MagicMock())

        assert result == 0

    def test_enriches_single_signal(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = [
            {"event_type": "risk_sentiment", "direction": "positive", "confidence": 0.7},
            {"event_type": "oil", "direction": "positive", "confidence": 0.5},
        ]
        repo.get_unenriched_global_context_signals.return_value = [
            {"id": 42, "signal_type": "up", "confidence": 0.60},
        ]
        repo.update_global_context_enrichment.return_value = True

        result = apply_global_context_enrichment(repo, MagicMock())

        assert result == 1
        call_kwargs = repo.update_global_context_enrichment.call_args[1]
        assert call_kwargs["global_alignment"] == ALIGNED
        assert call_kwargs["global_adjustment"] == 0.05
        assert abs(call_kwargs["global_adjusted_confidence"] - 0.65) < 1e-6

    def test_against_signal_enrichment(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = [
            {"event_type": "risk_sentiment", "direction": "negative", "confidence": 0.7},
            {"event_type": "oil", "direction": "negative", "confidence": 0.7},
        ]
        repo.get_unenriched_global_context_signals.return_value = [
            {"id": 99, "signal_type": "up", "confidence": 0.60},
        ]
        repo.update_global_context_enrichment.return_value = True

        result = apply_global_context_enrichment(repo, MagicMock())

        assert result == 1
        call_kwargs = repo.update_global_context_enrichment.call_args[1]
        assert call_kwargs["global_alignment"] == AGAINST
        assert call_kwargs["global_adjustment"] == -0.10
        assert abs(call_kwargs["global_adjusted_confidence"] - 0.50) < 1e-6

    def test_multiple_signals(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = [
            {"event_type": "risk_sentiment", "direction": "positive", "confidence": 0.7},
        ]
        repo.get_unenriched_global_context_signals.return_value = [
            {"id": 1, "signal_type": "up", "confidence": 0.50},
            {"id": 2, "signal_type": "down", "confidence": 0.70},
        ]
        repo.update_global_context_enrichment.return_value = True

        result = apply_global_context_enrichment(repo, MagicMock())

        assert result == 2

    def test_db_failure_counted_correctly(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = [
            {"event_type": "risk_sentiment", "direction": "positive", "confidence": 0.7},
        ]
        repo.get_unenriched_global_context_signals.return_value = [
            {"id": 1, "signal_type": "up", "confidence": 0.50},
            {"id": 2, "signal_type": "up", "confidence": 0.60},
        ]
        repo.update_global_context_enrichment.side_effect = [False, True]

        result = apply_global_context_enrichment(repo, MagicMock())

        assert result == 1

    def test_context_json_stored(self):
        repo = MagicMock()
        repo.get_global_context_for_enrichment.return_value = [
            {"event_type": "risk_sentiment", "direction": "positive", "confidence": 0.7},
        ]
        repo.get_unenriched_global_context_signals.return_value = [
            {"id": 1, "signal_type": "up", "confidence": 0.50},
        ]
        repo.update_global_context_enrichment.return_value = True

        apply_global_context_enrichment(repo, MagicMock())

        call_kwargs = repo.update_global_context_enrichment.call_args[1]
        import json
        ctx = json.loads(call_kwargs["global_context_json"])
        assert ctx["risk_sentiment"] == "positive"


# ================================================================
# F. Report building
# ================================================================


class TestBuildGlobalContextImpactReport:
    """Test report building and formatting."""

    def test_empty_report(self):
        repo = MagicMock()
        repo.get_source_weighting_baseline.return_value = {
            "total": 0, "resolved": 0, "wins": 0, "losses": 0,
            "avg_return": None,
        }
        repo.get_global_alignment_performance.return_value = []

        report = build_global_context_impact_report(repo)

        assert report.baseline.count == 0
        assert report.by_alignment == []
        assert any("no enriched signals" in i for i in report.insights)

    def test_report_with_data(self):
        repo = MagicMock()
        repo.get_source_weighting_baseline.return_value = {
            "total": 100, "resolved": 100, "wins": 55, "losses": 45,
            "avg_return": 0.001,
        }
        repo.get_global_alignment_performance.return_value = [
            {
                "alignment": ALIGNED, "total": 40, "resolved": 40,
                "wins": 26, "losses": 14, "avg_return": 0.002,
            },
            {
                "alignment": AGAINST, "total": 30, "resolved": 30,
                "wins": 12, "losses": 18, "avg_return": -0.001,
            },
            {
                "alignment": NEUTRAL, "total": 20, "resolved": 20,
                "wins": 10, "losses": 10, "avg_return": 0.0,
            },
        ]

        report = build_global_context_impact_report(repo)

        assert report.baseline.resolved == 100
        assert len(report.by_alignment) == 3
        aligned_perf = next(a for a in report.by_alignment if a.alignment == ALIGNED)
        assert aligned_perf.win_rate == 26 / 40

    def test_min_resolved_filter(self):
        repo = MagicMock()
        repo.get_source_weighting_baseline.return_value = {
            "total": 100, "resolved": 100, "wins": 55, "losses": 45,
            "avg_return": 0.001,
        }
        repo.get_global_alignment_performance.return_value = [
            {
                "alignment": ALIGNED, "total": 40, "resolved": 40,
                "wins": 26, "losses": 14, "avg_return": 0.002,
            },
            {
                "alignment": UNKNOWN, "total": 2, "resolved": 2,
                "wins": 1, "losses": 1, "avg_return": 0.0,
            },
        ]

        report = build_global_context_impact_report(repo, min_resolved=5)

        assert len(report.by_alignment) == 1
        assert report.by_alignment[0].alignment == ALIGNED


class TestFormatGlobalContextImpactReport:
    """Test report formatting output shape."""

    def test_format_contains_sections(self):
        report = GlobalContextImpactReport(
            baseline=AlignmentPerformance(
                alignment="baseline", count=100, resolved=100,
                wins=55, losses=45, avg_return=0.001,
            ),
            by_alignment=[
                AlignmentPerformance(
                    alignment=ALIGNED, count=40, resolved=40,
                    wins=26, losses=14, avg_return=0.002,
                ),
                AlignmentPerformance(
                    alignment=AGAINST, count=30, resolved=30,
                    wins=12, losses=18, avg_return=-0.001,
                ),
            ],
            insights=["aligned signals outperform baseline by 10.0pp win rate"],
        )

        output = format_global_context_impact_report(report)

        assert "shadow mode" in output
        assert "baseline" in output
        assert "aligned signals:" in output
        assert "against signals:" in output
        assert "win_rate:" in output
        assert "insights:" in output

    def test_format_empty_report(self):
        report = GlobalContextImpactReport(
            baseline=AlignmentPerformance(
                alignment="baseline", count=0, resolved=0,
            ),
        )

        output = format_global_context_impact_report(report)

        assert "baseline" in output
        assert "n/a" in output


# ================================================================
# G. AlignmentPerformance model
# ================================================================


class TestAlignmentPerformance:
    """Test AlignmentPerformance computed properties."""

    def test_win_rate_computed(self):
        perf = AlignmentPerformance(
            alignment=ALIGNED, resolved=100, wins=65,
        )
        assert abs(perf.win_rate - 0.65) < 1e-6

    def test_win_rate_none_when_no_resolved(self):
        perf = AlignmentPerformance(alignment=ALIGNED, resolved=0, wins=0)
        assert perf.win_rate is None

    def test_ev_computed(self):
        perf = AlignmentPerformance(
            alignment=ALIGNED, resolved=100, wins=60, avg_return=0.002,
        )
        assert abs(perf.ev - 0.6 * 0.002) < 1e-6

    def test_ev_none_when_no_return(self):
        perf = AlignmentPerformance(
            alignment=ALIGNED, resolved=100, wins=60, avg_return=None,
        )
        assert perf.ev is None


# ================================================================
# H. Insights generation
# ================================================================


class TestInsights:
    """Test insight generation logic."""

    def test_aligned_outperforms_insight(self):
        repo = MagicMock()
        repo.get_source_weighting_baseline.return_value = {
            "total": 100, "resolved": 100, "wins": 50, "losses": 50,
            "avg_return": 0.0,
        }
        repo.get_global_alignment_performance.return_value = [
            {
                "alignment": ALIGNED, "total": 40, "resolved": 40,
                "wins": 28, "losses": 12, "avg_return": 0.002,
            },
            {
                "alignment": AGAINST, "total": 30, "resolved": 30,
                "wins": 10, "losses": 20, "avg_return": -0.001,
            },
        ]

        report = build_global_context_impact_report(repo)

        assert any("outperform" in i for i in report.insights)
        assert any("underperform" in i for i in report.insights)
        assert any("predictive value" in i for i in report.insights)

    def test_no_data_insight(self):
        repo = MagicMock()
        repo.get_source_weighting_baseline.return_value = {
            "total": 0, "resolved": 0, "wins": 0, "losses": 0,
            "avg_return": None,
        }
        repo.get_global_alignment_performance.return_value = []

        report = build_global_context_impact_report(repo)

        assert any("no enriched signals" in i for i in report.insights)
