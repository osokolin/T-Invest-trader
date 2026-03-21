"""Tests for source-aware weighting service (shadow mode)."""

from __future__ import annotations

from unittest.mock import MagicMock

from tinvest_trader.services.source_weighting import (
    SourceWeightingConfig,
    SourceWeightingReport,
    WeightedPerformance,
    apply_source_weights,
    build_source_weighting_report,
    compute_source_weight,
    compute_weighted_confidence,
    format_source_weighting_report,
)

# -- A. Weight calculation --


class TestComputeSourceWeight:
    def test_none_stats_returns_neutral(self) -> None:
        result = compute_source_weight(None)
        assert result.weight == 1.0
        assert result.reason == "no_source_data"

    def test_insufficient_data(self) -> None:
        stats = {"source_channel": "test", "resolved": 2, "wins": 1, "avg_return": 0.01}
        result = compute_source_weight(stats)
        assert result.weight == 1.0
        assert result.reason == "insufficient_data"

    def test_good_source_positive_ev(self) -> None:
        stats = {
            "source_channel": "interfaxonline",
            "resolved": 20,
            "wins": 12,
            "avg_return": 0.03,
        }
        result = compute_source_weight(stats)
        assert result.weight > 1.0
        assert result.reason == "positive_ev"

    def test_weak_source_negative_ev(self) -> None:
        stats = {
            "source_channel": "banksta",
            "resolved": 15,
            "wins": 5,
            "avg_return": -0.01,
        }
        result = compute_source_weight(stats)
        assert result.weight < 1.0
        assert result.reason == "negative_ev"

    def test_neutral_source(self) -> None:
        # EV exactly 0, near-zero avg_return -> neutral
        stats = {
            "source_channel": "neutral_source",
            "resolved": 10,
            "wins": 5,
            "avg_return": 0.0,
        }
        result = compute_source_weight(stats)
        assert result.weight == 1.0
        assert result.reason == "neutral"

    def test_moderate_positive(self) -> None:
        stats = {
            "source_channel": "moderate",
            "resolved": 10,
            "wins": 5,  # 50% win rate - above weak, below good
            "avg_return": 0.005,
        }
        result = compute_source_weight(stats)
        assert result.weight == 1.05
        assert result.reason == "moderate_positive"

    def test_custom_config_min_resolved(self) -> None:
        config = SourceWeightingConfig(min_resolved=20)
        stats = {"source_channel": "test", "resolved": 10, "wins": 8, "avg_return": 0.05}
        result = compute_source_weight(stats, config=config)
        assert result.weight == 1.0
        assert result.reason == "insufficient_data"

    def test_result_has_stats(self) -> None:
        stats = {
            "source_channel": "test_ch",
            "resolved": 10,
            "wins": 6,
            "avg_return": 0.02,
        }
        result = compute_source_weight(stats)
        assert result.source_channel == "test_ch"
        assert result.resolved == 10
        assert result.win_rate is not None
        assert result.ev is not None


# -- B. Clamping behavior --


class TestClamping:
    def test_weight_clamped_at_max(self) -> None:
        # Very strong source should not exceed 1.5
        stats = {
            "source_channel": "super",
            "resolved": 100,
            "wins": 95,
            "avg_return": 0.5,
        }
        result = compute_source_weight(stats)
        assert result.weight <= 1.5

    def test_weight_clamped_at_min(self) -> None:
        # Very weak source should not go below 0.5
        stats = {
            "source_channel": "terrible",
            "resolved": 100,
            "wins": 5,
            "avg_return": -0.5,
        }
        result = compute_source_weight(stats)
        assert result.weight >= 0.5

    def test_custom_clamp_range(self) -> None:
        config = SourceWeightingConfig(weight_min=0.8, weight_max=1.2)
        stats = {
            "source_channel": "super",
            "resolved": 100,
            "wins": 95,
            "avg_return": 0.5,
        }
        result = compute_source_weight(stats, config=config)
        assert result.weight <= 1.2

        weak_stats = {
            "source_channel": "terrible",
            "resolved": 100,
            "wins": 5,
            "avg_return": -0.5,
        }
        result2 = compute_source_weight(weak_stats, config=config)
        assert result2.weight >= 0.8


# -- C. Weighted confidence calculation --


class TestWeightedConfidence:
    def test_basic_multiplication(self) -> None:
        assert compute_weighted_confidence(0.7, 1.2) is not None
        result = compute_weighted_confidence(0.7, 1.2)
        assert result is not None
        assert abs(result - 0.84) < 0.001

    def test_clamped_at_1(self) -> None:
        result = compute_weighted_confidence(0.9, 1.5)
        assert result is not None
        assert result <= 1.0

    def test_clamped_at_0(self) -> None:
        result = compute_weighted_confidence(0.1, 0.0)
        assert result is not None
        assert result >= 0.0

    def test_none_confidence(self) -> None:
        assert compute_weighted_confidence(None, 1.2) is None

    def test_weight_1_preserves_confidence(self) -> None:
        result = compute_weighted_confidence(0.65, 1.0)
        assert result is not None
        assert abs(result - 0.65) < 0.001


# -- D. NULL-safe behavior --


class TestNullSafety:
    def test_missing_source_channel(self) -> None:
        stats = {"resolved": 10, "wins": 5, "avg_return": 0.01}
        result = compute_source_weight(stats)
        assert result.source_channel == "unknown"

    def test_missing_wins(self) -> None:
        stats = {"source_channel": "test", "resolved": 10, "avg_return": 0.01}
        result = compute_source_weight(stats)
        assert result.weight >= 0.5

    def test_missing_avg_return(self) -> None:
        stats = {"source_channel": "test", "resolved": 10, "wins": 5}
        result = compute_source_weight(stats)
        # avg_return defaults to 0.0, so EV = 0 -> neutral
        assert result.weight == 1.0

    def test_none_avg_return(self) -> None:
        stats = {
            "source_channel": "test",
            "resolved": 10,
            "wins": 5,
            "avg_return": None,
        }
        result = compute_source_weight(stats)
        assert result.weight == 1.0

    def test_zero_resolved(self) -> None:
        stats = {"source_channel": "test", "resolved": 0, "wins": 0, "avg_return": 0.0}
        result = compute_source_weight(stats)
        assert result.weight == 1.0
        assert result.reason == "insufficient_data"


# -- E. Report aggregation --


class TestBuildReport:
    def test_builds_report_structure(self) -> None:
        repo = MagicMock()
        repo.get_source_weighting_baseline.return_value = {
            "total": 100, "resolved": 80, "wins": 44, "losses": 30,
            "avg_return": 0.002,
        }
        repo.get_weighted_performance.return_value = {
            "total": 60, "resolved": 50, "wins": 31, "losses": 15,
            "avg_return": 0.004,
        }
        repo.get_signal_stats_by_source.return_value = [
            {
                "source_channel": "good_src",
                "total": 50, "resolved": 40, "wins": 25,
                "losses": 10, "neutrals": 5, "avg_return": 0.03,
            },
            {
                "source_channel": "bad_src",
                "total": 30, "resolved": 20, "wins": 6,
                "losses": 12, "neutrals": 2, "avg_return": -0.01,
            },
        ]

        report = build_source_weighting_report(repo, threshold=0.6)

        assert isinstance(report, SourceWeightingReport)
        assert report.baseline.count == 100
        assert report.weighted.count == 60
        assert len(report.sources) == 2
        assert len(report.insights) > 0

    def test_empty_data(self) -> None:
        repo = MagicMock()
        repo.get_source_weighting_baseline.return_value = {
            "total": 0, "resolved": 0, "wins": 0, "losses": 0,
            "avg_return": None,
        }
        repo.get_weighted_performance.return_value = {
            "total": 0, "resolved": 0, "wins": 0, "losses": 0,
            "avg_return": None,
        }
        repo.get_signal_stats_by_source.return_value = []

        report = build_source_weighting_report(repo)
        assert report.baseline.count == 0
        assert report.weighted.count == 0
        assert len(report.sources) == 0

    def test_min_resolved_filter(self) -> None:
        repo = MagicMock()
        repo.get_source_weighting_baseline.return_value = {
            "total": 100, "resolved": 80, "wins": 44, "losses": 30,
            "avg_return": 0.002,
        }
        repo.get_weighted_performance.return_value = {
            "total": 60, "resolved": 50, "wins": 31, "losses": 15,
            "avg_return": 0.004,
        }
        repo.get_signal_stats_by_source.return_value = [
            {
                "source_channel": "big_src",
                "total": 50, "resolved": 40, "wins": 25,
                "losses": 10, "neutrals": 5, "avg_return": 0.03,
            },
            {
                "source_channel": "small_src",
                "total": 3, "resolved": 2, "wins": 1,
                "losses": 1, "neutrals": 0, "avg_return": 0.001,
            },
        ]

        report = build_source_weighting_report(repo, min_resolved=10)
        assert len(report.sources) == 1
        assert report.sources[0].source_channel == "big_src"


# -- F. CLI output shape --


class TestFormatReport:
    def test_output_has_sections(self) -> None:
        report = SourceWeightingReport(
            baseline=WeightedPerformance(
                label="baseline", count=100, resolved=80,
                wins=44, losses=30, avg_return=0.002,
            ),
            weighted=WeightedPerformance(
                label="weighted (threshold=0.6)", count=60, resolved=50,
                wins=31, losses=15, avg_return=0.004,
            ),
            sources=[],
            insights=["weighting improves win rate by 7.0pp"],
        )

        output = format_source_weighting_report(report)
        assert "source weighting report" in output
        assert "baseline" in output
        assert "weighted" in output
        assert "insights" in output
        assert "win_rate" in output

    def test_output_shows_sources(self) -> None:
        from tinvest_trader.services.source_weighting import SourceWeightSnapshot

        report = SourceWeightingReport(
            baseline=WeightedPerformance(
                label="baseline", count=10, resolved=10,
                wins=5, losses=5, avg_return=0.001,
            ),
            weighted=WeightedPerformance(
                label="weighted (threshold=0.6)", count=5, resolved=5,
                wins=3, losses=2, avg_return=0.002,
            ),
            sources=[
                SourceWeightSnapshot(
                    source_channel="good", weight=1.2, reason="positive_ev",
                    resolved=20, win_rate=0.6, ev=0.01,
                ),
                SourceWeightSnapshot(
                    source_channel="bad", weight=0.8, reason="negative_ev",
                    resolved=15, win_rate=0.4, ev=-0.005,
                ),
            ],
            insights=[],
        )

        output = format_source_weighting_report(report)
        assert "strong sources" in output
        assert "good" in output
        assert "weak sources" in output
        assert "bad" in output

    def test_empty_report(self) -> None:
        report = SourceWeightingReport(
            baseline=WeightedPerformance(
                label="baseline", count=0, resolved=0,
                wins=0, losses=0, avg_return=None,
            ),
            weighted=WeightedPerformance(
                label="weighted (threshold=0.6)", count=0, resolved=0,
                wins=0, losses=0, avg_return=None,
            ),
        )

        output = format_source_weighting_report(report)
        assert "source weighting report" in output
        assert "n/a" in output


# -- G. Apply source weights (batch) --


class TestApplySourceWeights:
    def test_applies_weights_to_unweighted_signals(self) -> None:
        repo = MagicMock()
        repo.get_signal_stats_by_source.return_value = [
            {
                "source_channel": "good_src",
                "total": 50, "resolved": 30, "wins": 20,
                "losses": 8, "neutrals": 2, "avg_return": 0.02,
            },
        ]
        repo.get_unweighted_signals.return_value = [
            {"id": 1, "source_channel": "good_src", "confidence": 0.7},
            {"id": 2, "source_channel": "good_src", "confidence": 0.5},
        ]
        repo.update_source_weight.return_value = True

        updated = apply_source_weights(repo, MagicMock())
        assert updated == 2
        assert repo.update_source_weight.call_count == 2

        # Check weight > 1.0 for good source
        first_call = repo.update_source_weight.call_args_list[0]
        assert first_call[0][0] == 1  # signal_id
        assert first_call[1]["source_weight"] > 1.0

    def test_no_unweighted_signals(self) -> None:
        repo = MagicMock()
        repo.get_signal_stats_by_source.return_value = []
        repo.get_unweighted_signals.return_value = []

        updated = apply_source_weights(repo, MagicMock())
        assert updated == 0

    def test_unknown_source_gets_neutral_weight(self) -> None:
        repo = MagicMock()
        repo.get_signal_stats_by_source.return_value = [
            {
                "source_channel": "known_src",
                "total": 30, "resolved": 20, "wins": 12,
                "losses": 6, "neutrals": 2, "avg_return": 0.01,
            },
        ]
        repo.get_unweighted_signals.return_value = [
            {"id": 10, "source_channel": "unknown_src", "confidence": 0.6},
        ]
        repo.update_source_weight.return_value = True

        updated = apply_source_weights(repo, MagicMock())
        assert updated == 1
        call_kwargs = repo.update_source_weight.call_args[1]
        assert call_kwargs["source_weight"] == 1.0

    def test_null_confidence_handled(self) -> None:
        repo = MagicMock()
        repo.get_signal_stats_by_source.return_value = []
        repo.get_unweighted_signals.return_value = [
            {"id": 5, "source_channel": "src", "confidence": None},
        ]
        repo.update_source_weight.return_value = True

        updated = apply_source_weights(repo, MagicMock())
        assert updated == 1
        call_kwargs = repo.update_source_weight.call_args[1]
        assert call_kwargs["weighted_confidence"] is None

    def test_db_failure_does_not_crash(self) -> None:
        repo = MagicMock()
        repo.get_signal_stats_by_source.return_value = []
        repo.get_unweighted_signals.return_value = [
            {"id": 1, "source_channel": "src", "confidence": 0.7},
        ]
        repo.update_source_weight.return_value = False

        updated = apply_source_weights(repo, MagicMock())
        assert updated == 0


# -- H. Weighted severity derivation --


class TestWeightedSeverity:
    def test_high_confidence_high_severity(self) -> None:
        from tinvest_trader.services.source_weighting import _derive_weighted_severity

        assert _derive_weighted_severity(0.7) == "HIGH"
        assert _derive_weighted_severity(0.6) == "HIGH"

    def test_medium_confidence_medium_severity(self) -> None:
        from tinvest_trader.services.source_weighting import _derive_weighted_severity

        assert _derive_weighted_severity(0.5) == "MEDIUM"
        assert _derive_weighted_severity(0.42) == "MEDIUM"

    def test_low_confidence_low_severity(self) -> None:
        from tinvest_trader.services.source_weighting import _derive_weighted_severity

        assert _derive_weighted_severity(0.3) == "LOW"
        assert _derive_weighted_severity(0.0) == "LOW"

    def test_none_confidence_none_severity(self) -> None:
        from tinvest_trader.services.source_weighting import _derive_weighted_severity

        assert _derive_weighted_severity(None) is None


# -- I. Insights generation --


class TestInsights:
    def test_win_rate_improvement_detected(self) -> None:
        from tinvest_trader.services.source_weighting import _generate_insights

        baseline = WeightedPerformance(
            label="baseline", count=100, resolved=80,
            wins=40, losses=40, avg_return=0.001,
        )
        weighted = WeightedPerformance(
            label="weighted", count=60, resolved=50,
            wins=35, losses=15, avg_return=0.003,
        )
        insights = _generate_insights(baseline, weighted, [])
        assert any("improves" in i for i in insights)

    def test_filtering_percentage(self) -> None:
        from tinvest_trader.services.source_weighting import _generate_insights

        baseline = WeightedPerformance(
            label="baseline", count=100, resolved=80,
            wins=40, losses=40, avg_return=0.001,
        )
        weighted = WeightedPerformance(
            label="weighted", count=50, resolved=40,
            wins=25, losses=15, avg_return=0.002,
        )
        insights = _generate_insights(baseline, weighted, [])
        assert any("filters out 50%" in i for i in insights)

    def test_weak_sources_flagged(self) -> None:
        from tinvest_trader.services.source_weighting import (
            SourceWeightSnapshot,
            _generate_insights,
        )

        baseline = WeightedPerformance(
            label="baseline", count=10, resolved=10,
            wins=5, losses=5, avg_return=0.0,
        )
        weighted = WeightedPerformance(
            label="weighted", count=5, resolved=5,
            wins=3, losses=2, avg_return=0.001,
        )
        sources = [
            SourceWeightSnapshot(
                source_channel="bad_ch", weight=0.8, reason="negative_ev",
                resolved=10, win_rate=0.3, ev=-0.01,
            ),
        ]
        insights = _generate_insights(baseline, weighted, sources)
        assert any("weak sources" in i for i in insights)
        assert any("bad_ch" in i for i in insights)


# -- J. WeightedPerformance properties --


class TestWeightedPerformance:
    def test_win_rate_calculation(self) -> None:
        perf = WeightedPerformance(
            label="test", count=10, resolved=10,
            wins=6, losses=4, avg_return=0.01,
        )
        assert perf.win_rate is not None
        assert abs(perf.win_rate - 0.6) < 0.001

    def test_ev_calculation(self) -> None:
        perf = WeightedPerformance(
            label="test", count=10, resolved=10,
            wins=6, losses=4, avg_return=0.01,
        )
        assert perf.ev is not None
        assert abs(perf.ev - 0.006) < 0.001

    def test_zero_resolved(self) -> None:
        perf = WeightedPerformance(
            label="test", count=0, resolved=0,
            wins=0, losses=0, avg_return=None,
        )
        assert perf.win_rate is None
        assert perf.ev is None
