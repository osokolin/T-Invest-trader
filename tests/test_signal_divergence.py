"""Tests for signal divergence tracking and reporting."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from tinvest_trader.services.signal_divergence import (
    ALL_STAGES,
    STAGE_DELIVERED,
    STAGE_GENERATED,
    STAGE_REJECTED_BINDING,
    STAGE_REJECTED_CALIBRATION,
    STAGE_REJECTED_SAFETY,
    DivergenceReport,
    StageStats,
    build_divergence_report,
    format_divergence_report,
)

# -- A. Stage transitions (constants) --


class TestStageConstants:
    def test_all_stages_defined(self) -> None:
        assert STAGE_GENERATED == "generated"
        assert STAGE_REJECTED_CALIBRATION == "rejected_calibration"
        assert STAGE_REJECTED_BINDING == "rejected_binding"
        assert STAGE_REJECTED_SAFETY == "rejected_safety"
        assert STAGE_DELIVERED == "delivered"

    def test_all_stages_tuple_order(self) -> None:
        assert ALL_STAGES[0] == STAGE_GENERATED
        assert ALL_STAGES[-1] == STAGE_DELIVERED


# -- B. StageStats --


class TestStageStats:
    def test_win_rate(self) -> None:
        s = StageStats(stage="delivered", resolved=10, wins=6)
        assert s.win_rate == 0.6

    def test_win_rate_zero_resolved(self) -> None:
        s = StageStats(stage="delivered", resolved=0, wins=0)
        assert s.win_rate is None

    def test_loss_rate(self) -> None:
        s = StageStats(stage="delivered", resolved=10, losses=4)
        assert s.loss_rate == 0.4


# -- C. DivergenceReport --


class TestDivergenceReport:
    def test_defaults(self) -> None:
        r = DivergenceReport()
        assert r.total == 0
        assert r.generated == 0
        assert r.delivered == 0
        assert r.by_stage == []


# -- D. build_divergence_report --


class TestBuildReport:
    def test_builds_from_repo(self) -> None:
        repo = MagicMock()
        repo.get_divergence_stats.return_value = {
            "total": 100,
            "generated": 10,
            "rejected_calibration": 30,
            "rejected_binding": 15,
            "rejected_safety": 5,
            "delivered": 40,
            "untracked": 0,
        }
        repo.get_divergence_stats_by_stage.return_value = [
            {
                "stage": "delivered",
                "total": 40,
                "resolved": 35,
                "wins": 20,
                "losses": 10,
                "neutrals": 5,
                "avg_return": 0.003,
            },
            {
                "stage": "rejected_calibration",
                "total": 30,
                "resolved": 25,
                "wins": 16,
                "losses": 7,
                "neutrals": 2,
                "avg_return": 0.005,
            },
        ]

        report = build_divergence_report(repo)
        assert report.total == 100
        assert report.delivered == 40
        assert report.rejected_calibration == 30
        assert len(report.by_stage) == 2
        assert report.by_stage[0].stage == "delivered"
        assert report.by_stage[0].wins == 20

    def test_empty_repo(self) -> None:
        repo = MagicMock()
        repo.get_divergence_stats.return_value = {}

        report = build_divergence_report(repo)
        assert report.total == 0
        assert report.by_stage == []


# -- E. format_divergence_report --


class TestFormatReport:
    def _make_report(self) -> DivergenceReport:
        return DivergenceReport(
            total=100,
            generated=10,
            rejected_calibration=30,
            rejected_binding=15,
            rejected_safety=5,
            delivered=40,
            untracked=0,
            by_stage=[
                StageStats(
                    stage="delivered",
                    total=40,
                    resolved=35,
                    wins=20,
                    losses=10,
                    neutrals=5,
                    avg_return=0.003,
                ),
                StageStats(
                    stage="rejected_calibration",
                    total=30,
                    resolved=25,
                    wins=16,
                    losses=7,
                    neutrals=2,
                    avg_return=0.005,
                ),
            ],
        )

    def test_contains_funnel(self) -> None:
        text = format_divergence_report(self._make_report())
        assert "Signal funnel:" in text
        assert "generated:" in text
        assert "after calibration:" in text
        assert "delivered:" in text

    def test_contains_win_rates(self) -> None:
        text = format_divergence_report(self._make_report())
        assert "Win rates by stage:" in text
        assert "delivered" in text

    def test_insight_higher_rejected_win_rate(self) -> None:
        text = format_divergence_report(self._make_report())
        # rejected_calibration has 64% WR vs delivered 57% -> insight
        assert "HIGHER win rate" in text

    def test_insight_higher_rejected_return(self) -> None:
        text = format_divergence_report(self._make_report())
        # rejected_calibration avg_return 0.005 > delivered 0.003
        assert "HIGHER avg return" in text

    def test_empty_report(self) -> None:
        text = format_divergence_report(DivergenceReport())
        assert "Signal funnel:" in text
        assert "no tracked signals yet" in text

    def test_untracked_shown(self) -> None:
        report = DivergenceReport(total=50, untracked=50)
        text = format_divergence_report(report)
        assert "untracked" in text


# -- F. NULL-safe behavior --


class TestNullSafe:
    def test_stage_stats_none_avg_return(self) -> None:
        s = StageStats(stage="test", avg_return=None)
        assert s.avg_return is None
        assert s.win_rate is None

    def test_report_no_by_stage(self) -> None:
        report = DivergenceReport(
            total=10, generated=5, delivered=5,
        )
        text = format_divergence_report(report)
        assert "Signal funnel:" in text

    def test_format_with_zero_counts(self) -> None:
        report = DivergenceReport(
            total=5, generated=5,
            rejected_calibration=0,
            rejected_binding=0,
            rejected_safety=0,
            delivered=0,
        )
        text = format_divergence_report(report)
        assert "generated:" in text


# -- G. Repository update_signal_stage --


class TestUpdateSignalStage:
    def test_update_calls_repo(self) -> None:
        repo = MagicMock()
        repo.update_signal_stage.return_value = True

        result = repo.update_signal_stage(
            123, "rejected_calibration", "low_confidence",
        )
        assert result is True
        repo.update_signal_stage.assert_called_once_with(
            123, "rejected_calibration", "low_confidence",
        )

    def test_update_with_none_reason(self) -> None:
        repo = MagicMock()
        repo.update_signal_stage.return_value = True

        result = repo.update_signal_stage(123, "delivered", None)
        assert result is True


# -- H. Rejected signals performance --


class TestRejectedSignals:
    def test_get_rejected_signals(self) -> None:
        repo = MagicMock()
        repo.get_rejected_signals.return_value = [
            {
                "id": 1,
                "ticker": "SBER",
                "signal_type": "up",
                "confidence": 0.45,
                "rejection_reason": "low_confidence",
                "created_at": datetime(2026, 3, 21, tzinfo=UTC),
                "return_pct": 0.003,
                "outcome_label": "win",
            },
        ]

        signals = repo.get_rejected_signals("rejected_calibration", 10)
        assert len(signals) == 1
        assert signals[0]["outcome_label"] == "win"
        assert signals[0]["rejection_reason"] == "low_confidence"
