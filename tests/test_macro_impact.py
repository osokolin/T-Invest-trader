"""Tests for macro impact analysis service."""

from __future__ import annotations

from unittest.mock import MagicMock

from tinvest_trader.services.macro_impact import (
    MacroImpactReport,
    TagPerformance,
    _generate_insights,
    _to_perf,
    build_macro_impact_report,
    format_macro_impact_report,
)

# ── _to_perf ──


class TestToPerf:
    def test_basic_conversion(self):
        row = {
            "total_signals": 42,
            "resolved": 38,
            "wins": 23,
            "losses": 12,
            "neutrals": 3,
            "avg_return": 0.05123,
        }
        perf = _to_perf("oil", row)
        assert perf.label == "oil"
        assert perf.total_signals == 42
        assert perf.resolved == 38
        assert perf.wins == 23
        assert perf.losses == 12
        assert perf.neutrals == 3
        assert perf.win_rate is not None
        assert abs(perf.win_rate - 23 / 38) < 0.001
        assert perf.avg_return is not None
        assert abs(perf.avg_return - 0.05123) < 0.0001

    def test_zero_resolved(self):
        row = {
            "total_signals": 5, "resolved": 0, "wins": 0,
            "losses": 0, "neutrals": 0, "avg_return": None,
        }
        perf = _to_perf("empty", row)
        assert perf.win_rate is None
        assert perf.avg_return is None

    def test_ev_equals_avg_return(self):
        row = {
            "total_signals": 10, "resolved": 10, "wins": 7,
            "losses": 3, "neutrals": 0, "avg_return": 0.03,
        }
        perf = _to_perf("test", row)
        assert perf.ev == perf.avg_return

    def test_missing_keys_default_to_zero(self):
        row = {}
        perf = _to_perf("sparse", row)
        assert perf.total_signals == 0
        assert perf.resolved == 0
        assert perf.wins == 0


# ── TagPerformance ──


class TestTagPerformance:
    def test_ev_is_avg_return(self):
        perf = TagPerformance(label="x", avg_return=0.05)
        assert perf.ev == 0.05

    def test_ev_none_when_no_return(self):
        perf = TagPerformance(label="x")
        assert perf.ev is None


# ── _generate_insights ──


class TestGenerateInsights:
    def test_no_baseline_no_insights(self):
        report = MacroImpactReport(baseline=None)
        insights = _generate_insights(report)
        assert insights == []

    def test_supportive_tag(self):
        report = MacroImpactReport(
            baseline=TagPerformance(label="all", win_rate=0.50),
            by_tag=[
                TagPerformance(label="oil", win_rate=0.65, resolved=10),
            ],
            min_resolved=5,
        )
        insights = _generate_insights(report)
        assert any("oil" in i and "supportive" in i for i in insights)

    def test_headwind_tag(self):
        report = MacroImpactReport(
            baseline=TagPerformance(label="all", win_rate=0.55),
            by_tag=[
                TagPerformance(label="risk", win_rate=0.40, resolved=10),
            ],
            min_resolved=5,
        )
        insights = _generate_insights(report)
        assert any("risk" in i and "headwind" in i for i in insights)

    def test_no_insight_when_close_to_baseline(self):
        report = MacroImpactReport(
            baseline=TagPerformance(label="all", win_rate=0.50),
            by_tag=[
                TagPerformance(label="gas", win_rate=0.52, resolved=10),
            ],
            min_resolved=5,
        )
        insights = _generate_insights(report)
        assert not any("gas" in i for i in insights)

    def test_insufficient_data_skipped(self):
        report = MacroImpactReport(
            baseline=TagPerformance(label="all", win_rate=0.50),
            by_tag=[
                TagPerformance(label="oil", win_rate=0.80, resolved=3),
            ],
            min_resolved=5,
        )
        insights = _generate_insights(report)
        assert not any("oil" in i for i in insights)

    def test_directional_bias_detected(self):
        report = MacroImpactReport(
            baseline=TagPerformance(label="all", win_rate=0.50),
            by_tag_direction=[
                TagPerformance(label="risk / up", win_rate=0.35, resolved=10),
                TagPerformance(label="risk / down", win_rate=0.65, resolved=10),
            ],
            min_resolved=5,
        )
        insights = _generate_insights(report)
        assert any("risk" in i and "directional" in i for i in insights)

    def test_no_directional_bias_when_close(self):
        report = MacroImpactReport(
            baseline=TagPerformance(label="all", win_rate=0.50),
            by_tag_direction=[
                TagPerformance(label="oil / up", win_rate=0.52, resolved=10),
                TagPerformance(label="oil / down", win_rate=0.48, resolved=10),
            ],
            min_resolved=5,
        )
        insights = _generate_insights(report)
        assert not any("directional" in i for i in insights)


# ── build_macro_impact_report ──


class TestBuildMacroImpactReport:
    def _mock_repo(self):
        repo = MagicMock()
        repo.get_macro_impact_baseline.return_value = {
            "total_signals": 100,
            "resolved": 80,
            "wins": 45,
            "losses": 30,
            "neutrals": 5,
            "avg_return": 0.02,
        }
        repo.get_macro_impact_by_tag.return_value = [
            {"tag": "oil", "total_signals": 20, "resolved": 18,
             "wins": 12, "losses": 5, "neutrals": 1, "avg_return": 0.04},
        ]
        repo.get_macro_impact_by_tag_and_ticker.return_value = [
            {"tag": "oil", "ticker": "LKOH", "total_signals": 10,
             "resolved": 9, "wins": 7, "losses": 2, "neutrals": 0,
             "avg_return": 0.06},
        ]
        repo.get_macro_impact_by_tag_and_direction.return_value = [
            {"tag": "oil", "direction": "up", "total_signals": 15,
             "resolved": 13, "wins": 9, "losses": 3, "neutrals": 1,
             "avg_return": 0.05},
        ]
        return repo

    def test_report_has_all_sections(self):
        repo = self._mock_repo()
        report = build_macro_impact_report(repo)
        assert report.baseline is not None
        assert len(report.by_tag) == 1
        assert len(report.by_tag_ticker) == 1
        assert len(report.by_tag_direction) == 1

    def test_report_passes_window_minutes(self):
        repo = self._mock_repo()
        build_macro_impact_report(repo, window_minutes=30)
        repo.get_macro_impact_by_tag.assert_called_once_with(
            window_minutes=30, min_resolved=5,
        )

    def test_report_passes_min_resolved(self):
        repo = self._mock_repo()
        build_macro_impact_report(repo, min_resolved=10)
        repo.get_macro_impact_by_tag.assert_called_once_with(
            window_minutes=60, min_resolved=10,
        )

    def test_empty_repo(self):
        repo = MagicMock()
        repo.get_macro_impact_baseline.return_value = {}
        repo.get_macro_impact_by_tag.return_value = []
        repo.get_macro_impact_by_tag_and_ticker.return_value = []
        repo.get_macro_impact_by_tag_and_direction.return_value = []
        report = build_macro_impact_report(repo)
        assert report.baseline is None
        assert report.by_tag == []


# ── format_macro_impact_report ──


class TestFormatReport:
    def test_format_contains_sections(self):
        report = MacroImpactReport(
            window_minutes=60,
            min_resolved=5,
            baseline=TagPerformance(
                label="all_signals",
                total_signals=100,
                resolved=80,
                wins=45,
                losses=30,
                neutrals=5,
                win_rate=0.5625,
                avg_return=0.02,
            ),
            by_tag=[
                TagPerformance(
                    label="oil",
                    total_signals=20,
                    resolved=18,
                    wins=12,
                    losses=5,
                    neutrals=1,
                    win_rate=0.6667,
                    avg_return=0.04,
                ),
            ],
        )
        text = format_macro_impact_report(report)
        assert "macro impact report" in text
        assert "baseline" in text
        assert "by tag:" in text
        assert "oil" in text
        assert "window: 60m" in text

    def test_format_empty_report(self):
        report = MacroImpactReport()
        text = format_macro_impact_report(report)
        assert "not enough data" in text

    def test_format_with_insights(self):
        report = MacroImpactReport(
            insights=["oil: supportive (+15% vs baseline)"],
        )
        text = format_macro_impact_report(report)
        assert "insights:" in text
        assert "oil: supportive" in text

    def test_format_tag_direction(self):
        report = MacroImpactReport(
            by_tag_direction=[
                TagPerformance(
                    label="risk / up",
                    total_signals=15,
                    resolved=12,
                    wins=5,
                    losses=6,
                    neutrals=1,
                    win_rate=0.4167,
                    avg_return=-0.01,
                ),
            ],
        )
        text = format_macro_impact_report(report)
        assert "risk / up" in text
        assert "by tag + direction:" in text
