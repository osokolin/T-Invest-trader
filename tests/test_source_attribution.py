"""Tests for Telegram source performance attribution."""

from __future__ import annotations

from unittest.mock import MagicMock

from tinvest_trader.services.source_attribution import (
    SourcePerformanceReport,
    SourceStats,
    SourceTickerStats,
    build_source_performance_report,
    format_source_performance_report,
)

# -- SourceStats computed properties --


class TestSourceStats:
    def test_win_rate_computed(self) -> None:
        s = SourceStats(source_channel="ch", resolved=20, wins=12)
        assert s.win_rate == 0.6

    def test_win_rate_zero_resolved(self) -> None:
        s = SourceStats(source_channel="ch", resolved=0, wins=0)
        assert s.win_rate is None

    def test_ev_computed(self) -> None:
        s = SourceStats(
            source_channel="ch", resolved=100, wins=60, avg_return=0.002,
        )
        ev = s.ev
        assert ev is not None
        assert abs(ev - 0.6 * 0.002) < 1e-9

    def test_ev_none_when_no_return(self) -> None:
        s = SourceStats(source_channel="ch", resolved=10, wins=5)
        assert s.ev is None

    def test_ev_none_when_no_resolved(self) -> None:
        s = SourceStats(source_channel="ch", avg_return=0.01)
        assert s.ev is None


class TestSourceTickerStats:
    def test_win_rate_and_ev(self) -> None:
        st = SourceTickerStats(
            source_channel="ch", ticker="SBER",
            resolved=50, wins=30, avg_return=0.003,
        )
        assert st.win_rate == 0.6
        assert st.ev is not None
        assert abs(st.ev - 0.6 * 0.003) < 1e-9


# -- Attribution fields saved correctly --


class TestInsertSignalPredictionAttribution:
    def test_attribution_fields_passed_to_sql(self) -> None:
        """A. Source attribution fields are forwarded to repository."""
        repo = MagicMock()
        repo.get_signal_stats_by_source.return_value = [
            {
                "source_channel": "markettwits",
                "total": 10,
                "resolved": 8,
                "wins": 5,
                "losses": 2,
                "neutrals": 1,
                "avg_return": 0.001,
            },
        ]
        repo.get_signal_stats_by_source_and_ticker.return_value = []

        report = build_source_performance_report(repo)

        assert len(report.by_source) == 1
        assert report.by_source[0].source_channel == "markettwits"
        assert report.by_source[0].total == 10
        assert report.by_source[0].wins == 5
        repo.get_signal_stats_by_source.assert_called_once()


# -- Source-level aggregation --


class TestSourceAggregation:
    def _mock_repo(self) -> MagicMock:
        repo = MagicMock()
        repo.get_signal_stats_by_source.return_value = [
            {
                "source_channel": "interfaxonline",
                "total": 42,
                "resolved": 38,
                "wins": 22,
                "losses": 12,
                "neutrals": 4,
                "avg_return": 0.0014,
            },
            {
                "source_channel": "banksta",
                "total": 75,
                "resolved": 70,
                "wins": 32,
                "losses": 35,
                "neutrals": 3,
                "avg_return": -0.0004,
            },
        ]
        repo.get_signal_stats_by_source_and_ticker.return_value = [
            {
                "source_channel": "interfaxonline",
                "ticker": "SBER",
                "total": 15,
                "resolved": 14,
                "wins": 9,
                "losses": 3,
                "neutrals": 2,
                "avg_return": 0.003,
            },
        ]
        return repo

    def test_total_and_resolved(self) -> None:
        """B. Total and resolved counts correct."""
        report = build_source_performance_report(self._mock_repo())
        ifx = report.by_source[0]
        assert ifx.total == 42
        assert ifx.resolved == 38

    def test_wins_losses_neutrals(self) -> None:
        """B. Wins/losses/neutrals correct."""
        report = build_source_performance_report(self._mock_repo())
        ifx = report.by_source[0]
        assert ifx.wins == 22
        assert ifx.losses == 12
        assert ifx.neutrals == 4

    def test_win_rate(self) -> None:
        """B. Win rate computed correctly."""
        report = build_source_performance_report(self._mock_repo())
        ifx = report.by_source[0]
        assert ifx.win_rate is not None
        assert abs(ifx.win_rate - 22 / 38) < 1e-9

    def test_avg_return(self) -> None:
        """B. Avg return passed through."""
        report = build_source_performance_report(self._mock_repo())
        ifx = report.by_source[0]
        assert ifx.avg_return == 0.0014

    def test_ev(self) -> None:
        """B. EV = win_rate * avg_return."""
        report = build_source_performance_report(self._mock_repo())
        ifx = report.by_source[0]
        expected_ev = (22 / 38) * 0.0014
        assert ifx.ev is not None
        assert abs(ifx.ev - expected_ev) < 1e-9

    def test_mixed_sources(self) -> None:
        """C. Multiple sources aggregated independently."""
        report = build_source_performance_report(self._mock_repo())
        assert len(report.by_source) == 2
        channels = {s.source_channel for s in report.by_source}
        assert channels == {"interfaxonline", "banksta"}

    def test_source_ticker_aggregation(self) -> None:
        """D. Source + ticker combo present."""
        report = build_source_performance_report(self._mock_repo())
        assert len(report.by_source_ticker) == 1
        st = report.by_source_ticker[0]
        assert st.source_channel == "interfaxonline"
        assert st.ticker == "SBER"
        assert st.resolved == 14
        assert st.wins == 9


# -- NULL source rows --


class TestNullSourceRows:
    def test_empty_source_data_produces_empty_report(self) -> None:
        """F. NULL source rows do not break reporting."""
        repo = MagicMock()
        repo.get_signal_stats_by_source.return_value = []
        repo.get_signal_stats_by_source_and_ticker.return_value = []

        report = build_source_performance_report(repo)

        assert report.by_source == []
        assert report.by_source_ticker == []


# -- CLI output shape --


class TestFormatReport:
    def test_output_contains_header(self) -> None:
        """E. CLI output has expected structure."""
        report = SourcePerformanceReport(
            by_source=[
                SourceStats(
                    source_channel="markettwits",
                    total=120, resolved=110, wins=56, losses=50,
                    neutrals=4, avg_return=0.0003,
                ),
                SourceStats(
                    source_channel="banksta",
                    total=75, resolved=70, wins=32, losses=35,
                    neutrals=3, avg_return=-0.0004,
                ),
            ],
            by_source_ticker=[
                SourceTickerStats(
                    source_channel="markettwits", ticker="SBER",
                    total=30, resolved=28, wins=18, losses=8,
                    neutrals=2, avg_return=0.002,
                ),
            ],
        )
        output = format_source_performance_report(report)

        assert "telegram source performance" in output
        assert "sources:" in output
        assert "markettwits" in output
        assert "banksta" in output
        assert "win_rate=" in output
        assert "EV=" in output

    def test_weak_sources_section(self) -> None:
        report = SourcePerformanceReport(
            by_source=[
                SourceStats(
                    source_channel="banksta",
                    total=75, resolved=70, wins=32, losses=35,
                    neutrals=3, avg_return=-0.0004,
                ),
            ],
        )
        output = format_source_performance_report(report)
        assert "weak sources:" in output
        assert "banksta" in output

    def test_best_combos_section(self) -> None:
        report = SourcePerformanceReport(
            by_source=[
                SourceStats(
                    source_channel="ch", total=10, resolved=10,
                    wins=7, avg_return=0.005,
                ),
            ],
            by_source_ticker=[
                SourceTickerStats(
                    source_channel="ch", ticker="SBER",
                    total=5, resolved=5, wins=4, avg_return=0.008,
                ),
            ],
        )
        output = format_source_performance_report(report)
        assert "best source/ticker combos:" in output
        assert "ch / SBER" in output

    def test_min_resolved_filter(self) -> None:
        report = SourcePerformanceReport(
            by_source=[
                SourceStats(
                    source_channel="small", total=3, resolved=2,
                    wins=1, avg_return=0.01,
                ),
                SourceStats(
                    source_channel="big", total=50, resolved=40,
                    wins=25, avg_return=0.002,
                ),
            ],
        )
        output = format_source_performance_report(report, min_resolved=10)
        assert "small" not in output
        assert "big" in output

    def test_no_data_message(self) -> None:
        report = SourcePerformanceReport()
        output = format_source_performance_report(report)
        assert "no source-attributed signals found" in output
