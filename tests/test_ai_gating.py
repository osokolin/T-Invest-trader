"""Tests for AI shadow-mode gating."""

from __future__ import annotations

from unittest.mock import MagicMock

from tinvest_trader.services.ai_gating import (
    GATE_ALLOW,
    GATE_BLOCK,
    GATE_CAUTION,
    AIGateDecision,
    decide_ai_gate,
)
from tinvest_trader.services.ai_gating_report import (
    AIGatingReport,
    build_ai_gating_report,
    format_ai_gating_report,
)

# -- A. Gating decision rules --


class TestDecideAiGate:
    # BLOCK rules
    def test_block_low_confidence(self) -> None:
        result = decide_ai_gate("LOW", "CONSIDER")
        assert result.decision == GATE_BLOCK
        assert "LOW" in result.reason

    def test_block_weak_actionability(self) -> None:
        result = decide_ai_gate("HIGH", "WEAK")
        assert result.decision == GATE_BLOCK
        assert "WEAK" in result.reason

    def test_block_bearish_divergence(self) -> None:
        result = decide_ai_gate("MEDIUM", "CONSIDER", "ai_more_bearish")
        assert result.decision == GATE_BLOCK
        assert "bearish" in result.reason

    # ALLOW rules
    def test_allow_high_consider(self) -> None:
        result = decide_ai_gate("HIGH", "CONSIDER")
        assert result.decision == GATE_ALLOW
        assert "high_confidence" in result.reason

    def test_allow_high_consider_agree(self) -> None:
        result = decide_ai_gate("HIGH", "CONSIDER", "agree_strong")
        assert result.decision == GATE_ALLOW

    # CAUTION rules
    def test_caution_medium_confidence(self) -> None:
        result = decide_ai_gate("MEDIUM", "CONSIDER")
        assert result.decision == GATE_CAUTION
        assert "MEDIUM" in result.reason

    def test_caution_watch_actionability(self) -> None:
        result = decide_ai_gate("HIGH", "WATCH")
        assert result.decision == GATE_CAUTION
        assert "WATCH" in result.reason

    def test_caution_unknown_confidence(self) -> None:
        result = decide_ai_gate("UNKNOWN", "CONSIDER")
        assert result.decision == GATE_CAUTION
        assert "incomplete" in result.reason

    def test_caution_unknown_actionability(self) -> None:
        result = decide_ai_gate("HIGH", "UNKNOWN")
        assert result.decision == GATE_CAUTION
        assert "incomplete" in result.reason

    def test_caution_is_default(self) -> None:
        result = decide_ai_gate("MEDIUM", "WATCH")
        assert result.decision == GATE_CAUTION

    # Priority: BLOCK > ALLOW > CAUTION
    def test_block_trumps_allow(self) -> None:
        # LOW confidence blocks even if actionability is CONSIDER
        result = decide_ai_gate("LOW", "CONSIDER")
        assert result.decision == GATE_BLOCK

    def test_block_trumps_caution(self) -> None:
        result = decide_ai_gate("MEDIUM", "WEAK")
        assert result.decision == GATE_BLOCK


# -- B. NULL-safe behavior --


class TestNullSafety:
    def test_empty_strings(self) -> None:
        result = decide_ai_gate("", "", "")
        assert result.decision == GATE_CAUTION

    def test_none_like_values(self) -> None:
        result = decide_ai_gate("UNKNOWN", "UNKNOWN", "unknown")
        assert result.decision == GATE_CAUTION

    def test_result_is_dataclass(self) -> None:
        result = decide_ai_gate("HIGH", "CONSIDER")
        assert isinstance(result, AIGateDecision)
        assert result.decision == GATE_ALLOW
        assert isinstance(result.reason, str)


# -- C. Aggregation metrics via report --


def _make_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_ai_gating_stats.return_value = {
        "total_signals": 100,
        "total_with_gate": 80,
        "blocked": 25,
        "caution": 35,
        "allow": 20,
    }
    repo.get_ai_gating_performance.return_value = {
        "baseline": {
            "total": 80,
            "resolved": 60,
            "wins": 33,
            "losses": 22,
            "neutrals": 5,
            "avg_return": 0.008,
        },
        "ai_filtered": {
            "total": 55,
            "resolved": 42,
            "wins": 27,
            "losses": 12,
            "neutrals": 3,
            "avg_return": 0.015,
        },
        "blocked": {
            "total": 25,
            "resolved": 18,
            "wins": 6,
            "losses": 10,
            "neutrals": 2,
            "avg_return": -0.008,
        },
    }
    return repo


class TestBuildAiGatingReport:
    def test_builds_report(self) -> None:
        report = build_ai_gating_report(_make_repo())
        assert report.total_signals == 100
        assert report.total_with_gate == 80
        assert report.blocked_count == 25

    def test_blocked_pct(self) -> None:
        report = build_ai_gating_report(_make_repo())
        assert report.blocked_pct is not None
        assert abs(report.blocked_pct - 25 / 80) < 0.01

    def test_baseline_stats(self) -> None:
        report = build_ai_gating_report(_make_repo())
        assert report.baseline.resolved == 60
        assert report.baseline.wins == 33
        assert report.baseline.win_rate is not None
        assert abs(report.baseline.win_rate - 33 / 60) < 0.01

    def test_filtered_stats(self) -> None:
        report = build_ai_gating_report(_make_repo())
        assert report.ai_filtered.resolved == 42
        assert report.ai_filtered.wins == 27
        assert report.ai_filtered.win_rate is not None
        assert abs(report.ai_filtered.win_rate - 27 / 42) < 0.01

    def test_blocked_stats(self) -> None:
        report = build_ai_gating_report(_make_repo())
        assert report.blocked.resolved == 18
        assert report.blocked.wins == 6

    def test_empty_stats(self) -> None:
        repo = MagicMock()
        repo.get_ai_gating_stats.return_value = {}
        report = build_ai_gating_report(repo)
        assert report.total_signals == 0

    def test_stats_without_perf(self) -> None:
        repo = MagicMock()
        repo.get_ai_gating_stats.return_value = {
            "total_signals": 50,
            "total_with_gate": 30,
            "blocked": 10,
            "caution": 15,
            "allow": 5,
        }
        repo.get_ai_gating_performance.return_value = {}
        report = build_ai_gating_report(repo)
        assert report.total_signals == 50
        assert report.blocked_count == 10
        assert report.baseline.resolved == 0


# -- D. Filtered vs baseline comparison --


class TestFilteredVsBaseline:
    def test_filtered_better_win_rate(self) -> None:
        report = build_ai_gating_report(_make_repo())
        b_wr = report.baseline.win_rate
        f_wr = report.ai_filtered.win_rate
        assert b_wr is not None and f_wr is not None
        assert f_wr > b_wr  # filtering should improve win rate

    def test_blocked_worse_avg_return(self) -> None:
        report = build_ai_gating_report(_make_repo())
        assert report.blocked.avg_return is not None
        assert report.blocked.avg_return < 0

    def test_filtered_better_avg_return(self) -> None:
        report = build_ai_gating_report(_make_repo())
        assert report.ai_filtered.avg_return is not None
        assert report.baseline.avg_return is not None
        assert report.ai_filtered.avg_return > report.baseline.avg_return

    def test_ev_computation(self) -> None:
        report = build_ai_gating_report(_make_repo())
        assert report.baseline.ev is not None
        assert report.ai_filtered.ev is not None
        expected = (33 / 60) * 0.008
        assert abs(report.baseline.ev - expected) < 0.001


# -- E. CLI output shape --


class TestFormatAiGatingReport:
    def test_contains_header(self) -> None:
        report = build_ai_gating_report(_make_repo())
        output = format_ai_gating_report(report)
        assert "SHADOW MODE" in output

    def test_contains_totals(self) -> None:
        report = build_ai_gating_report(_make_repo())
        output = format_ai_gating_report(report)
        assert "total signals: 100" in output
        assert "blocked: 25" in output

    def test_contains_baseline(self) -> None:
        report = build_ai_gating_report(_make_repo())
        output = format_ai_gating_report(report)
        assert "Baseline" in output
        assert "win_rate:" in output

    def test_contains_filtered(self) -> None:
        report = build_ai_gating_report(_make_repo())
        output = format_ai_gating_report(report)
        assert "AI-filtered" in output

    def test_contains_blocked(self) -> None:
        report = build_ai_gating_report(_make_repo())
        output = format_ai_gating_report(report)
        assert "Blocked" in output

    def test_empty_report(self) -> None:
        report = AIGatingReport()
        output = format_ai_gating_report(report)
        assert "no resolved signals yet" in output

    def test_insights_positive_impact(self) -> None:
        report = build_ai_gating_report(_make_repo())
        output = format_ai_gating_report(report)
        # filtered win_rate (64%) > baseline (55%), delta > 2%
        assert "improves win rate" in output

    def test_insights_negative_ev_blocked(self) -> None:
        report = build_ai_gating_report(_make_repo())
        output = format_ai_gating_report(report)
        # blocked avg_return is -0.008
        assert "negative-EV" in output

    def test_no_insights_on_empty(self) -> None:
        report = AIGatingReport()
        output = format_ai_gating_report(report)
        assert "Insights" not in output
