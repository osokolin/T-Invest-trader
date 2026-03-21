"""Tests for signal calibration -- execution gate based on historical stats."""

from __future__ import annotations

from tinvest_trader.services.signal_calibration import (
    CalibrationConfig,
    CalibrationDecision,
    compute_ev,
    format_calibration_report,
    should_execute_signal,
)


def _ticker_stats(
    ticker: str = "SBER",
    total: int = 20,
    resolved: int = 20,
    wins: int = 12,
    avg_return: float = 0.001,
) -> dict:
    return {
        "ticker": ticker,
        "total": total,
        "resolved": resolved,
        "wins": wins,
        "avg_return": avg_return,
    }


def _type_stats(
    signal_type: str = "up",
    total: int = 20,
    resolved: int = 20,
    wins: int = 12,
    avg_return: float = 0.001,
) -> dict:
    return {
        "signal_type": signal_type,
        "total": total,
        "resolved": resolved,
        "wins": wins,
        "avg_return": avg_return,
    }


# -- EV calculation --

class TestComputeEv:
    def test_positive_ev(self) -> None:
        assert compute_ev(0.6, 0.01) > 0

    def test_negative_ev(self) -> None:
        assert compute_ev(0.4, -0.01) < 0

    def test_zero_ev(self) -> None:
        assert compute_ev(0.5, 0.0) == 0.0


# -- Confidence threshold --

class TestConfidenceThreshold:
    def test_passes_above_threshold(self) -> None:
        config = CalibrationConfig(min_confidence=0.55)
        result = should_execute_signal(
            "SBER", "up", 0.60, None, None, config,
        )
        assert result.allowed is True

    def test_rejects_below_threshold(self) -> None:
        config = CalibrationConfig(min_confidence=0.55)
        result = should_execute_signal(
            "SBER", "up", 0.50, None, None, config,
        )
        assert result.allowed is False
        assert any("low_confidence" in r for r in result.reasons)

    def test_no_confidence_skips_check(self) -> None:
        config = CalibrationConfig(min_confidence=0.55)
        result = should_execute_signal(
            "SBER", "up", None, None, None, config,
        )
        assert result.allowed is True

    def test_zero_threshold_allows_all(self) -> None:
        config = CalibrationConfig(min_confidence=0.0)
        result = should_execute_signal(
            "SBER", "up", 0.01, None, None, config,
        )
        assert result.allowed is True


# -- Ticker filter --

class TestTickerFilter:
    def test_good_ticker_passes(self) -> None:
        config = CalibrationConfig(min_win_rate=0.5)
        stats = _ticker_stats(wins=12, resolved=20)  # 60% win rate
        result = should_execute_signal(
            "SBER", "up", 0.8, stats, None, config,
        )
        assert result.allowed is True

    def test_bad_ticker_rejected_low_win_rate(self) -> None:
        config = CalibrationConfig(min_win_rate=0.5)
        stats = _ticker_stats(wins=4, resolved=20)  # 20% win rate
        result = should_execute_signal(
            "SBER", "up", 0.8, stats, None, config,
        )
        assert result.allowed is False
        assert any("low_ticker_win_rate" in r for r in result.reasons)

    def test_bad_ticker_rejected_negative_return(self) -> None:
        config = CalibrationConfig()
        stats = _ticker_stats(wins=12, resolved=20, avg_return=-0.005)
        result = should_execute_signal(
            "SBER", "up", 0.8, stats, None, config,
        )
        assert result.allowed is False
        assert any("negative_ticker_return" in r for r in result.reasons)

    def test_insufficient_data_skips_filter(self) -> None:
        config = CalibrationConfig(min_win_rate=0.5, min_resolved_for_filter=10)
        stats = _ticker_stats(wins=1, resolved=3)  # only 3 resolved
        result = should_execute_signal(
            "SBER", "up", 0.8, stats, None, config,
        )
        assert result.allowed is True

    def test_no_stats_passes(self) -> None:
        config = CalibrationConfig(min_win_rate=0.5)
        result = should_execute_signal(
            "SBER", "up", 0.8, None, None, config,
        )
        assert result.allowed is True


# -- Signal type filter --

class TestSignalTypeFilter:
    def test_up_disabled(self) -> None:
        config = CalibrationConfig(enable_up=False)
        result = should_execute_signal(
            "SBER", "up", 0.8, None, None, config,
        )
        assert result.allowed is False
        assert "signal_type_up_disabled" in result.reasons

    def test_down_disabled(self) -> None:
        config = CalibrationConfig(enable_down=False)
        result = should_execute_signal(
            "SBER", "down", 0.8, None, None, config,
        )
        assert result.allowed is False
        assert "signal_type_down_disabled" in result.reasons

    def test_both_enabled(self) -> None:
        config = CalibrationConfig(enable_up=True, enable_down=True)
        r1 = should_execute_signal("SBER", "up", 0.8, None, None, config)
        r2 = should_execute_signal("SBER", "down", 0.8, None, None, config)
        assert r1.allowed is True
        assert r2.allowed is True

    def test_per_type_stats_low_win_rate(self) -> None:
        config = CalibrationConfig(min_win_rate=0.5)
        type_stats = _type_stats(wins=3, resolved=20)  # 15%
        result = should_execute_signal(
            "SBER", "up", 0.8, None, type_stats, config,
        )
        assert result.allowed is False
        assert any("low_type_win_rate" in r for r in result.reasons)


# -- EV filter --

class TestEvFilter:
    def test_positive_ev_passes(self) -> None:
        config = CalibrationConfig(min_ev=0.0001)
        stats = _ticker_stats(wins=12, resolved=20, avg_return=0.002)
        result = should_execute_signal(
            "SBER", "up", 0.8, stats, None, config,
        )
        assert result.allowed is True

    def test_low_ev_rejected(self) -> None:
        config = CalibrationConfig(min_ev=0.001)
        stats = _ticker_stats(wins=10, resolved=20, avg_return=0.0001)
        # EV = 0.5 * 0.0001 = 0.00005 < 0.001
        result = should_execute_signal(
            "SBER", "up", 0.8, stats, None, config,
        )
        assert result.allowed is False
        assert any("low_ev" in r for r in result.reasons)


# -- Multiple reasons --

class TestMultipleReasons:
    def test_accumulates_reasons(self) -> None:
        config = CalibrationConfig(
            min_confidence=0.55,
            min_win_rate=0.5,
        )
        stats = _ticker_stats(wins=3, resolved=20, avg_return=-0.01)
        result = should_execute_signal(
            "SBER", "up", 0.40, stats, None, config,
        )
        assert result.allowed is False
        assert len(result.reasons) >= 2


# -- CalibrationDecision --

class TestCalibrationDecision:
    def test_allowed_has_no_reasons(self) -> None:
        d = CalibrationDecision(allowed=True)
        assert d.reasons == []

    def test_rejected_has_reasons(self) -> None:
        d = CalibrationDecision(allowed=False, reasons=["test"])
        assert d.reasons == ["test"]


# -- Report formatting --

class TestFormatCalibrationReport:
    def test_basic_report(self) -> None:
        config = CalibrationConfig(
            min_confidence=0.55, min_win_rate=0.5,
        )
        by_ticker = [
            _ticker_stats("SBER", total=20, resolved=20, wins=12, avg_return=0.002),
            _ticker_stats("YNDX", total=15, resolved=15, wins=6, avg_return=-0.001),
        ]
        by_type = [
            _type_stats("up", total=20, resolved=18, wins=10, avg_return=0.001),
            _type_stats("down", total=15, resolved=12, wins=5, avg_return=-0.002),
        ]
        output = format_calibration_report(config, by_ticker, by_type)
        assert "min_confidence: 0.55" in output
        assert "SBER" in output
        assert "YNDX" in output
        assert "FILTERED" in output  # YNDX has negative return
        assert "up:" in output
        assert "down:" in output

    def test_pending_ticker(self) -> None:
        config = CalibrationConfig()
        by_ticker = [_ticker_stats("NEW", total=5, resolved=0, wins=0)]
        output = format_calibration_report(config, by_ticker, [])
        assert "pending" in output

    def test_disabled_type(self) -> None:
        config = CalibrationConfig(enable_down=False)
        by_type = [
            _type_stats("down", total=10, resolved=8, wins=3, avg_return=-0.001),
        ]
        output = format_calibration_report(config, [], by_type)
        assert "(disabled)" in output

    def test_empty_data(self) -> None:
        config = CalibrationConfig()
        output = format_calibration_report(config, [], [])
        assert "signal calibration report" in output


# -- Config integration --

class TestConfigIntegration:
    def test_config_fields_exist(self) -> None:
        from tinvest_trader.app.config import SignalCalibrationConfig
        cfg = SignalCalibrationConfig(
            min_confidence=0.55,
            min_win_rate=0.5,
            min_ev=0.001,
            enable_up=True,
            enable_down=False,
            min_resolved_for_filter=10,
        )
        assert cfg.min_confidence == 0.55
        assert cfg.min_win_rate == 0.5
        assert cfg.min_ev == 0.001
        assert cfg.enable_up is True
        assert cfg.enable_down is False
        assert cfg.min_resolved_for_filter == 10
