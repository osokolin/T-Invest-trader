"""Tests for signal severity classification and enriched formatting."""

from __future__ import annotations

from datetime import UTC, datetime

from tinvest_trader.services.signal_severity import (
    SeverityConfig,
    SeverityResult,
    classify_signal_severity,
    format_enriched_signal_message,
    severity_sort_key,
)


def _make_signal(**overrides: object) -> dict:
    base = {
        "id": 1,
        "ticker": "SBER",
        "signal_type": "up",
        "confidence": 0.63,
        "source": "fusion",
        "price_at_signal": 320.50,
        "created_at": datetime(2026, 3, 21, 12, 5, tzinfo=UTC),
        "source_channel": "interfaxonline",
        "return_pct": None,
        "outcome_label": None,
    }
    base.update(overrides)
    return base


def _make_ticker_stats(**overrides: object) -> dict:
    base = {
        "ticker": "SBER",
        "total": 20,
        "resolved": 15,
        "wins": 9,
        "avg_return": 0.005,
    }
    base.update(overrides)
    return base


def _make_type_stats(**overrides: object) -> dict:
    base = {
        "signal_type": "up",
        "total": 30,
        "resolved": 25,
        "wins": 15,
        "avg_return": 0.004,
    }
    base.update(overrides)
    return base


def _make_source_stats(**overrides: object) -> dict:
    base = {
        "source_channel": "interfaxonline",
        "total": 10,
        "resolved": 8,
        "wins": 5,
        "losses": 2,
        "neutrals": 1,
        "avg_return": 0.006,
    }
    base.update(overrides)
    return base


# -- A. Severity classification --


class TestClassifyHigh:
    def test_high_confidence_and_strong_ev(self) -> None:
        signal = _make_signal(confidence=0.75)
        ticker_stats = _make_ticker_stats(wins=12, resolved=15, avg_return=0.01)
        result = classify_signal_severity(signal, ticker_stats=ticker_stats)
        assert result.level == "HIGH"

    def test_high_confidence_strong_ev_with_source(self) -> None:
        signal = _make_signal(confidence=0.75)
        ticker_stats = _make_ticker_stats(wins=12, resolved=15, avg_return=0.01)
        source_stats = _make_source_stats(wins=6, resolved=8, avg_return=0.008)
        result = classify_signal_severity(
            signal, ticker_stats=ticker_stats, source_stats=source_stats,
        )
        assert result.level == "HIGH"

    def test_reasons_populated(self) -> None:
        signal = _make_signal(confidence=0.75)
        result = classify_signal_severity(signal)
        assert len(result.reasons) > 0
        assert any("confidence" in r for r in result.reasons)


class TestClassifyMedium:
    def test_moderate_confidence_no_stats(self) -> None:
        signal = _make_signal(confidence=0.50)
        result = classify_signal_severity(signal)
        assert result.level == "MEDIUM"

    def test_high_confidence_but_weak_ev(self) -> None:
        signal = _make_signal(confidence=0.65)
        ticker_stats = _make_ticker_stats(wins=5, resolved=15, avg_return=-0.002)
        result = classify_signal_severity(signal, ticker_stats=ticker_stats)
        assert result.level == "MEDIUM"


class TestClassifyLow:
    def test_low_confidence(self) -> None:
        signal = _make_signal(confidence=0.20)
        result = classify_signal_severity(signal)
        assert result.level == "LOW"

    def test_low_confidence_negative_ev(self) -> None:
        signal = _make_signal(confidence=0.20)
        ticker_stats = _make_ticker_stats(wins=3, resolved=15, avg_return=-0.01)
        result = classify_signal_severity(signal, ticker_stats=ticker_stats)
        assert result.level == "LOW"


class TestClassifyEdgeCases:
    def test_none_confidence(self) -> None:
        signal = _make_signal(confidence=None)
        result = classify_signal_severity(signal)
        assert result.level in ("HIGH", "MEDIUM", "LOW")

    def test_no_stats(self) -> None:
        signal = _make_signal(confidence=0.50)
        result = classify_signal_severity(signal)
        assert result.level in ("HIGH", "MEDIUM", "LOW")

    def test_stats_below_min_resolved(self) -> None:
        signal = _make_signal(confidence=0.50)
        ticker_stats = _make_ticker_stats(resolved=1)
        result = classify_signal_severity(signal, ticker_stats=ticker_stats)
        # Stats with too few resolved signals are ignored
        assert result.level in ("MEDIUM", "LOW")

    def test_custom_config(self) -> None:
        cfg = SeverityConfig(high_confidence=0.9, high_ev=0.1)
        signal = _make_signal(confidence=0.75)
        result = classify_signal_severity(signal, config=cfg)
        # 0.75 < 0.9 so not top tier
        assert result.level != "HIGH"


# -- B. Enriched message formatting --


class TestFormatEnrichedMessage:
    def test_contains_severity_header(self) -> None:
        severity = SeverityResult(level="HIGH", reasons=["confidence OK"])
        msg = format_enriched_signal_message(_make_signal(), severity)
        assert "HIGH" in msg
        assert "SBER" in msg

    def test_contains_direction(self) -> None:
        severity = SeverityResult(level="MEDIUM", reasons=[])
        msg = format_enriched_signal_message(_make_signal(), severity)
        assert "UP" in msg

    def test_contains_confidence(self) -> None:
        severity = SeverityResult(level="MEDIUM", reasons=[])
        msg = format_enriched_signal_message(_make_signal(), severity)
        assert "0.63" in msg

    def test_contains_price(self) -> None:
        severity = SeverityResult(level="MEDIUM", reasons=[])
        msg = format_enriched_signal_message(_make_signal(), severity)
        assert "320.50" in msg

    def test_contains_source(self) -> None:
        severity = SeverityResult(level="MEDIUM", reasons=[])
        msg = format_enriched_signal_message(_make_signal(), severity)
        assert "interfaxonline" in msg

    def test_contains_time(self) -> None:
        severity = SeverityResult(level="MEDIUM", reasons=[])
        msg = format_enriched_signal_message(_make_signal(), severity)
        assert "2026-03-21 15:05 MSK" in msg

    def test_contains_ticker_stats(self) -> None:
        severity = SeverityResult(level="HIGH", reasons=["ev ok"])
        ticker_stats = _make_ticker_stats()
        msg = format_enriched_signal_message(
            _make_signal(), severity, ticker_stats=ticker_stats,
        )
        assert "Ticker:" in msg
        assert "win" in msg

    def test_contains_type_stats(self) -> None:
        severity = SeverityResult(level="HIGH", reasons=[])
        type_stats = _make_type_stats()
        msg = format_enriched_signal_message(
            _make_signal(), severity, type_stats=type_stats,
        )
        assert "Type UP:" in msg

    def test_contains_pass_reasons(self) -> None:
        severity = SeverityResult(
            level="HIGH", reasons=["confidence OK", "positive EV"],
        )
        msg = format_enriched_signal_message(_make_signal(), severity)
        assert "Passed:" in msg
        assert "confidence OK" in msg

    def test_no_source_channel_omits_source_line(self) -> None:
        severity = SeverityResult(level="MEDIUM", reasons=[])
        signal = _make_signal(source_channel=None, source=None)
        msg = format_enriched_signal_message(signal, severity)
        assert "Source:" not in msg

    def test_none_values_show_na(self) -> None:
        severity = SeverityResult(level="LOW", reasons=[])
        signal = _make_signal(confidence=None, price_at_signal=None)
        msg = format_enriched_signal_message(signal, severity)
        assert "n/a" in msg

    def test_outcome_included(self) -> None:
        severity = SeverityResult(level="MEDIUM", reasons=[])
        signal = _make_signal(outcome_label="win", return_pct=0.003)
        msg = format_enriched_signal_message(signal, severity)
        assert "win" in msg
        assert "+0.30" in msg

    def test_max_three_reasons(self) -> None:
        reasons = ["r1", "r2", "r3", "r4", "r5"]
        severity = SeverityResult(level="HIGH", reasons=reasons)
        msg = format_enriched_signal_message(_make_signal(), severity)
        assert "r1" in msg
        assert "r3" in msg
        assert "r4" not in msg


# -- C. Ordering (HIGH before LOW) --


class TestSeverityOrdering:
    def test_high_before_medium_before_low(self) -> None:
        levels = ["LOW", "HIGH", "MEDIUM", "LOW", "HIGH"]
        sorted_levels = sorted(levels, key=severity_sort_key)
        assert sorted_levels == ["HIGH", "HIGH", "MEDIUM", "LOW", "LOW"]

    def test_unknown_level_sorts_last(self) -> None:
        assert severity_sort_key("UNKNOWN") == 2
        assert severity_sort_key("HIGH") == 0


# -- D. SeverityResult dataclass --


class TestSeverityResult:
    def test_defaults(self) -> None:
        r = SeverityResult(level="MEDIUM")
        assert r.level == "MEDIUM"
        assert r.reasons == []

    def test_with_reasons(self) -> None:
        r = SeverityResult(level="HIGH", reasons=["a", "b"])
        assert len(r.reasons) == 2
