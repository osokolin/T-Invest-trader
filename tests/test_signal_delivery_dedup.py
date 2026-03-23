"""Tests for semantic delivery deduplication."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from tinvest_trader.services.signal_delivery_dedup import (
    DeliveryDecision,
    DeliveryDedupConfig,
    should_deliver_signal,
)


def _signal(**kw) -> dict:
    base = {
        "id": 1,
        "ticker": "SBER",
        "signal_type": "up",
        "confidence": 0.33,
        "severity": "MEDIUM",
    }
    base.update(kw)
    return base


def _prev(**kw) -> dict:
    base = {
        "id": 0,
        "ticker": "SBER",
        "signal_type": "up",
        "confidence": 0.33,
        "severity": "MEDIUM",
        "delivered_at": datetime.now(UTC) - timedelta(minutes=5),
    }
    base.update(kw)
    return base


# -- No previous -> deliver --


class TestNoPrevious:
    def test_no_previous_delivers(self):
        d = should_deliver_signal(_signal(), None)
        assert d.deliver is True
        assert d.reason == "no_previous"


# -- Same state -> suppressed --


class TestSameState:
    def test_same_state_suppressed(self):
        d = should_deliver_signal(_signal(), _prev())
        assert d.deliver is False
        assert d.reason == "same_state_suppressed"

    def test_small_confidence_delta_suppressed(self):
        d = should_deliver_signal(
            _signal(confidence=0.35),
            _prev(confidence=0.33),
        )
        assert d.deliver is False

    def test_same_state_with_none_severity_suppressed(self):
        d = should_deliver_signal(
            _signal(severity=None),
            _prev(severity=None),
        )
        assert d.deliver is False


# -- Direction change -> deliver --


class TestDirectionChange:
    def test_direction_changed_delivers(self):
        d = should_deliver_signal(
            _signal(signal_type="down"),
            _prev(signal_type="up"),
        )
        assert d.deliver is True
        assert d.reason == "direction_changed"


# -- Severity change -> deliver --


class TestSeverityChange:
    def test_severity_upgrade_delivers(self):
        d = should_deliver_signal(
            _signal(severity="HIGH"),
            _prev(severity="MEDIUM"),
        )
        assert d.deliver is True
        assert d.reason == "severity_changed"

    def test_severity_downgrade_delivers(self):
        d = should_deliver_signal(
            _signal(severity="LOW"),
            _prev(severity="HIGH"),
        )
        assert d.deliver is True
        assert d.reason == "severity_changed"


# -- Confidence jump -> deliver --


class TestConfidenceJump:
    def test_confidence_jump_delivers(self):
        d = should_deliver_signal(
            _signal(confidence=0.50),
            _prev(confidence=0.33),
        )
        assert d.deliver is True
        assert d.reason == "confidence_jump"

    def test_confidence_drop_delivers(self):
        d = should_deliver_signal(
            _signal(confidence=0.20),
            _prev(confidence=0.33),
        )
        assert d.deliver is True
        assert d.reason == "confidence_jump"

    def test_just_below_threshold_suppressed(self):
        d = should_deliver_signal(
            _signal(confidence=0.42),
            _prev(confidence=0.33),
            DeliveryDedupConfig(confidence_delta=0.10),
        )
        assert d.deliver is False


# -- Repeat-after timeout -> deliver --


class TestRepeatAfter:
    def test_old_delivery_allows_repeat(self):
        old = _prev(
            delivered_at=datetime.now(UTC) - timedelta(hours=5),
        )
        d = should_deliver_signal(
            _signal(), old,
            DeliveryDedupConfig(repeat_after_minutes=240),
        )
        assert d.deliver is True
        assert d.reason == "repeat_after_timeout"

    def test_recent_delivery_suppressed(self):
        recent = _prev(
            delivered_at=datetime.now(UTC) - timedelta(minutes=10),
        )
        d = should_deliver_signal(
            _signal(), recent,
            DeliveryDedupConfig(repeat_after_minutes=240),
        )
        assert d.deliver is False


# -- Dedup disabled -> always deliver --


class TestDisabled:
    def test_disabled_always_delivers(self):
        d = should_deliver_signal(
            _signal(), _prev(),
            DeliveryDedupConfig(enabled=False),
        )
        assert d.deliver is True
        assert d.reason == "dedup_disabled"


# -- Suppressed signals don't get retried --


class TestSuppressedNotRetried:
    def test_suppressed_stage_set(self):
        """The delivery integration marks suppressed signals with
        pipeline_stage='suppressed_delivery' so they don't appear
        in list_undelivered_signals (which filters on 'generated').
        This test verifies the decision logic only."""
        d = should_deliver_signal(_signal(), _prev())
        assert d.deliver is False
        # The caller is responsible for setting pipeline_stage


# -- DeliveryDecision --


class TestDeliveryDecision:
    def test_dataclass_fields(self):
        d = DeliveryDecision(deliver=True, reason="test")
        assert d.deliver is True
        assert d.reason == "test"
