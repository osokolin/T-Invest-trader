"""Tests for AI agreement snapshot in signal messages."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tinvest_trader.services.signal_severity import (
    AGREE,
    DISAGREE,
    NOT_ANALYZED,
    PARTIAL,
    classify_agreement,
    format_ai_snapshot,
)

# -- A. Agreement classification --


class TestClassifyAgreement:
    # AGREE: same level
    def test_high_high(self) -> None:
        assert classify_agreement("HIGH", "HIGH") == AGREE

    def test_medium_medium(self) -> None:
        assert classify_agreement("MEDIUM", "MEDIUM") == AGREE

    def test_low_low(self) -> None:
        assert classify_agreement("LOW", "LOW") == AGREE

    # PARTIAL: one step apart
    def test_high_medium(self) -> None:
        assert classify_agreement("HIGH", "MEDIUM") == PARTIAL

    def test_medium_high(self) -> None:
        assert classify_agreement("MEDIUM", "HIGH") == PARTIAL

    def test_medium_low(self) -> None:
        assert classify_agreement("MEDIUM", "LOW") == PARTIAL

    def test_low_medium(self) -> None:
        assert classify_agreement("LOW", "MEDIUM") == PARTIAL

    # DISAGREE: two steps apart
    def test_high_low(self) -> None:
        assert classify_agreement("HIGH", "LOW") == DISAGREE

    def test_low_high(self) -> None:
        assert classify_agreement("LOW", "HIGH") == DISAGREE

    # NOT_ANALYZED: unknown inputs
    def test_unknown_confidence(self) -> None:
        assert classify_agreement("HIGH", "UNKNOWN") == NOT_ANALYZED

    def test_unknown_severity(self) -> None:
        assert classify_agreement("UNKNOWN", "HIGH") == NOT_ANALYZED

    def test_both_unknown(self) -> None:
        assert classify_agreement("UNKNOWN", "UNKNOWN") == NOT_ANALYZED

    def test_empty_strings(self) -> None:
        assert classify_agreement("", "") == NOT_ANALYZED

    # Case insensitive
    def test_case_insensitive(self) -> None:
        assert classify_agreement("high", "high") == AGREE
        assert classify_agreement("High", "Low") == DISAGREE


# -- B. Formatting with AI present --


class TestFormatAiSnapshotPresent:
    def test_agree(self) -> None:
        snapshot = {"ai_confidence": "HIGH", "ai_actionability": "CONSIDER"}
        result = format_ai_snapshot(snapshot, "HIGH")
        assert "AI: HIGH / CONSIDER" in result
        assert "AGREE" in result
        assert "\u2705" in result  # check mark emoji

    def test_disagree(self) -> None:
        snapshot = {"ai_confidence": "LOW", "ai_actionability": "WATCH"}
        result = format_ai_snapshot(snapshot, "HIGH")
        assert "AI: LOW / WATCH" in result
        assert "DISAGREE" in result
        assert "\u26a0" in result  # warning emoji

    def test_partial(self) -> None:
        snapshot = {"ai_confidence": "MEDIUM", "ai_actionability": "CONSIDER"}
        result = format_ai_snapshot(snapshot, "HIGH")
        assert "PARTIAL" in result
        assert "\u2696" in result  # balance emoji

    def test_two_lines(self) -> None:
        snapshot = {"ai_confidence": "HIGH", "ai_actionability": "CONSIDER"}
        result = format_ai_snapshot(snapshot, "HIGH")
        lines = result.strip().split("\n")
        assert len(lines) == 2


# -- C. Formatting without AI --


class TestFormatAiSnapshotMissing:
    def test_none_snapshot(self) -> None:
        result = format_ai_snapshot(None, "HIGH")
        assert "not analyzed yet" in result
        assert "\u2139" in result  # info emoji

    def test_unknown_confidence(self) -> None:
        snapshot = {"ai_confidence": "UNKNOWN", "ai_actionability": "UNKNOWN"}
        result = format_ai_snapshot(snapshot, "HIGH")
        assert "not analyzed yet" in result

    def test_missing_confidence_key(self) -> None:
        snapshot = {"ai_actionability": "CONSIDER"}
        result = format_ai_snapshot(snapshot, "HIGH")
        assert "not analyzed yet" in result


# -- D. Message length sanity --


class TestMessageLength:
    def test_snapshot_compact(self) -> None:
        snapshot = {"ai_confidence": "HIGH", "ai_actionability": "CONSIDER"}
        result = format_ai_snapshot(snapshot, "HIGH")
        assert len(result) < 100

    def test_not_analyzed_compact(self) -> None:
        result = format_ai_snapshot(None, "HIGH")
        assert len(result) < 50


# -- E. No crash on missing fields --


class TestNoCrashOnMissingFields:
    def test_empty_dict(self) -> None:
        result = format_ai_snapshot({}, "HIGH")
        assert "not analyzed yet" in result

    def test_none_values(self) -> None:
        snapshot = {"ai_confidence": None, "ai_actionability": None}
        result = format_ai_snapshot(snapshot, "HIGH")
        assert "not analyzed yet" in result

    def test_empty_severity(self) -> None:
        snapshot = {"ai_confidence": "HIGH", "ai_actionability": "CONSIDER"}
        result = format_ai_snapshot(snapshot, "")
        assert "NOT_ANALYZED" in result


# -- F. Delivery integration --


class TestDeliveryIntegration:
    def test_message_includes_ai_snapshot(self) -> None:
        """deliver_signal appends AI snapshot to message."""
        from tinvest_trader.services.signal_delivery import deliver_signal

        signal = {
            "id": 77,
            "ticker": "SBER",
            "signal_type": "up",
            "confidence": 0.7,
            "source": "fusion",
            "price_at_signal": 250.0,
            "created_at": datetime(2025, 3, 20, 12, 0, tzinfo=UTC),
            "source_channel": "interfaxonline",
            "return_pct": 0.005,
            "outcome_label": "win",
        }
        repo = MagicMock()
        repo.get_signal_stats_by_ticker.return_value = []
        repo.get_signal_stats_by_type.return_value = []
        repo.get_signal_stats_by_source.return_value = []
        repo.get_ai_snapshot.return_value = {
            "ai_confidence": "LOW",
            "ai_actionability": "WATCH",
        }

        with patch(
            "tinvest_trader.services.signal_delivery.send_telegram_message",
        ) as mock_send:
            mock_send.return_value = True
            deliver_signal(
                signal, "tok", "123",
                repository=repo, logger=MagicMock(),
            )
            mock_send.assert_called_once()
            text = mock_send.call_args[0][2]
            assert "AI: LOW / WATCH" in text
            # conf=0.7 -> score=2 -> MEDIUM severity; MEDIUM vs LOW = PARTIAL
            assert "PARTIAL" in text

    def test_message_without_ai(self) -> None:
        """deliver_signal shows 'not analyzed' when no AI data."""
        from tinvest_trader.services.signal_delivery import deliver_signal

        signal = {
            "id": 78,
            "ticker": "GAZP",
            "signal_type": "down",
            "confidence": 0.5,
            "source": "fusion",
            "price_at_signal": 150.0,
            "created_at": datetime(2025, 3, 20, 11, 0, tzinfo=UTC),
            "source_channel": None,
            "return_pct": None,
            "outcome_label": None,
        }
        repo = MagicMock()
        repo.get_signal_stats_by_ticker.return_value = []
        repo.get_signal_stats_by_type.return_value = []
        repo.get_signal_stats_by_source.return_value = []
        repo.get_ai_snapshot.return_value = None

        with patch(
            "tinvest_trader.services.signal_delivery.send_telegram_message",
        ) as mock_send:
            mock_send.return_value = True
            deliver_signal(
                signal, "tok", "123",
                repository=repo, logger=MagicMock(),
            )
            text = mock_send.call_args[0][2]
            assert "not analyzed yet" in text

    def test_ai_snapshot_failure_does_not_block(self) -> None:
        """AI snapshot lookup failure doesn't prevent delivery."""
        from tinvest_trader.services.signal_delivery import deliver_signal

        signal = {
            "id": 79,
            "ticker": "VTBR",
            "signal_type": "up",
            "confidence": 0.6,
            "source": "fusion",
            "price_at_signal": 100.0,
            "created_at": datetime(2025, 3, 20, 10, 0, tzinfo=UTC),
            "source_channel": None,
            "return_pct": None,
            "outcome_label": None,
        }
        repo = MagicMock()
        repo.get_signal_stats_by_ticker.return_value = []
        repo.get_signal_stats_by_type.return_value = []
        repo.get_signal_stats_by_source.return_value = []
        repo.get_ai_snapshot.side_effect = RuntimeError("db down")

        with patch(
            "tinvest_trader.services.signal_delivery.send_telegram_message",
        ) as mock_send:
            mock_send.return_value = True
            result = deliver_signal(
                signal, "tok", "123",
                repository=repo, logger=MagicMock(),
            )
            assert result.sent is True
            text = mock_send.call_args[0][2]
            assert "not analyzed yet" in text
