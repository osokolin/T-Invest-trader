"""Tests for Telegram signal delivery service."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tinvest_trader.services.signal_delivery import (
    DeliveryResult,
    deliver_pending_signals,
    deliver_signal,
    format_signal_message,
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
        "return_pct": 0.0014,
        "outcome_label": "win",
    }
    base.update(overrides)
    return base


def _make_repo() -> MagicMock:
    repo = MagicMock()
    repo.mark_signal_delivered.return_value = True
    repo.get_signal_stats_by_ticker.return_value = []
    repo.get_signal_stats_by_type.return_value = []
    repo.get_signal_stats_by_source.return_value = []
    repo.get_last_delivered_signal.return_value = None
    return repo


# -- A. Legacy message formatting --


class TestFormatSignalMessage:
    def test_contains_ticker(self) -> None:
        msg = format_signal_message(_make_signal())
        assert "SBER" in msg

    def test_contains_direction(self) -> None:
        msg = format_signal_message(_make_signal())
        assert "UP" in msg

    def test_contains_confidence(self) -> None:
        msg = format_signal_message(_make_signal())
        assert "0.63" in msg

    def test_contains_price(self) -> None:
        msg = format_signal_message(_make_signal())
        assert "320.50" in msg

    def test_contains_source_channel(self) -> None:
        msg = format_signal_message(_make_signal())
        assert "interfaxonline" in msg

    def test_contains_time(self) -> None:
        msg = format_signal_message(_make_signal())
        assert "2026-03-21 15:05 MSK" in msg

    def test_contains_outcome(self) -> None:
        msg = format_signal_message(_make_signal(outcome_label="win"))
        assert "win" in msg

    def test_contains_return(self) -> None:
        msg = format_signal_message(_make_signal(return_pct=0.0014))
        assert "+0.14" in msg

    def test_no_source_channel_falls_back_to_source(self) -> None:
        msg = format_signal_message(_make_signal(source_channel=None))
        assert "fusion" in msg

    def test_none_values_show_na(self) -> None:
        msg = format_signal_message(_make_signal(
            confidence=None, price_at_signal=None,
        ))
        assert "n/a" in msg

    def test_no_outcome_omits_line(self) -> None:
        msg = format_signal_message(_make_signal(outcome_label=None))
        assert "Outcome" not in msg


# -- B. Delivery success -> delivered_at set --


class TestDeliverSignalSuccess:
    @patch("tinvest_trader.services.signal_delivery.send_telegram_message")
    def test_success_marks_delivered(self, mock_send: MagicMock) -> None:
        mock_send.return_value = True
        repo = _make_repo()

        result = deliver_signal(
            _make_signal(), "token", "chat",
            repository=repo,
        )

        assert result.sent is True
        assert result.signal_id == 1
        repo.mark_signal_delivered.assert_called_once_with(1)

    @patch("tinvest_trader.services.signal_delivery.send_telegram_message")
    def test_success_without_repo(self, mock_send: MagicMock) -> None:
        mock_send.return_value = True

        result = deliver_signal(_make_signal(), "token", "chat")

        assert result.sent is True


# -- C. Delivery failure -> not marked --


class TestDeliverSignalFailure:
    @patch("tinvest_trader.services.signal_delivery.send_telegram_message")
    def test_failure_not_marked(self, mock_send: MagicMock) -> None:
        mock_send.return_value = False
        repo = _make_repo()

        result = deliver_signal(
            _make_signal(), "token", "chat",
            repository=repo,
        )

        assert result.sent is False
        assert result.error is not None
        repo.mark_signal_delivered.assert_not_called()


# -- D. Dedup (no double send) --


class TestDedup:
    @patch("tinvest_trader.services.signal_delivery.send_telegram_message")
    def test_already_delivered_not_returned_by_list(
        self, mock_send: MagicMock,
    ) -> None:
        """Signals with delivered_at set are excluded by list_undelivered_signals."""
        mock_send.return_value = True
        repo = _make_repo()
        repo.list_undelivered_signals.side_effect = [
            [_make_signal(id=1)],
            [],
        ]

        sent1 = deliver_pending_signals(
            "token", "chat", repo, MagicMock(), limit=50,
            weekend_high_only=False,
        )
        assert sent1 == 1

        sent2 = deliver_pending_signals(
            "token", "chat", repo, MagicMock(), limit=50,
            weekend_high_only=False,
        )
        assert sent2 == 0

    @patch("tinvest_trader.services.signal_delivery.send_telegram_message")
    def test_multiple_signals_delivered(
        self, mock_send: MagicMock,
    ) -> None:
        mock_send.return_value = True
        repo = _make_repo()
        repo.list_undelivered_signals.return_value = [
            _make_signal(id=1),
            _make_signal(id=2, ticker="GAZP"),
        ]

        sent = deliver_pending_signals(
            "token", "chat", repo, MagicMock(), limit=50,
            weekend_high_only=False,
        )

        assert sent == 2
        assert repo.mark_signal_delivered.call_count == 2


# -- E. DeliveryResult dataclass --


class TestDeliveryResult:
    def test_default_values(self) -> None:
        r = DeliveryResult(signal_id=42)
        assert r.signal_id == 42
        assert r.sent is False
        assert r.error is None


# -- F. Empty signals list --


class TestEmptySignals:
    @patch("tinvest_trader.services.signal_delivery.send_telegram_message")
    def test_no_signals_returns_zero(self, mock_send: MagicMock) -> None:
        repo = _make_repo()
        repo.list_undelivered_signals.return_value = []

        sent = deliver_pending_signals(
            "token", "chat", repo, MagicMock(), limit=50,
        )

        assert sent == 0
        mock_send.assert_not_called()


# -- G. Max-per-cycle --


class TestMaxPerCycle:
    @patch("tinvest_trader.services.signal_delivery.send_telegram_message")
    def test_max_per_cycle_limits_delivery(self, mock_send: MagicMock) -> None:
        mock_send.return_value = True
        repo = _make_repo()
        repo.list_undelivered_signals.return_value = [
            _make_signal(id=1),
            _make_signal(id=2, ticker="GAZP"),
            _make_signal(id=3, ticker="VTBR"),
        ]

        sent = deliver_pending_signals(
            "token", "chat", repo, MagicMock(), limit=50,
            max_per_cycle=2, weekend_high_only=False,
        )

        assert sent == 2
        assert repo.mark_signal_delivered.call_count == 2

    @patch("tinvest_trader.services.signal_delivery.send_telegram_message")
    def test_zero_max_means_no_limit(self, mock_send: MagicMock) -> None:
        mock_send.return_value = True
        repo = _make_repo()
        repo.list_undelivered_signals.return_value = [
            _make_signal(id=1),
            _make_signal(id=2, ticker="GAZP"),
        ]

        sent = deliver_pending_signals(
            "token", "chat", repo, MagicMock(), limit=50,
            max_per_cycle=0, weekend_high_only=False,
        )

        assert sent == 2


# -- H. Severity ordering in delivery --


class TestSeverityOrdering:
    @patch("tinvest_trader.services.signal_delivery.send_telegram_message")
    def test_high_severity_delivered_first(self, mock_send: MagicMock) -> None:
        """When max_per_cycle=1, only the highest-severity signal is sent."""
        mock_send.return_value = True
        repo = _make_repo()
        # Low confidence signal first, high confidence second
        repo.list_undelivered_signals.return_value = [
            _make_signal(id=1, confidence=0.20, ticker="WEAK"),
            _make_signal(id=2, confidence=0.80, ticker="STRONG"),
        ]

        sent = deliver_pending_signals(
            "token", "chat", repo, MagicMock(), limit=50,
            max_per_cycle=1, weekend_high_only=False,
        )

        assert sent == 1
        # The high-confidence signal (id=2) should be delivered
        repo.mark_signal_delivered.assert_called_once_with(2)


# -- I. Weekend HIGH-only filter --


class TestWeekendHighOnly:
    @patch("tinvest_trader.services.signal_delivery._is_weekend_msk", return_value=True)
    @patch("tinvest_trader.services.signal_delivery.send_telegram_message")
    def test_weekend_suppresses_non_high(
        self, mock_send: MagicMock, _mock_weekend: MagicMock,
    ) -> None:
        """On weekends only HIGH-severity signals are delivered."""
        mock_send.return_value = True
        repo = _make_repo()
        # Provide strong stats so high-confidence signal classifies as HIGH
        repo.get_signal_stats_by_ticker.return_value = [
            {"ticker": "STRONG", "resolved": 50, "wins": 35, "avg_return": 0.03},
        ]
        repo.get_signal_stats_by_type.return_value = [
            {"signal_type": "up", "resolved": 100, "wins": 60},
        ]
        repo.list_undelivered_signals.return_value = [
            _make_signal(id=1, confidence=0.20, ticker="WEAK"),
            _make_signal(id=2, confidence=0.80, ticker="STRONG"),
        ]

        sent = deliver_pending_signals(
            "token", "chat", repo, MagicMock(), limit=50,
            weekend_high_only=True,
        )

        # Only the HIGH signal should be delivered
        assert sent == 1
        repo.mark_signal_delivered.assert_called_once_with(2)

    @patch("tinvest_trader.services.signal_delivery._is_weekend_msk", return_value=False)
    @patch("tinvest_trader.services.signal_delivery.send_telegram_message")
    def test_weekday_delivers_all(
        self, mock_send: MagicMock, _mock_weekend: MagicMock,
    ) -> None:
        """On weekdays all signals are delivered regardless of severity."""
        mock_send.return_value = True
        repo = _make_repo()
        repo.list_undelivered_signals.return_value = [
            _make_signal(id=1, confidence=0.20, ticker="WEAK"),
            _make_signal(id=2, confidence=0.80, ticker="STRONG"),
        ]

        sent = deliver_pending_signals(
            "token", "chat", repo, MagicMock(), limit=50,
            weekend_high_only=True,
        )

        assert sent == 2
