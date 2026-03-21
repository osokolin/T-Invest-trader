"""Tests for bot UX v2 -- inline navigation and signal action buttons."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tinvest_trader.services.bot_commands import (
    build_delivery_keyboard,
    build_signal_detail_keyboard,
    build_signal_list_keyboard,
    handle_last_signals_with_buttons,
    handle_signal_detail_with_buttons,
    handle_ticker_stats,
)
from tinvest_trader.services.telegram_bot_handler import (
    _process_callback_query,
    parse_callback_data,
)

# -- A. Callback data parsing --


class TestParseCallbackDataV2:
    # Legacy format
    def test_legacy_ai_signal(self) -> None:
        assert parse_callback_data("ai:signal:42") == ("ai_analysis", 42)

    # New: signal:<id>:<action>
    def test_signal_details(self) -> None:
        assert parse_callback_data("signal:42:details") == ("signal_details", 42)

    def test_signal_ai(self) -> None:
        assert parse_callback_data("signal:42:ai") == ("ai_analysis", 42)

    def test_signal_stats(self) -> None:
        assert parse_callback_data("signal:42:stats") == ("signal_stats", 42)

    # Navigation
    def test_nav_last_signals(self) -> None:
        assert parse_callback_data("nav:last_signals") == ("nav_last_signals", 0)

    # Invalid
    def test_unknown_action(self) -> None:
        assert parse_callback_data("signal:42:unknown") is None

    def test_invalid_signal_id(self) -> None:
        assert parse_callback_data("signal:abc:details") is None

    def test_unknown_nav(self) -> None:
        assert parse_callback_data("nav:something") is None

    def test_empty(self) -> None:
        assert parse_callback_data("") is None

    def test_garbage(self) -> None:
        assert parse_callback_data("foo:bar:baz:qux") is None

    def test_single_part(self) -> None:
        assert parse_callback_data("hello") is None


# -- B. Inline keyboard structure --


class TestBuildSignalListKeyboard:
    def test_one_signal(self) -> None:
        signals = [{"id": 1, "ticker": "SBER", "signal_type": "up"}]
        kb = build_signal_list_keyboard(signals)
        assert len(kb) == 1
        assert len(kb[0]) == 1
        btn = kb[0][0]
        assert "SBER" in btn["text"]
        assert btn["callback_data"] == "signal:1:details"

    def test_multiple_signals(self) -> None:
        signals = [
            {"id": 10, "ticker": "SBER", "signal_type": "up"},
            {"id": 9, "ticker": "GAZP", "signal_type": "down"},
            {"id": 8, "ticker": "VTBR", "signal_type": "up"},
        ]
        kb = build_signal_list_keyboard(signals)
        assert len(kb) == 3
        assert kb[0][0]["callback_data"] == "signal:10:details"
        assert kb[1][0]["callback_data"] == "signal:9:details"
        assert kb[2][0]["callback_data"] == "signal:8:details"

    def test_up_arrow(self) -> None:
        signals = [{"id": 1, "ticker": "X", "signal_type": "up"}]
        kb = build_signal_list_keyboard(signals)
        assert "\u2191" in kb[0][0]["text"]

    def test_down_arrow(self) -> None:
        signals = [{"id": 1, "ticker": "X", "signal_type": "down"}]
        kb = build_signal_list_keyboard(signals)
        assert "\u2193" in kb[0][0]["text"]

    def test_empty(self) -> None:
        kb = build_signal_list_keyboard([])
        assert kb == []


class TestBuildSignalDetailKeyboard:
    def test_structure(self) -> None:
        kb = build_signal_detail_keyboard(42)
        assert len(kb) == 2
        # Row 1: AI + Stats
        assert len(kb[0]) == 2
        assert kb[0][0]["callback_data"] == "signal:42:ai"
        assert kb[0][1]["callback_data"] == "signal:42:stats"
        # Row 2: Back
        assert len(kb[1]) == 1
        assert kb[1][0]["callback_data"] == "nav:last_signals"

    def test_labels(self) -> None:
        kb = build_signal_detail_keyboard(1)
        assert "AI" in kb[0][0]["text"]


class TestBuildDeliveryKeyboard:
    def test_structure(self) -> None:
        kb = build_delivery_keyboard(99)
        assert len(kb) == 2
        # Row 1: Details + AI
        assert len(kb[0]) == 2
        assert kb[0][0]["callback_data"] == "signal:99:details"
        assert kb[0][1]["callback_data"] == "signal:99:ai"
        # Row 2: Stats
        assert len(kb[1]) == 1
        assert kb[1][0]["callback_data"] == "signal:99:stats"


# -- C. /last_signals with buttons --


def _sample_signals() -> list[dict]:
    return [
        {
            "id": 5, "ticker": "SBER", "signal_type": "up",
            "confidence": 0.67, "pipeline_stage": "delivered",
            "created_at": datetime(2025, 3, 20, 12, 5, tzinfo=UTC),
        },
        {
            "id": 4, "ticker": "GAZP", "signal_type": "down",
            "confidence": 0.58, "pipeline_stage": "delivered",
            "created_at": datetime(2025, 3, 20, 11, 0, tzinfo=UTC),
        },
    ]


class TestHandleLastSignalsWithButtons:
    def test_returns_text_and_keyboard(self) -> None:
        repo = MagicMock()
        repo.list_recent_signals.return_value = _sample_signals()
        text, kb = handle_last_signals_with_buttons(repo, "")
        assert "Recent signals:" in text
        assert "#5" in text
        assert len(kb) == 2
        assert kb[0][0]["callback_data"] == "signal:5:details"

    def test_empty_returns_no_keyboard(self) -> None:
        repo = MagicMock()
        repo.list_recent_signals.return_value = []
        text, kb = handle_last_signals_with_buttons(repo, "")
        assert "No signals" in text
        assert kb == []

    def test_invalid_limit_returns_no_keyboard(self) -> None:
        repo = MagicMock()
        text, kb = handle_last_signals_with_buttons(repo, "abc")
        assert "Usage" in text
        assert kb == []


# -- D. Signal detail via button --


def _make_signal_detail(**overrides: object) -> dict:
    base = {
        "id": 42,
        "ticker": "SBER",
        "signal_type": "up",
        "confidence": 0.67,
        "source": "fusion",
        "price_at_signal": 250.0,
        "created_at": datetime(2025, 3, 20, 12, 5, tzinfo=UTC),
        "source_channel": "interfaxonline",
        "return_pct": 0.0042,
        "outcome_label": "win",
        "pipeline_stage": "delivered",
        "rejection_reason": None,
        "delivered_at": datetime(2025, 3, 20, 12, 10, tzinfo=UTC),
    }
    base.update(overrides)
    return base


class TestHandleSignalDetailWithButtons:
    def test_returns_text_and_keyboard(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = _make_signal_detail()
        text, kb = handle_signal_detail_with_buttons(repo, 42)
        assert "Signal #42" in text
        assert "SBER" in text
        assert len(kb) == 2
        assert kb[0][0]["callback_data"] == "signal:42:ai"
        assert kb[0][1]["callback_data"] == "signal:42:stats"
        assert kb[1][0]["callback_data"] == "nav:last_signals"

    def test_not_found_returns_no_keyboard(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = None
        text, kb = handle_signal_detail_with_buttons(repo, 999)
        assert "not found" in text
        assert kb == []


# -- E. Ticker stats via button --


class TestHandleTickerStats:
    def test_success(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = _make_signal_detail()
        repo.get_signal_stats_by_ticker.return_value = [
            {"ticker": "SBER", "total": 10, "resolved": 8,
             "wins": 5, "avg_return": 0.012},
        ]
        result = handle_ticker_stats(repo, 42)
        assert "SBER" in result
        assert "total: 10" in result
        assert "win rate: 62%" in result

    def test_signal_not_found(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = None
        result = handle_ticker_stats(repo, 999)
        assert "not found" in result

    def test_no_stats_for_ticker(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = _make_signal_detail(ticker="RARE")
        repo.get_signal_stats_by_ticker.return_value = [
            {"ticker": "SBER", "total": 10, "resolved": 8,
             "wins": 5, "avg_return": 0.012},
        ]
        result = handle_ticker_stats(repo, 42)
        assert "No stats" in result

    def test_stats_exception(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = _make_signal_detail()
        repo.get_signal_stats_by_ticker.side_effect = RuntimeError("db")
        result = handle_ticker_stats(repo, 42)
        assert "unavailable" in result


# -- F. Callback routing integration --


class TestCallbackRouting:
    def test_details_callback_sends_with_keyboard(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = _make_signal_detail()

        callback_query = {"id": "cb1", "data": "signal:42:details"}

        with patch(
            "tinvest_trader.services.telegram_bot_handler._bot_api_request",
        ) as mock_api:
            mock_api.return_value = {"ok": True}
            _process_callback_query(
                callback_query,
                bot_token="tok",
                chat_id="123",
                repository=repo,
                logger=MagicMock(),
            )
            # Should have called answerCallbackQuery + sendMessage
            calls = mock_api.call_args_list
            methods = [c[0][1] for c in calls]
            assert "answerCallbackQuery" in methods
            assert "sendMessage" in methods

            # sendMessage should include reply_markup
            send_call = [c for c in calls if c[0][1] == "sendMessage"][0]
            params = send_call[0][2]
            assert "reply_markup" in params
            assert "inline_keyboard" in params["reply_markup"]

    def test_stats_callback_sends_text(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = _make_signal_detail()
        repo.get_signal_stats_by_ticker.return_value = [
            {"ticker": "SBER", "total": 10, "resolved": 8,
             "wins": 5, "avg_return": 0.012},
        ]

        callback_query = {"id": "cb1", "data": "signal:42:stats"}

        with patch(
            "tinvest_trader.services.telegram_bot_handler._bot_api_request",
        ) as mock_api:
            mock_api.return_value = {"ok": True}
            _process_callback_query(
                callback_query,
                bot_token="tok",
                chat_id="123",
                repository=repo,
                logger=MagicMock(),
            )
            send_call = [
                c for c in mock_api.call_args_list
                if c[0][1] == "sendMessage"
            ][0]
            params = send_call[0][2]
            assert "SBER" in params["text"]

    def test_nav_back_sends_signal_list(self) -> None:
        repo = MagicMock()
        repo.list_recent_signals.return_value = _sample_signals()

        callback_query = {"id": "cb1", "data": "nav:last_signals"}

        with patch(
            "tinvest_trader.services.telegram_bot_handler._bot_api_request",
        ) as mock_api:
            mock_api.return_value = {"ok": True}
            _process_callback_query(
                callback_query,
                bot_token="tok",
                chat_id="123",
                repository=repo,
                logger=MagicMock(),
            )
            send_call = [
                c for c in mock_api.call_args_list
                if c[0][1] == "sendMessage"
            ][0]
            params = send_call[0][2]
            assert "Recent signals:" in params["text"]
            assert "reply_markup" in params
            assert "inline_keyboard" in params["reply_markup"]

    def test_unknown_callback_answers_unknown(self) -> None:
        callback_query = {"id": "cb1", "data": "garbage:data"}

        with patch(
            "tinvest_trader.services.telegram_bot_handler._bot_api_request",
        ) as mock_api:
            mock_api.return_value = {"ok": True}
            _process_callback_query(
                callback_query,
                bot_token="tok",
                chat_id="123",
                repository=MagicMock(),
                logger=MagicMock(),
            )
            answer_call = [
                c for c in mock_api.call_args_list
                if c[0][1] == "answerCallbackQuery"
            ]
            assert len(answer_call) == 1
            assert "Unknown" in answer_call[0][0][2]["text"]

    def test_callback_exception_sends_error(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.side_effect = RuntimeError("db down")

        callback_query = {"id": "cb1", "data": "signal:42:details"}

        with patch(
            "tinvest_trader.services.telegram_bot_handler._bot_api_request",
        ) as mock_api:
            mock_api.return_value = {"ok": True}
            _process_callback_query(
                callback_query,
                bot_token="tok",
                chat_id="123",
                repository=repo,
                logger=MagicMock(),
            )
            send_call = [
                c for c in mock_api.call_args_list
                if c[0][1] == "sendMessage"
            ]
            assert len(send_call) == 1
            assert "wrong" in send_call[0][0][2]["text"].lower()


# -- G. /last_signals command now sends keyboard --


class TestLastSignalsCommandWithButtons:
    def test_sends_keyboard(self) -> None:
        from tinvest_trader.services.telegram_bot_handler import (
            _process_message_command,
        )

        repo = MagicMock()
        repo.list_recent_signals.return_value = _sample_signals()

        message = {"chat": {"id": 123}, "text": "/last_signals"}

        with patch(
            "tinvest_trader.services.telegram_bot_handler._send_reply",
        ) as mock_send:
            _process_message_command(
                message,
                bot_token="tok",
                chat_id="123",
                repository=repo,
                logger=MagicMock(),
            )
            mock_send.assert_called_once()
            kwargs = mock_send.call_args[1]
            assert kwargs.get("reply_markup") is not None
            assert "inline_keyboard" in kwargs["reply_markup"]


# -- H. Delivery keyboard integration --


class TestDeliveryKeyboardIntegration:
    def test_delivery_uses_new_keyboard(self) -> None:
        """Verify deliver_signal builds expanded keyboard."""
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

        with patch(
            "tinvest_trader.services.signal_delivery.send_telegram_message",
        ) as mock_send:
            mock_send.return_value = True
            deliver_signal(
                signal, "tok", "123",
                repository=repo, logger=MagicMock(),
            )
            mock_send.assert_called_once()
            kwargs = mock_send.call_args[1]
            markup = kwargs.get("reply_markup", "")
            assert "signal:77:details" in markup
            assert "signal:77:ai" in markup
            assert "signal:77:stats" in markup
