"""Tests for Telegram bot operator commands."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tinvest_trader.services.bot_commands import (
    handle_help,
    handle_last_signals,
    handle_signal,
    handle_stats,
    parse_command,
)
from tinvest_trader.services.telegram_bot_handler import (
    _process_message_command,
)

# -- A. Command parsing --


class TestParseCommand:
    def test_simple_command(self) -> None:
        assert parse_command("/stats") == ("stats", "")

    def test_command_with_args(self) -> None:
        assert parse_command("/signal 42") == ("signal", "42")

    def test_command_with_bot_suffix(self) -> None:
        assert parse_command("/stats@my_bot") == ("stats", "")

    def test_command_with_bot_suffix_and_args(self) -> None:
        assert parse_command("/signal@my_bot 42") == ("signal", "42")

    def test_last_signals_with_arg(self) -> None:
        assert parse_command("/last_signals 10") == ("last_signals", "10")

    def test_non_command(self) -> None:
        assert parse_command("hello") == ("", "hello")

    def test_empty(self) -> None:
        assert parse_command("") == ("", "")

    def test_command_case_insensitive(self) -> None:
        assert parse_command("/STATS") == ("stats", "")

    def test_leading_whitespace(self) -> None:
        assert parse_command("  /help ") == ("help", "")


# -- B. /last_signals --


def _make_repo_with_signals(signals: list[dict]) -> MagicMock:
    repo = MagicMock()
    repo.list_recent_signals.return_value = signals
    return repo


class TestHandleLastSignals:
    def test_default_limit(self) -> None:
        repo = _make_repo_with_signals([])
        handle_last_signals(repo, "")
        repo.list_recent_signals.assert_called_once_with(5)

    def test_custom_limit(self) -> None:
        repo = _make_repo_with_signals([])
        handle_last_signals(repo, "8")
        repo.list_recent_signals.assert_called_once_with(8)

    def test_invalid_limit(self) -> None:
        repo = MagicMock()
        result = handle_last_signals(repo, "abc")
        assert "Usage" in result

    def test_limit_out_of_range(self) -> None:
        repo = MagicMock()
        result = handle_last_signals(repo, "20")
        assert "Usage" in result

    def test_zero_limit(self) -> None:
        repo = MagicMock()
        result = handle_last_signals(repo, "0")
        assert "Usage" in result

    def test_empty_result(self) -> None:
        repo = _make_repo_with_signals([])
        result = handle_last_signals(repo, "")
        assert "No signals" in result

    def test_formatting(self) -> None:
        signals = [
            {
                "id": 124,
                "ticker": "SBER",
                "signal_type": "up",
                "confidence": 0.67,
                "pipeline_stage": "delivered",
                "created_at": datetime(2025, 3, 20, 12, 5, tzinfo=UTC),
            },
            {
                "id": 123,
                "ticker": "GAZP",
                "signal_type": "down",
                "confidence": 0.58,
                "pipeline_stage": "rejected_calibration",
                "created_at": datetime(2025, 3, 20, 11, 42, tzinfo=UTC),
            },
        ]
        repo = _make_repo_with_signals(signals)
        result = handle_last_signals(repo, "")
        assert "Recent signals:" in result
        assert "#124" in result
        assert "SBER" in result
        assert "UP" in result
        assert "0.67" in result
        assert "#123" in result
        assert "GAZP" in result
        assert "DOWN" in result

    def test_missing_fields_safe(self) -> None:
        signals = [
            {
                "id": 1,
                "ticker": "X",
                "signal_type": None,
                "confidence": None,
                "pipeline_stage": None,
                "created_at": None,
            },
        ]
        repo = _make_repo_with_signals(signals)
        result = handle_last_signals(repo, "")
        assert "#1" in result
        assert "X" in result


# -- C. /signal <id> --


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


class TestHandleSignal:
    def test_success(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = _make_signal_detail()
        result = handle_signal(repo, "42")
        assert "Signal #42" in result
        assert "SBER" in result
        assert "UP" in result
        assert "0.67" in result
        assert "delivered" in result
        assert "interfaxonline" in result
        assert "win" in result

    def test_not_found(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = None
        result = handle_signal(repo, "999")
        assert "not found" in result

    def test_no_args(self) -> None:
        repo = MagicMock()
        result = handle_signal(repo, "")
        assert "Usage" in result

    def test_invalid_id(self) -> None:
        repo = MagicMock()
        result = handle_signal(repo, "abc")
        assert "Usage" in result

    def test_rejected_signal(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = _make_signal_detail(
            pipeline_stage="rejected_calibration",
            rejection_reason="low_confidence",
            outcome_label=None,
            return_pct=None,
            delivered_at=None,
        )
        result = handle_signal(repo, "42")
        assert "rejected_calibration" in result
        assert "low_confidence" in result
        assert "Outcome" not in result

    def test_unresolved_signal(self) -> None:
        repo = MagicMock()
        repo.get_signal_detail.return_value = _make_signal_detail(
            outcome_label=None,
            return_pct=None,
        )
        result = handle_signal(repo, "42")
        assert "Outcome" not in result


# -- D. /ai <id> --


class TestHandleAiCommand:
    def test_cached_result(self) -> None:
        repo = MagicMock()
        repo.get_signal_prediction.return_value = {
            "id": 5, "ticker": "SBER", "signal_type": "up",
            "confidence": 0.6, "source": "fusion",
            "price_at_signal": 250, "created_at": None,
            "source_channel": None, "return_pct": None,
            "outcome_label": None,
        }
        repo.get_cached_ai_analysis.return_value = {
            "analysis_text": "test analysis",
            "model": "test-model",
            "created_at": None,
        }

        from tinvest_trader.services.telegram_bot_handler import (
            _handle_ai_command,
        )

        with patch(
            "tinvest_trader.services.signal_ai_analysis.call_anthropic",
        ):
            result = _handle_ai_command(
                repository=repo,
                logger=MagicMock(),
                args="5",
                api_key="test-key",
            )
        assert "SBER" in result
        assert "(cached)" in result

    def test_no_args(self) -> None:
        from tinvest_trader.services.telegram_bot_handler import (
            _handle_ai_command,
        )

        result = _handle_ai_command(
            repository=MagicMock(),
            logger=MagicMock(),
            args="",
            api_key="test-key",
        )
        assert "Usage" in result

    def test_invalid_id(self) -> None:
        from tinvest_trader.services.telegram_bot_handler import (
            _handle_ai_command,
        )

        result = _handle_ai_command(
            repository=MagicMock(),
            logger=MagicMock(),
            args="abc",
            api_key="test-key",
        )
        assert "Usage" in result

    def test_no_api_key(self) -> None:
        from tinvest_trader.services.telegram_bot_handler import (
            _handle_ai_command,
        )

        result = _handle_ai_command(
            repository=MagicMock(),
            logger=MagicMock(),
            args="5",
            api_key="",
        )
        assert "unavailable" in result

    def test_signal_not_found(self) -> None:
        from tinvest_trader.services.telegram_bot_handler import (
            _handle_ai_command,
        )

        repo = MagicMock()
        repo.get_signal_prediction.return_value = None
        result = _handle_ai_command(
            repository=repo,
            logger=MagicMock(),
            args="999",
            api_key="test-key",
        )
        assert "not found" in result


# -- E. /stats --


class TestHandleStats:
    def test_output_shape(self) -> None:
        repo = MagicMock()
        repo.get_signal_stats.return_value = {
            "total": 100, "resolved": 60, "wins": 33,
            "losses": 22, "neutrals": 5, "avg_return": 0.008,
        }
        repo.get_signal_stats_by_source.return_value = [
            {"source_channel": "interfaxonline", "total": 50,
             "resolved": 30, "wins": 18, "avg_return": 0.01},
        ]
        result = handle_stats(repo)
        assert "Stats:" in result
        assert "total signals: 100" in result
        assert "resolved: 60" in result
        assert "win rate: 55%" in result
        assert "interfaxonline" in result

    def test_no_data(self) -> None:
        repo = MagicMock()
        repo.get_signal_stats.return_value = {"total": 0}
        result = handle_stats(repo)
        assert "No signal data" in result

    def test_empty_stats(self) -> None:
        repo = MagicMock()
        repo.get_signal_stats.return_value = {}
        result = handle_stats(repo)
        assert "No signal data" in result

    def test_no_sources(self) -> None:
        repo = MagicMock()
        repo.get_signal_stats.return_value = {
            "total": 10, "resolved": 5, "wins": 3,
            "losses": 2, "neutrals": 0, "avg_return": 0.005,
        }
        repo.get_signal_stats_by_source.return_value = []
        result = handle_stats(repo)
        assert "Stats:" in result
        assert "top source" not in result


# -- F. /help --


class TestHandleHelp:
    def test_lists_commands(self) -> None:
        result = handle_help()
        assert "/last_signals" in result
        assert "/signal" in result
        assert "/ai" in result
        assert "/stats" in result
        assert "/help" in result


# -- G. Unknown command handling --


class TestUnknownCommand:
    def test_unknown_command_ignored(self) -> None:
        """Unknown commands should not generate a response."""
        repo = MagicMock()
        logger = MagicMock()

        message = {
            "chat": {"id": 123},
            "text": "/unknown_command",
        }

        with patch(
            "tinvest_trader.services.telegram_bot_handler._send_reply",
        ) as mock_send:
            _process_message_command(
                message,
                bot_token="test",
                chat_id="123",
                repository=repo,
                logger=logger,
            )
            mock_send.assert_not_called()


# -- H. Access control --


class TestAccessControl:
    def test_ignores_other_chat(self) -> None:
        """Commands from non-operator chats are ignored."""
        repo = MagicMock()
        logger = MagicMock()

        message = {
            "chat": {"id": 999},
            "text": "/stats",
        }

        with patch(
            "tinvest_trader.services.telegram_bot_handler._send_reply",
        ) as mock_send:
            _process_message_command(
                message,
                bot_token="test",
                chat_id="123",
                repository=repo,
                logger=logger,
            )
            mock_send.assert_not_called()

    def test_ignores_non_command_text(self) -> None:
        """Plain text (non-command) messages are ignored."""
        repo = MagicMock()
        logger = MagicMock()

        message = {
            "chat": {"id": 123},
            "text": "hello bot",
        }

        with patch(
            "tinvest_trader.services.telegram_bot_handler._send_reply",
        ) as mock_send:
            _process_message_command(
                message,
                bot_token="test",
                chat_id="123",
                repository=repo,
                logger=logger,
            )
            mock_send.assert_not_called()

    def test_ignores_empty_text(self) -> None:
        """Messages without text are ignored."""
        repo = MagicMock()
        logger = MagicMock()

        message = {
            "chat": {"id": 123},
        }

        with patch(
            "tinvest_trader.services.telegram_bot_handler._send_reply",
        ) as mock_send:
            _process_message_command(
                message,
                bot_token="test",
                chat_id="123",
                repository=repo,
                logger=logger,
            )
            mock_send.assert_not_called()


# -- I. Command routing integration --


class TestCommandRouting:
    def test_stats_routes_and_replies(self) -> None:
        repo = MagicMock()
        repo.get_signal_stats.return_value = {
            "total": 10, "resolved": 5, "wins": 3,
            "losses": 2, "neutrals": 0, "avg_return": 0.005,
        }
        repo.get_signal_stats_by_source.return_value = []
        logger = MagicMock()

        message = {
            "chat": {"id": 123},
            "text": "/stats",
        }

        with patch(
            "tinvest_trader.services.telegram_bot_handler._send_reply",
        ) as mock_send:
            _process_message_command(
                message,
                bot_token="test-token",
                chat_id="123",
                repository=repo,
                logger=logger,
            )
            mock_send.assert_called_once()
            sent_text = mock_send.call_args[0][2]
            assert "Stats:" in sent_text

    def test_help_routes_and_replies(self) -> None:
        logger = MagicMock()
        message = {
            "chat": {"id": 123},
            "text": "/help",
        }

        with patch(
            "tinvest_trader.services.telegram_bot_handler._send_reply",
        ) as mock_send:
            _process_message_command(
                message,
                bot_token="test-token",
                chat_id="123",
                repository=MagicMock(),
                logger=logger,
            )
            mock_send.assert_called_once()
            sent_text = mock_send.call_args[0][2]
            assert "/last_signals" in sent_text

    def test_exception_returns_error_message(self) -> None:
        repo = MagicMock()
        repo.get_signal_stats.side_effect = RuntimeError("db down")
        logger = MagicMock()

        message = {
            "chat": {"id": 123},
            "text": "/stats",
        }

        with patch(
            "tinvest_trader.services.telegram_bot_handler._send_reply",
        ) as mock_send:
            _process_message_command(
                message,
                bot_token="test-token",
                chat_id="123",
                repository=repo,
                logger=logger,
            )
            mock_send.assert_called_once()
            sent_text = mock_send.call_args[0][2]
            assert "wrong" in sent_text.lower()

    def test_start_command_returns_help(self) -> None:
        logger = MagicMock()
        message = {
            "chat": {"id": 123},
            "text": "/start",
        }

        with patch(
            "tinvest_trader.services.telegram_bot_handler._send_reply",
        ) as mock_send:
            _process_message_command(
                message,
                bot_token="test-token",
                chat_id="123",
                repository=MagicMock(),
                logger=logger,
            )
            mock_send.assert_called_once()
            sent_text = mock_send.call_args[0][2]
            assert "/stats" in sent_text
