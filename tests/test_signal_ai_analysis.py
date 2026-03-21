"""Tests for AI signal analysis and Telegram bot callback handler."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tinvest_trader.services.signal_ai_analysis import (
    _SYSTEM_PROMPT,
    AiAnalysisResult,
    analyze_signal,
    build_ai_prompt,
    build_signal_context,
    call_anthropic,
    format_ai_response,
)
from tinvest_trader.services.telegram_bot_handler import (
    parse_callback_data,
    poll_and_handle_callbacks,
)


def _make_signal(**overrides: object) -> dict:
    base = {
        "id": 42,
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
    repo.get_cached_ai_analysis.return_value = None
    repo.insert_ai_analysis.return_value = True
    repo.get_signal_prediction.return_value = _make_signal()
    repo.get_signal_stats_by_ticker.return_value = []
    repo.get_signal_stats_by_type.return_value = []
    repo.get_signal_stats_by_source.return_value = []
    return repo


# -- A. build_signal_context --


class TestBuildSignalContext:
    def test_basic_fields(self) -> None:
        ctx = build_signal_context(_make_signal())
        assert ctx["ticker"] == "SBER"
        assert ctx["direction"] == "UP"
        assert ctx["confidence"] == 0.63

    def test_strips_none_values(self) -> None:
        sig = _make_signal(source_channel=None)
        ctx = build_signal_context(sig)
        assert "source_channel" not in ctx

    def test_datetime_formatted(self) -> None:
        ctx = build_signal_context(_make_signal())
        assert ctx["created_at"] == "2026-03-21 12:05"

    def test_ticker_stats_enrichment(self) -> None:
        stats = {"resolved": 100, "wins": 60, "avg_return": 0.015}
        ctx = build_signal_context(_make_signal(), ticker_stats=stats)
        assert ctx["ticker_win_rate"] == 0.6
        assert ctx["ticker_avg_return"] == 0.015
        assert ctx["ticker_resolved"] == 100

    def test_type_stats_enrichment(self) -> None:
        stats = {"resolved": 50, "wins": 30, "avg_return": 0.008}
        ctx = build_signal_context(_make_signal(), type_stats=stats)
        assert ctx["type_win_rate"] == 0.6
        assert ctx["type_avg_return"] == 0.008
        assert ctx["type_resolved"] == 50

    def test_source_stats_enrichment(self) -> None:
        stats = {"resolved": 80, "wins": 40, "avg_return": 0.01}
        ctx = build_signal_context(_make_signal(), source_stats=stats)
        assert ctx["source_win_rate"] == 0.5
        assert ctx["source_avg_return"] == 0.01

    def test_skips_stats_with_zero_resolved(self) -> None:
        stats = {"resolved": 0, "wins": 0}
        ctx = build_signal_context(_make_signal(), ticker_stats=stats)
        assert "ticker_win_rate" not in ctx

    def test_pipeline_stage_included(self) -> None:
        ctx = build_signal_context(_make_signal(pipeline_stage="delivered"))
        assert ctx["pipeline_stage"] == "delivered"

    def test_pipeline_stage_absent_when_none(self) -> None:
        ctx = build_signal_context(_make_signal(pipeline_stage=None))
        assert "pipeline_stage" not in ctx

    def test_type_avg_return_absent_when_no_stats(self) -> None:
        ctx = build_signal_context(_make_signal())
        assert "type_avg_return" not in ctx


# -- A2. System prompt guardrails --


class TestSystemPromptGuardrails:
    def test_no_hallucination_instruction(self) -> None:
        assert "Do NOT invent" in _SYSTEM_PROMPT

    def test_uncertainty_instruction(self) -> None:
        assert "uncertainty" in _SYSTEM_PROMPT

    def test_russian_output(self) -> None:
        assert "Russian" in _SYSTEM_PROMPT

    def test_practical_verdict(self) -> None:
        assert "практический вердикт" in _SYSTEM_PROMPT

    def test_win_rate_guidance(self) -> None:
        assert "win_rate > 0.5" in _SYSTEM_PROMPT

    def test_missing_stats_guidance(self) -> None:
        assert "Missing statistics" in _SYSTEM_PROMPT


# -- B. build_ai_prompt --


class TestBuildAiPrompt:
    def test_contains_signal_data(self) -> None:
        ctx = {"ticker": "SBER", "direction": "UP", "confidence": 0.63}
        prompt = build_ai_prompt(ctx)
        assert "SBER" in prompt
        assert "UP" in prompt
        assert "0.63" in prompt

    def test_starts_with_instruction(self) -> None:
        prompt = build_ai_prompt({"ticker": "GAZP"})
        assert prompt.startswith("Analyze this trading signal:")


# -- C. format_ai_response --


class TestFormatAiResponse:
    def test_contains_ticker(self) -> None:
        result = format_ai_response("SBER", "Summary: good signal")
        assert "SBER" in result

    def test_contains_analysis(self) -> None:
        result = format_ai_response("SBER", "Summary: good signal")
        assert "Summary: good signal" in result

    def test_has_emoji_prefix(self) -> None:
        result = format_ai_response("SBER", "text")
        assert result.startswith("\U0001f916")


# -- D. call_anthropic --


class TestCallAnthropic:
    @patch("tinvest_trader.services.signal_ai_analysis.urllib.request.urlopen")
    def test_sends_correct_request(self, mock_urlopen: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "content": [{"type": "text", "text": "Summary: test"}],
        }).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = call_anthropic("test-key", "analyze this")
        assert result == "Summary: test"

        req = mock_urlopen.call_args[0][0]
        assert req.get_header("X-api-key") == "test-key"
        assert req.get_header("Content-type") == "application/json"

        body = json.loads(req.data.decode())
        assert body["model"] == "claude-sonnet-4-20250514"
        assert body["messages"][0]["content"] == "analyze this"
        assert "system" in body

    @patch("tinvest_trader.services.signal_ai_analysis.urllib.request.urlopen")
    def test_extracts_multiple_text_blocks(
        self, mock_urlopen: MagicMock,
    ) -> None:
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "content": [
                {"type": "text", "text": "part1"},
                {"type": "text", "text": "part2"},
            ],
        }).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = call_anthropic("key", "msg")
        assert result == "part1\npart2"


# -- E. analyze_signal (orchestration) --


class TestAnalyzeSignal:
    @patch("tinvest_trader.services.signal_ai_analysis.call_anthropic")
    @patch(
        "tinvest_trader.services.signal_delivery._lookup_stats_for_signal",
        return_value=(None, None, None),
    )
    def test_returns_result_on_success(
        self, _mock_stats: MagicMock, mock_api: MagicMock,
    ) -> None:
        mock_api.return_value = "Summary: looks good"
        repo = _make_repo()

        result = analyze_signal(
            _make_signal(), "test-key", repository=repo,
        )

        assert isinstance(result, AiAnalysisResult)
        assert result.signal_id == 42
        assert result.analysis_text == "Summary: looks good"
        assert result.error is None
        assert result.cached is False

    @patch("tinvest_trader.services.signal_ai_analysis.call_anthropic")
    @patch(
        "tinvest_trader.services.signal_delivery._lookup_stats_for_signal",
        return_value=(None, None, None),
    )
    def test_caches_result_in_db(
        self, _mock_stats: MagicMock, mock_api: MagicMock,
    ) -> None:
        mock_api.return_value = "Summary: cached"
        repo = _make_repo()

        analyze_signal(_make_signal(), "test-key", repository=repo)

        repo.insert_ai_analysis.assert_called_once_with(
            42, "Summary: cached", "claude-sonnet-4-20250514",
        )

    def test_returns_cached_result(self) -> None:
        repo = _make_repo()
        repo.get_cached_ai_analysis.return_value = {
            "analysis_text": "Summary: from cache",
            "model": "claude-sonnet-4-20250514",
        }

        result = analyze_signal(_make_signal(), "test-key", repository=repo)

        assert result.cached is True
        assert result.analysis_text == "Summary: from cache"
        # Should NOT call the API
        assert result.error is None

    @patch("tinvest_trader.services.signal_ai_analysis.call_anthropic")
    @patch(
        "tinvest_trader.services.signal_delivery._lookup_stats_for_signal",
        return_value=(None, None, None),
    )
    def test_no_duplicate_api_call_on_cache_hit(
        self, _mock_stats: MagicMock, mock_api: MagicMock,
    ) -> None:
        repo = _make_repo()
        repo.get_cached_ai_analysis.return_value = {
            "analysis_text": "cached",
            "model": "test",
        }

        analyze_signal(_make_signal(), "test-key", repository=repo)
        mock_api.assert_not_called()

    @patch("tinvest_trader.services.signal_ai_analysis.call_anthropic")
    @patch(
        "tinvest_trader.services.signal_delivery._lookup_stats_for_signal",
        return_value=(None, None, None),
    )
    def test_handles_api_failure(
        self, _mock_stats: MagicMock, mock_api: MagicMock,
    ) -> None:
        mock_api.side_effect = ConnectionError("timeout")
        repo = _make_repo()

        result = analyze_signal(
            _make_signal(), "test-key",
            repository=repo,
            logger=logging.getLogger("test"),
        )

        assert result.error is not None
        assert "timeout" in result.error
        assert result.analysis_text == ""

    @patch("tinvest_trader.services.signal_ai_analysis.call_anthropic")
    @patch(
        "tinvest_trader.services.signal_delivery._lookup_stats_for_signal",
        return_value=(None, None, None),
    )
    def test_does_not_cache_on_error(
        self, _mock_stats: MagicMock, mock_api: MagicMock,
    ) -> None:
        mock_api.side_effect = ConnectionError("fail")
        repo = _make_repo()

        analyze_signal(_make_signal(), "test-key", repository=repo)

        repo.insert_ai_analysis.assert_not_called()


# -- F. parse_callback_data --


class TestParseCallbackData:
    def test_valid_ai_callback(self) -> None:
        result = parse_callback_data("ai:signal:42")
        assert result == ("ai_analysis", 42)

    def test_invalid_format(self) -> None:
        assert parse_callback_data("unknown:data") is None

    def test_non_numeric_id(self) -> None:
        assert parse_callback_data("ai:signal:abc") is None

    def test_empty_string(self) -> None:
        assert parse_callback_data("") is None

    def test_too_few_parts(self) -> None:
        assert parse_callback_data("ai:signal") is None

    def test_too_many_parts(self) -> None:
        assert parse_callback_data("ai:signal:42:extra") is None

    def test_wrong_prefix(self) -> None:
        assert parse_callback_data("xx:signal:42") is None


# -- G. poll_and_handle_callbacks --


class TestPollAndHandleCallbacks:
    @patch(
        "tinvest_trader.services.telegram_bot_handler._bot_api_request",
    )
    def test_returns_last_offset_on_empty(
        self, mock_api: MagicMock,
    ) -> None:
        mock_api.return_value = {"ok": True, "result": []}
        repo = _make_repo()
        logger = logging.getLogger("test")

        offset = poll_and_handle_callbacks(
            "token", "123", repo, logger, last_update_id=5,
        )
        assert offset == 5

    @patch(
        "tinvest_trader.services.telegram_bot_handler._bot_api_request",
    )
    def test_returns_last_offset_on_failure(
        self, mock_api: MagicMock,
    ) -> None:
        mock_api.return_value = None
        repo = _make_repo()
        logger = logging.getLogger("test")

        offset = poll_and_handle_callbacks(
            "token", "123", repo, logger, last_update_id=10,
        )
        assert offset == 10

    @patch(
        "tinvest_trader.services.telegram_bot_handler._send_reply",
    )
    @patch(
        "tinvest_trader.services.telegram_bot_handler._answer_callback",
    )
    @patch(
        "tinvest_trader.services.telegram_bot_handler._bot_api_request",
    )
    def test_advances_offset_on_callback(
        self,
        mock_api: MagicMock,
        mock_answer: MagicMock,
        mock_reply: MagicMock,
    ) -> None:
        mock_api.return_value = {
            "ok": True,
            "result": [{
                "update_id": 100,
                "callback_query": {
                    "id": "cb1",
                    "data": "ai:signal:42",
                },
            }],
        }
        mock_reply.return_value = True
        repo = _make_repo()
        logger = logging.getLogger("test")

        with patch(
            "tinvest_trader.services.telegram_bot_handler"
            "._handle_ai_callback",
        ):
            offset = poll_and_handle_callbacks(
                "token", "123", repo, logger,
                api_key="key", last_update_id=0,
            )

        assert offset == 101

    @patch(
        "tinvest_trader.services.telegram_bot_handler._answer_callback",
    )
    @patch(
        "tinvest_trader.services.telegram_bot_handler._bot_api_request",
    )
    def test_answers_unknown_callback(
        self, mock_api: MagicMock, mock_answer: MagicMock,
    ) -> None:
        mock_api.return_value = {
            "ok": True,
            "result": [{
                "update_id": 200,
                "callback_query": {
                    "id": "cb2",
                    "data": "unknown:action",
                },
            }],
        }
        repo = _make_repo()
        logger = logging.getLogger("test")

        poll_and_handle_callbacks(
            "token", "123", repo, logger, last_update_id=0,
        )

        mock_answer.assert_called_once()
        args = mock_answer.call_args
        assert args[0][2] == "Unknown action"
