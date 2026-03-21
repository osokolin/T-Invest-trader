"""Tests for Telegram ingestion pipeline v1.

Covers: normalization, dedup, incremental fetch, pacing, stats, CLI.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tinvest_trader.sentiment.models import TelegramMessage
from tinvest_trader.services.telegram_normalization import (
    build_dedup_hash,
    normalize_message_text,
    strip_urls,
)
from tinvest_trader.services.telegram_sentiment_service import (
    IngestionResult,
    TelegramSentimentService,
)

NOW = datetime(2026, 3, 21, 12, 0, 0, tzinfo=UTC)


def _make_msg(
    channel: str = "markettwits",
    msg_id: str = "1001",
    text: str = "#SBER растет",
) -> TelegramMessage:
    return TelegramMessage(
        channel_name=channel,
        message_id=msg_id,
        message_text=text,
        published_at=NOW,
    )


def _make_service(
    messages: list[TelegramMessage] | None = None,
    repo: MagicMock | None = None,
    pacing: float = 0.0,
) -> TelegramSentimentService:
    source = MagicMock()
    source.fetch_recent_messages.return_value = messages or []

    if repo is None:
        repo = MagicMock()
        repo.insert_telegram_message_raw.return_value = True
        repo.check_dedup_hash_exists.return_value = False
        repo.get_latest_message_id_by_channel.return_value = None

    return TelegramSentimentService(
        source=source,
        parser_fn=lambda text: [],
        mapper=MagicMock(),
        scorer=MagicMock(),
        repository=repo,
        logger=logging.getLogger("test"),
        channel_pacing_seconds=pacing,
    )


# -- Text normalization tests --

class TestNormalizeMessageText:
    def test_strips_whitespace(self) -> None:
        assert normalize_message_text("  hello  ") == "hello"

    def test_collapses_whitespace(self) -> None:
        assert normalize_message_text("a   b\t\tc") == "a b c"

    def test_lowercases(self) -> None:
        assert normalize_message_text("HELLO World") == "hello world"

    def test_unicode_normalization(self) -> None:
        # NFC normalization
        result = normalize_message_text("cafe\u0301")
        assert result == "caf\u00e9"

    def test_preserves_content(self) -> None:
        text = "#SBER растет на фоне сильных результатов"
        result = normalize_message_text(text)
        assert "sber" in result
        assert "растет" in result

    def test_empty_string(self) -> None:
        assert normalize_message_text("") == ""


class TestStripUrls:
    def test_removes_urls(self) -> None:
        text = "Check https://example.com/news for details"
        assert strip_urls(text) == "Check  for details"

    def test_no_urls(self) -> None:
        assert strip_urls("plain text") == "plain text"

    def test_multiple_urls(self) -> None:
        text = "see http://a.com and https://b.com"
        result = strip_urls(text)
        assert "http" not in result


class TestBuildDedupHash:
    def test_deterministic(self) -> None:
        h1 = build_dedup_hash("markettwits", "hello world")
        h2 = build_dedup_hash("markettwits", "hello world")
        assert h1 == h2

    def test_different_sources_different_hash(self) -> None:
        h1 = build_dedup_hash("markettwits", "hello")
        h2 = build_dedup_hash("banksta", "hello")
        assert h1 != h2

    def test_ignores_whitespace_differences(self) -> None:
        h1 = build_dedup_hash("src", "hello  world")
        h2 = build_dedup_hash("src", "hello world")
        assert h1 == h2

    def test_ignores_urls(self) -> None:
        h1 = build_dedup_hash("src", "news https://example.com")
        h2 = build_dedup_hash("src", "news")
        assert h1 == h2

    def test_returns_hex_string(self) -> None:
        h = build_dedup_hash("src", "text")
        assert len(h) == 16
        assert all(c in "0123456789abcdef" for c in h)


# -- Hard dedup tests --

class TestHardDedup:
    def test_hard_duplicate_detected(self) -> None:
        """B. Same (source, message_id) -> hard duplicate."""
        msg = _make_msg()
        repo = MagicMock()
        repo.insert_telegram_message_raw.return_value = False  # already exists
        repo.check_dedup_hash_exists.return_value = False
        repo.get_latest_message_id_by_channel.return_value = None
        svc = _make_service([msg], repo=repo)

        result = svc.ingest_channel("markettwits")

        assert result.hard_duplicates == 1
        assert result.inserted == 0

    def test_new_message_inserted(self) -> None:
        msg = _make_msg()
        repo = MagicMock()
        repo.insert_telegram_message_raw.return_value = True
        repo.check_dedup_hash_exists.return_value = False
        repo.get_latest_message_id_by_channel.return_value = None
        svc = _make_service([msg], repo=repo)

        result = svc.ingest_channel("markettwits")

        assert result.inserted == 1
        assert result.hard_duplicates == 0


# -- Soft dedup tests --

class TestSoftDedup:
    def test_soft_duplicate_detected(self) -> None:
        """C. Same normalized text hash -> soft duplicate."""
        msg = _make_msg()
        repo = MagicMock()
        repo.check_dedup_hash_exists.return_value = True
        repo.get_latest_message_id_by_channel.return_value = None
        svc = _make_service([msg], repo=repo)

        result = svc.ingest_channel("markettwits")

        assert result.soft_duplicates == 1
        assert result.inserted == 0

    def test_no_soft_duplicate_proceeds(self) -> None:
        msg = _make_msg()
        repo = MagicMock()
        repo.check_dedup_hash_exists.return_value = False
        repo.insert_telegram_message_raw.return_value = True
        repo.get_latest_message_id_by_channel.return_value = None
        svc = _make_service([msg], repo=repo)

        result = svc.ingest_channel("markettwits")

        assert result.inserted == 1


# -- Incremental fetch tests --

class TestIncrementalFetch:
    def test_passes_min_id_to_source(self) -> None:
        """E. Incremental: passes latest stored message_id as min_id."""
        repo = MagicMock()
        repo.get_latest_message_id_by_channel.return_value = 5000
        repo.insert_telegram_message_raw.return_value = True
        repo.check_dedup_hash_exists.return_value = False
        svc = _make_service(repo=repo)
        svc._source.fetch_recent_messages.return_value = []

        svc.ingest_all_channels(("markettwits",))

        svc._source.fetch_recent_messages.assert_called_once_with(
            "markettwits", min_id=5000,
        )

    def test_no_stored_messages_fetches_all(self) -> None:
        repo = MagicMock()
        repo.get_latest_message_id_by_channel.return_value = None
        repo.insert_telegram_message_raw.return_value = True
        repo.check_dedup_hash_exists.return_value = False
        svc = _make_service(repo=repo)
        svc._source.fetch_recent_messages.return_value = []

        svc.ingest_all_channels(("markettwits",))

        svc._source.fetch_recent_messages.assert_called_once_with(
            "markettwits", min_id=None,
        )


# -- Source failure isolation --

class TestSourceFailureIsolation:
    def test_failed_source_does_not_break_others(self) -> None:
        """F. One source fails -> others still processed."""
        repo = MagicMock()
        repo.get_latest_message_id_by_channel.return_value = None
        repo.insert_telegram_message_raw.return_value = True
        repo.check_dedup_hash_exists.return_value = False

        svc = _make_service(repo=repo, pacing=0.0)

        call_count = 0

        def side_effect(channel, min_id=None):
            nonlocal call_count
            call_count += 1
            if channel == "banksta":
                msg = "connection error"
                raise ConnectionError(msg)
            return [_make_msg(channel, "1")]

        svc._source.fetch_recent_messages.side_effect = side_effect

        result = svc.ingest_all_channels_detailed(("banksta", "markettwits"))

        assert result.sources_processed == 2
        assert result.failed_sources == ["banksta"]
        assert result.inserted == 1

    def test_failed_source_logged_in_result(self) -> None:
        svc = _make_service(pacing=0.0)
        svc._source.fetch_recent_messages.side_effect = RuntimeError("fail")

        result = svc.ingest_channel("bad_channel")

        assert "bad_channel" in result.failed_sources
        assert result.messages_fetched == 0


# -- Multiple source ingestion --

class TestMultipleSourceIngestion:
    def test_all_channels_processed(self) -> None:
        """A. Multiple sources all ingested."""
        repo = MagicMock()
        repo.get_latest_message_id_by_channel.return_value = None
        repo.insert_telegram_message_raw.return_value = True
        repo.check_dedup_hash_exists.return_value = False

        svc = _make_service(repo=repo, pacing=0.0)
        svc._source.fetch_recent_messages.side_effect = lambda ch, min_id=None: [
            _make_msg(ch, "1"),
        ]

        result = svc.ingest_all_channels_detailed(
            ("markettwits", "banksta", "cbrstocks"),
        )

        assert result.sources_processed == 3
        assert result.inserted == 3


# -- Channel pacing --

class TestChannelPacing:
    def test_pacing_between_channels(self) -> None:
        repo = MagicMock()
        repo.get_latest_message_id_by_channel.return_value = None
        repo.insert_telegram_message_raw.return_value = True
        repo.check_dedup_hash_exists.return_value = False

        svc = _make_service(repo=repo, pacing=0.1)
        svc._source.fetch_recent_messages.return_value = []

        with patch("tinvest_trader.services.telegram_sentiment_service.time.sleep") as mock_sleep:
            svc.ingest_all_channels(("a", "b", "c"))
            assert mock_sleep.call_count == 2  # between a-b and b-c


# -- IngestionResult --

class TestIngestionResult:
    def test_default_values(self) -> None:
        r = IngestionResult()
        assert r.sources_processed == 0
        assert r.messages_fetched == 0
        assert r.inserted == 0
        assert r.hard_duplicates == 0
        assert r.soft_duplicates == 0
        assert r.failed_sources == []


# -- Normalization stored with raw message --

class TestNormalizationStored:
    def test_normalized_text_passed_to_repository(self) -> None:
        msg = _make_msg(text="  HELLO  World  ")
        repo = MagicMock()
        repo.insert_telegram_message_raw.return_value = True
        repo.check_dedup_hash_exists.return_value = False
        repo.get_latest_message_id_by_channel.return_value = None
        svc = _make_service([msg], repo=repo)

        svc.ingest_channel("markettwits")

        call_kwargs = repo.insert_telegram_message_raw.call_args
        assert call_kwargs[1]["normalized_text"] == "hello world"
        assert call_kwargs[1]["dedup_hash"] is not None
