"""Tests for services/telegram_sentiment_service.py -- orchestration flow."""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

from tinvest_trader.sentiment.instrument_mapper import InstrumentMapper
from tinvest_trader.sentiment.models import TelegramMessage
from tinvest_trader.sentiment.parser import extract_tickers
from tinvest_trader.sentiment.scorer import StubSentimentScorer
from tinvest_trader.sentiment.source import StubMessageSource
from tinvest_trader.services.telegram_sentiment_service import TelegramSentimentService


def _make_repo(**overrides: object) -> MagicMock:
    repo = MagicMock()
    repo.insert_telegram_message_raw.return_value = overrides.get(
        "insert_raw", True,
    )
    repo.check_dedup_hash_exists.return_value = overrides.get(
        "dedup_exists", False,
    )
    repo.get_latest_message_id_by_channel.return_value = overrides.get(
        "latest_msg_id",
    )
    return repo


def _make_service(
    messages: list[TelegramMessage] | None = None,
    tracked_tickers: frozenset[str] | None = None,
    repository: MagicMock | None = None,
) -> TelegramSentimentService:
    source = StubMessageSource(messages=messages)
    scorer = StubSentimentScorer(model_name="test")
    mapper = InstrumentMapper(
        ticker_to_figi={"SBER": "BBG004730N88", "GAZP": "BBG004730RP0"},
        tracked_tickers=tracked_tickers or frozenset(),
    )
    if repository is None:
        repository = _make_repo()
    return TelegramSentimentService(
        source=source,
        parser_fn=extract_tickers,
        mapper=mapper,
        scorer=scorer,
        repository=repository,
        logger=logging.getLogger("test"),
        channel_pacing_seconds=0.0,
    )


def _msg(message_id: str, text: str) -> TelegramMessage:
    return TelegramMessage(
        channel_name="TestChannel",
        message_id=message_id,
        message_text=text,
        published_at=datetime(2026, 3, 18, 12, 0, tzinfo=UTC),
    )


def test_ingest_channel_returns_result():
    messages = [_msg("1", "#SBER рост"), _msg("2", "#GAZP падение")]
    svc = _make_service(messages=messages)
    result = svc.ingest_channel("TestChannel")
    assert result.inserted == 2
    assert result.sources_processed == 1


def test_ingest_stores_raw_messages():
    repo = _make_repo()
    messages = [_msg("1", "#SBER рост")]
    svc = _make_service(messages=messages, repository=repo)
    svc.ingest_channel("TestChannel")
    repo.insert_telegram_message_raw.assert_called_once()


def test_ingest_stores_mentions():
    repo = _make_repo()
    messages = [_msg("1", "#SBER рост")]
    svc = _make_service(messages=messages, repository=repo)
    svc.ingest_channel("TestChannel")
    repo.insert_telegram_message_mention.assert_called_once()


def test_ingest_stores_sentiment():
    repo = _make_repo()
    messages = [_msg("1", "#SBER рост")]
    svc = _make_service(messages=messages, repository=repo)
    svc.ingest_channel("TestChannel")
    repo.insert_telegram_sentiment_event.assert_called_once()


def test_ingest_skips_duplicate_messages():
    repo = _make_repo(insert_raw=False)
    messages = [_msg("1", "#SBER рост")]
    svc = _make_service(messages=messages, repository=repo)
    result = svc.ingest_channel("TestChannel")
    assert result.hard_duplicates == 1
    assert result.inserted == 0
    repo.insert_telegram_message_mention.assert_not_called()
    repo.insert_telegram_sentiment_event.assert_not_called()


def test_ingest_filters_by_tracked_tickers():
    repo = _make_repo()
    messages = [_msg("1", "#SBER рост и #UNKNOWN тоже")]
    svc = _make_service(
        messages=messages,
        tracked_tickers=frozenset({"SBER"}),
        repository=repo,
    )
    svc.ingest_channel("TestChannel")
    # Only SBER mention should be stored
    assert repo.insert_telegram_message_mention.call_count == 1
    assert repo.insert_telegram_sentiment_event.call_count == 1


def test_ingest_no_tickers_still_stores_raw():
    repo = _make_repo()
    messages = [_msg("1", "Рынок без изменений")]
    svc = _make_service(messages=messages, repository=repo)
    result = svc.ingest_channel("TestChannel")
    assert result.inserted == 1
    repo.insert_telegram_message_raw.assert_called_once()
    repo.insert_telegram_message_mention.assert_not_called()


def test_ingest_all_channels():
    messages = [_msg("1", "#SBER рост")]
    svc = _make_service(messages=messages)
    count = svc.ingest_all_channels(("ch1", "ch2"))
    assert count == 2  # 1 message per channel


def test_ingest_works_without_repository():
    svc = TelegramSentimentService(
        source=StubMessageSource(messages=[_msg("1", "#SBER рост")]),
        parser_fn=extract_tickers,
        mapper=InstrumentMapper(ticker_to_figi={}, tracked_tickers=frozenset()),
        scorer=StubSentimentScorer(),
        repository=None,
        logger=logging.getLogger("test"),
    )
    result = svc.ingest_channel("TestChannel")
    assert result.inserted == 1


def test_ingest_survives_repository_failure():
    repo = _make_repo()
    repo.insert_telegram_message_raw.side_effect = RuntimeError("db down")
    messages = [_msg("1", "#SBER рост")]
    svc = _make_service(messages=messages, repository=repo)
    result = svc.ingest_channel("TestChannel")
    # Should not crash -- _store_raw catches exceptions and returns True
    assert result.inserted == 1
