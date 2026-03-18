"""Tests for telegram repository methods."""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

from tinvest_trader.infra.storage.repository import TradingRepository
from tinvest_trader.sentiment.models import SentimentResult, TelegramMessage, TickerMention


def _make_repo():
    pool = MagicMock()
    conn = MagicMock()
    pool.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
    pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)
    logger = logging.getLogger("test")
    repo = TradingRepository(pool=pool, logger=logger)
    return repo, conn


def _make_msg() -> TelegramMessage:
    return TelegramMessage(
        channel_name="MarketTwits",
        message_id="1001",
        message_text="#SBER растет",
        published_at=datetime(2026, 3, 18, 12, 0, tzinfo=UTC),
    )


def _make_mention() -> TickerMention:
    return TickerMention(ticker="SBER", mention_type="hashtag", figi="BBG004730N88")


def _make_sentiment() -> SentimentResult:
    return SentimentResult(
        label="positive",
        score_positive=0.75,
        score_negative=0.10,
        score_neutral=0.15,
        model_name="stub",
        scored_at=datetime(2026, 3, 18, 12, 0, tzinfo=UTC),
    )


def test_insert_telegram_message_raw():
    repo, conn = _make_repo()
    cur = MagicMock()
    cur.fetchone.return_value = (1,)
    conn.execute.return_value = cur
    result = repo.insert_telegram_message_raw(_make_msg())
    assert result is True
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()
    sql = conn.execute.call_args[0][0]
    assert "telegram_messages_raw" in sql
    assert "ON CONFLICT" in sql


def test_insert_telegram_message_raw_duplicate():
    repo, conn = _make_repo()
    cur = MagicMock()
    cur.fetchone.return_value = None
    conn.execute.return_value = cur
    result = repo.insert_telegram_message_raw(_make_msg())
    assert result is False


def test_insert_telegram_message_mention():
    repo, conn = _make_repo()
    repo.insert_telegram_message_mention(_make_msg(), _make_mention())
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()
    sql = conn.execute.call_args[0][0]
    assert "telegram_message_mentions" in sql


def test_insert_telegram_sentiment_event():
    repo, conn = _make_repo()
    repo.insert_telegram_sentiment_event(_make_msg(), _make_mention(), _make_sentiment())
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()
    sql = conn.execute.call_args[0][0]
    assert "telegram_sentiment_events" in sql


def test_ticker_stored_uppercase():
    repo, conn = _make_repo()
    mention = TickerMention(ticker="sber", mention_type="hashtag")
    repo.insert_telegram_message_mention(_make_msg(), mention)
    params = conn.execute.call_args[0][1]
    # ticker param should be uppercased
    assert params[4] == "SBER"
