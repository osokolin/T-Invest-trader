from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from tinvest_trader.app.config import SentimentConfig
from tinvest_trader.sentiment.telethon_source import (
    TelethonConfigError,
    TelethonMessageSource,
    TelethonRuntimeError,
    build_telethon_message_source,
    normalize_channel_identifier,
)


def test_build_telethon_message_source_requires_config():
    cfg = SentimentConfig(source_backend="telethon")

    with pytest.raises(TelethonConfigError):
        build_telethon_message_source(cfg)


def test_build_telethon_message_source_uses_minimal_runtime_config():
    cfg = SentimentConfig(
        source_backend="telethon",
        telethon_api_id=12345,
        telethon_api_hash="hash-value",
        telethon_session_path="/tmp/test.session",
        telethon_poll_limit=25,
        telethon_request_timeout_sec=4.0,
    )

    source = build_telethon_message_source(cfg)

    assert isinstance(source, TelethonMessageSource)
    assert source._poll_limit == 25
    assert source._request_timeout_sec == 4.0


def test_normalize_channel_identifier_accepts_common_forms():
    assert normalize_channel_identifier("markettwits") == "markettwits"
    assert normalize_channel_identifier("@markettwits") == "markettwits"
    assert normalize_channel_identifier("https://t.me/markettwits") == "markettwits"


def test_fetch_recent_messages_normalizes_channel_and_maps_messages(monkeypatch):
    source = TelethonMessageSource(
        api_id=12345,
        api_hash="hash-value",
        session_path="/tmp/test.session",
        poll_limit=10,
    )
    captured: dict[str, str] = {}
    raw_messages = [
        SimpleNamespace(
            id=1001,
            message="#SBER рост",
            date=datetime(2026, 3, 18, 12, 0, tzinfo=UTC),
        ),
        SimpleNamespace(id=1002, message="", date=datetime(2026, 3, 18, 12, 5, tzinfo=UTC)),
    ]

    async def fake_fetch(channel_name: str):
        captured["channel_name"] = channel_name
        return [source._map_message(channel_name, item) for item in raw_messages if item.message]

    monkeypatch.setattr(source, "_fetch_recent_messages_async", fake_fetch)

    messages = source.fetch_recent_messages("https://t.me/markettwits")

    assert captured["channel_name"] == "markettwits"
    assert len(messages) == 1
    assert messages[0].channel_name == "markettwits"
    assert messages[0].message_id == "1001"
    assert messages[0].message_text == "#SBER рост"
    assert messages[0].source_payload is None


def test_fetch_recent_messages_raises_runtime_error_on_failure(monkeypatch):
    source = TelethonMessageSource(
        api_id=12345,
        api_hash="hash-value",
        session_path="/tmp/test.session",
    )

    async def fake_fetch(channel_name: str):
        raise RuntimeError(f"boom: {channel_name}")

    monkeypatch.setattr(source, "_fetch_recent_messages_async", fake_fetch)

    with pytest.raises(TelethonRuntimeError):
        source.fetch_recent_messages("@markettwits")


def test_map_message_adds_utc_to_naive_datetime():
    message = TelethonMessageSource._map_message(
        "markettwits",
        SimpleNamespace(
            id=1001,
            message="#SBER рост",
            date=datetime(2026, 3, 18, 12, 0),
        ),
    )

    assert message.published_at is not None
    assert message.published_at.tzinfo is UTC
