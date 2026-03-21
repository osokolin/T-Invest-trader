"""Telethon-backed message source for Telegram channel ingestion."""

from __future__ import annotations

import asyncio
from datetime import UTC
from urllib.parse import urlparse

from tinvest_trader.app.config import SentimentConfig
from tinvest_trader.sentiment.models import TelegramMessage
from tinvest_trader.sentiment.source import MessageSource


class TelethonConfigError(ValueError):
    """Raised when Telethon backend configuration is incomplete."""


class TelethonRuntimeError(RuntimeError):
    """Raised when Telethon backend cannot fetch messages."""


def build_telethon_message_source(config: SentimentConfig) -> TelethonMessageSource:
    """Validate config and construct a Telethon-backed message source."""
    missing: list[str] = []
    if config.telethon_api_id is None:
        missing.append("telethon_api_id")
    if not config.telethon_api_hash.strip():
        missing.append("telethon_api_hash")
    if not config.telethon_session_path.strip():
        missing.append("telethon_session_path")

    if missing:
        raise TelethonConfigError(
            "telethon backend requires: " + ", ".join(missing),
        )

    proxy = None
    if config.telethon_proxy_type and config.telethon_proxy_host:
        proxy = (
            config.telethon_proxy_type,
            config.telethon_proxy_host,
            config.telethon_proxy_port,
            config.telethon_proxy_user or None,
            config.telethon_proxy_pass or None,
        )

    return TelethonMessageSource(
        api_id=config.telethon_api_id,
        api_hash=config.telethon_api_hash,
        session_path=config.telethon_session_path,
        poll_limit=config.telethon_poll_limit,
        request_timeout_sec=config.telethon_request_timeout_sec,
        proxy=proxy,
    )


def normalize_channel_identifier(channel_name: str) -> str:
    """Normalize common Telegram channel forms to a bare identifier."""
    value = channel_name.strip()
    if not value:
        return value

    parsed = urlparse(value)
    if parsed.scheme and parsed.netloc in {"t.me", "telegram.me", "www.t.me"}:
        value = parsed.path.strip("/")

    if value.startswith("@"):
        value = value[1:]

    return value.strip("/")


class TelethonMessageSource(MessageSource):
    """Fetches recent Telegram messages through Telethon."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_path: str,
        poll_limit: int = 50,
        request_timeout_sec: float | None = None,
        proxy: tuple | None = None,
    ) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._session_path = session_path
        self._poll_limit = poll_limit
        self._request_timeout_sec = request_timeout_sec
        self._proxy = proxy

    def fetch_recent_messages(
        self,
        channel_name: str,
        min_id: int | None = None,
    ) -> list[TelegramMessage]:
        normalized = normalize_channel_identifier(channel_name)
        try:
            return asyncio.run(
                self._fetch_recent_messages_async(normalized, min_id=min_id),
            )
        except TelethonRuntimeError:
            raise
        except Exception as exc:
            raise TelethonRuntimeError(
                f"failed to fetch Telegram messages for channel '{normalized}'",
            ) from exc

    async def _fetch_recent_messages_async(
        self,
        channel_name: str,
        min_id: int | None = None,
    ) -> list[TelegramMessage]:
        client = self._build_client()
        try:
            await self._run_with_timeout(client.connect())
            if not await self._run_with_timeout(client.is_user_authorized()):
                raise TelethonRuntimeError(
                    f"Telethon session is not authorized: {self._session_path}",
                )
            kwargs: dict = {"limit": self._poll_limit}
            if min_id is not None:
                kwargs["min_id"] = min_id
            raw_messages = await self._run_with_timeout(
                client.get_messages(channel_name, **kwargs),
            )
            return [
                self._map_message(channel_name, raw)
                for raw in raw_messages
                if getattr(raw, "message", None)
            ]
        except TelethonRuntimeError:
            raise
        except Exception as exc:
            raise TelethonRuntimeError(
                f"failed to fetch Telegram messages for channel '{channel_name}'",
            ) from exc
        finally:
            await self._safe_disconnect(client)

    def _build_client(self) -> object:
        try:
            from telethon import TelegramClient
        except ImportError as exc:
            raise TelethonRuntimeError(
                "telethon backend requires the 'telethon' package to be installed",
            ) from exc

        kwargs = {}
        if self._proxy is not None:
            import socks
            proxy_type_map = {
                "socks5": socks.SOCKS5,
                "socks4": socks.SOCKS4,
                "http": socks.HTTP,
            }
            ptype, phost, pport, puser, ppass = self._proxy
            stype = proxy_type_map.get(ptype.lower())
            if stype is not None:
                kwargs["proxy"] = (stype, phost, pport, True, puser, ppass)

        return TelegramClient(
            self._session_path,
            self._api_id,
            self._api_hash,
            **kwargs,
        )

    async def _run_with_timeout(self, awaitable: object) -> object:
        if self._request_timeout_sec is None:
            return await awaitable
        return await asyncio.wait_for(awaitable, timeout=self._request_timeout_sec)

    async def _safe_disconnect(self, client: object) -> None:
        try:
            await self._run_with_timeout(client.disconnect())
        except Exception:
            return

    @staticmethod
    def _map_message(channel_name: str, raw: object) -> TelegramMessage:
        published_at = getattr(raw, "date", None)
        if published_at is not None and published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=UTC)

        return TelegramMessage(
            channel_name=channel_name,
            message_id=str(raw.id),
            message_text=raw.message,
            published_at=published_at,
            source_payload=None,
        )
