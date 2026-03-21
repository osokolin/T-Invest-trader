"""Telegram Bot handler -- polls getUpdates for callbacks and commands.

Handles:
- Callback queries from inline keyboard buttons (ai:signal:<id>)
- Operator commands via plain text messages (/last_signals, /signal, /ai, /stats, /help)
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


def _bot_api_request(
    bot_token: str,
    method: str,
    params: dict | None = None,
    *,
    timeout_sec: float = 10.0,
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
) -> dict | None:
    """Make a Telegram Bot API request. Returns parsed JSON or None."""
    url = f"https://api.telegram.org/bot{bot_token}/{method}"

    if params:
        data = json.dumps(params).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, method="POST",
            headers={"Content-Type": "application/json"},
        )
    else:
        req = urllib.request.Request(url, method="GET")

    try:
        if proxy_host and proxy_port:
            from tinvest_trader.services.signal_delivery import (
                _build_socks5_opener,
            )
            opener = _build_socks5_opener(
                proxy_host, proxy_port,
                proxy_user=proxy_user or None,
                proxy_pass=proxy_pass or None,
            )
            with opener.open(req, timeout=timeout_sec) as resp:
                return json.loads(resp.read().decode("utf-8"))
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, json.JSONDecodeError):
        return None


def parse_callback_data(data: str) -> tuple[str, int] | None:
    """Parse callback_data. Returns (action, signal_id) or None.

    Supported formats:
      ai:signal:<id>           -> ("ai_analysis", <id>)
      signal:<id>:details      -> ("signal_details", <id>)
      signal:<id>:ai           -> ("ai_analysis", <id>)
      signal:<id>:stats        -> ("signal_stats", <id>)
      nav:last_signals         -> ("nav_last_signals", 0)
    """
    parts = data.split(":")

    # Legacy: ai:signal:<id>
    if len(parts) == 3 and parts[0] == "ai" and parts[1] == "signal":
        try:
            return ("ai_analysis", int(parts[2]))
        except ValueError:
            return None

    # New: signal:<id>:<action>
    if len(parts) == 3 and parts[0] == "signal":
        try:
            signal_id = int(parts[1])
        except ValueError:
            return None
        action = parts[2]
        if action == "details":
            return ("signal_details", signal_id)
        if action == "ai":
            return ("ai_analysis", signal_id)
        if action == "stats":
            return ("signal_stats", signal_id)
        return None

    # Navigation: nav:<target>
    if len(parts) == 2 and parts[0] == "nav":
        if parts[1] == "last_signals":
            return ("nav_last_signals", 0)
        return None

    return None


def poll_and_handle_callbacks(
    bot_token: str,
    chat_id: str,
    repository: TradingRepository,
    logger: logging.Logger,
    *,
    api_key: str = "",
    ai_model: str = "",
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
    last_update_id: int = 0,
) -> int:
    """Poll for callback queries and handle them.

    Returns the next update_id offset to use (for persistent polling).
    """
    proxy_kw = {
        "proxy_host": proxy_host,
        "proxy_port": proxy_port,
        "proxy_user": proxy_user,
        "proxy_pass": proxy_pass,
    }

    params: dict = {
        "timeout": 1,
        "allowed_updates": ["callback_query", "message"],
    }
    if last_update_id > 0:
        params["offset"] = last_update_id

    result = _bot_api_request(
        bot_token, "getUpdates", params, timeout_sec=15, **proxy_kw,
    )

    if not result or not result.get("ok"):
        return last_update_id

    updates = result.get("result", [])
    if not updates:
        return last_update_id

    next_offset = last_update_id
    for update in updates:
        update_id = update.get("update_id", 0)
        next_offset = max(next_offset, update_id + 1)

        # -- Callback queries (inline button clicks) --
        callback_query = update.get("callback_query")
        if callback_query:
            _process_callback_query(
                callback_query,
                bot_token=bot_token,
                chat_id=chat_id,
                repository=repository,
                logger=logger,
                api_key=api_key,
                ai_model=ai_model,
                **proxy_kw,
            )
            continue

        # -- Message commands (/last_signals, /signal, etc.) --
        message = update.get("message")
        if message:
            _process_message_command(
                message,
                bot_token=bot_token,
                chat_id=chat_id,
                repository=repository,
                logger=logger,
                api_key=api_key,
                ai_model=ai_model,
                **proxy_kw,
            )

    return next_offset


def _answer_callback(
    bot_token: str,
    callback_id: str,
    text: str = "",
    *,
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
) -> None:
    """Answer a callback query (dismisses loading spinner)."""
    _bot_api_request(
        bot_token, "answerCallbackQuery",
        {"callback_query_id": callback_id, "text": text},
        proxy_host=proxy_host,
        proxy_port=proxy_port,
        proxy_user=proxy_user,
        proxy_pass=proxy_pass,
    )


def _send_reply(
    bot_token: str,
    chat_id: str,
    text: str,
    *,
    reply_markup: dict | None = None,
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
) -> bool:
    """Send a reply message, optionally with inline keyboard."""
    params: dict = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        params["reply_markup"] = reply_markup
    result = _bot_api_request(
        bot_token, "sendMessage", params,
        proxy_host=proxy_host,
        proxy_port=proxy_port,
        proxy_user=proxy_user,
        proxy_pass=proxy_pass,
    )
    return result is not None and result.get("ok", False)


def _handle_ai_callback(
    bot_token: str,
    chat_id: str,
    callback_id: str,
    signal_id: int,
    repository: TradingRepository,
    logger: logging.Logger,
    *,
    api_key: str = "",
    ai_model: str = "",
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
) -> None:
    """Handle an AI analysis callback for a signal."""
    from tinvest_trader.services.signal_ai_analysis import (
        AiAnalysisResult,
        analyze_signal,
        format_ai_response,
    )

    proxy_kw = {
        "proxy_host": proxy_host,
        "proxy_port": proxy_port,
        "proxy_user": proxy_user,
        "proxy_pass": proxy_pass,
    }

    # Acknowledge click immediately
    _answer_callback(bot_token, callback_id, "Analyzing...", **proxy_kw)

    if not api_key:
        _send_reply(
            bot_token, chat_id,
            "AI analysis unavailable: API key not configured",
            **proxy_kw,
        )
        return

    # Load signal from DB
    signal = repository.get_signal_prediction(signal_id)
    if signal is None:
        _send_reply(
            bot_token, chat_id,
            f"Signal #{signal_id} not found",
            **proxy_kw,
        )
        return

    model = ai_model or "claude-sonnet-4-20250514"
    result: AiAnalysisResult = analyze_signal(
        signal, api_key,
        repository=repository,
        logger=logger,
        model=model,
        **proxy_kw,
    )

    if result.error:
        _send_reply(
            bot_token, chat_id,
            "AI analysis unavailable right now",
            **proxy_kw,
        )
        return

    ticker = signal.get("ticker", "???")
    reply_text = format_ai_response(ticker, result.analysis_text)

    if result.cached:
        reply_text += "\n(cached)"

    _send_reply(bot_token, chat_id, reply_text, **proxy_kw)

    logger.info(
        "ai callback handled",
        extra={
            "component": "bot_handler",
            "signal_id": signal_id,
            "cached": result.cached,
        },
    )


def _process_callback_query(
    callback_query: dict,
    *,
    bot_token: str,
    chat_id: str,
    repository: TradingRepository,
    logger: logging.Logger,
    api_key: str = "",
    ai_model: str = "",
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
) -> None:
    """Route a callback_query update."""
    from tinvest_trader.services.bot_commands import (
        handle_last_signals_with_buttons,
        handle_signal_detail_with_buttons,
        handle_ticker_stats,
    )

    proxy_kw = {
        "proxy_host": proxy_host,
        "proxy_port": proxy_port,
        "proxy_user": proxy_user,
        "proxy_pass": proxy_pass,
    }

    callback_data = callback_query.get("data", "")
    callback_id = callback_query.get("id", "")

    parsed = parse_callback_data(callback_data)
    if parsed is None:
        _answer_callback(
            bot_token, callback_id, "Unknown action", **proxy_kw,
        )
        return

    action, signal_id = parsed

    if action == "ai_analysis":
        _handle_ai_callback(
            bot_token=bot_token,
            chat_id=chat_id,
            callback_id=callback_id,
            signal_id=signal_id,
            repository=repository,
            logger=logger,
            api_key=api_key,
            ai_model=ai_model,
            **proxy_kw,
        )
        return

    # Acknowledge button immediately for non-AI actions
    _answer_callback(bot_token, callback_id, "", **proxy_kw)

    try:
        if action == "signal_details":
            text, keyboard = handle_signal_detail_with_buttons(
                repository, signal_id,
            )
            markup = {"inline_keyboard": keyboard} if keyboard else None
            _send_reply(
                bot_token, chat_id, text,
                reply_markup=markup, **proxy_kw,
            )

        elif action == "signal_stats":
            text = handle_ticker_stats(repository, signal_id)
            _send_reply(bot_token, chat_id, text, **proxy_kw)

        elif action == "nav_last_signals":
            text, keyboard = handle_last_signals_with_buttons(
                repository, "",
            )
            markup = {"inline_keyboard": keyboard} if keyboard else None
            _send_reply(
                bot_token, chat_id, text,
                reply_markup=markup, **proxy_kw,
            )

    except Exception:
        logger.exception(
            "callback action failed",
            extra={"component": "bot_handler", "action": action},
        )
        _send_reply(
            bot_token, chat_id, "Something went wrong", **proxy_kw,
        )
        return

    logger.info(
        "bot_action",
        extra={
            "component": "bot_handler",
            "type": "callback",
            "action": action,
            "signal_id": signal_id,
        },
    )


def _process_message_command(
    message: dict,
    *,
    bot_token: str,
    chat_id: str,
    repository: TradingRepository,
    logger: logging.Logger,
    api_key: str = "",
    ai_model: str = "",
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
) -> None:
    """Route a text message command. Only responds in configured chat."""
    # Access control: only respond in the configured operator chat
    msg_chat_id = str(message.get("chat", {}).get("id", ""))
    if msg_chat_id != str(chat_id):
        return

    text = message.get("text", "")
    if not text or not text.startswith("/"):
        return

    from tinvest_trader.services.bot_commands import (
        handle_help,
        handle_last_signals_with_buttons,
        handle_signal,
        handle_stats,
        parse_command,
    )

    proxy_kw = {
        "proxy_host": proxy_host,
        "proxy_port": proxy_port,
        "proxy_user": proxy_user,
        "proxy_pass": proxy_pass,
    }

    command, args = parse_command(text)

    response: str | None = None
    reply_markup: dict | None = None

    try:
        if command == "last_signals":
            text_resp, keyboard = handle_last_signals_with_buttons(
                repository, args,
            )
            response = text_resp
            if keyboard:
                reply_markup = {"inline_keyboard": keyboard}
        elif command == "signal":
            response = handle_signal(repository, args)
        elif command == "ai":
            response = _handle_ai_command(
                repository=repository,
                logger=logger,
                args=args,
                api_key=api_key,
                ai_model=ai_model,
                proxy_host=proxy_host,
                proxy_port=proxy_port,
                proxy_user=proxy_user,
                proxy_pass=proxy_pass,
            )
        elif command == "stats":
            response = handle_stats(repository)
        elif command == "help" or command == "start":
            response = handle_help()
        else:
            # Unknown command -- ignore silently
            return
    except Exception:
        logger.exception(
            "bot command failed",
            extra={"component": "bot_handler", "command": command},
        )
        response = "Something went wrong"

    if response:
        _send_reply(
            bot_token, chat_id, response,
            reply_markup=reply_markup, **proxy_kw,
        )

    logger.info(
        "bot_command",
        extra={
            "component": "bot_handler",
            "command": command,
            "ok": response is not None,
        },
    )


def _handle_ai_command(
    repository: TradingRepository,
    logger: logging.Logger,
    args: str,
    *,
    api_key: str = "",
    ai_model: str = "",
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
) -> str:
    """Handle /ai <signal_id> command. Returns response text."""
    if not args:
        return "Usage: /ai <id>"
    try:
        signal_id = int(args)
    except ValueError:
        return "Usage: /ai <id>"

    if not api_key:
        return "AI analysis unavailable: API key not configured"

    from tinvest_trader.services.signal_ai_analysis import (
        analyze_signal,
        format_ai_response,
    )

    signal = repository.get_signal_prediction(signal_id)
    if signal is None:
        return f"Signal #{signal_id} not found"

    model = ai_model or "claude-sonnet-4-20250514"
    result = analyze_signal(
        signal, api_key,
        repository=repository,
        logger=logger,
        model=model,
        proxy_host=proxy_host,
        proxy_port=proxy_port,
        proxy_user=proxy_user,
        proxy_pass=proxy_pass,
    )

    if result.error:
        return "AI analysis unavailable right now"

    ticker = signal.get("ticker", "???")
    reply = format_ai_response(ticker, result.analysis_text)
    if result.cached:
        reply += "\n(cached)"
    return reply
