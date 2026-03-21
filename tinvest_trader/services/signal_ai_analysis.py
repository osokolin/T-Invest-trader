"""AI signal analysis via Anthropic Claude API.

Provides structured analysis for trading signals on demand.
Uses simple HTTP (urllib) -- no SDK dependency required.
Results are cached in DB to avoid duplicate API calls.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 1024
CACHE_TTL_SECONDS = 30  # dedup window for rapid clicks


@dataclass
class AiAnalysisResult:
    """Result of an AI analysis request."""

    signal_id: int
    analysis_text: str
    model: str = DEFAULT_MODEL
    cached: bool = False
    error: str | None = None


def build_signal_context(
    signal: dict,
    ticker_stats: dict | None = None,
    type_stats: dict | None = None,
    source_stats: dict | None = None,
) -> dict:
    """Build a structured context dict for the AI prompt."""
    ctx: dict = {
        "ticker": signal.get("ticker", "???"),
        "direction": (signal.get("signal_type") or "???").upper(),
        "confidence": signal.get("confidence"),
        "price_at_signal": signal.get("price_at_signal"),
        "source_channel": signal.get("source_channel"),
        "source": signal.get("source"),
        "outcome_label": signal.get("outcome_label"),
        "return_pct": signal.get("return_pct"),
    }

    created_at = signal.get("created_at")
    if isinstance(created_at, datetime):
        ctx["created_at"] = created_at.strftime("%Y-%m-%d %H:%M")
    elif created_at:
        ctx["created_at"] = str(created_at)

    if ticker_stats and ticker_stats.get("resolved", 0) > 0:
        wins = ticker_stats.get("wins", 0)
        resolved = ticker_stats["resolved"]
        avg_ret = ticker_stats.get("avg_return", 0.0) or 0.0
        wr = wins / resolved if resolved else 0.0
        ctx["ticker_win_rate"] = round(wr, 3)
        ctx["ticker_avg_return"] = round(avg_ret, 6)
        ctx["ticker_resolved"] = resolved

    if type_stats and type_stats.get("resolved", 0) > 0:
        wins = type_stats.get("wins", 0)
        resolved = type_stats["resolved"]
        wr = wins / resolved if resolved else 0.0
        ctx["type_win_rate"] = round(wr, 3)
        ctx["type_resolved"] = resolved

    if source_stats and source_stats.get("resolved", 0) > 0:
        wins = source_stats.get("wins", 0)
        resolved = source_stats["resolved"]
        avg_ret = source_stats.get("avg_return", 0.0) or 0.0
        wr = wins / resolved if resolved else 0.0
        ctx["source_win_rate"] = round(wr, 3)
        ctx["source_avg_return"] = round(avg_ret, 6)
        ctx["source_resolved"] = resolved

    # Remove None values for cleaner prompt
    return {k: v for k, v in ctx.items() if v is not None}


_SYSTEM_PROMPT = """\
You are a trading signal analyst. Analyze the signal context provided.
Be concise. Use plain text, no markdown. Keep total response under 800 chars.
Respond in Russian.

Output EXACTLY this format:

Итог: <1-2 предложения>
Быки: <1 предложение>
Медведи: <1 предложение>
Риски: <1 предложение>
Применимость: <1 предложение>
Уверенность ИИ: <НИЗКАЯ or СРЕДНЯЯ or ВЫСОКАЯ>"""


def build_ai_prompt(context: dict) -> str:
    """Build the user message for Claude."""
    ctx_str = json.dumps(context, ensure_ascii=False, indent=2)
    return f"Analyze this trading signal:\n{ctx_str}"


def call_anthropic(
    api_key: str,
    user_message: str,
    *,
    model: str = DEFAULT_MODEL,
    max_tokens: int = MAX_TOKENS,
    timeout_sec: float = 30.0,
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
) -> str:
    """Call Anthropic Messages API. Returns response text or raises."""
    payload = json.dumps({
        "model": model,
        "max_tokens": max_tokens,
        "system": _SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_message}],
    }).encode("utf-8")

    req = urllib.request.Request(
        ANTHROPIC_API_URL,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
    )

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
            body = json.loads(resp.read().decode("utf-8"))
    else:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            body = json.loads(resp.read().decode("utf-8"))

    # Extract text from Messages API response
    content = body.get("content", [])
    parts = [block["text"] for block in content if block.get("type") == "text"]
    return "\n".join(parts)


def format_ai_response(ticker: str, analysis_text: str) -> str:
    """Format AI analysis for Telegram delivery."""
    return f"\U0001f916 AI Analysis -- {ticker}\n\n{analysis_text}"


def analyze_signal(
    signal: dict,
    api_key: str,
    repository: TradingRepository | None = None,
    logger: logging.Logger | None = None,
    *,
    model: str = DEFAULT_MODEL,
    proxy_host: str = "",
    proxy_port: int = 0,
    proxy_user: str = "",
    proxy_pass: str = "",
) -> AiAnalysisResult:
    """Run AI analysis for a signal. Uses cache if available."""
    signal_id = signal.get("id", 0)
    ticker = signal.get("ticker", "???")

    # Check cache
    if repository is not None and signal_id:
        cached = repository.get_cached_ai_analysis(signal_id)
        if cached is not None:
            if logger:
                logger.info(
                    "ai analysis cache hit",
                    extra={
                        "component": "ai_analysis",
                        "signal_id": signal_id,
                    },
                )
            return AiAnalysisResult(
                signal_id=signal_id,
                analysis_text=cached["analysis_text"],
                model=cached.get("model", model),
                cached=True,
            )

    # Fetch stats
    ticker_stats: dict | None = None
    type_stats: dict | None = None
    source_stats: dict | None = None

    if repository is not None:
        from tinvest_trader.services.signal_delivery import (
            _lookup_stats_for_signal,
        )
        ticker_stats, type_stats, source_stats = _lookup_stats_for_signal(
            signal, repository,
        )

    # Build prompt and call API
    context = build_signal_context(
        signal,
        ticker_stats=ticker_stats,
        type_stats=type_stats,
        source_stats=source_stats,
    )
    user_message = build_ai_prompt(context)

    try:
        analysis_text = call_anthropic(
            api_key, user_message,
            model=model,
            proxy_host=proxy_host,
            proxy_port=proxy_port,
            proxy_user=proxy_user,
            proxy_pass=proxy_pass,
        )
    except Exception as exc:
        error_msg = f"API call failed: {exc}"
        if logger:
            logger.warning(
                "ai analysis failed",
                extra={
                    "component": "ai_analysis",
                    "signal_id": signal_id,
                    "error": str(exc),
                },
            )
        return AiAnalysisResult(
            signal_id=signal_id,
            analysis_text="",
            model=model,
            error=error_msg,
        )

    # Cache result
    if repository is not None and signal_id:
        repository.insert_ai_analysis(signal_id, analysis_text, model)

    if logger:
        logger.info(
            "ai analysis complete",
            extra={
                "component": "ai_analysis",
                "signal_id": signal_id,
                "ticker": ticker,
                "model": model,
            },
        )

    return AiAnalysisResult(
        signal_id=signal_id,
        analysis_text=analysis_text,
        model=model,
    )
