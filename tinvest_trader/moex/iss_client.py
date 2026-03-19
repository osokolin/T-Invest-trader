"""MOEX ISS HTTP client -- stdlib only, no external dependencies."""

from __future__ import annotations

import json
import logging
import urllib.request
from urllib.error import HTTPError, URLError

ISS_BASE = "https://iss.moex.com"
_TIMEOUT = 30
_USER_AGENT = "tinvest_trader/1.0"


def fetch_iss_json(
    path: str,
    logger: logging.Logger,
    params: dict[str, str] | None = None,
) -> dict | None:
    """Fetch a JSON response from MOEX ISS API.

    ``path`` should start with ``/`` (e.g. ``/iss/securities/SBER.json``).
    Returns parsed JSON dict or None on failure.
    """
    url = ISS_BASE + path
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"

    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            data = resp.read()
        return json.loads(data)
    except (HTTPError, URLError, json.JSONDecodeError, OSError) as exc:
        logger.warning(
            "moex iss fetch failed",
            extra={"url": url, "error": str(exc)},
        )
        return None


def fetch_security_info(secid: str, logger: logging.Logger) -> dict | None:
    """Fetch security metadata from /iss/securities/{secid}.json."""
    return fetch_iss_json(f"/iss/securities/{secid}.json", logger)


def fetch_market_history(
    secid: str,
    engine: str,
    market: str,
    logger: logging.Logger,
    date_from: str | None = None,
    date_till: str | None = None,
    start: int = 0,
) -> dict | None:
    """Fetch daily market history page from ISS.

    ``date_from`` and ``date_till`` use YYYY-MM-DD format.
    ``start`` is the pagination offset (default 0).
    """
    path = f"/iss/history/engines/{engine}/markets/{market}/securities/{secid}.json"
    params: dict[str, str] = {"start": str(start)}
    if date_from:
        params["from"] = date_from
    if date_till:
        params["till"] = date_till
    return fetch_iss_json(path, logger, params)
