"""CBR RSS feed fetcher -- downloads feed XML via urllib (stdlib, no deps)."""

from __future__ import annotations

import logging
from urllib.error import URLError
from urllib.request import Request, urlopen

_USER_AGENT = "tinvest_trader/0.1 (cbr-rss)"
_TIMEOUT_SEC = 30


def fetch_rss(url: str, logger: logging.Logger) -> bytes | None:
    """Fetch RSS feed XML from *url*. Returns bytes or None on failure."""
    req = Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urlopen(req, timeout=_TIMEOUT_SEC) as resp:  # noqa: S310
            return resp.read()
    except (URLError, TimeoutError, OSError):
        logger.exception(
            "cbr rss fetch failed",
            extra={"component": "cbr", "url": url},
        )
        return None
