from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from tinvest_trader.app.config import LoggingConfig


class JSONFormatter(logging.Formatter):
    """Structured JSON log formatter with correlation_id support."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", None),
        }

        # Include extra fields passed via `extra={...}`
        for key in ("environment", "signal", "component"):
            value = getattr(record, key, None)
            if value is not None:
                log_entry[key] = value

        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def setup_logging(config: LoggingConfig) -> logging.Logger:
    """Create and configure the application logger."""
    logger = logging.getLogger("tinvest_trader")
    logger.setLevel(config.level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        if config.json_output:
            handler.setFormatter(JSONFormatter())
        else:
            handler.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
            )
        logger.addHandler(handler)

    return logger


def new_correlation_id() -> str:
    return uuid.uuid4().hex[:12]
