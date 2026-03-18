from __future__ import annotations

import signal
import sys

from tinvest_trader.app.config import load_config
from tinvest_trader.app.container import build_container


def main() -> None:
    config = load_config()
    container = build_container(config)
    logger = container.logger

    logger.info("tinvest_trader starting", extra={"environment": config.environment})

    shutdown_requested = False

    def handle_signal(signum: int, _frame: object) -> None:
        nonlocal shutdown_requested
        sig_name = signal.Signals(signum).name
        logger.info("shutdown signal received", extra={"signal": sig_name})
        shutdown_requested = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Database setup (optional -- skipped when no DSN configured)
    if container.storage_pool is not None:
        container.storage_pool.initialize_schema()
        if not container.storage_pool.health_check():
            logger.error("database health check failed, exiting")
            sys.exit(1)
        logger.info("database connected and schema ready")

    # Broker startup
    container.tbank_client.connect()
    healthy = container.tbank_client.health_check()
    if not healthy:
        logger.error("broker health check failed, exiting")
        _shutdown(container, logger)
        sys.exit(1)

    container.trading_service.start()

    # Sentiment status
    if container.telegram_sentiment_service is not None:
        logger.info("sentiment pipeline ready", extra={"component": "sentiment"})
    else:
        logger.info("sentiment pipeline disabled", extra={"component": "sentiment"})

    # Observation status
    if container.observation_service is not None:
        logger.info("observation pipeline ready", extra={"component": "observation"})
    else:
        logger.info("observation pipeline disabled", extra={"component": "observation"})

    logger.info("tinvest_trader started successfully")

    # In future milestones this will be an event loop.
    # For now, just log and exit cleanly.
    if not shutdown_requested:
        logger.info("skeleton mode: no trading loop, shutting down")

    _shutdown(container, logger)


def _shutdown(container: object, logger: object) -> None:
    """Clean up resources."""
    pool = getattr(container, "storage_pool", None)
    if pool is not None:
        pool.close()
    logger.info("tinvest_trader shutdown complete")


if __name__ == "__main__":
    main()
