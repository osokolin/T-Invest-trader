from __future__ import annotations

import signal
import sys
import threading

from tinvest_trader.app.config import load_config
from tinvest_trader.app.container import build_container


def main() -> None:
    config = load_config()
    container = build_container(config)
    logger = container.logger

    logger.info("tinvest_trader starting", extra={"environment": config.environment})

    stop_event = threading.Event()

    def handle_signal(signum: int, _frame: object) -> None:
        sig_name = signal.Signals(signum).name
        logger.info("shutdown signal received", extra={"signal": sig_name})
        stop_event.set()

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

    # Broker events status
    if container.broker_event_ingestion_service is not None:
        logger.info("broker events pipeline ready", extra={"component": "broker_events"})
    else:
        logger.info("broker events pipeline disabled", extra={"component": "broker_events"})

    # Fusion status
    if container.fusion_service is not None:
        logger.info("fusion layer ready", extra={"component": "fusion"})
    else:
        logger.info("fusion layer disabled", extra={"component": "fusion"})

    # Background runner status
    if container.background_runner is not None:
        logger.info("background runner enabled", extra={"component": "background_runner"})
        container.background_runner.start()
    else:
        logger.info("background runner disabled", extra={"component": "background_runner"})

    logger.info("tinvest_trader started successfully")

    # Block until SIGINT/SIGTERM. Future milestones will replace this
    # with a real event/trading loop.
    logger.info("waiting for shutdown signal (SIGINT/SIGTERM)")
    stop_event.wait()

    _shutdown(container, logger)


def _shutdown(container: object, logger: object) -> None:
    """Clean up resources."""
    runner = getattr(container, "background_runner", None)
    if runner is not None:
        runner.stop()
    pool = getattr(container, "storage_pool", None)
    if pool is not None:
        pool.close()
    logger.info("tinvest_trader shutdown complete")


if __name__ == "__main__":
    main()
