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

    # control check: raw branch formatting verification
    # Startup sequence
    container.tbank_client.connect()
    healthy = container.tbank_client.health_check()
    if not healthy:
        logger.error("broker health check failed, exiting")
        sys.exit(1)

    container.trading_service.start()

    logger.info("tinvest_trader started successfully")

    # In future milestones this will be an event loop.
    # For now, just log and exit cleanly.
    if not shutdown_requested:
        logger.info("skeleton mode: no trading loop, shutting down")

    logger.info("tinvest_trader shutdown complete")


if __name__ == "__main__":
    main()
