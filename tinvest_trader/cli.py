from __future__ import annotations

import argparse
from collections.abc import Sequence

from tinvest_trader.app.config import AppConfig, load_config
from tinvest_trader.app.container import Container, build_container


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tinvest_trader.cli",
        description="Operational CLI for tinvest_trader",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("status", help="Show operational status")
    subparsers.add_parser("ingest-sentiment", help="Run sentiment ingestion once")
    subparsers.add_parser("observe", help="Run observation aggregation once")
    subparsers.add_parser("db-summary", help="Show operational database counts")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = load_config()
    container = build_container(config)
    try:
        if args.command == "status":
            return _run_status(config, container)
        if args.command == "ingest-sentiment":
            return _run_ingest_sentiment(config, container)
        if args.command == "observe":
            return _run_observe(container)
        if args.command == "db-summary":
            return _run_db_summary(container)
    finally:
        _close_container(container)

    parser.error(f"unsupported command: {args.command}")
    return 2


def _run_status(config: AppConfig, container: Container) -> int:
    print(f"db_configured: {bool(config.database.postgres_dsn)}")
    print(f"sentiment_enabled: {config.sentiment.enabled}")
    print(f"observation_enabled: {config.observation.enabled}")
    print(f"background_enabled: {config.background.enabled}")
    print(f"sentiment_backend: {config.sentiment.source_backend}")
    print(f"sentiment_service_ready: {container.telegram_sentiment_service is not None}")
    print(f"observation_service_ready: {container.observation_service is not None}")
    print(f"background_runner_ready: {container.background_runner is not None}")
    return 0


def _run_ingest_sentiment(config: AppConfig, container: Container) -> int:
    service = container.telegram_sentiment_service
    if service is None:
        print("sentiment pipeline is disabled")
        return 0

    processed = service.ingest_all_channels(config.sentiment.channels)
    print(f"sentiment_processed: {processed}")
    return 0


def _run_observe(container: Container) -> int:
    service = container.observation_service
    if service is None:
        print("observation pipeline is disabled")
        return 0

    observations = service.observe_all()
    print(f"observations_generated: {len(observations)}")
    return 0


def _run_db_summary(container: Container) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 0

    summary = repository.fetch_operational_summary()
    for key, value in summary.items():
        print(f"{key}: {value}")
    return 0


def _close_container(container: Container) -> None:
    if container.storage_pool is not None:
        container.storage_pool.close()


if __name__ == "__main__":
    raise SystemExit(main())
