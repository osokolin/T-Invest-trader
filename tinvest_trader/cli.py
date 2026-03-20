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
    subparsers.add_parser("list-instruments", help="List all instruments in DB")
    subparsers.add_parser("list-tracked", help="List tracked instruments")

    track_parser = subparsers.add_parser("track", help="Mark a ticker as tracked")
    track_parser.add_argument("ticker", help="Ticker to track (e.g. SBER)")

    untrack_parser = subparsers.add_parser("untrack", help="Unmark a ticker as tracked")
    untrack_parser.add_argument("ticker", help="Ticker to untrack (e.g. SBER)")

    enrich_parser = subparsers.add_parser(
        "enrich-instruments",
        help="Enrich tracked instruments with T-Bank API data",
    )
    enrich_parser.add_argument(
        "--limit", type=int, default=0,
        help="Max instruments to enrich (0 = all)",
    )

    health_parser = subparsers.add_parser(
        "instrument-health",
        help="Check data quality of tracked instruments",
    )
    health_parser.add_argument(
        "--fail-on-issues", action="store_true",
        help="Exit with code 1 if any issues detected",
    )

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
        if args.command == "list-instruments":
            return _run_list_instruments(container)
        if args.command == "list-tracked":
            return _run_list_tracked(container)
        if args.command == "track":
            return _run_track(container, args.ticker)
        if args.command == "untrack":
            return _run_untrack(container, args.ticker)
        if args.command == "enrich-instruments":
            return _run_enrich_instruments(container, args.limit)
        if args.command == "instrument-health":
            return _run_instrument_health(
                container, fail_on_issues=args.fail_on_issues,
            )
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


def _run_list_instruments(container: Container) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 0

    instruments = repository.list_all_instruments()
    if not instruments:
        print("no instruments in database")
        return 0

    print(f"{'TICKER':<10} {'FIGI':<20} {'TRACKED':<9} {'ENABLED':<9} {'MOEX_SECID':<12} {'NAME'}")
    for inst in instruments:
        print(
            f"{inst['ticker']:<10} "
            f"{inst['figi']:<20} "
            f"{'yes' if inst['tracked'] else 'no':<9} "
            f"{'yes' if inst['enabled'] else 'no':<9} "
            f"{inst['moex_secid']:<12} "
            f"{inst['name']}"
        )
    print(f"\ntotal: {len(instruments)}")
    return 0


def _run_list_tracked(container: Container) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 0

    tracked = repository.list_tracked_instruments()
    if not tracked:
        print("no tracked instruments")
        return 0

    print(f"{'TICKER':<10} {'FIGI':<20} {'MOEX_SECID':<12} {'NAME'}")
    for inst in tracked:
        print(
            f"{inst['ticker']:<10} "
            f"{inst['figi']:<20} "
            f"{inst['moex_secid']:<12} "
            f"{inst['name']}"
        )
    print(f"\ntracked: {len(tracked)}")
    return 0


def _run_track(container: Container, ticker: str) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    ticker = ticker.upper()
    # Ensure instrument exists and set tracked
    repository.ensure_instrument(ticker=ticker, tracked=True)
    print(f"tracked: {ticker}")
    return 0


def _run_untrack(container: Container, ticker: str) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    ticker = ticker.upper()
    updated = repository.set_tracked_status(ticker=ticker, tracked=False)
    if updated:
        print(f"untracked: {ticker}")
    else:
        print(f"not found: {ticker}")
    return 0


def _run_instrument_health(
    container: Container,
    *,
    fail_on_issues: bool,
) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.instrument_health import evaluate_instrument_health

    report = evaluate_instrument_health(repository)
    print(f"tracked_total: {report.total_tracked}")
    print(f"complete: {report.complete}")
    print(f"placeholder_figi: {report.placeholder_figi_count}")
    print(f"missing_metadata: {report.missing_metadata_count}")
    print(f"stale_gt_7d: {report.stale_count}")
    if report.instruments_with_issues:
        print("issues:")
        for item in report.instruments_with_issues:
            print(f"  {item.ticker}: {', '.join(item.issues)}")

    if fail_on_issues and report.has_issues:
        return 1
    return 0



def _run_enrich_instruments(container: Container, limit: int) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.instrument_enrichment import enrich_instruments

    result = enrich_instruments(
        repository=repository,
        client=container.tbank_client,
        logger=container.logger,
        limit=limit,
    )
    print(f"processed: {result.processed}")
    print(f"updated: {result.updated}")
    print(f"skipped: {result.skipped}")
    print(f"failed: {result.failed}")
    if result.errors:
        for err in result.errors:
            print(f"  error: {err}")
    return 0


def _close_container(container: Container) -> None:
    if container.storage_pool is not None:
        container.storage_pool.close()


if __name__ == "__main__":
    raise SystemExit(main())
