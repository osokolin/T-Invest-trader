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

    ingest_tg_parser = subparsers.add_parser(
        "ingest-telegram",
        help="One-shot Telegram ingestion with detailed stats",
    )
    ingest_tg_parser.add_argument(
        "--limit-per-source", type=int, default=0,
        help="Override fetch limit per source (0 = use config default)",
    )

    subparsers.add_parser(
        "telegram-sources",
        help="List configured Telegram source channels",
    )
    subparsers.add_parser(
        "telegram-ingest-status",
        help="Show per-channel Telegram ingestion status",
    )
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
    enrich_parser.add_argument(
        "--alert", action="store_true",
        help="Send health alert after enrichment if issues remain",
    )

    sync_parser = subparsers.add_parser(
        "sync-share-catalog",
        help="Bulk-sync T-Bank share catalog into local DB",
    )
    sync_parser.add_argument(
        "--limit", type=int, default=0,
        help="Max shares to sync (0 = all)",
    )

    health_parser = subparsers.add_parser(
        "instrument-health",
        help="Check data quality of tracked instruments",
    )
    health_parser.add_argument(
        "--fail-on-issues", action="store_true",
        help="Exit with code 1 if any issues detected",
    )
    health_parser.add_argument(
        "--alert", action="store_true",
        help="Send alert via Telegram if issues detected",
    )

    fetch_status_parser = subparsers.add_parser(
        "broker-fetch-policy-status",
        help="Show broker event fetch policy status",
    )
    fetch_status_parser.add_argument(
        "--limit", type=int, default=10,
        help="Max examples to show per category (default 10)",
    )
    fetch_status_parser.add_argument(
        "--stale-seconds", type=int, default=172800,
        help="Stale threshold in seconds (default 172800 = 48h)",
    )
    fetch_status_parser.add_argument(
        "--alert", action="store_true",
        help="Send alert via Telegram if issues detected",
    )
    fetch_status_parser.add_argument(
        "--fail-on-issues", action="store_true",
        help="Exit with code 1 if any issues detected",
    )

    binding_parser = subparsers.add_parser(
        "market-binding-debug",
        help="Debug market binding for a ticker",
    )
    binding_parser.add_argument("ticker", help="Ticker to bind (e.g. SBER)")
    binding_parser.add_argument(
        "--direction", type=str, default=None,
        help="Signal direction (e.g. up, down, buy, sell)",
    )
    binding_parser.add_argument(
        "--window", type=str, default=None,
        help="Signal window/timeframe (e.g. 5m, 1h, day)",
    )
    binding_parser.add_argument(
        "--min-score", type=float, default=0.5,
        help="Minimum score threshold (default 0.5)",
    )
    binding_parser.add_argument(
        "--min-gap", type=float, default=0.2,
        help="Minimum gap between top candidates (default 0.2)",
    )
    binding_parser.add_argument(
        "--no-exact-ticker", action="store_true",
        help="Allow non-exact ticker matches",
    )

    # -- execution-safety-debug --
    safety_parser = subparsers.add_parser(
        "execution-safety-debug",
        help="Debug execution safety checks for a ticker",
    )
    safety_parser.add_argument("ticker", help="Ticker (e.g. SBER)")
    safety_parser.add_argument(
        "--close-minutes", type=int, default=None,
        help="Simulated minutes until market close (default: no close time)",
    )
    safety_parser.add_argument(
        "--market-status", type=str, default=None,
        help="Simulated market status (open, closed, expired)",
    )
    safety_parser.add_argument(
        "--min-time-to-close", type=int, default=90,
        help="Min seconds before close to allow execution (default 90)",
    )

    # -- signal-stats --
    subparsers.add_parser(
        "signal-stats",
        help="Show signal prediction statistics",
    )

    # -- signal-calibration-report --
    subparsers.add_parser(
        "signal-calibration-report",
        help="Show signal calibration report with EV and filtering status",
    )

    # -- sync-quotes --
    sync_quotes_parser = subparsers.add_parser(
        "sync-quotes",
        help="Bulk-fetch last prices from T-Bank for tracked instruments",
    )
    sync_quotes_parser.add_argument(
        "--limit", type=int, default=0,
        help="Max instruments to fetch (0 = all)",
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
        if args.command == "ingest-telegram":
            return _run_ingest_telegram(config, container)
        if args.command == "telegram-sources":
            return _run_telegram_sources(config)
        if args.command == "telegram-ingest-status":
            return _run_telegram_ingest_status(container)
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
        if args.command == "sync-share-catalog":
            return _run_sync_share_catalog(container, args.limit)
        if args.command == "enrich-instruments":
            return _run_enrich_instruments(
                container, args.limit, alert=args.alert,
            )
        if args.command == "instrument-health":
            return _run_instrument_health(
                container,
                fail_on_issues=args.fail_on_issues,
                alert=args.alert,
            )
        if args.command == "broker-fetch-policy-status":
            return _run_broker_fetch_policy_status(
                container,
                config,
                limit=args.limit,
                stale_seconds=args.stale_seconds,
                alert=args.alert,
                fail_on_issues=args.fail_on_issues,
            )
        if args.command == "signal-stats":
            return _run_signal_stats(container)
        if args.command == "signal-calibration-report":
            return _run_signal_calibration_report(container, config)
        if args.command == "market-binding-debug":
            return _run_market_binding_debug(
                container,
                ticker=args.ticker,
                direction=args.direction,
                window=args.window,
                min_score=args.min_score,
                min_gap=args.min_gap,
                require_exact=not args.no_exact_ticker,
            )
        if args.command == "execution-safety-debug":
            return _run_execution_safety_debug(
                ticker=args.ticker,
                close_minutes=args.close_minutes,
                market_status=args.market_status,
                min_time_to_close=args.min_time_to_close,
            )
        if args.command == "sync-quotes":
            return _run_sync_quotes(container, limit=args.limit)
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


def _run_ingest_telegram(config: AppConfig, container: Container) -> int:
    service = container.telegram_sentiment_service
    if service is None:
        print("sentiment pipeline is disabled")
        return 0

    result = service.ingest_all_channels_detailed(config.sentiment.channels)
    print(f"sources_processed: {result.sources_processed}")
    print(f"messages_fetched: {result.messages_fetched}")
    print(f"inserted: {result.inserted}")
    print(f"hard_duplicates: {result.hard_duplicates}")
    print(f"soft_duplicates: {result.soft_duplicates}")
    print(f"failed_sources: {len(result.failed_sources)}")
    if result.failed_sources:
        for src in result.failed_sources:
            print(f"  failed: {src}")
    return 0


def _run_telegram_sources(config: AppConfig) -> int:
    channels = config.sentiment.channels
    if not channels:
        print("no telegram channels configured")
        return 0

    print(f"configured channels ({len(channels)}):")
    for ch in channels:
        print(f"  {ch}")
    print(f"backend: {config.sentiment.source_backend}")
    print(f"poll_limit: {config.sentiment.telethon_poll_limit}")
    return 0


def _run_telegram_ingest_status(container: Container) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    statuses = repository.get_telegram_ingest_status()
    if not statuses:
        print("no telegram messages ingested yet")
        return 0

    print(f"{'CHANNEL':<20} {'MESSAGES':>10} {'LATEST PUBLISHED':<28} {'MAX MSG ID':>12}")
    for s in statuses:
        pub = str(s["latest_published"] or "n/a")[:25]
        mid = str(s["max_message_id"] or "n/a")
        print(
            f"{s['channel']:<20} {s['total_messages']:>10} "
            f"{pub:<28} {mid:>12}",
        )
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
    alert: bool = False,
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

    if alert:
        from tinvest_trader.services.instrument_alerting import (
            send_instrument_health_alert,
        )
        send_instrument_health_alert(report)

    if fail_on_issues and report.has_issues:
        return 1
    return 0



def _run_sync_share_catalog(container: Container, limit: int) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.share_catalog_sync import sync_share_catalog

    result = sync_share_catalog(
        repository=repository,
        client=container.tbank_client,
        logger=container.logger,
        limit=limit,
    )
    print(f"synced: {result.synced}")
    print(f"inserted: {result.inserted}")
    print(f"updated: {result.updated}")
    print(f"skipped: {result.skipped}")
    print(f"failed: {result.failed}")
    return 0


def _run_enrich_instruments(
    container: Container,
    limit: int,
    *,
    alert: bool = False,
) -> int:
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

    if alert:
        from tinvest_trader.services.instrument_alerting import (
            send_instrument_health_alert,
        )
        from tinvest_trader.services.instrument_health import (
            evaluate_instrument_health,
        )
        report = evaluate_instrument_health(repository)
        send_instrument_health_alert(report)

    return 0


def _run_broker_fetch_policy_status(
    container: Container,
    config: AppConfig,
    *,
    limit: int = 10,
    stale_seconds: int = 172800,
    alert: bool = False,
    fail_on_issues: bool = False,
) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.broker_fetch_policy_observability import (
        build_fetch_policy_report,
        format_report,
        send_fetch_policy_alert,
    )
    from tinvest_trader.services.tbank_event_fetch_policy import FetchPolicyConfig

    cfg = config.broker_events
    policy_config = FetchPolicyConfig(
        enabled=cfg.fetch_policy_enabled,
        dividends_ttl_seconds=cfg.dividends_ttl_seconds,
        reports_ttl_seconds=cfg.reports_ttl_seconds,
        insider_deals_ttl_seconds=cfg.insider_deals_ttl_seconds,
        failure_cooldown_seconds=cfg.fetch_policy_failure_cooldown_seconds,
        max_consecutive_failures=cfg.fetch_policy_max_consecutive_failures,
        max_fetches_per_cycle=cfg.fetch_policy_max_fetches_per_cycle,
    )

    report = build_fetch_policy_report(
        repository,
        policy_config,
        stale_seconds=stale_seconds,
        limit=limit,
    )
    print(format_report(report))

    if alert:
        send_fetch_policy_alert(report)

    if fail_on_issues and report.has_issues:
        return 1
    return 0


def _run_market_binding_debug(
    container: Container,
    *,
    ticker: str,
    direction: str | None = None,
    window: str | None = None,
    min_score: float = 0.5,
    min_gap: float = 0.2,
    require_exact: bool = True,
) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.market_binding import (
        BindingConfig,
        bind_signal,
        build_signal,
        candidates_from_instruments,
        format_binding_debug,
    )

    instruments = repository.list_all_instruments()
    if not instruments:
        print("no instruments in database")
        return 1

    signal = build_signal(
        ticker=ticker,
        direction=direction,
        window=window,
    )
    candidates = candidates_from_instruments(instruments)
    config = BindingConfig(
        min_score=min_score,
        min_gap=min_gap,
        require_exact_ticker=require_exact,
        require_market_open=False,  # CLI debug -- don't check market state
    )
    result = bind_signal(
        signal=signal,
        market_candidates=candidates,
        config=config,
        logger=container.logger,
    )

    # Show signal info
    lines = [f"signal: ticker={signal.ticker}"]
    if signal.direction:
        lines.append(f"  direction={signal.direction}")
    if signal.window:
        lines.append(f"  window={signal.window}")
    print("\n".join(lines))

    print(format_binding_debug(result, ticker))
    return 0


def _run_execution_safety_debug(
    *,
    ticker: str,
    close_minutes: int | None = None,
    market_status: str | None = None,
    min_time_to_close: int = 90,
) -> int:
    from datetime import UTC, datetime, timedelta

    from tinvest_trader.services.execution_safety import (
        ExecutionSafetyConfig,
        check_pre_execution,
        format_safety_debug,
    )

    now = datetime.now(UTC)
    close_time = None
    if close_minutes is not None:
        close_time = now + timedelta(minutes=close_minutes)

    config = ExecutionSafetyConfig(
        enabled=True,
        min_time_to_close_seconds=min_time_to_close,
    )

    pre = check_pre_execution(
        close_time=close_time,
        market_status=market_status,
        config=config,
        now=now,
    )

    print(format_safety_debug(ticker, pre, close_time, config, now))
    return 0


def _run_signal_stats(container: Container) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.signal_outcome import format_signal_stats

    stats = repository.get_signal_stats()
    by_ticker = repository.get_signal_stats_by_ticker()
    by_type = repository.get_signal_stats_by_type()
    print(format_signal_stats(stats, by_ticker, by_type))
    return 0


def _run_signal_calibration_report(
    container: Container, config: AppConfig,
) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.signal_calibration import (
        CalibrationConfig,
        format_calibration_report,
    )

    cal_cfg = config.signal_calibration
    cal = CalibrationConfig(
        min_confidence=cal_cfg.min_confidence,
        min_win_rate=cal_cfg.min_win_rate,
        min_ev=cal_cfg.min_ev,
        enable_up=cal_cfg.enable_up,
        enable_down=cal_cfg.enable_down,
        min_resolved_for_filter=cal_cfg.min_resolved_for_filter,
    )
    by_ticker = repository.get_signal_stats_by_ticker()
    by_type = repository.get_signal_stats_by_type()
    print(format_calibration_report(cal, by_ticker, by_type))
    return 0


def _run_sync_quotes(container: Container, *, limit: int = 0) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.quote_sync import sync_quotes

    result = sync_quotes(
        client=container.tbank_client,
        repository=repository,
        logger=container.logger,
        limit=limit,
    )
    print(f"requested: {result.requested}")
    print(f"received: {result.received}")
    print(f"inserted: {result.inserted}")
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
