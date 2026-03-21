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

    # -- telegram-source-report --
    tg_source_report_parser = subparsers.add_parser(
        "telegram-source-report",
        help="Show signal performance attribution by Telegram source",
    )
    tg_source_report_parser.add_argument(
        "--min-resolved", type=int, default=0,
        help="Min resolved signals to include a source (default 0)",
    )

    # -- send-test-signal --
    test_signal_parser = subparsers.add_parser(
        "send-test-signal",
        help="Send a test signal to Telegram to verify bot setup",
    )
    test_signal_parser.add_argument(
        "ticker", type=str, help="Ticker symbol (e.g. SBER)",
    )
    test_signal_parser.add_argument(
        "--direction", type=str, default="up",
        help="Signal direction: up or down (default up)",
    )
    test_signal_parser.add_argument(
        "--confidence", type=float, default=0.5,
        help="Confidence value (default 0.5)",
    )

    # -- deliver-signals --
    subparsers.add_parser(
        "deliver-signals",
        help="Deliver pending undelivered signals to Telegram",
    )

    # -- preview-signal-message --
    preview_parser = subparsers.add_parser(
        "preview-signal-message",
        help="Preview enriched Telegram message for a test signal",
    )
    preview_parser.add_argument(
        "ticker", type=str, help="Ticker symbol (e.g. SBER)",
    )
    preview_parser.add_argument(
        "--direction", type=str, default="up",
        help="Signal direction: up or down (default up)",
    )
    preview_parser.add_argument(
        "--confidence", type=float, default=0.65,
        help="Confidence value (default 0.65)",
    )

    # -- signal-divergence-report --
    subparsers.add_parser(
        "signal-divergence-report",
        help="Show signal pipeline funnel and divergence analysis",
    )

    # -- ai-divergence-report --
    ai_div_parser = subparsers.add_parser(
        "ai-divergence-report",
        help="Show AI vs system severity divergence analysis",
    )
    ai_div_parser.add_argument(
        "--min-resolved", type=int, default=0,
        help="Min resolved signals per bucket to include (default 0)",
    )

    # -- ai-gating-report --
    subparsers.add_parser(
        "ai-gating-report",
        help="Show AI shadow-mode gating comparison report",
    )

    # -- test-ai-analysis --
    ai_parser = subparsers.add_parser(
        "test-ai-analysis",
        help="Run AI analysis for a signal (by signal_id)",
    )
    ai_parser.add_argument(
        "signal_id", type=int, help="Signal prediction ID",
    )

    # -- source-weighting-report --
    sw_report_parser = subparsers.add_parser(
        "source-weighting-report",
        help="Show source-aware weighting shadow analysis report",
    )
    sw_report_parser.add_argument(
        "--min-resolved", type=int, default=0,
        help="Min resolved signals to include a source (default 0)",
    )
    sw_report_parser.add_argument(
        "--threshold", type=float, default=0.6,
        help="Weighted confidence threshold for comparison (default 0.6)",
    )

    # -- apply-source-weights --
    sw_apply_parser = subparsers.add_parser(
        "apply-source-weights",
        help="Compute and store source weights for unweighted signals (shadow)",
    )
    sw_apply_parser.add_argument(
        "--limit", type=int, default=500,
        help="Max signals to process (default 500)",
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
        if args.command == "send-test-signal":
            return _run_send_test_signal(
                config, container,
                ticker=args.ticker,
                direction=args.direction,
                confidence=args.confidence,
            )
        if args.command == "deliver-signals":
            return _run_deliver_signals(config, container)
        if args.command == "telegram-source-report":
            return _run_telegram_source_report(
                container, min_resolved=args.min_resolved,
            )
        if args.command == "preview-signal-message":
            return _run_preview_signal_message(
                config, container,
                ticker=args.ticker,
                direction=args.direction,
                confidence=args.confidence,
            )
        if args.command == "signal-divergence-report":
            return _run_signal_divergence_report(container)
        if args.command == "ai-divergence-report":
            return _run_ai_divergence_report(
                container, min_resolved=args.min_resolved,
            )
        if args.command == "ai-gating-report":
            return _run_ai_gating_report(container)
        if args.command == "source-weighting-report":
            return _run_source_weighting_report(
                container,
                min_resolved=args.min_resolved,
                threshold=args.threshold,
            )
        if args.command == "apply-source-weights":
            return _run_apply_source_weights(
                container, limit=args.limit,
            )
        if args.command == "test-ai-analysis":
            return _run_test_ai_analysis(
                config, container, signal_id=args.signal_id,
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


def _run_telegram_source_report(
    container: Container, *, min_resolved: int = 0,
) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.source_attribution import (
        build_source_performance_report,
        format_source_performance_report,
    )

    report = build_source_performance_report(repository)
    print(format_source_performance_report(report, min_resolved=min_resolved))
    return 0


def _run_send_test_signal(
    config: AppConfig,
    container: Container,
    *,
    ticker: str,
    direction: str,
    confidence: float,
) -> int:
    cfg = config.signal_delivery
    if not cfg.bot_token or not cfg.chat_id:
        print("TINVEST_TELEGRAM_BOT_TOKEN and TINVEST_TELEGRAM_CHAT_ID must be set")
        return 1

    from datetime import UTC, datetime

    from tinvest_trader.services.signal_delivery import send_telegram_message
    from tinvest_trader.services.signal_severity import (
        SeverityConfig,
        classify_signal_severity,
        format_enriched_signal_message,
    )

    signal = {
        "id": 0,
        "ticker": ticker,
        "signal_type": direction,
        "confidence": confidence,
        "price_at_signal": None,
        "created_at": datetime.now(tz=UTC),
        "source_channel": "test",
        "source": "test",
        "return_pct": None,
        "outcome_label": None,
    }

    sev_cfg = SeverityConfig(
        high_confidence=cfg.high_confidence_threshold,
        high_ev=cfg.high_ev_threshold,
    )
    severity = classify_signal_severity(signal, config=sev_cfg)
    text = format_enriched_signal_message(signal, severity)
    print(f"severity: {severity.level}")
    print(f"message:\n{text}\n")

    import json as _json

    keyboard = _json.dumps({
        "inline_keyboard": [[
            {"text": "\U0001f50d AI", "callback_data": "ai:signal:0"},
        ]],
    })

    sent = send_telegram_message(
        cfg.bot_token, cfg.chat_id, text,
        proxy_host=cfg.proxy_host,
        proxy_port=cfg.proxy_port,
        proxy_user=cfg.proxy_user,
        proxy_pass=cfg.proxy_pass,
        reply_markup=keyboard,
    )
    if sent:
        print("sent: ok")
        return 0
    print("sent: failed")
    return 1


def _run_deliver_signals(config: AppConfig, container: Container) -> int:
    cfg = config.signal_delivery
    if not cfg.bot_token or not cfg.chat_id:
        print("TINVEST_TELEGRAM_BOT_TOKEN and TINVEST_TELEGRAM_CHAT_ID must be set")
        return 1
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.signal_delivery import deliver_pending_signals
    from tinvest_trader.services.signal_severity import SeverityConfig

    sev_cfg = SeverityConfig(
        high_confidence=cfg.high_confidence_threshold,
        high_ev=cfg.high_ev_threshold,
    )

    sent = deliver_pending_signals(
        bot_token=cfg.bot_token,
        chat_id=cfg.chat_id,
        repository=repository,
        logger=container.logger,
        proxy_host=cfg.proxy_host,
        proxy_port=cfg.proxy_port,
        proxy_user=cfg.proxy_user,
        proxy_pass=cfg.proxy_pass,
        max_per_cycle=cfg.max_per_cycle,
        severity_config=sev_cfg,
    )
    print(f"delivered: {sent}")
    return 0


def _run_preview_signal_message(
    config: AppConfig,
    container: Container,
    *,
    ticker: str,
    direction: str,
    confidence: float,
) -> int:
    from datetime import UTC, datetime

    from tinvest_trader.services.signal_delivery import _lookup_stats_for_signal
    from tinvest_trader.services.signal_severity import (
        SeverityConfig,
        classify_signal_severity,
        format_enriched_signal_message,
    )

    cfg = config.signal_delivery
    signal = {
        "id": 0,
        "ticker": ticker,
        "signal_type": direction,
        "confidence": confidence,
        "price_at_signal": None,
        "created_at": datetime.now(tz=UTC),
        "source_channel": None,
        "source": "preview",
        "return_pct": None,
        "outcome_label": None,
    }

    ticker_stats: dict | None = None
    type_stats: dict | None = None
    source_stats: dict | None = None
    if container.repository is not None:
        ticker_stats, type_stats, source_stats = _lookup_stats_for_signal(
            signal, container.repository,
        )

    sev_cfg = SeverityConfig(
        high_confidence=cfg.high_confidence_threshold,
        high_ev=cfg.high_ev_threshold,
    )
    severity = classify_signal_severity(
        signal,
        ticker_stats=ticker_stats,
        type_stats=type_stats,
        source_stats=source_stats,
        config=sev_cfg,
    )

    text = format_enriched_signal_message(
        signal, severity,
        ticker_stats=ticker_stats,
        type_stats=type_stats,
    )

    print(f"severity: {severity.level}")
    if severity.reasons:
        for r in severity.reasons:
            print(f"  - {r}")
    print(f"\n{text}")
    return 0


def _run_signal_divergence_report(container: Container) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.signal_divergence import (
        build_divergence_report,
        format_divergence_report,
    )

    report = build_divergence_report(repository)
    print(format_divergence_report(report))
    return 0


def _run_ai_divergence_report(
    container: Container,
    *,
    min_resolved: int = 0,
) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.ai_divergence import (
        build_ai_divergence_report,
        format_ai_divergence_report,
    )

    report = build_ai_divergence_report(
        repository, min_resolved=min_resolved,
    )
    print(format_ai_divergence_report(report))
    return 0


def _run_ai_gating_report(container: Container) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.ai_gating_report import (
        build_ai_gating_report,
        format_ai_gating_report,
    )

    report = build_ai_gating_report(repository)
    print(format_ai_gating_report(report))
    return 0


def _run_test_ai_analysis(
    config: AppConfig,
    container: Container,
    *,
    signal_id: int,
) -> int:
    cfg = config.signal_delivery
    if not cfg.anthropic_api_key:
        print("TINVEST_ANTHROPIC_API_KEY must be set")
        return 1
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    signal = repository.get_signal_prediction(signal_id)
    if signal is None:
        print(f"signal #{signal_id} not found")
        return 1

    from tinvest_trader.services.signal_ai_analysis import (
        analyze_signal,
        format_ai_response,
    )

    result = analyze_signal(
        signal, cfg.anthropic_api_key,
        repository=repository,
        logger=container.logger,
        model=cfg.ai_model,
        proxy_host=cfg.proxy_host,
        proxy_port=cfg.proxy_port,
        proxy_user=cfg.proxy_user,
        proxy_pass=cfg.proxy_pass,
    )

    if result.error:
        print(f"error: {result.error}")
        return 1

    ticker = signal.get("ticker", "???")
    print(f"model: {result.model}")
    print(f"cached: {result.cached}")
    print()
    print(format_ai_response(ticker, result.analysis_text))
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


def _run_source_weighting_report(
    container: Container,
    *,
    min_resolved: int = 0,
    threshold: float = 0.6,
) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.source_weighting import (
        build_source_weighting_report,
        format_source_weighting_report,
    )

    report = build_source_weighting_report(
        repository,
        threshold=threshold,
        min_resolved=min_resolved,
    )
    print(format_source_weighting_report(report))
    return 0


def _run_apply_source_weights(
    container: Container,
    *,
    limit: int = 500,
) -> int:
    repository = container.repository
    if repository is None:
        print("database is not configured")
        return 1

    from tinvest_trader.services.source_weighting import apply_source_weights

    updated = apply_source_weights(
        repository, container.logger, limit=limit,
    )
    print(f"signals_updated: {updated}")
    return 0


def _close_container(container: Container) -> None:
    if container.storage_pool is not None:
        container.storage_pool.close()


if __name__ == "__main__":
    raise SystemExit(main())
