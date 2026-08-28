from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class BrokerConfig:
    token: str = ""
    sandbox: bool = True
    app_name: str = "tinvest_trader"
    account_id: str = ""


@dataclass(frozen=True)
class TradingConfig:
    max_position_size: int = 1
    max_order_size: int = 1
    max_daily_loss_pct: float = 2.0
    max_trades_per_session: int = 10
    enabled_instruments: tuple[str, ...] = ()


@dataclass(frozen=True)
class MarketDataConfig:
    tracked_instruments: tuple[str, ...] = ()


@dataclass(frozen=True)
class DatabaseConfig:
    postgres_dsn: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 5


@dataclass(frozen=True)
class SentimentConfig:
    enabled: bool = False
    channels: tuple[str, ...] = ()
    tracked_tickers: tuple[str, ...] = ()
    model_name: str = "stub"
    source_backend: str = "stub"
    telethon_api_id: int | None = None
    telethon_api_hash: str = ""
    telethon_session_path: str = ""
    telethon_poll_limit: int = 50
    telethon_request_timeout_sec: float | None = None
    telethon_proxy_type: str = ""
    telethon_proxy_host: str = ""
    telethon_proxy_port: int = 0
    telethon_proxy_user: str = ""
    telethon_proxy_pass: str = ""


@dataclass(frozen=True)
class ObservationConfig:
    enabled: bool = False
    windows: tuple[str, ...] = ("5m", "15m", "1h")
    persist_derived_metrics: bool = True
    tracked_tickers: tuple[str, ...] = ()


@dataclass(frozen=True)
class BackgroundConfig:
    enabled: bool = False
    sentiment_ingest_interval_seconds: int = 300
    observation_interval_seconds: int = 600
    fusion_interval_seconds: int = 600
    run_sentiment: bool = True
    run_observation: bool = True
    run_fusion: bool = True
    run_cbr: bool = True
    run_moex: bool = True
    run_quote_sync: bool = True
    run_signal_delivery: bool = True
    run_global_context: bool = True
    run_global_market_data: bool = True
    run_signal_generation: bool = True
    run_alerting: bool = True
    run_daily_digest: bool = True
    run_signal_resolution: bool = True
    run_paper_portfolio: bool = True
    run_market_activity: bool = True
    run_market_activity_outcomes: bool = True
    run_activity_paper_strategy: bool = True


@dataclass(frozen=True)
class BrokerEventsConfig:
    enabled: bool = False
    event_types: tuple[str, ...] = ("dividends", "reports", "insider_deals")
    poll_interval_seconds: int = 1800
    lookback_days: int | None = None
    dividends_lookback_days: int = 365
    reports_lookback_days: int = 365
    insider_deals_lookback_days: int = 3650
    tracked_figis_override: tuple[str, ...] = ()
    # Fetch policy settings
    fetch_policy_enabled: bool = True
    dividends_ttl_seconds: int = 86400
    reports_ttl_seconds: int = 86400
    insider_deals_ttl_seconds: int = 86400
    fetch_policy_failure_cooldown_seconds: int = 3600
    fetch_policy_max_consecutive_failures: int = 5
    fetch_policy_max_fetches_per_cycle: int = 0


@dataclass(frozen=True)
class FusionConfig:
    enabled: bool = False
    windows: tuple[str, ...] = ("5m", "15m", "1h", "1d", "7d", "30d")
    persist: bool = True
    tracked_tickers: tuple[str, ...] = ()


@dataclass(frozen=True)
class CbrConfig:
    enabled: bool = False
    rss_enabled: bool = True
    rss_urls: tuple[str, ...] = (
        "http://www.cbr.ru/rss/eventrss",
        "http://www.cbr.ru/rss/RssPress",
    )
    poll_interval_seconds: int = 3600
    store_raw_payloads: bool = True


@dataclass(frozen=True)
class MoexConfig:
    enabled: bool = False
    metadata_enabled: bool = True
    history_enabled: bool = True
    poll_interval_seconds: int = 3600
    history_lookback_days: int = 90
    tracked_tickers_override: tuple[str, ...] = ()
    engine: str = "stock"
    market: str = "shares"
    board: str = "TQBR"


@dataclass(frozen=True)
class ExecutionSafetyEnvConfig:
    enabled: bool = True
    min_time_to_close_seconds: int = 90


@dataclass(frozen=True)
class SignalGenerationConfig:
    enabled: bool = False
    min_message_count: int = 3
    min_sentiment_balance: float = 0.3
    lookback_minutes: int = 30
    cooldown_minutes: int = 30
    poll_interval_seconds: int = 60


@dataclass(frozen=True)
class SignalCalibrationConfig:
    enabled: bool = False
    eval_window_seconds: int = 300
    dry_run: bool = False
    min_confidence: float = 0.0
    min_win_rate: float = 0.0
    min_ev: float = 0.0
    enable_up: bool = True
    enable_down: bool = True
    min_resolved_for_filter: int = 5


@dataclass(frozen=True)
class SignalResolutionConfig:
    enabled: bool = True
    eval_window_seconds: int = 300
    max_quote_delay_seconds: int = 900
    poll_interval_seconds: int = 120


@dataclass(frozen=True)
class PaperPortfolioConfig:
    """Configuration for the virtual portfolio used in shadow trading."""

    enabled: bool = False
    name: str = "shadow-v1"
    initial_cash: float = 1_000_000.0
    position_fraction: float = 0.10
    max_open_positions: int = 5
    commission_rate: float = 0.0005
    slippage_rate: float = 0.0005
    poll_interval_seconds: int = 60
    unresolved_position_expiry_minutes: int = 180
    entry_stages: tuple[str, ...] = ("delivered",)


@dataclass(frozen=True)
class QuoteSyncConfig:
    enabled: bool = False
    poll_interval_seconds: int = 60
    max_source_age_seconds: int = 604800


@dataclass(frozen=True)
class MarketActivityConfig:
    """Settings for observational market-activity spike detection."""

    enabled: bool = False
    poll_interval_seconds: int = 60
    candle_interval: str = "CANDLE_INTERVAL_1_MIN"
    lookback_minutes: int = 60
    baseline_candles: int = 20
    min_volume: int = 1
    volume_spike_multiplier: float = 3.0
    price_change_spike_pct: float = 0.01
    session_filter_enabled: bool = True
    session_start_hour_moscow: int = 9
    session_start_minute_moscow: int = 50
    session_end_hour_moscow: int = 18
    session_end_minute_moscow: int = 50


@dataclass(frozen=True)
class MarketActivityOutcomeConfig:
    """Settings for shadow evaluation of detected activity spikes."""

    enabled: bool = False
    poll_interval_seconds: int = 60
    horizons_minutes: tuple[int, ...] = (5, 15, 60)
    neutral_threshold_pct: float = 0.0005
    eod_enabled: bool = True
    eod_hour_moscow: int = 23
    eod_minute_moscow: int = 50
    lookback_days: int = 30
    resolution_limit: int = 500
    max_price_delay_minutes: int = 30


@dataclass(frozen=True)
class ActivityPaperConfig:
    """Settings for isolated market-activity virtual portfolios."""

    enabled: bool = False
    poll_interval_seconds: int = 60
    momentum_portfolio_name: str = "activity-momentum-v1"
    reversion_portfolio_name: str = "activity-reversion-v1"
    volume_confirmed_enabled: bool = False
    volume_confirmed_portfolio_name: str = "activity-volume-confirmed-v1"
    volume_confirmation_min_move_pct: float = 0.002
    volume_confirmation_max_delay_minutes: int = 3
    volume_confirmed_v2_enabled: bool = False
    volume_confirmed_v2_portfolio_name: str = "activity-volume-confirmed-v2"
    volume_confirmed_v2_min_score: float = 80.0
    volume_confirmed_v2_min_volume_ratio: float = 5.0
    volume_confirmed_v2_min_move_pct: float = 0.004
    volume_confirmed_v2_max_delay_minutes: int = 2
    volume_confirmed_v2_cooldown_minutes: int = 120
    volume_confirmed_v2_max_entries_per_day: int = 20
    horizon: str = "15m"
    initial_cash: float = 1_000_000.0
    position_fraction: float = 0.02
    max_open_positions: int = 10
    max_open_positions_per_ticker: int = 1
    commission_rate: float = 0.0005
    slippage_rate: float = 0.0005
    min_score: float = 45.0
    allowed_severities: tuple[str, ...] = ("medium", "high")
    allowed_spike_types: tuple[str, ...] = ("volume_price", "price_momentum")
    cooldown_minutes: int = 30
    max_candidate_age_minutes: int = 10
    unresolved_position_expiry_minutes: int = 180


@dataclass(frozen=True)
class SignalDeliveryConfig:
    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""
    delivery_interval_seconds: int = 60
    proxy_host: str = ""
    proxy_port: int = 0
    proxy_user: str = ""
    proxy_pass: str = ""
    max_per_cycle: int = 0
    high_confidence_threshold: float = 0.6
    high_ev_threshold: float = 0.00015
    min_resolved_for_stats: int = 10
    min_win_rate_for_strong_ev: float = 0.35
    semantic_dedup_enabled: bool = True
    confidence_delta: float = 0.10
    repeat_after_minutes: int = 240
    anthropic_api_key: str = ""
    ai_model: str = "claude-sonnet-4-20250514"
    callback_poll_interval_seconds: int = 5
    weekend_high_only: bool = True


@dataclass(frozen=True)
class GlobalMarketDataConfig:
    enabled: bool = False
    poll_interval_seconds: int = 300
    symbols: tuple[str, ...] = (
        "^GSPC", "^NDX", "^VIX", "BZ=F", "DX-Y.NYB",
    )


@dataclass(frozen=True)
class GlobalContextConfig:
    enabled: bool = False
    channels: tuple[str, ...] = (
        "financialjuice", "oilpricee", "cointelegraph",
    )
    poll_interval_seconds: int = 120
    fetch_limit_per_source: int = 20


@dataclass(frozen=True)
class AlertingConfig:
    enabled: bool = False
    check_interval_seconds: int = 300
    cooldown_seconds: int = 3600
    # Thresholds
    signal_gap_minutes: int = 120
    telegram_gap_minutes: int = 60
    quote_gap_minutes: int = 30
    global_context_gap_minutes: int = 60
    pending_signals_alert_enabled: bool = False
    pending_signals_max: int = 50
    win_rate_min: float = 0.3
    win_rate_lookback_days: int = 7
    win_rate_min_resolved: int = 10


@dataclass(frozen=True)
class DailyDigestConfig:
    enabled: bool = False
    hour: int = 20
    minute: int = 0
    skip_weekends: bool = True


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "INFO"
    json_output: bool = True


@dataclass(frozen=True)
class AppConfig:
    broker: BrokerConfig = field(default_factory=BrokerConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    market_data: MarketDataConfig = field(default_factory=MarketDataConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    sentiment: SentimentConfig = field(default_factory=SentimentConfig)
    observation: ObservationConfig = field(default_factory=ObservationConfig)
    background: BackgroundConfig = field(default_factory=BackgroundConfig)
    broker_events: BrokerEventsConfig = field(default_factory=BrokerEventsConfig)
    fusion: FusionConfig = field(default_factory=FusionConfig)
    cbr: CbrConfig = field(default_factory=CbrConfig)
    moex: MoexConfig = field(default_factory=MoexConfig)
    execution_safety: ExecutionSafetyEnvConfig = field(
        default_factory=ExecutionSafetyEnvConfig,
    )
    signal_generation: SignalGenerationConfig = field(
        default_factory=SignalGenerationConfig,
    )
    signal_calibration: SignalCalibrationConfig = field(
        default_factory=SignalCalibrationConfig,
    )
    quote_sync: QuoteSyncConfig = field(default_factory=QuoteSyncConfig)
    market_activity: MarketActivityConfig = field(
        default_factory=MarketActivityConfig,
    )
    market_activity_outcomes: MarketActivityOutcomeConfig = field(
        default_factory=MarketActivityOutcomeConfig,
    )
    activity_paper: ActivityPaperConfig = field(default_factory=ActivityPaperConfig)
    signal_delivery: SignalDeliveryConfig = field(
        default_factory=SignalDeliveryConfig,
    )
    global_context: GlobalContextConfig = field(
        default_factory=GlobalContextConfig,
    )
    global_market_data: GlobalMarketDataConfig = field(
        default_factory=GlobalMarketDataConfig,
    )
    signal_resolution: SignalResolutionConfig = field(
        default_factory=SignalResolutionConfig,
    )
    paper_portfolio: PaperPortfolioConfig = field(
        default_factory=PaperPortfolioConfig,
    )
    alerting: AlertingConfig = field(default_factory=AlertingConfig)
    daily_digest: DailyDigestConfig = field(default_factory=DailyDigestConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    environment: str = "sandbox"


def _parse_csv(value: str) -> tuple[str, ...]:
    """Parse comma-separated string into a tuple of stripped, non-empty values."""
    if not value.strip():
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def load_config() -> AppConfig:
    """Load configuration from environment variables."""
    broker_events_legacy_lookback = os.environ.get(
        "TINVEST_BROKER_EVENTS_LOOKBACK_DAYS", "",
    ).strip()
    broker_events_legacy_lookback_days = (
        int(broker_events_legacy_lookback)
        if broker_events_legacy_lookback
        else None
    )

    return AppConfig(
        broker=BrokerConfig(
            token=os.environ.get("TINVEST_TOKEN", ""),
            sandbox=os.environ.get("TINVEST_SANDBOX", "true").lower() == "true",
            app_name=os.environ.get("TINVEST_APP_NAME", "tinvest_trader"),
            account_id=os.environ.get("TINVEST_ACCOUNT_ID", ""),
        ),
        trading=TradingConfig(
            max_position_size=int(os.environ.get("TINVEST_MAX_POSITION_SIZE", "1")),
            max_order_size=int(os.environ.get("TINVEST_MAX_ORDER_SIZE", "1")),
            max_daily_loss_pct=float(os.environ.get("TINVEST_MAX_DAILY_LOSS_PCT", "2.0")),
            max_trades_per_session=int(os.environ.get("TINVEST_MAX_TRADES_PER_SESSION", "10")),
            enabled_instruments=_parse_csv(
                os.environ.get("TINVEST_ENABLED_INSTRUMENTS", ""),
            ),
        ),
        market_data=MarketDataConfig(
            tracked_instruments=_parse_csv(
                os.environ.get("TINVEST_TRACKED_INSTRUMENTS", ""),
            ),
        ),
        database=DatabaseConfig(
            postgres_dsn=os.environ.get("TINVEST_POSTGRES_DSN", ""),
            pool_min_size=int(os.environ.get("TINVEST_DB_POOL_MIN", "2")),
            pool_max_size=int(os.environ.get("TINVEST_DB_POOL_MAX", "5")),
        ),
        observation=ObservationConfig(
            enabled=os.environ.get(
                "TINVEST_OBSERVATION_ENABLED", "false",
            ).lower() == "true",
            windows=_parse_csv(
                os.environ.get("TINVEST_OBSERVATION_WINDOWS", "5m,15m,1h"),
            ),
            persist_derived_metrics=os.environ.get(
                "TINVEST_OBSERVATION_PERSIST_DERIVED", "true",
            ).lower() == "true",
            tracked_tickers=_parse_csv(
                os.environ.get("TINVEST_OBSERVATION_TRACKED_TICKERS", ""),
            ),
        ),
        background=BackgroundConfig(
            enabled=os.environ.get("TINVEST_BACKGROUND_ENABLED", "false").lower() == "true",
            sentiment_ingest_interval_seconds=int(
                os.environ.get("TINVEST_BACKGROUND_SENTIMENT_INTERVAL_SECONDS", "300"),
            ),
            observation_interval_seconds=int(
                os.environ.get("TINVEST_BACKGROUND_OBSERVATION_INTERVAL_SECONDS", "600"),
            ),
            fusion_interval_seconds=int(
                os.environ.get("TINVEST_BACKGROUND_FUSION_INTERVAL_SECONDS", "600"),
            ),
            run_sentiment=os.environ.get(
                "TINVEST_BACKGROUND_RUN_SENTIMENT", "true",
            ).lower() == "true",
            run_observation=os.environ.get(
                "TINVEST_BACKGROUND_RUN_OBSERVATION", "true",
            ).lower() == "true",
            run_fusion=os.environ.get(
                "TINVEST_BACKGROUND_RUN_FUSION", "true",
            ).lower() == "true",
            run_cbr=os.environ.get(
                "TINVEST_BACKGROUND_RUN_CBR", "true",
            ).lower() == "true",
            run_moex=os.environ.get(
                "TINVEST_BACKGROUND_RUN_MOEX", "true",
            ).lower() == "true",
            run_quote_sync=os.environ.get(
                "TINVEST_BACKGROUND_RUN_QUOTE_SYNC", "true",
            ).lower() == "true",
            run_signal_delivery=os.environ.get(
                "TINVEST_BACKGROUND_RUN_SIGNAL_DELIVERY", "true",
            ).lower() == "true",
            run_global_context=os.environ.get(
                "TINVEST_BACKGROUND_RUN_GLOBAL_CONTEXT", "true",
            ).lower() == "true",
            run_global_market_data=os.environ.get(
                "TINVEST_BACKGROUND_RUN_GLOBAL_MARKET_DATA", "true",
            ).lower() == "true",
            run_signal_generation=os.environ.get(
                "TINVEST_BACKGROUND_RUN_SIGNAL_GENERATION", "true",
            ).lower() == "true",
            run_alerting=os.environ.get(
                "TINVEST_BACKGROUND_RUN_ALERTING", "true",
            ).lower() == "true",
            run_daily_digest=os.environ.get(
                "TINVEST_BACKGROUND_RUN_DAILY_DIGEST", "true",
            ).lower() == "true",
            run_signal_resolution=os.environ.get(
                "TINVEST_BACKGROUND_RUN_SIGNAL_RESOLUTION", "true",
            ).lower() == "true",
            run_paper_portfolio=os.environ.get(
                "TINVEST_BACKGROUND_RUN_PAPER_PORTFOLIO", "true",
            ).lower() == "true",
            run_market_activity=os.environ.get(
                "TINVEST_BACKGROUND_RUN_MARKET_ACTIVITY", "true",
            ).lower() == "true",
            run_market_activity_outcomes=os.environ.get(
                "TINVEST_BACKGROUND_RUN_MARKET_ACTIVITY_OUTCOMES", "true",
            ).lower() == "true",
            run_activity_paper_strategy=os.environ.get(
                "TINVEST_BACKGROUND_RUN_ACTIVITY_PAPER_STRATEGY", "true",
            ).lower() == "true",
        ),
        signal_generation=SignalGenerationConfig(
            enabled=os.environ.get(
                "TINVEST_SIGNAL_GENERATION_ENABLED", "false",
            ).lower() == "true",
            min_message_count=int(os.environ.get(
                "TINVEST_SIGNAL_MIN_MESSAGE_COUNT", "3",
            )),
            min_sentiment_balance=float(os.environ.get(
                "TINVEST_SIGNAL_MIN_SENTIMENT_BALANCE", "0.3",
            )),
            lookback_minutes=int(os.environ.get(
                "TINVEST_SIGNAL_LOOKBACK_MINUTES", "30",
            )),
            cooldown_minutes=int(os.environ.get(
                "TINVEST_SIGNAL_COOLDOWN_MINUTES", "30",
            )),
            poll_interval_seconds=int(os.environ.get(
                "TINVEST_SIGNAL_GENERATION_POLL_INTERVAL_SECONDS", "60",
            )),
        ),
        broker_events=BrokerEventsConfig(
            enabled=os.environ.get(
                "TINVEST_BROKER_EVENTS_ENABLED", "false",
            ).lower() == "true",
            event_types=_parse_csv(
                os.environ.get(
                    "TINVEST_BROKER_EVENTS_TYPES",
                    "dividends,reports,insider_deals",
                ),
            ),
            poll_interval_seconds=int(
                os.environ.get("TINVEST_BROKER_EVENTS_POLL_INTERVAL_SECONDS", "1800"),
            ),
            lookback_days=broker_events_legacy_lookback_days,
            dividends_lookback_days=int(
                os.environ.get(
                    "TINVEST_BROKER_EVENTS_DIVIDENDS_LOOKBACK_DAYS",
                    str(broker_events_legacy_lookback_days or 365),
                ),
            ),
            reports_lookback_days=int(
                os.environ.get(
                    "TINVEST_BROKER_EVENTS_REPORTS_LOOKBACK_DAYS",
                    str(broker_events_legacy_lookback_days or 365),
                ),
            ),
            insider_deals_lookback_days=int(
                os.environ.get(
                    "TINVEST_BROKER_EVENTS_INSIDER_DEALS_LOOKBACK_DAYS",
                    str(broker_events_legacy_lookback_days or 3650),
                ),
            ),
            tracked_figis_override=_parse_csv(
                os.environ.get("TINVEST_BROKER_EVENTS_TRACKED_FIGIS", ""),
            ),
            fetch_policy_enabled=os.environ.get(
                "TINVEST_BROKER_EVENTS_FETCH_POLICY_ENABLED", "true",
            ).lower() == "true",
            dividends_ttl_seconds=int(
                os.environ.get(
                    "TINVEST_BROKER_EVENTS_DIVIDENDS_TTL_SECONDS", "86400",
                ),
            ),
            reports_ttl_seconds=int(
                os.environ.get(
                    "TINVEST_BROKER_EVENTS_REPORTS_TTL_SECONDS", "86400",
                ),
            ),
            insider_deals_ttl_seconds=int(
                os.environ.get(
                    "TINVEST_BROKER_EVENTS_INSIDER_DEALS_TTL_SECONDS", "86400",
                ),
            ),
            fetch_policy_failure_cooldown_seconds=int(
                os.environ.get(
                    "TINVEST_BROKER_EVENTS_FAILURE_COOLDOWN_SECONDS", "3600",
                ),
            ),
            fetch_policy_max_consecutive_failures=int(
                os.environ.get(
                    "TINVEST_BROKER_EVENTS_MAX_CONSECUTIVE_FAILURES", "5",
                ),
            ),
            fetch_policy_max_fetches_per_cycle=int(
                os.environ.get(
                    "TINVEST_BROKER_EVENTS_MAX_FETCHES_PER_CYCLE", "0",
                ),
            ),
        ),
        sentiment=SentimentConfig(
            enabled=os.environ.get("TINVEST_SENTIMENT_ENABLED", "false").lower() == "true",
            channels=_parse_csv(os.environ.get("TINVEST_SENTIMENT_CHANNELS", "")),
            tracked_tickers=_parse_csv(
                os.environ.get("TINVEST_SENTIMENT_TRACKED_TICKERS", ""),
            ),
            model_name=os.environ.get("TINVEST_SENTIMENT_MODEL_NAME", "stub"),
            source_backend=os.environ.get("TINVEST_SENTIMENT_SOURCE_BACKEND", "stub"),
            telethon_api_id=(
                int(os.environ["TINVEST_SENTIMENT_TELETHON_API_ID"])
                if os.environ.get("TINVEST_SENTIMENT_TELETHON_API_ID", "").strip()
                else None
            ),
            telethon_api_hash=os.environ.get("TINVEST_SENTIMENT_TELETHON_API_HASH", ""),
            telethon_session_path=os.environ.get(
                "TINVEST_SENTIMENT_TELETHON_SESSION_PATH", "",
            ),
            telethon_poll_limit=int(
                os.environ.get("TINVEST_SENTIMENT_TELETHON_POLL_LIMIT", "50"),
            ),
            telethon_request_timeout_sec=(
                float(os.environ["TINVEST_SENTIMENT_TELETHON_TIMEOUT_SEC"])
                if os.environ.get("TINVEST_SENTIMENT_TELETHON_TIMEOUT_SEC", "").strip()
                else None
            ),
            telethon_proxy_type=os.environ.get(
                "TINVEST_SENTIMENT_TELETHON_PROXY_TYPE", "",
            ),
            telethon_proxy_host=os.environ.get(
                "TINVEST_SENTIMENT_TELETHON_PROXY_HOST", "",
            ),
            telethon_proxy_port=int(
                os.environ.get("TINVEST_SENTIMENT_TELETHON_PROXY_PORT", "0"),
            ),
            telethon_proxy_user=os.environ.get(
                "TINVEST_SENTIMENT_TELETHON_PROXY_USER", "",
            ),
            telethon_proxy_pass=os.environ.get(
                "TINVEST_SENTIMENT_TELETHON_PROXY_PASS", "",
            ),
        ),
        fusion=FusionConfig(
            enabled=os.environ.get(
                "TINVEST_FUSION_ENABLED", "false",
            ).lower() == "true",
            windows=_parse_csv(
                os.environ.get("TINVEST_FUSION_WINDOWS", "5m,15m,1h,1d,7d,30d"),
            ),
            persist=os.environ.get(
                "TINVEST_FUSION_PERSIST", "true",
            ).lower() == "true",
            tracked_tickers=_parse_csv(
                os.environ.get("TINVEST_FUSION_TRACKED_TICKERS", ""),
            ),
        ),
        cbr=CbrConfig(
            enabled=os.environ.get(
                "TINVEST_CBR_ENABLED", "false",
            ).lower() == "true",
            rss_enabled=os.environ.get(
                "TINVEST_CBR_RSS_ENABLED", "true",
            ).lower() == "true",
            rss_urls=_parse_csv(
                os.environ.get(
                    "TINVEST_CBR_RSS_URLS",
                    "http://www.cbr.ru/rss/eventrss,http://www.cbr.ru/rss/RssPress",
                ),
            ),
            poll_interval_seconds=int(
                os.environ.get("TINVEST_CBR_POLL_INTERVAL_SECONDS", "3600"),
            ),
            store_raw_payloads=os.environ.get(
                "TINVEST_CBR_STORE_RAW_PAYLOADS", "true",
            ).lower() == "true",
        ),
        execution_safety=ExecutionSafetyEnvConfig(
            enabled=os.environ.get(
                "TINVEST_EXECUTION_SAFETY_ENABLED", "true",
            ).lower() == "true",
            min_time_to_close_seconds=int(
                os.environ.get(
                    "TINVEST_EXECUTION_MIN_TIME_TO_CLOSE_SECONDS", "90",
                ),
            ),
        ),
        signal_calibration=SignalCalibrationConfig(
            enabled=os.environ.get(
                "TINVEST_SIGNAL_CALIBRATION_ENABLED", "false",
            ).lower() == "true",
            eval_window_seconds=int(
                os.environ.get(
                    "TINVEST_SIGNAL_EVAL_WINDOW_SECONDS", "300",
                ),
            ),
            dry_run=os.environ.get(
                "TINVEST_DRY_RUN_ENABLED", "false",
            ).lower() == "true",
            min_confidence=float(
                os.environ.get("TINVEST_SIGNAL_MIN_CONFIDENCE", "0"),
            ),
            min_win_rate=float(
                os.environ.get("TINVEST_SIGNAL_MIN_WIN_RATE", "0"),
            ),
            min_ev=float(
                os.environ.get("TINVEST_SIGNAL_MIN_EV", "0"),
            ),
            enable_up=os.environ.get(
                "TINVEST_SIGNAL_ENABLE_UP", "true",
            ).lower() == "true",
            enable_down=os.environ.get(
                "TINVEST_SIGNAL_ENABLE_DOWN", "true",
            ).lower() == "true",
            min_resolved_for_filter=int(
                os.environ.get("TINVEST_SIGNAL_MIN_RESOLVED_FOR_FILTER", "5"),
            ),
        ),
        quote_sync=QuoteSyncConfig(
            enabled=os.environ.get(
                "TINVEST_QUOTE_SYNC_ENABLED", "false",
            ).lower() == "true",
            poll_interval_seconds=int(
                os.environ.get(
                    "TINVEST_QUOTE_SYNC_POLL_INTERVAL_SECONDS", "60",
                ),
            ),
            max_source_age_seconds=int(os.environ.get(
                "TINVEST_QUOTE_SYNC_MAX_SOURCE_AGE_SECONDS", "604800",
            )),
        ),
        market_activity=MarketActivityConfig(
            enabled=os.environ.get(
                "TINVEST_MARKET_ACTIVITY_ENABLED", "false",
            ).lower() == "true",
            poll_interval_seconds=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_POLL_INTERVAL_SECONDS", "60",
            )),
            candle_interval=os.environ.get(
                "TINVEST_MARKET_ACTIVITY_CANDLE_INTERVAL",
                "CANDLE_INTERVAL_1_MIN",
            ),
            lookback_minutes=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_LOOKBACK_MINUTES", "60",
            )),
            baseline_candles=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_BASELINE_CANDLES", "20",
            )),
            min_volume=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_MIN_VOLUME", "1",
            )),
            volume_spike_multiplier=float(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_VOLUME_SPIKE_MULTIPLIER", "3.0",
            )),
            price_change_spike_pct=float(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_PRICE_CHANGE_SPIKE_PCT", "0.01",
            )),
            session_filter_enabled=os.environ.get(
                "TINVEST_MARKET_ACTIVITY_SESSION_FILTER_ENABLED", "true",
            ).lower() == "true",
            session_start_hour_moscow=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_SESSION_START_HOUR_MOSCOW", "9",
            )),
            session_start_minute_moscow=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_SESSION_START_MINUTE_MOSCOW", "50",
            )),
            session_end_hour_moscow=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_SESSION_END_HOUR_MOSCOW", "18",
            )),
            session_end_minute_moscow=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_SESSION_END_MINUTE_MOSCOW", "50",
            )),
        ),
        market_activity_outcomes=MarketActivityOutcomeConfig(
            enabled=os.environ.get(
                "TINVEST_MARKET_ACTIVITY_OUTCOMES_ENABLED", "false",
            ).lower() == "true",
            poll_interval_seconds=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_OUTCOMES_POLL_INTERVAL_SECONDS", "60",
            )),
            horizons_minutes=tuple(
                int(value)
                for value in _parse_csv(os.environ.get(
                    "TINVEST_MARKET_ACTIVITY_OUTCOMES_HORIZONS_MINUTES",
                    "5,15,60",
                ))
                if int(value) > 0
            ),
            neutral_threshold_pct=float(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_OUTCOMES_NEUTRAL_THRESHOLD_PCT",
                "0.0005",
            )),
            eod_enabled=os.environ.get(
                "TINVEST_MARKET_ACTIVITY_OUTCOMES_EOD_ENABLED", "true",
            ).lower() == "true",
            eod_hour_moscow=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_OUTCOMES_EOD_HOUR_MOSCOW", "23",
            )),
            eod_minute_moscow=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_OUTCOMES_EOD_MINUTE_MOSCOW", "50",
            )),
            lookback_days=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_OUTCOMES_LOOKBACK_DAYS", "30",
            )),
            resolution_limit=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_OUTCOMES_RESOLUTION_LIMIT", "500",
            )),
            max_price_delay_minutes=int(os.environ.get(
                "TINVEST_MARKET_ACTIVITY_OUTCOMES_MAX_PRICE_DELAY_MINUTES", "30",
            )),
        ),
        activity_paper=ActivityPaperConfig(
            enabled=os.environ.get(
                "TINVEST_ACTIVITY_PAPER_ENABLED", "false",
            ).lower() == "true",
            poll_interval_seconds=int(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_POLL_INTERVAL_SECONDS", "60",
            )),
            momentum_portfolio_name=os.environ.get(
                "TINVEST_ACTIVITY_PAPER_MOMENTUM_NAME", "activity-momentum-v1",
            ),
            reversion_portfolio_name=os.environ.get(
                "TINVEST_ACTIVITY_PAPER_REVERSION_NAME", "activity-reversion-v1",
            ),
            volume_confirmed_enabled=os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_ENABLED", "false",
            ).lower() == "true",
            volume_confirmed_portfolio_name=os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_NAME",
                "activity-volume-confirmed-v1",
            ),
            volume_confirmation_min_move_pct=float(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMATION_MIN_MOVE_PCT",
                "0.002",
            )),
            volume_confirmation_max_delay_minutes=int(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMATION_MAX_DELAY_MINUTES",
                "3",
            )),
            volume_confirmed_v2_enabled=os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_ENABLED", "false",
            ).lower() == "true",
            volume_confirmed_v2_portfolio_name=os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_NAME",
                "activity-volume-confirmed-v2",
            ),
            volume_confirmed_v2_min_score=float(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MIN_SCORE", "80",
            )),
            volume_confirmed_v2_min_volume_ratio=float(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MIN_VOLUME_RATIO",
                "5",
            )),
            volume_confirmed_v2_min_move_pct=float(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MIN_MOVE_PCT",
                "0.004",
            )),
            volume_confirmed_v2_max_delay_minutes=int(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MAX_DELAY_MINUTES",
                "2",
            )),
            volume_confirmed_v2_cooldown_minutes=int(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_COOLDOWN_MINUTES",
                "120",
            )),
            volume_confirmed_v2_max_entries_per_day=int(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MAX_ENTRIES_PER_DAY",
                "20",
            )),
            horizon=os.environ.get("TINVEST_ACTIVITY_PAPER_HORIZON", "15m"),
            initial_cash=float(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_INITIAL_CASH", "1000000",
            )),
            position_fraction=float(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_POSITION_FRACTION", "0.02",
            )),
            max_open_positions=int(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_MAX_OPEN_POSITIONS", "10",
            )),
            max_open_positions_per_ticker=int(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_MAX_OPEN_PER_TICKER", "1",
            )),
            commission_rate=float(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_COMMISSION_RATE", "0.0005",
            )),
            slippage_rate=float(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_SLIPPAGE_RATE", "0.0005",
            )),
            min_score=float(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_MIN_SCORE", "45",
            )),
            allowed_severities=_parse_csv(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_ALLOWED_SEVERITIES", "medium,high",
            )),
            allowed_spike_types=_parse_csv(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_ALLOWED_SPIKE_TYPES",
                "volume_price,price_momentum",
            )),
            cooldown_minutes=int(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_COOLDOWN_MINUTES", "30",
            )),
            max_candidate_age_minutes=int(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_MAX_CANDIDATE_AGE_MINUTES", "10",
            )),
            unresolved_position_expiry_minutes=int(os.environ.get(
                "TINVEST_ACTIVITY_PAPER_UNRESOLVED_EXPIRY_MINUTES", "180",
            )),
        ),
        signal_delivery=SignalDeliveryConfig(
            enabled=os.environ.get(
                "TINVEST_SIGNAL_DELIVERY_ENABLED", "false",
            ).lower() == "true",
            bot_token=os.environ.get("TINVEST_TELEGRAM_BOT_TOKEN", ""),
            chat_id=os.environ.get("TINVEST_TELEGRAM_CHAT_ID", ""),
            delivery_interval_seconds=int(
                os.environ.get(
                    "TINVEST_SIGNAL_DELIVERY_INTERVAL_SECONDS", "60",
                ),
            ),
            proxy_host=os.environ.get("TINVEST_TELEGRAM_BOT_PROXY_HOST", ""),
            proxy_port=int(
                os.environ.get("TINVEST_TELEGRAM_BOT_PROXY_PORT", "0"),
            ),
            proxy_user=os.environ.get("TINVEST_TELEGRAM_BOT_PROXY_USER", ""),
            proxy_pass=os.environ.get("TINVEST_TELEGRAM_BOT_PROXY_PASS", ""),
            max_per_cycle=int(
                os.environ.get("TINVEST_SIGNAL_DELIVERY_MAX_PER_CYCLE", "0"),
            ),
            high_confidence_threshold=float(
                os.environ.get("TINVEST_SIGNAL_HIGH_CONFIDENCE_THRESHOLD", "0.6"),
            ),
            high_ev_threshold=float(
                os.environ.get("TINVEST_SIGNAL_HIGH_EV_THRESHOLD", "0.00015"),
            ),
            min_resolved_for_stats=int(
                os.environ.get("TINVEST_SIGNAL_MIN_RESOLVED_FOR_STATS", "10"),
            ),
            min_win_rate_for_strong_ev=float(
                os.environ.get(
                    "TINVEST_SIGNAL_MIN_WIN_RATE_FOR_STRONG_EV", "0.35",
                ),
            ),
            semantic_dedup_enabled=os.environ.get(
                "TINVEST_SIGNAL_DELIVERY_SEMANTIC_DEDUP_ENABLED", "true",
            ).lower() == "true",
            confidence_delta=float(os.environ.get(
                "TINVEST_SIGNAL_DELIVERY_CONFIDENCE_DELTA", "0.10",
            )),
            repeat_after_minutes=int(os.environ.get(
                "TINVEST_SIGNAL_DELIVERY_REPEAT_AFTER_MINUTES", "240",
            )),
            anthropic_api_key=os.environ.get(
                "TINVEST_ANTHROPIC_API_KEY", "",
            ),
            ai_model=os.environ.get(
                "TINVEST_AI_MODEL", "claude-sonnet-4-20250514",
            ),
            callback_poll_interval_seconds=int(
                os.environ.get(
                    "TINVEST_CALLBACK_POLL_INTERVAL_SECONDS", "5",
                ),
            ),
        ),
        global_context=GlobalContextConfig(
            enabled=os.environ.get(
                "TINVEST_GLOBAL_CONTEXT_ENABLED", "false",
            ).lower() == "true",
            channels=_parse_csv(
                os.environ.get(
                    "TINVEST_GLOBAL_CONTEXT_CHANNELS",
                    "financialjuice,oilpricee,cointelegraph",
                ),
            ),
            poll_interval_seconds=int(
                os.environ.get(
                    "TINVEST_GLOBAL_CONTEXT_POLL_INTERVAL_SECONDS", "120",
                ),
            ),
            fetch_limit_per_source=int(
                os.environ.get(
                    "TINVEST_GLOBAL_CONTEXT_FETCH_LIMIT_PER_SOURCE", "20",
                ),
            ),
        ),
        global_market_data=GlobalMarketDataConfig(
            enabled=os.environ.get(
                "TINVEST_GLOBAL_MARKET_DATA_ENABLED", "false",
            ).lower() == "true",
            poll_interval_seconds=int(
                os.environ.get(
                    "TINVEST_GLOBAL_MARKET_DATA_POLL_INTERVAL_SECONDS",
                    "300",
                ),
            ),
            symbols=_parse_csv(
                os.environ.get(
                    "TINVEST_GLOBAL_MARKET_DATA_SYMBOLS",
                    "^GSPC,^NDX,^VIX,BZ=F,DX-Y.NYB",
                ),
            ),
        ),
        signal_resolution=SignalResolutionConfig(
            enabled=os.environ.get(
                "TINVEST_SIGNAL_RESOLUTION_ENABLED", "true",
            ).lower() == "true",
            eval_window_seconds=int(os.environ.get(
                "TINVEST_SIGNAL_RESOLUTION_EVAL_WINDOW_SECONDS", "300",
            )),
            max_quote_delay_seconds=int(os.environ.get(
                "TINVEST_SIGNAL_RESOLUTION_MAX_QUOTE_DELAY_SECONDS", "900",
            )),
            poll_interval_seconds=int(os.environ.get(
                "TINVEST_SIGNAL_RESOLUTION_POLL_INTERVAL_SECONDS", "120",
            )),
        ),
        paper_portfolio=PaperPortfolioConfig(
            enabled=os.environ.get(
                "TINVEST_PAPER_PORTFOLIO_ENABLED", "false",
            ).lower() == "true",
            name=os.environ.get("TINVEST_PAPER_PORTFOLIO_NAME", "shadow-v1"),
            initial_cash=float(os.environ.get(
                "TINVEST_PAPER_PORTFOLIO_INITIAL_CASH", "1000000",
            )),
            position_fraction=float(os.environ.get(
                "TINVEST_PAPER_PORTFOLIO_POSITION_FRACTION", "0.10",
            )),
            max_open_positions=int(os.environ.get(
                "TINVEST_PAPER_PORTFOLIO_MAX_OPEN_POSITIONS", "5",
            )),
            commission_rate=float(os.environ.get(
                "TINVEST_PAPER_PORTFOLIO_COMMISSION_RATE", "0.0005",
            )),
            slippage_rate=float(os.environ.get(
                "TINVEST_PAPER_PORTFOLIO_SLIPPAGE_RATE", "0.0005",
            )),
            poll_interval_seconds=int(os.environ.get(
                "TINVEST_PAPER_PORTFOLIO_POLL_INTERVAL_SECONDS", "60",
            )),
            unresolved_position_expiry_minutes=int(os.environ.get(
                "TINVEST_PAPER_PORTFOLIO_UNRESOLVED_EXPIRY_MINUTES", "180",
            )),
            entry_stages=_parse_csv(os.environ.get(
                "TINVEST_PAPER_PORTFOLIO_ENTRY_STAGES", "delivered",
            )),
        ),
        alerting=AlertingConfig(
            enabled=os.environ.get(
                "TINVEST_ALERTING_ENABLED", "false",
            ).lower() == "true",
            check_interval_seconds=int(
                os.environ.get(
                    "TINVEST_ALERTING_CHECK_INTERVAL_SECONDS", "300",
                ),
            ),
            cooldown_seconds=int(
                os.environ.get(
                    "TINVEST_ALERTING_COOLDOWN_SECONDS", "3600",
                ),
            ),
            signal_gap_minutes=int(
                os.environ.get(
                    "TINVEST_ALERTING_SIGNAL_GAP_MINUTES", "120",
                ),
            ),
            telegram_gap_minutes=int(
                os.environ.get(
                    "TINVEST_ALERTING_TELEGRAM_GAP_MINUTES", "60",
                ),
            ),
            quote_gap_minutes=int(
                os.environ.get(
                    "TINVEST_ALERTING_QUOTE_GAP_MINUTES", "30",
                ),
            ),
            global_context_gap_minutes=int(
                os.environ.get(
                    "TINVEST_ALERTING_GLOBAL_CONTEXT_GAP_MINUTES", "60",
                ),
            ),
            pending_signals_alert_enabled=os.environ.get(
                "TINVEST_ALERTING_PENDING_SIGNALS_ENABLED", "false",
            ).lower() == "true",
            pending_signals_max=int(
                os.environ.get(
                    "TINVEST_ALERTING_PENDING_SIGNALS_MAX", "50",
                ),
            ),
            win_rate_min=float(
                os.environ.get(
                    "TINVEST_ALERTING_WIN_RATE_MIN", "0.3",
                ),
            ),
            win_rate_lookback_days=int(
                os.environ.get(
                    "TINVEST_ALERTING_WIN_RATE_LOOKBACK_DAYS", "7",
                ),
            ),
            win_rate_min_resolved=int(
                os.environ.get(
                    "TINVEST_ALERTING_WIN_RATE_MIN_RESOLVED", "10",
                ),
            ),
        ),
        daily_digest=DailyDigestConfig(
            enabled=os.environ.get(
                "TINVEST_DAILY_DIGEST_ENABLED", "false",
            ).lower() == "true",
            hour=int(
                os.environ.get("TINVEST_DAILY_DIGEST_HOUR", "20"),
            ),
            minute=int(
                os.environ.get("TINVEST_DAILY_DIGEST_MINUTE", "0"),
            ),
        ),
        moex=MoexConfig(
            enabled=os.environ.get(
                "TINVEST_MOEX_ENABLED", "false",
            ).lower() == "true",
            metadata_enabled=os.environ.get(
                "TINVEST_MOEX_METADATA_ENABLED", "true",
            ).lower() == "true",
            history_enabled=os.environ.get(
                "TINVEST_MOEX_HISTORY_ENABLED", "true",
            ).lower() == "true",
            poll_interval_seconds=int(
                os.environ.get("TINVEST_MOEX_POLL_INTERVAL_SECONDS", "3600"),
            ),
            history_lookback_days=int(
                os.environ.get("TINVEST_MOEX_HISTORY_LOOKBACK_DAYS", "90"),
            ),
            tracked_tickers_override=_parse_csv(
                os.environ.get("TINVEST_MOEX_TRACKED_TICKERS", ""),
            ),
            engine=os.environ.get("TINVEST_MOEX_ENGINE", "stock"),
            market=os.environ.get("TINVEST_MOEX_MARKET", "shares"),
            board=os.environ.get("TINVEST_MOEX_BOARD", "TQBR"),
        ),
        logging=LoggingConfig(
            level=os.environ.get("TINVEST_LOG_LEVEL", "INFO"),
            json_output=os.environ.get("TINVEST_LOG_JSON", "true").lower() == "true",
        ),
        environment=os.environ.get("TINVEST_ENVIRONMENT", "sandbox"),
    )
