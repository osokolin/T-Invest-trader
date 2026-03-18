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
    run_sentiment: bool = True
    run_observation: bool = True


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
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    environment: str = "sandbox"


def _parse_csv(value: str) -> tuple[str, ...]:
    """Parse comma-separated string into a tuple of stripped, non-empty values."""
    if not value.strip():
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def load_config() -> AppConfig:
    """Load configuration from environment variables."""
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
            run_sentiment=os.environ.get(
                "TINVEST_BACKGROUND_RUN_SENTIMENT", "true",
            ).lower() == "true",
            run_observation=os.environ.get(
                "TINVEST_BACKGROUND_RUN_OBSERVATION", "true",
            ).lower() == "true",
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
        ),
        logging=LoggingConfig(
            level=os.environ.get("TINVEST_LOG_LEVEL", "INFO"),
            json_output=os.environ.get("TINVEST_LOG_JSON", "true").lower() == "true",
        ),
        environment=os.environ.get("TINVEST_ENVIRONMENT", "sandbox"),
    )
