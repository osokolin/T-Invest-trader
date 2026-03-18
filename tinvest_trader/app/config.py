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
class LoggingConfig:
    level: str = "INFO"
    json_output: bool = True


@dataclass(frozen=True)
class AppConfig:
    broker: BrokerConfig = field(default_factory=BrokerConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    market_data: MarketDataConfig = field(default_factory=MarketDataConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
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
        logging=LoggingConfig(
            level=os.environ.get("TINVEST_LOG_LEVEL", "INFO"),
            json_output=os.environ.get("TINVEST_LOG_JSON", "true").lower() == "true",
        ),
        environment=os.environ.get("TINVEST_ENVIRONMENT", "sandbox"),
    )
