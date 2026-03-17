from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class BrokerConfig:
    token: str = ""
    sandbox: bool = True
    app_name: str = "tinvest_trader"


@dataclass(frozen=True)
class TradingConfig:
    max_position_size: int = 1
    max_order_size: int = 1
    max_daily_loss_pct: float = 2.0
    max_trades_per_session: int = 10


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "INFO"
    json_output: bool = True


@dataclass(frozen=True)
class AppConfig:
    broker: BrokerConfig = field(default_factory=BrokerConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    environment: str = "sandbox"


def load_config() -> AppConfig:
    """Load configuration from environment variables."""
    return AppConfig(
        broker=BrokerConfig(
            token=os.environ.get("TINVEST_TOKEN", ""),
            sandbox=os.environ.get("TINVEST_SANDBOX", "true").lower() == "true",
            app_name=os.environ.get("TINVEST_APP_NAME", "tinvest_trader"),
        ),
        trading=TradingConfig(
            max_position_size=int(os.environ.get("TINVEST_MAX_POSITION_SIZE", "1")),
            max_order_size=int(os.environ.get("TINVEST_MAX_ORDER_SIZE", "1")),
            max_daily_loss_pct=float(os.environ.get("TINVEST_MAX_DAILY_LOSS_PCT", "2.0")),
            max_trades_per_session=int(os.environ.get("TINVEST_MAX_TRADES_PER_SESSION", "10")),
        ),
        logging=LoggingConfig(
            level=os.environ.get("TINVEST_LOG_LEVEL", "INFO"),
            json_output=os.environ.get("TINVEST_LOG_JSON", "true").lower() == "true",
        ),
        environment=os.environ.get("TINVEST_ENVIRONMENT", "sandbox"),
    )
