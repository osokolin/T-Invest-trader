"""Dependency container -- wires all components at startup."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from tinvest_trader.app.config import AppConfig
from tinvest_trader.execution.engine import ExecutionEngine
from tinvest_trader.infra.logging.journal import setup_logging
from tinvest_trader.infra.storage.postgres import PostgresPool
from tinvest_trader.infra.storage.repository import TradingRepository
from tinvest_trader.infra.tbank.client import TBankClient
from tinvest_trader.instruments.registry import InstrumentRegistry
from tinvest_trader.market_data.service import MarketDataService
from tinvest_trader.portfolio.state import PortfolioState
from tinvest_trader.services.trading_service import TradingService


@dataclass
class Container:
    """Dependency container. Wires all components."""

    config: AppConfig
    logger: logging.Logger = field(init=False)
    tbank_client: TBankClient = field(init=False)
    instrument_registry: InstrumentRegistry = field(init=False)
    storage_pool: PostgresPool | None = field(init=False, default=None)
    repository: TradingRepository | None = field(init=False, default=None)
    market_data: MarketDataService = field(init=False)
    execution_engine: ExecutionEngine = field(init=False)
    portfolio: PortfolioState = field(init=False)
    trading_service: TradingService = field(init=False)

    def __post_init__(self) -> None:
        self.logger = setup_logging(self.config.logging)
        self.tbank_client = TBankClient(
            config=self.config.broker,
            logger=self.logger,
        )
        self.instrument_registry = InstrumentRegistry(
            tracked=self.config.market_data.tracked_instruments,
            enabled=self.config.trading.enabled_instruments,
            logger=self.logger,
        )

        if self.config.database.postgres_dsn:
            self.storage_pool = PostgresPool(
                config=self.config.database,
                logger=self.logger,
            )
            self.repository = TradingRepository(
                pool=self.storage_pool,
                logger=self.logger,
            )

        self.market_data = MarketDataService(
            client=self.tbank_client,
            logger=self.logger,
            repository=self.repository,
        )
        self.execution_engine = ExecutionEngine(
            client=self.tbank_client,
            logger=self.logger,
            repository=self.repository,
            account_id=self.config.broker.account_id,
        )
        self.portfolio = PortfolioState()
        self.trading_service = TradingService(logger=self.logger)


def build_container(config: AppConfig) -> Container:
    """Build and return a fully wired container."""
    return Container(config=config)
