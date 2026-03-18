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
from tinvest_trader.sentiment.instrument_mapper import InstrumentMapper
from tinvest_trader.sentiment.parser import extract_tickers
from tinvest_trader.sentiment.scorer import StubSentimentScorer
from tinvest_trader.sentiment.source import StubMessageSource
from tinvest_trader.services.telegram_sentiment_service import TelegramSentimentService
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
    telegram_sentiment_service: TelegramSentimentService | None = field(
        init=False, default=None,
    )

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

        # Sentiment pipeline (optional, disabled by default)
        if self.config.sentiment.enabled:
            self._wire_sentiment()

    def _wire_sentiment(self) -> None:
        """Wire sentiment components when enabled."""
        cfg = self.config.sentiment

        # Source backend
        source = StubMessageSource()

        # Scorer
        scorer = StubSentimentScorer(model_name=cfg.model_name)

        # Tracked tickers: sentiment config takes precedence, else fall back
        tracked_tickers: frozenset[str]
        if cfg.tracked_tickers:
            tracked_tickers = frozenset(cfg.tracked_tickers)
        else:
            tracked_tickers = frozenset(self.config.market_data.tracked_instruments)

        mapper = InstrumentMapper(
            ticker_to_figi={},  # populated from instrument_catalog in future
            tracked_tickers=tracked_tickers,
        )

        self.telegram_sentiment_service = TelegramSentimentService(
            source=source,
            parser_fn=extract_tickers,
            mapper=mapper,
            scorer=scorer,
            repository=self.repository,
            logger=self.logger,
        )

        self.logger.info(
            "sentiment pipeline initialized",
            extra={
                "component": "sentiment",
                "channels": len(cfg.channels),
                "backend": cfg.source_backend,
                "model": cfg.model_name,
            },
        )


def build_container(config: AppConfig) -> Container:
    """Build and return a fully wired container."""
    return Container(config=config)
