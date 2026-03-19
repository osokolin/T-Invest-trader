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
from tinvest_trader.observation.windows import parse_windows
from tinvest_trader.portfolio.state import PortfolioState
from tinvest_trader.sentiment.instrument_mapper import InstrumentMapper
from tinvest_trader.sentiment.parser import extract_tickers
from tinvest_trader.sentiment.scorer import StubSentimentScorer
from tinvest_trader.sentiment.source import StubMessageSource
from tinvest_trader.sentiment.telethon_source import build_telethon_message_source
from tinvest_trader.services.background_runner import BackgroundRunner
from tinvest_trader.services.broker_event_ingestion_service import (
    BrokerEventIngestionService,
)
from tinvest_trader.services.observation_service import ObservationService
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
    observation_service: ObservationService | None = field(init=False, default=None)
    broker_event_ingestion_service: BrokerEventIngestionService | None = field(
        init=False, default=None,
    )
    background_runner: BackgroundRunner | None = field(init=False, default=None)

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

        # Observation pipeline (optional, disabled by default)
        if self.config.observation.enabled:
            self._wire_observation()

        # Broker-side structured event ingestion (optional, disabled by default)
        if self.config.broker_events.enabled:
            self._wire_broker_events()

        # Background runner (optional, disabled by default)
        if self.config.background.enabled:
            self._wire_background_runner()

    def _wire_sentiment(self) -> None:
        """Wire sentiment components when enabled."""
        cfg = self.config.sentiment

        # Source backend
        if cfg.source_backend == "stub":
            source = StubMessageSource()
        elif cfg.source_backend == "telethon":
            source = build_telethon_message_source(cfg)
        else:
            raise ValueError(f"unsupported sentiment source backend: {cfg.source_backend}")

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

    def _wire_observation(self) -> None:
        """Wire observation components when enabled."""
        cfg = self.config.observation
        windows = parse_windows(cfg.windows)

        # Tracked tickers precedence: observation > sentiment > market_data
        tracked: frozenset[str]
        if cfg.tracked_tickers:
            tracked = frozenset(cfg.tracked_tickers)
        elif self.config.sentiment.tracked_tickers:
            tracked = frozenset(self.config.sentiment.tracked_tickers)
        else:
            tracked = frozenset()  # will discover from DB

        self.observation_service = ObservationService(
            repository=self.repository,
            windows=windows,
            tracked_tickers=tracked,
            persist=cfg.persist_derived_metrics,
            logger=self.logger,
        )

        self.logger.info(
            "observation pipeline initialized",
            extra={
                "component": "observation",
                "windows": [w.label for w in windows],
                "tracked_tickers": len(tracked),
                "persist": cfg.persist_derived_metrics,
            },
        )

    def _wire_broker_events(self) -> None:
        """Wire broker-side structured event ingestion when enabled."""
        cfg = self.config.broker_events
        tracked_figis = (
            cfg.tracked_figis_override
            if cfg.tracked_figis_override
            else self.config.market_data.tracked_instruments
        )

        self.broker_event_ingestion_service = BrokerEventIngestionService(
            client=self.tbank_client,
            repository=self.repository,
            logger=self.logger,
            account_id=self.config.broker.account_id,
            tracked_figis=tracked_figis,
            event_types=cfg.event_types,
            lookback_days_by_event_type={
                "dividends": cfg.dividends_lookback_days,
                "reports": cfg.reports_lookback_days,
                "insider_deals": cfg.insider_deals_lookback_days,
            },
        )

        self.logger.info(
            "broker events pipeline initialized",
            extra={
                "component": "broker_events",
                "event_types": list(cfg.event_types),
                "tracked_figis": len(tracked_figis),
                "lookback_days_by_event_type": {
                    "dividends": cfg.dividends_lookback_days,
                    "reports": cfg.reports_lookback_days,
                    "insider_deals": cfg.insider_deals_lookback_days,
                },
            },
        )

    def _wire_background_runner(self) -> None:
        """Wire background runner when enabled."""
        self.background_runner = BackgroundRunner(
            config=self.config.background,
            logger=self.logger,
            telegram_sentiment_service=self.telegram_sentiment_service,
            observation_service=self.observation_service,
            broker_event_ingestion_service=self.broker_event_ingestion_service,
            sentiment_channels=self.config.sentiment.channels,
            broker_event_interval_seconds=self.config.broker_events.poll_interval_seconds,
        )

        self.logger.info(
            "background runner initialized",
            extra={
                "component": "background_runner",
                "run_sentiment": self.config.background.run_sentiment,
                "run_observation": self.config.background.run_observation,
                "run_broker_events": self.broker_event_ingestion_service is not None,
            },
        )


def build_container(config: AppConfig) -> Container:
    """Build and return a fully wired container."""
    return Container(config=config)
