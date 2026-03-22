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
from tinvest_trader.services.cbr_ingestion_service import CbrIngestionService
from tinvest_trader.services.fusion_service import FusionService
from tinvest_trader.services.global_context_ingestion import (
    GlobalContextIngestionService,
)
from tinvest_trader.services.moex_ingestion_service import MoexIngestionService
from tinvest_trader.services.observation_service import ObservationService
from tinvest_trader.services.tbank_event_fetch_policy import FetchPolicyConfig
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
    fusion_service: FusionService | None = field(init=False, default=None)
    cbr_ingestion_service: CbrIngestionService | None = field(init=False, default=None)
    moex_ingestion_service: MoexIngestionService | None = field(init=False, default=None)
    global_context_service: GlobalContextIngestionService | None = field(
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
        self.trading_service = TradingService(
            logger=self.logger,
            repository=self.repository,
        )

        # Sentiment pipeline (optional, disabled by default)
        if self.config.sentiment.enabled:
            self._wire_sentiment()

        # Observation pipeline (optional, disabled by default)
        if self.config.observation.enabled:
            self._wire_observation()

        # Broker-side structured event ingestion (optional, disabled by default)
        if self.config.broker_events.enabled:
            self._wire_broker_events()

        # Signal fusion layer (optional, disabled by default)
        if self.config.fusion.enabled:
            self._wire_fusion()

        # CBR event ingestion (optional, disabled by default)
        if self.config.cbr.enabled:
            self._wire_cbr()

        # MOEX ISS market data ingestion (optional, disabled by default)
        if self.config.moex.enabled:
            self._wire_moex()

        # Global market context ingestion (optional, disabled by default)
        if self.config.global_context.enabled:
            self._wire_global_context()

        # Global market data sync callable (used by background runner and CLI)
        self._global_market_data_fn = self._build_global_market_data_fn()

        # Quote sync callable (used by background runner and CLI)
        self._quote_sync_fn = self._build_quote_sync_fn()

        # Signal delivery callable (used by background runner and CLI)
        self._signal_delivery_fn = self._build_signal_delivery_fn()

        # Callback handler callable (used by background runner)
        self._callback_handler_fn = self._build_callback_handler_fn()

        # Alerting callable (used by background runner and CLI)
        self._alerting_fn = self._build_alerting_fn()

        # Background runner (optional, disabled by default)
        if self.config.background.enabled:
            self._wire_background_runner()

    def _resolve_tracked_instruments(self) -> list[dict]:
        """Resolve tracked instruments from DB, bootstrapping from env if empty.

        Flow:
        1. If DB has tracked instruments -> return those (DB wins)
        2. If DB is empty -> bootstrap from TINVEST_SENTIMENT_TRACKED_TICKERS,
           then return newly seeded instruments from DB
        3. If no DB -> return empty list
        """
        if self.repository is None:
            return []
        try:
            db_tracked = self.repository.list_tracked_instruments()
            if db_tracked:
                return db_tracked

            # DB is empty -- bootstrap from env once
            env_tickers = self.config.sentiment.tracked_tickers
            if env_tickers:
                seeded = self.repository.bootstrap_tracked_instruments(
                    env_tickers,
                )
                if seeded > 0:
                    self.logger.info(
                        "bootstrapped tracked instruments from env",
                        extra={"component": "instruments", "seeded": seeded},
                    )
                    return self.repository.list_tracked_instruments()
        except Exception:
            self.logger.exception(
                "failed to resolve tracked instruments from DB",
                extra={"component": "container"},
            )
        return []

    def _resolve_tracked_tickers(self) -> frozenset[str]:
        """Resolve tracked tickers. DB first, env fallback if no DB."""
        db_instruments = self._resolve_tracked_instruments()
        if db_instruments:
            return frozenset(row["ticker"] for row in db_instruments)
        # Fallback: env-based sentiment tracked tickers (no DB available)
        if self.config.sentiment.tracked_tickers:
            return frozenset(self.config.sentiment.tracked_tickers)
        return frozenset()

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

        # Tracked tickers: DB preferred, then env fallback
        tracked_tickers = self._resolve_tracked_tickers()

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

        # Tracked tickers: DB preferred, then env fallback
        tracked = self._resolve_tracked_tickers()

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
        # Broker API requires FIGIs. DB first, then override, then env fallback.
        db_instruments = self._resolve_tracked_instruments()
        db_figis = tuple(
            row["figi"] for row in db_instruments
            if row["figi"] and not row["figi"].startswith("TICKER:")
        )
        if db_figis:
            tracked_figis: tuple[str, ...] = db_figis
        elif cfg.tracked_figis_override:
            tracked_figis = cfg.tracked_figis_override
        else:
            tracked_figis = self.config.market_data.tracked_instruments

        fetch_policy = FetchPolicyConfig(
            enabled=cfg.fetch_policy_enabled,
            dividends_ttl_seconds=cfg.dividends_ttl_seconds,
            reports_ttl_seconds=cfg.reports_ttl_seconds,
            insider_deals_ttl_seconds=cfg.insider_deals_ttl_seconds,
            failure_cooldown_seconds=cfg.fetch_policy_failure_cooldown_seconds,
            max_consecutive_failures=cfg.fetch_policy_max_consecutive_failures,
            max_fetches_per_cycle=cfg.fetch_policy_max_fetches_per_cycle,
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
            fetch_policy_config=fetch_policy,
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

    def _wire_fusion(self) -> None:
        """Wire signal fusion layer when enabled."""
        cfg = self.config.fusion
        windows = parse_windows(cfg.windows)

        # Tracked tickers: DB preferred, then env fallback
        tracked = self._resolve_tracked_tickers()

        self.fusion_service = FusionService(
            repository=self.repository,
            windows=windows,
            tracked_tickers=tracked,
            persist=cfg.persist,
            logger=self.logger,
        )

        self.logger.info(
            "fusion layer initialized",
            extra={
                "component": "fusion",
                "windows": [w.label for w in windows],
                "tracked_tickers": len(tracked),
                "persist": cfg.persist,
            },
        )

    def _wire_cbr(self) -> None:
        """Wire CBR event ingestion when enabled."""
        cfg = self.config.cbr
        rss_urls = cfg.rss_urls if cfg.rss_enabled else ()

        self.cbr_ingestion_service = CbrIngestionService(
            repository=self.repository,
            logger=self.logger,
            rss_urls=rss_urls,
            store_raw_payloads=cfg.store_raw_payloads,
        )

        self.logger.info(
            "cbr pipeline initialized",
            extra={
                "component": "cbr",
                "rss_enabled": cfg.rss_enabled,
                "rss_urls": len(rss_urls),
                "poll_interval_seconds": cfg.poll_interval_seconds,
            },
        )

    def _wire_moex(self) -> None:
        """Wire MOEX ISS ingestion when enabled."""
        cfg = self.config.moex
        # MOEX override takes priority, else DB-backed tracked set
        if cfg.tracked_tickers_override:
            tracked_tickers: tuple[str, ...] | frozenset[str] = cfg.tracked_tickers_override
        else:
            db_tracked = self._resolve_tracked_tickers()
            tracked_tickers = tuple(sorted(db_tracked)) if db_tracked else ()

        self.moex_ingestion_service = MoexIngestionService(
            repository=self.repository,
            logger=self.logger,
            tracked_tickers=tracked_tickers,
            engine=cfg.engine,
            market=cfg.market,
            board=cfg.board,
            history_lookback_days=cfg.history_lookback_days,
            metadata_enabled=cfg.metadata_enabled,
            history_enabled=cfg.history_enabled,
        )

        self.logger.info(
            "moex pipeline initialized",
            extra={
                "component": "moex",
                "metadata_enabled": cfg.metadata_enabled,
                "history_enabled": cfg.history_enabled,
                "tracked_tickers": len(tracked_tickers),
                "history_lookback_days": cfg.history_lookback_days,
                "engine": cfg.engine,
                "market": cfg.market,
                "board": cfg.board,
            },
        )

    def _wire_global_context(self) -> None:
        """Wire global market context ingestion when enabled."""
        cfg = self.config.global_context

        # Reuse existing Telegram source infrastructure
        scfg = self.config.sentiment
        if scfg.source_backend == "telethon":
            source = build_telethon_message_source(scfg)
        else:
            source = StubMessageSource()

        self.global_context_service = GlobalContextIngestionService(
            source=source,
            repository=self.repository,
            logger=self.logger,
            channels=cfg.channels,
            fetch_limit=cfg.fetch_limit_per_source,
        )

        self.logger.info(
            "global context pipeline initialized",
            extra={
                "component": "global_context",
                "channels": len(cfg.channels),
                "poll_interval_seconds": cfg.poll_interval_seconds,
            },
        )

    def _build_global_market_data_fn(self):
        """Build a callable for global market data sync."""
        cfg = self.config.global_market_data
        if not cfg.enabled:
            return None
        if self.repository is None:
            return None

        from tinvest_trader.infra.market_data.global_api_client import (
            DEFAULT_SYMBOLS,
        )
        from tinvest_trader.services.global_market_data_sync import (
            sync_global_market_data,
        )

        # Build symbol map from configured symbols
        symbol_map = {
            sym: info
            for sym, info in DEFAULT_SYMBOLS.items()
            if sym in cfg.symbols
        }
        if not symbol_map:
            symbol_map = dict(DEFAULT_SYMBOLS)

        def _sync():
            return sync_global_market_data(
                repository=self.repository,
                logger=self.logger,
                symbols=symbol_map,
            )

        self.logger.info(
            "global market data sync initialized",
            extra={
                "component": "global_market_data",
                "symbols": len(symbol_map),
                "poll_interval_seconds": cfg.poll_interval_seconds,
            },
        )
        return _sync

    def _build_quote_sync_fn(self):
        """Build a callable for quote sync if prerequisites are met."""
        if not self.config.quote_sync.enabled:
            return None
        if self.repository is None:
            return None

        from tinvest_trader.services.quote_sync import sync_quotes

        def _sync():
            return sync_quotes(
                client=self.tbank_client,
                repository=self.repository,
                logger=self.logger,
            )

        self.logger.info(
            "quote sync initialized",
            extra={
                "component": "quote_sync",
                "poll_interval_seconds": self.config.quote_sync.poll_interval_seconds,
            },
        )
        return _sync

    def _build_signal_delivery_fn(self):
        """Build a callable for signal delivery if prerequisites are met."""
        cfg = self.config.signal_delivery
        if not cfg.enabled or not cfg.bot_token or not cfg.chat_id:
            return None
        if self.repository is None:
            return None

        from tinvest_trader.services.signal_delivery import deliver_pending_signals
        from tinvest_trader.services.signal_severity import SeverityConfig

        sev_cfg = SeverityConfig(
            high_confidence=cfg.high_confidence_threshold,
            high_ev=cfg.high_ev_threshold,
        )

        def _deliver():
            return deliver_pending_signals(
                bot_token=cfg.bot_token,
                chat_id=cfg.chat_id,
                repository=self.repository,
                logger=self.logger,
                proxy_host=cfg.proxy_host,
                proxy_port=cfg.proxy_port,
                proxy_user=cfg.proxy_user,
                proxy_pass=cfg.proxy_pass,
                max_per_cycle=cfg.max_per_cycle,
                severity_config=sev_cfg,
            )

        self.logger.info(
            "signal delivery initialized",
            extra={"component": "signal_delivery"},
        )
        return _deliver

    def _build_callback_handler_fn(self):
        """Build a callable for Telegram callback polling."""
        cfg = self.config.signal_delivery
        if not cfg.enabled or not cfg.bot_token or not cfg.chat_id:
            return None
        if self.repository is None:
            return None

        from tinvest_trader.services.telegram_bot_handler import (
            poll_and_handle_callbacks,
        )

        state = {"last_update_id": 0}

        def _poll():
            state["last_update_id"] = poll_and_handle_callbacks(
                bot_token=cfg.bot_token,
                chat_id=cfg.chat_id,
                repository=self.repository,
                logger=self.logger,
                api_key=cfg.anthropic_api_key,
                ai_model=cfg.ai_model,
                proxy_host=cfg.proxy_host,
                proxy_port=cfg.proxy_port,
                proxy_user=cfg.proxy_user,
                proxy_pass=cfg.proxy_pass,
                last_update_id=state["last_update_id"],
            )

        if cfg.anthropic_api_key:
            self.logger.info(
                "callback handler initialized",
                extra={"component": "bot_handler"},
            )
        return _poll

    def _build_alerting_fn(self):
        """Build a callable for alerting checks."""
        cfg = self.config.alerting
        if not cfg.enabled:
            return None
        if self.repository is None:
            return None

        from tinvest_trader.services.alerting import run_alert_check

        delivery_cfg = self.config.signal_delivery

        def _check():
            return run_alert_check(
                alerting_config=cfg,
                delivery_config=delivery_cfg,
                repository=self.repository,
                logger=self.logger,
                send=True,
            )

        self.logger.info(
            "alerting initialized",
            extra={
                "component": "alerting",
                "check_interval_seconds": cfg.check_interval_seconds,
                "cooldown_seconds": cfg.cooldown_seconds,
            },
        )
        return _check

    def _wire_background_runner(self) -> None:
        """Wire background runner when enabled."""
        self.background_runner = BackgroundRunner(
            config=self.config.background,
            logger=self.logger,
            telegram_sentiment_service=self.telegram_sentiment_service,
            observation_service=self.observation_service,
            broker_event_ingestion_service=self.broker_event_ingestion_service,
            fusion_service=self.fusion_service,
            cbr_ingestion_service=self.cbr_ingestion_service,
            moex_ingestion_service=self.moex_ingestion_service,
            quote_sync_config=self.config.quote_sync,
            quote_sync_fn=self._quote_sync_fn,
            sentiment_channels=self.config.sentiment.channels,
            broker_event_interval_seconds=self.config.broker_events.poll_interval_seconds,
            cbr_interval_seconds=self.config.cbr.poll_interval_seconds,
            moex_interval_seconds=self.config.moex.poll_interval_seconds,
            global_context_service=self.global_context_service,
            global_context_interval_seconds=self.config.global_context.poll_interval_seconds,
            global_market_data_fn=self._global_market_data_fn,
            global_market_data_interval_seconds=self.config.global_market_data.poll_interval_seconds,
            signal_delivery_config=self.config.signal_delivery,
            signal_delivery_fn=self._signal_delivery_fn,
            callback_handler_fn=self._callback_handler_fn,
            alerting_fn=self._alerting_fn,
            alerting_interval_seconds=self.config.alerting.check_interval_seconds,
        )

        self.logger.info(
            "background runner initialized",
            extra={
                "component": "background_runner",
                "run_sentiment": self.config.background.run_sentiment,
                "run_observation": self.config.background.run_observation,
                "run_broker_events": self.broker_event_ingestion_service is not None,
                "run_fusion": self.fusion_service is not None,
                "run_cbr": self.cbr_ingestion_service is not None,
                "run_moex": self.moex_ingestion_service is not None,
            },
        )


def build_container(config: AppConfig) -> Container:
    """Build and return a fully wired container."""
    return Container(config=config)
