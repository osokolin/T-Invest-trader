"""Background runner for periodic sentiment ingestion and observation.

GUARDRAIL: orchestration only -- delegates to services.
- Must not contain business logic (scoring, filtering, enrichment).
- One failing service must not crash the loop.
- See SYSTEM_GUARDRAILS.md section 12.
"""

from __future__ import annotations

import contextlib
import logging
import pathlib
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.app.config import (
        BackgroundConfig,
        DailyDigestConfig,
        QuoteSyncConfig,
        SignalDeliveryConfig,
        SignalGenerationConfig,
        SignalResolutionConfig,
    )
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
    from tinvest_trader.services.telegram_sentiment_service import TelegramSentimentService


HEARTBEAT_PATH = pathlib.Path("/tmp/tinvest_heartbeat")  # noqa: S108


class BackgroundRunner:
    """Runs periodic background cycles in a single in-process worker thread."""

    def __init__(
        self,
        config: BackgroundConfig,
        logger: logging.Logger,
        telegram_sentiment_service: TelegramSentimentService | None = None,
        observation_service: ObservationService | None = None,
        broker_event_ingestion_service: BrokerEventIngestionService | None = None,
        fusion_service: FusionService | None = None,
        cbr_ingestion_service: CbrIngestionService | None = None,
        moex_ingestion_service: MoexIngestionService | None = None,
        global_context_service: GlobalContextIngestionService | None = None,
        quote_sync_config: QuoteSyncConfig | None = None,
        quote_sync_fn: Callable[[], object] | None = None,
        sentiment_channels: tuple[str, ...] = (),
        broker_event_interval_seconds: int = 1800,
        cbr_interval_seconds: int = 3600,
        moex_interval_seconds: int = 3600,
        global_context_interval_seconds: int = 120,
        global_market_data_fn: Callable[[], object] | None = None,
        global_market_data_interval_seconds: int = 300,
        signal_delivery_config: SignalDeliveryConfig | None = None,
        signal_delivery_fn: Callable[[], int] | None = None,
        callback_handler_fn: Callable[[], None] | None = None,
        alerting_fn: Callable[[], object] | None = None,
        alerting_interval_seconds: int = 300,
        signal_generation_fn: Callable[[], object] | None = None,
        signal_generation_config: SignalGenerationConfig | None = None,
        signal_resolution_fn: Callable[[], int] | None = None,
        signal_resolution_config: SignalResolutionConfig | None = None,
        daily_digest_fn: Callable[[], object] | None = None,
        daily_digest_config: DailyDigestConfig | None = None,
        time_fn: Callable[[], float] | None = None,
    ) -> None:
        self._config = config
        self._logger = logger
        self._telegram_sentiment_service = telegram_sentiment_service
        self._observation_service = observation_service
        self._broker_event_ingestion_service = broker_event_ingestion_service
        self._fusion_service = fusion_service
        self._cbr_ingestion_service = cbr_ingestion_service
        self._moex_ingestion_service = moex_ingestion_service
        self._global_context_service = global_context_service
        self._quote_sync_config = quote_sync_config
        self._quote_sync_fn = quote_sync_fn
        self._sentiment_channels = sentiment_channels
        self._broker_event_interval_seconds = broker_event_interval_seconds
        self._cbr_interval_seconds = cbr_interval_seconds
        self._moex_interval_seconds = moex_interval_seconds
        self._global_context_interval_seconds = global_context_interval_seconds
        self._global_market_data_fn = global_market_data_fn
        self._global_market_data_interval_seconds = global_market_data_interval_seconds
        self._signal_delivery_config = signal_delivery_config
        self._signal_delivery_fn = signal_delivery_fn
        self._callback_handler_fn = callback_handler_fn
        self._alerting_fn = alerting_fn
        self._alerting_interval_seconds = alerting_interval_seconds
        self._signal_generation_fn = signal_generation_fn
        self._signal_generation_config = signal_generation_config
        self._signal_resolution_fn = signal_resolution_fn
        self._signal_resolution_config = signal_resolution_config
        self._daily_digest_fn = daily_digest_fn
        self._daily_digest_config = daily_digest_config
        self._daily_digest_sent_today: str = ""
        self._time_fn = time_fn or time.monotonic
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the worker thread if background execution is enabled."""
        if not self._config.enabled:
            self._logger.info(
                "background runner disabled",
                extra={"component": "background_runner"},
            )
            return

        if not self._has_runnable_tasks():
            self._logger.info(
                "background runner enabled but no tasks are runnable",
                extra={"component": "background_runner"},
            )
            return

        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="tinvest-background-runner",
                daemon=True,
            )
            self._thread.start()

        self._logger.info(
            "background runner started",
            extra={
                "component": "background_runner",
                "run_sentiment": self._config.run_sentiment,
                "run_observation": self._config.run_observation,
                "sentiment_interval_seconds": (
                    self._config.sentiment_ingest_interval_seconds
                ),
                "observation_interval_seconds": self._config.observation_interval_seconds,
                "broker_events_interval_seconds": self._broker_event_interval_seconds,
            },
        )

    def stop(self) -> None:
        """Stop the worker thread and wait briefly for clean shutdown."""
        with self._lock:
            thread = self._thread
            if thread is None:
                return
            self._stop_event.set()

        thread.join(timeout=5)

        if thread.is_alive():
            self._logger.warning(
                "background runner stop timed out",
                extra={"component": "background_runner"},
            )
            return

        with self._lock:
            if self._thread is thread:
                self._thread = None

        self._logger.info(
            "background runner stopped",
            extra={"component": "background_runner"},
        )

    def run_sentiment_cycle(self) -> None:
        """Run one sentiment ingestion cycle safely."""
        if not self._config.run_sentiment:
            return
        if self._telegram_sentiment_service is None:
            self._logger.info(
                "skipping sentiment cycle: service unavailable",
                extra={"component": "background_runner"},
            )
            return

        try:
            processed = self._telegram_sentiment_service.ingest_all_channels(
                self._sentiment_channels,
            )
            self._logger.info(
                "background sentiment cycle complete",
                extra={
                    "component": "background_runner",
                    "channels": len(self._sentiment_channels),
                    "processed": processed,
                },
            )
        except Exception:
            self._logger.exception(
                "background sentiment cycle failed",
                extra={"component": "background_runner"},
            )

    def run_observation_cycle(self) -> None:
        """Run one observation aggregation cycle safely."""
        if not self._config.run_observation:
            return
        if self._observation_service is None:
            self._logger.info(
                "skipping observation cycle: service unavailable",
                extra={"component": "background_runner"},
            )
            return

        try:
            observations = self._observation_service.observe_all()
            self._logger.info(
                "background observation cycle complete",
                extra={
                    "component": "background_runner",
                    "observations": len(observations),
                },
            )
        except Exception:
            self._logger.exception(
                "background observation cycle failed",
                extra={"component": "background_runner"},
            )

    def _has_runnable_tasks(self) -> bool:
        return (
            self._sentiment_is_runnable()
            or self._observation_is_runnable()
            or self._broker_events_is_runnable()
            or self._fusion_is_runnable()
            or self._signal_generation_is_runnable()
            or self._signal_resolution_is_runnable()
            or self._cbr_is_runnable()
            or self._moex_is_runnable()
            or self._global_context_is_runnable()
            or self._global_market_data_is_runnable()
            or self._quote_sync_is_runnable()
            or self._signal_delivery_is_runnable()
            or self._callback_handler_is_runnable()
            or self._alerting_is_runnable()
            or self._daily_digest_is_runnable()
        )

    def _sentiment_is_runnable(self) -> bool:
        return self._config.run_sentiment and self._telegram_sentiment_service is not None

    def _observation_is_runnable(self) -> bool:
        return self._config.run_observation and self._observation_service is not None

    def _broker_events_is_runnable(self) -> bool:
        return self._broker_event_ingestion_service is not None

    def _fusion_is_runnable(self) -> bool:
        return self._config.run_fusion and self._fusion_service is not None

    def _signal_generation_is_runnable(self) -> bool:
        return (
            self._config.run_signal_generation
            and self._signal_generation_config is not None
            and self._signal_generation_config.enabled
            and self._signal_generation_fn is not None
        )

    def _cbr_is_runnable(self) -> bool:
        return self._config.run_cbr and self._cbr_ingestion_service is not None

    def _moex_is_runnable(self) -> bool:
        return self._config.run_moex and self._moex_ingestion_service is not None

    def _global_context_is_runnable(self) -> bool:
        return (
            self._config.run_global_context
            and self._global_context_service is not None
        )

    def _global_market_data_is_runnable(self) -> bool:
        return (
            self._config.run_global_market_data
            and self._global_market_data_fn is not None
        )

    def _quote_sync_is_runnable(self) -> bool:
        return (
            self._config.run_quote_sync
            and self._quote_sync_config is not None
            and self._quote_sync_config.enabled
            and self._quote_sync_fn is not None
        )

    def _signal_delivery_is_runnable(self) -> bool:
        return (
            self._config.run_signal_delivery
            and self._signal_delivery_config is not None
            and self._signal_delivery_config.enabled
            and self._signal_delivery_fn is not None
        )

    def _callback_handler_is_runnable(self) -> bool:
        return (
            self._signal_delivery_config is not None
            and self._signal_delivery_config.enabled
            and self._callback_handler_fn is not None
        )

    def _alerting_is_runnable(self) -> bool:
        return (
            self._config.run_alerting
            and self._alerting_fn is not None
        )

    def _daily_digest_is_runnable(self) -> bool:
        return (
            self._config.run_daily_digest
            and self._daily_digest_fn is not None
            and self._daily_digest_config is not None
            and self._daily_digest_config.enabled
        )

    def run_broker_event_cycle(self) -> None:
        """Run one broker structured-event ingestion cycle safely."""
        if self._broker_event_ingestion_service is None:
            self._logger.info(
                "skipping broker event cycle: service unavailable",
                extra={"component": "background_runner"},
            )
            return

        try:
            processed = self._broker_event_ingestion_service.ingest_all()
            self._logger.info(
                "background broker event cycle complete",
                extra={
                    "component": "background_runner",
                    "processed": processed,
                },
            )
        except Exception:
            self._logger.exception(
                "background broker event cycle failed",
                extra={"component": "background_runner"},
            )

    def run_fusion_cycle(self) -> None:
        """Run one signal fusion cycle safely."""
        if not self._config.run_fusion:
            return
        if self._fusion_service is None:
            self._logger.info(
                "skipping fusion cycle: service unavailable",
                extra={"component": "background_runner"},
            )
            return

        try:
            features = self._fusion_service.fuse_all()
            self._logger.info(
                "background fusion cycle complete",
                extra={
                    "component": "background_runner",
                    "fused_features": len(features),
                },
            )
        except Exception:
            self._logger.exception(
                "background fusion cycle failed",
                extra={"component": "background_runner"},
            )

    def run_signal_generation_cycle(self) -> None:
        """Run one signal generation cycle safely."""
        if not self._signal_generation_is_runnable():
            return

        try:
            result = self._signal_generation_fn()
            self._logger.info(
                "background signal generation cycle complete",
                extra={
                    "component": "background_runner",
                    "result": str(result) if result else "none",
                },
            )
        except Exception:
            self._logger.exception(
                "background signal generation cycle failed",
                extra={"component": "background_runner"},
            )

    def _signal_resolution_is_runnable(self) -> bool:
        return (
            self._config.run_signal_resolution
            and self._signal_resolution_fn is not None
            and self._signal_resolution_config is not None
        )

    def run_signal_resolution_cycle(self) -> None:
        """Run one signal resolution cycle safely."""
        if not self._signal_resolution_is_runnable():
            return

        try:
            resolved = self._signal_resolution_fn()
            if resolved:
                self._logger.info(
                    "background signal resolution cycle complete",
                    extra={
                        "component": "background_runner",
                        "resolved": resolved,
                    },
                )
        except Exception:
            self._logger.exception(
                "background signal resolution cycle failed",
                extra={"component": "background_runner"},
            )

    def run_cbr_cycle(self) -> None:
        """Run one CBR RSS ingestion cycle safely."""
        if not self._config.run_cbr:
            return
        if self._cbr_ingestion_service is None:
            self._logger.info(
                "skipping cbr cycle: service unavailable",
                extra={"component": "background_runner"},
            )
            return

        try:
            persisted = self._cbr_ingestion_service.ingest_all()
            self._logger.info(
                "background cbr cycle complete",
                extra={
                    "component": "background_runner",
                    "persisted": persisted,
                },
            )
        except Exception:
            self._logger.exception(
                "background cbr cycle failed",
                extra={"component": "background_runner"},
            )

    def run_moex_cycle(self) -> None:
        """Run one MOEX ISS ingestion cycle safely."""
        if not self._config.run_moex:
            return
        if self._moex_ingestion_service is None:
            self._logger.info(
                "skipping moex cycle: service unavailable",
                extra={"component": "background_runner"},
            )
            return

        try:
            persisted = self._moex_ingestion_service.ingest_all()
            self._logger.info(
                "background moex cycle complete",
                extra={
                    "component": "background_runner",
                    "persisted": persisted,
                },
            )
        except Exception:
            self._logger.exception(
                "background moex cycle failed",
                extra={"component": "background_runner"},
            )

    def run_global_context_cycle(self) -> None:
        """Run one global context ingestion cycle safely."""
        if not self._global_context_is_runnable():
            return

        try:
            persisted = self._global_context_service.ingest_all()
            self._logger.info(
                "background global context cycle complete",
                extra={
                    "component": "background_runner",
                    "persisted": persisted,
                },
            )
        except Exception:
            self._logger.exception(
                "background global context cycle failed",
                extra={"component": "background_runner"},
            )

    def run_global_market_data_cycle(self) -> None:
        """Run one global market data sync cycle safely."""
        if not self._global_market_data_is_runnable():
            return

        try:
            result = self._global_market_data_fn()
            self._logger.info(
                "background global market data cycle complete",
                extra={
                    "component": "background_runner",
                    "result": str(result),
                },
            )
        except Exception:
            self._logger.exception(
                "background global market data cycle failed",
                extra={"component": "background_runner"},
            )

    def run_quote_sync_cycle(self) -> None:
        """Run one quote sync cycle safely."""
        if not self._quote_sync_is_runnable():
            return

        try:
            result = self._quote_sync_fn()
            self._logger.info(
                "background quote sync cycle complete",
                extra={
                    "component": "background_runner",
                    "result": str(result),
                },
            )
        except Exception:
            self._logger.exception(
                "background quote sync cycle failed",
                extra={"component": "background_runner"},
            )

    def run_signal_delivery_cycle(self) -> None:
        """Run one signal delivery cycle safely."""
        if not self._signal_delivery_is_runnable():
            return

        try:
            sent = self._signal_delivery_fn()
            self._logger.info(
                "background signal delivery cycle complete",
                extra={
                    "component": "background_runner",
                    "sent": sent,
                },
            )
        except Exception:
            self._logger.exception(
                "background signal delivery cycle failed",
                extra={"component": "background_runner"},
            )

    def run_daily_digest_cycle(self) -> None:
        """Run daily digest if it's the right time and not yet sent today."""
        if not self._daily_digest_is_runnable():
            return

        from datetime import UTC, datetime

        now = datetime.now(UTC)
        today_str = now.strftime("%Y-%m-%d")

        # Already sent today (in-memory dedup)
        if self._daily_digest_sent_today == today_str:
            return

        cfg = self._daily_digest_config
        if now.hour < cfg.hour or (now.hour == cfg.hour and now.minute < cfg.minute):
            return

        try:
            result = self._daily_digest_fn()
            self._daily_digest_sent_today = today_str
            self._logger.info(
                "background daily digest cycle complete",
                extra={
                    "component": "background_runner",
                    "result": str(result),
                },
            )
        except Exception:
            self._logger.exception(
                "background daily digest cycle failed",
                extra={"component": "background_runner"},
            )

    def run_alerting_cycle(self) -> None:
        """Run one alerting check cycle safely."""
        if not self._alerting_is_runnable():
            return

        try:
            result = self._alerting_fn()
            self._logger.info(
                "background alerting cycle complete",
                extra={
                    "component": "background_runner",
                    "result": str(result),
                },
            )
        except Exception:
            self._logger.exception(
                "background alerting cycle failed",
                extra={"component": "background_runner"},
            )

    def run_callback_handler_cycle(self) -> None:
        """Run one callback polling cycle safely."""
        if not self._callback_handler_is_runnable():
            return

        try:
            self._callback_handler_fn()
        except Exception:
            self._logger.exception(
                "background callback handler cycle failed",
                extra={"component": "background_runner"},
            )

    def _run_loop(self) -> None:
        next_sentiment_run = self._time_fn()
        next_observation_run = self._time_fn()
        next_broker_event_run = self._time_fn()
        next_fusion_run = self._time_fn()
        next_signal_generation_run = self._time_fn()
        next_cbr_run = self._time_fn()
        next_moex_run = self._time_fn()
        next_global_context_run = self._time_fn()
        next_global_market_data_run = self._time_fn()
        next_quote_sync_run = self._time_fn()
        next_signal_resolution_run = self._time_fn()
        next_signal_delivery_run = self._time_fn()
        next_callback_handler_run = self._time_fn()
        next_alerting_run = self._time_fn()
        next_daily_digest_run = self._time_fn()

        while not self._stop_event.is_set():
            now = self._time_fn()
            next_wait: float | None = None

            if self._sentiment_is_runnable():
                if now >= next_sentiment_run:
                    self.run_sentiment_cycle()
                    next_sentiment_run = self._time_fn() + max(
                        1, self._config.sentiment_ingest_interval_seconds,
                    )
                next_wait = max(0.0, next_sentiment_run - self._time_fn())

            if self._observation_is_runnable():
                if now >= next_observation_run:
                    self.run_observation_cycle()
                    next_observation_run = self._time_fn() + max(
                        1, self._config.observation_interval_seconds,
                    )
                observation_wait = max(0.0, next_observation_run - self._time_fn())
                next_wait = (
                    observation_wait
                    if next_wait is None
                    else min(next_wait, observation_wait)
                )

            if self._broker_events_is_runnable():
                if now >= next_broker_event_run:
                    self.run_broker_event_cycle()
                    next_broker_event_run = self._time_fn() + max(
                        1, self._broker_event_interval_seconds,
                    )
                broker_event_wait = max(0.0, next_broker_event_run - self._time_fn())
                next_wait = (
                    broker_event_wait
                    if next_wait is None
                    else min(next_wait, broker_event_wait)
                )

            if self._fusion_is_runnable():
                if now >= next_fusion_run:
                    self.run_fusion_cycle()
                    next_fusion_run = self._time_fn() + max(
                        1, self._config.fusion_interval_seconds,
                    )
                fusion_wait = max(0.0, next_fusion_run - self._time_fn())
                next_wait = (
                    fusion_wait
                    if next_wait is None
                    else min(next_wait, fusion_wait)
                )

            if self._signal_generation_is_runnable():
                if now >= next_signal_generation_run:
                    self.run_signal_generation_cycle()
                    interval = self._signal_generation_config.poll_interval_seconds
                    next_signal_generation_run = (
                        self._time_fn() + max(1, interval)
                    )
                sg_wait = max(
                    0.0, next_signal_generation_run - self._time_fn(),
                )
                next_wait = (
                    sg_wait
                    if next_wait is None
                    else min(next_wait, sg_wait)
                )

            if self._cbr_is_runnable():
                if now >= next_cbr_run:
                    self.run_cbr_cycle()
                    next_cbr_run = self._time_fn() + max(
                        1, self._cbr_interval_seconds,
                    )
                cbr_wait = max(0.0, next_cbr_run - self._time_fn())
                next_wait = (
                    cbr_wait
                    if next_wait is None
                    else min(next_wait, cbr_wait)
                )

            if self._moex_is_runnable():
                if now >= next_moex_run:
                    self.run_moex_cycle()
                    next_moex_run = self._time_fn() + max(
                        1, self._moex_interval_seconds,
                    )
                moex_wait = max(0.0, next_moex_run - self._time_fn())
                next_wait = (
                    moex_wait
                    if next_wait is None
                    else min(next_wait, moex_wait)
                )

            if self._global_context_is_runnable():
                if now >= next_global_context_run:
                    self.run_global_context_cycle()
                    next_global_context_run = self._time_fn() + max(
                        1, self._global_context_interval_seconds,
                    )
                gc_wait = max(0.0, next_global_context_run - self._time_fn())
                next_wait = (
                    gc_wait
                    if next_wait is None
                    else min(next_wait, gc_wait)
                )

            if self._global_market_data_is_runnable():
                if now >= next_global_market_data_run:
                    self.run_global_market_data_cycle()
                    next_global_market_data_run = self._time_fn() + max(
                        1, self._global_market_data_interval_seconds,
                    )
                gmd_wait = max(
                    0.0, next_global_market_data_run - self._time_fn(),
                )
                next_wait = (
                    gmd_wait
                    if next_wait is None
                    else min(next_wait, gmd_wait)
                )

            if self._quote_sync_is_runnable():
                if now >= next_quote_sync_run:
                    self.run_quote_sync_cycle()
                    interval = self._quote_sync_config.poll_interval_seconds
                    next_quote_sync_run = self._time_fn() + max(1, interval)
                quote_sync_wait = max(0.0, next_quote_sync_run - self._time_fn())
                next_wait = (
                    quote_sync_wait
                    if next_wait is None
                    else min(next_wait, quote_sync_wait)
                )

            if self._signal_resolution_is_runnable():
                if now >= next_signal_resolution_run:
                    self.run_signal_resolution_cycle()
                    interval = self._signal_resolution_config.poll_interval_seconds
                    next_signal_resolution_run = (
                        self._time_fn() + max(1, interval)
                    )
                sr_wait = max(
                    0.0, next_signal_resolution_run - self._time_fn(),
                )
                next_wait = (
                    sr_wait
                    if next_wait is None
                    else min(next_wait, sr_wait)
                )

            if self._signal_delivery_is_runnable():
                if now >= next_signal_delivery_run:
                    self.run_signal_delivery_cycle()
                    interval = self._signal_delivery_config.delivery_interval_seconds
                    next_signal_delivery_run = self._time_fn() + max(1, interval)
                delivery_wait = max(0.0, next_signal_delivery_run - self._time_fn())
                next_wait = (
                    delivery_wait
                    if next_wait is None
                    else min(next_wait, delivery_wait)
                )

            if self._callback_handler_is_runnable():
                if now >= next_callback_handler_run:
                    self.run_callback_handler_cycle()
                    interval = (
                        self._signal_delivery_config.callback_poll_interval_seconds
                    )
                    next_callback_handler_run = (
                        self._time_fn() + max(1, interval)
                    )
                cb_wait = max(
                    0.0, next_callback_handler_run - self._time_fn(),
                )
                next_wait = (
                    cb_wait
                    if next_wait is None
                    else min(next_wait, cb_wait)
                )

            if self._alerting_is_runnable():
                if now >= next_alerting_run:
                    self.run_alerting_cycle()
                    next_alerting_run = self._time_fn() + max(
                        1, self._alerting_interval_seconds,
                    )
                alerting_wait = max(
                    0.0, next_alerting_run - self._time_fn(),
                )
                next_wait = (
                    alerting_wait
                    if next_wait is None
                    else min(next_wait, alerting_wait)
                )

            if self._daily_digest_is_runnable():
                if now >= next_daily_digest_run:
                    self.run_daily_digest_cycle()
                    next_daily_digest_run = self._time_fn() + 60
                digest_wait = max(
                    0.0, next_daily_digest_run - self._time_fn(),
                )
                next_wait = (
                    digest_wait
                    if next_wait is None
                    else min(next_wait, digest_wait)
                )

            if next_wait is None:
                return

            with contextlib.suppress(OSError):
                HEARTBEAT_PATH.write_text(str(time.time()))

            self._stop_event.wait(timeout=next_wait)
