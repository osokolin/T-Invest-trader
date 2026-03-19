"""Background runner for periodic sentiment ingestion and observation."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.app.config import BackgroundConfig
    from tinvest_trader.services.broker_event_ingestion_service import (
        BrokerEventIngestionService,
    )
    from tinvest_trader.services.cbr_ingestion_service import CbrIngestionService
    from tinvest_trader.services.fusion_service import FusionService
    from tinvest_trader.services.observation_service import ObservationService
    from tinvest_trader.services.telegram_sentiment_service import TelegramSentimentService


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
        sentiment_channels: tuple[str, ...] = (),
        broker_event_interval_seconds: int = 1800,
        cbr_interval_seconds: int = 3600,
        time_fn: Callable[[], float] | None = None,
    ) -> None:
        self._config = config
        self._logger = logger
        self._telegram_sentiment_service = telegram_sentiment_service
        self._observation_service = observation_service
        self._broker_event_ingestion_service = broker_event_ingestion_service
        self._fusion_service = fusion_service
        self._cbr_ingestion_service = cbr_ingestion_service
        self._sentiment_channels = sentiment_channels
        self._broker_event_interval_seconds = broker_event_interval_seconds
        self._cbr_interval_seconds = cbr_interval_seconds
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
            or self._cbr_is_runnable()
        )

    def _sentiment_is_runnable(self) -> bool:
        return self._config.run_sentiment and self._telegram_sentiment_service is not None

    def _observation_is_runnable(self) -> bool:
        return self._config.run_observation and self._observation_service is not None

    def _broker_events_is_runnable(self) -> bool:
        return self._broker_event_ingestion_service is not None

    def _fusion_is_runnable(self) -> bool:
        return self._config.run_fusion and self._fusion_service is not None

    def _cbr_is_runnable(self) -> bool:
        return self._config.run_cbr and self._cbr_ingestion_service is not None

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

    def _run_loop(self) -> None:
        next_sentiment_run = self._time_fn()
        next_observation_run = self._time_fn()
        next_broker_event_run = self._time_fn()
        next_fusion_run = self._time_fn()
        next_cbr_run = self._time_fn()

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

            if next_wait is None:
                return

            self._stop_event.wait(timeout=next_wait)
