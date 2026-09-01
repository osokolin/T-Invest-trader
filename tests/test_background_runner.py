import logging
import threading
from unittest.mock import MagicMock

from tinvest_trader.app.config import BackgroundConfig
from tinvest_trader.services.background_runner import BackgroundRunner


def _make_runner(
    *,
    enabled: bool = True,
    run_sentiment: bool = True,
    run_observation: bool = True,
    sentiment_service: MagicMock | None = None,
    observation_service: MagicMock | None = None,
    broker_event_service: MagicMock | None = None,
    sentiment_channels: tuple[str, ...] = ("TestChannel",),
) -> BackgroundRunner:
    config = BackgroundConfig(
        enabled=enabled,
        sentiment_ingest_interval_seconds=1,
        observation_interval_seconds=1,
        run_sentiment=run_sentiment,
        run_observation=run_observation,
    )
    return BackgroundRunner(
        config=config,
        logger=logging.getLogger("test"),
        telegram_sentiment_service=sentiment_service,
        observation_service=observation_service,
        broker_event_ingestion_service=broker_event_service,
        sentiment_channels=sentiment_channels,
        broker_event_interval_seconds=1,
    )


def test_runner_sentiment_cycle_skips_when_service_missing():
    runner = _make_runner(sentiment_service=None, observation_service=MagicMock())
    runner.run_sentiment_cycle()


def test_runner_observation_cycle_skips_when_service_missing():
    runner = _make_runner(sentiment_service=MagicMock(), observation_service=None)
    runner.run_observation_cycle()


def test_runner_sentiment_cycle_calls_service():
    sentiment_service = MagicMock()
    sentiment_service.ingest_all_channels.return_value = 3
    runner = _make_runner(
        sentiment_service=sentiment_service,
        observation_service=MagicMock(),
        sentiment_channels=("ch1", "ch2"),
    )

    runner.run_sentiment_cycle()

    sentiment_service.ingest_all_channels.assert_called_once_with(("ch1", "ch2"))


def test_runner_observation_cycle_calls_service():
    observation_service = MagicMock()
    observation_service.observe_all.return_value = [object(), object()]
    runner = _make_runner(
        sentiment_service=MagicMock(),
        observation_service=observation_service,
    )

    runner.run_observation_cycle()

    observation_service.observe_all.assert_called_once_with()


def test_runner_broker_event_cycle_calls_service():
    broker_event_service = MagicMock()
    broker_event_service.ingest_all.return_value = 4
    runner = _make_runner(
        sentiment_service=MagicMock(),
        observation_service=MagicMock(),
        broker_event_service=broker_event_service,
    )

    runner.run_broker_event_cycle()

    broker_event_service.ingest_all.assert_called_once_with()


def test_runner_market_activity_cycle_calls_service():
    market_activity_service = MagicMock()
    runner = BackgroundRunner(
        config=BackgroundConfig(enabled=True),
        logger=logging.getLogger("test"),
        market_activity_service=market_activity_service,
    )

    runner.run_market_activity_cycle()

    market_activity_service.observe_all.assert_called_once_with()


def test_runner_market_activity_failure_is_safe():
    market_activity_service = MagicMock()
    market_activity_service.observe_all.side_effect = RuntimeError("boom")
    observation_service = MagicMock()
    runner = BackgroundRunner(
        config=BackgroundConfig(enabled=True),
        logger=logging.getLogger("test"),
        observation_service=observation_service,
        market_activity_service=market_activity_service,
    )

    runner.run_market_activity_cycle()
    runner.run_observation_cycle()

    observation_service.observe_all.assert_called_once_with()


def test_runner_market_activity_outcome_cycle_calls_service():
    outcome_service = MagicMock()
    runner = BackgroundRunner(
        config=BackgroundConfig(enabled=True),
        logger=logging.getLogger("test"),
        market_activity_outcome_service=outcome_service,
    )

    runner.run_market_activity_outcome_cycle()

    outcome_service.resolve_all.assert_called_once_with()


def test_runner_market_activity_outcome_failure_is_safe():
    outcome_service = MagicMock()
    outcome_service.resolve_all.side_effect = RuntimeError("boom")
    observation_service = MagicMock()
    runner = BackgroundRunner(
        config=BackgroundConfig(enabled=True),
        logger=logging.getLogger("test"),
        observation_service=observation_service,
        market_activity_outcome_service=outcome_service,
    )

    runner.run_market_activity_outcome_cycle()
    runner.run_observation_cycle()

    observation_service.observe_all.assert_called_once_with()


def test_runner_activity_paper_strategy_cycle_calls_service():
    from tinvest_trader.app.config import ActivityPaperConfig

    strategy_service = MagicMock()
    runner = BackgroundRunner(
        config=BackgroundConfig(enabled=True),
        logger=logging.getLogger("test"),
        activity_paper_strategy_service=strategy_service,
        activity_paper_config=ActivityPaperConfig(enabled=True),
    )

    runner.run_activity_paper_strategy_cycle()

    strategy_service.run_cycle.assert_called_once_with()


def test_runner_activity_paper_failure_does_not_stop_observation():
    from tinvest_trader.app.config import ActivityPaperConfig

    strategy_service = MagicMock()
    strategy_service.run_cycle.side_effect = RuntimeError("boom")
    observation_service = MagicMock()
    runner = BackgroundRunner(
        config=BackgroundConfig(enabled=True),
        logger=logging.getLogger("test"),
        observation_service=observation_service,
        activity_paper_strategy_service=strategy_service,
        activity_paper_config=ActivityPaperConfig(enabled=True),
    )

    runner.run_activity_paper_strategy_cycle()
    runner.run_observation_cycle()

    observation_service.observe_all.assert_called_once_with()


def test_runner_medium_term_paper_cycle_is_safe_and_isolated():
    from tinvest_trader.app.config import MediumTermPaperConfig

    strategy_service = MagicMock()
    strategy_service.run_cycle.side_effect = RuntimeError("boom")
    observation_service = MagicMock()
    runner = BackgroundRunner(
        config=BackgroundConfig(enabled=True),
        logger=logging.getLogger("test"),
        observation_service=observation_service,
        medium_term_paper_strategy_service=strategy_service,
        medium_term_paper_config=MediumTermPaperConfig(enabled=True),
    )

    runner.run_medium_term_paper_strategy_cycle()
    runner.run_observation_cycle()

    strategy_service.run_cycle.assert_called_once_with()
    observation_service.observe_all.assert_called_once_with()


def test_runner_sentiment_failure_is_safe():
    sentiment_service = MagicMock()
    sentiment_service.ingest_all_channels.side_effect = RuntimeError("boom")
    observation_service = MagicMock()
    broker_event_service = MagicMock()
    runner = _make_runner(
        sentiment_service=sentiment_service,
        observation_service=observation_service,
        broker_event_service=broker_event_service,
    )

    runner.run_sentiment_cycle()
    runner.run_observation_cycle()
    runner.run_broker_event_cycle()

    observation_service.observe_all.assert_called_once_with()
    broker_event_service.ingest_all.assert_called_once_with()


def test_runner_observation_failure_is_safe():
    sentiment_service = MagicMock()
    observation_service = MagicMock()
    observation_service.observe_all.side_effect = RuntimeError("boom")
    broker_event_service = MagicMock()
    runner = _make_runner(
        sentiment_service=sentiment_service,
        observation_service=observation_service,
        broker_event_service=broker_event_service,
    )

    runner.run_observation_cycle()
    runner.run_sentiment_cycle()
    runner.run_broker_event_cycle()

    sentiment_service.ingest_all_channels.assert_called_once_with(("TestChannel",))
    broker_event_service.ingest_all.assert_called_once_with()


def test_runner_broker_event_failure_is_safe():
    sentiment_service = MagicMock()
    observation_service = MagicMock()
    broker_event_service = MagicMock()
    broker_event_service.ingest_all.side_effect = RuntimeError("boom")
    runner = _make_runner(
        sentiment_service=sentiment_service,
        observation_service=observation_service,
        broker_event_service=broker_event_service,
    )

    runner.run_broker_event_cycle()
    runner.run_sentiment_cycle()
    runner.run_observation_cycle()

    sentiment_service.ingest_all_channels.assert_called_once_with(("TestChannel",))
    observation_service.observe_all.assert_called_once_with()


def test_runner_start_does_not_create_thread_when_disabled():
    runner = _make_runner(enabled=False, sentiment_service=MagicMock(), observation_service=None)

    runner.start()

    assert runner._thread is None


def test_runner_start_does_not_create_thread_without_runnable_tasks():
    runner = _make_runner(sentiment_service=None, observation_service=None)

    runner.start()

    assert runner._thread is None


def test_runner_stop_is_safe_without_start():
    runner = _make_runner(sentiment_service=MagicMock(), observation_service=MagicMock())
    runner.stop()


def test_runner_stop_joins_active_thread():
    runner = _make_runner(sentiment_service=MagicMock(), observation_service=MagicMock())
    joined = threading.Event()

    class FakeThread:
        def __init__(self) -> None:
            self._alive = True

        def is_alive(self) -> bool:
            return self._alive

        def join(self, timeout: float | None = None) -> None:
            self._alive = False
            joined.set()

    runner._thread = FakeThread()

    runner.stop()

    assert joined.is_set()
    assert runner._thread is None
