"""Tests for global market context ingestion pipeline."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from tinvest_trader.global_context.classifier import (
    ClassificationResult,
    classify_global_context,
)
from tinvest_trader.global_context.models import GlobalContextEvent
from tinvest_trader.sentiment.models import TelegramMessage
from tinvest_trader.sentiment.source import StubMessageSource
from tinvest_trader.services.global_context_ingestion import (
    GlobalContextIngestionService,
)
from tinvest_trader.services.telegram_normalization import (
    build_dedup_hash,
    normalize_message_text,
)

# -- A. Classification (rule-based) --


class TestClassifyGlobalContext:
    # Risk sentiment
    def test_risk_on_positive(self) -> None:
        result = classify_global_context("Markets rally as risk-on sentiment dominates")
        assert result.event_type == "risk_sentiment"
        assert result.direction == "positive"

    def test_risk_off_negative(self) -> None:
        result = classify_global_context("Major selloff across global markets")
        assert result.event_type == "risk_sentiment"
        assert result.direction == "negative"

    def test_futures_higher(self) -> None:
        result = classify_global_context("US futures higher ahead of jobs data")
        assert result.event_type == "risk_sentiment"
        assert result.direction == "positive"

    def test_futures_lower(self) -> None:
        result = classify_global_context("Futures lower as trade tensions escalate")
        assert result.event_type == "risk_sentiment"
        assert result.direction == "negative"

    # Oil
    def test_oil_positive(self) -> None:
        result = classify_global_context("Oil rises 3% on OPEC cut agreement")
        assert result.event_type == "oil"
        assert result.direction == "positive"

    def test_brent_down(self) -> None:
        result = classify_global_context("Brent down 2% on demand concerns")
        assert result.event_type == "oil"
        assert result.direction == "negative"

    def test_crude_higher(self) -> None:
        result = classify_global_context("Crude higher after inventory drawdown")
        assert result.event_type == "oil"
        assert result.direction == "positive"

    # Crypto
    def test_bitcoin_rallies(self) -> None:
        result = classify_global_context("Bitcoin rallies past $100k")
        assert result.event_type == "crypto"
        assert result.direction == "positive"

    def test_bitcoin_dumps(self) -> None:
        result = classify_global_context("Bitcoin dumps 10% in flash crash")
        assert result.event_type == "crypto"
        assert result.direction == "negative"

    def test_crypto_surge(self) -> None:
        result = classify_global_context("Crypto surge leads alt season")
        assert result.event_type == "crypto"
        assert result.direction == "positive"

    # Macro
    def test_rate_cut_positive(self) -> None:
        result = classify_global_context("Fed signals rate cut at next meeting")
        assert result.event_type == "macro"
        assert result.direction == "positive"

    def test_rate_hike_negative(self) -> None:
        result = classify_global_context("Unexpected rate hike shocks markets")
        assert result.event_type == "macro"
        assert result.direction == "negative"

    # Unknown
    def test_unknown_text(self) -> None:
        result = classify_global_context("Weather forecast for tomorrow")
        assert result.event_type == "unknown"
        assert result.direction == "unknown"
        assert result.confidence == 0.0

    def test_empty_text(self) -> None:
        result = classify_global_context("")
        assert result.event_type == "unknown"

    # Confidence levels
    def test_oil_high_confidence(self) -> None:
        result = classify_global_context("Oil rises sharply")
        assert result.confidence == 0.7

    def test_macro_moderate_confidence(self) -> None:
        result = classify_global_context("Fed signals rate cut")
        assert result.confidence == 0.5

    # Case insensitive
    def test_case_insensitive(self) -> None:
        result = classify_global_context("BITCOIN RALLIES to new high")
        assert result.event_type == "crypto"
        assert result.direction == "positive"

    # Oil takes priority over risk
    def test_oil_priority_over_risk(self) -> None:
        # "oil rises" and "rally" -- oil should match first
        result = classify_global_context("Oil rises amid rally")
        assert result.event_type == "oil"


# -- B. Normalization --


class TestNormalization:
    def test_basic_normalization(self) -> None:
        result = normalize_message_text("  Hello   WORLD  ")
        assert result == "hello world"

    def test_url_stripping_in_dedup(self) -> None:
        hash1 = build_dedup_hash("src", "Oil rises https://example.com/article")
        hash2 = build_dedup_hash("src", "Oil rises")
        assert hash1 == hash2

    def test_different_sources_different_hash(self) -> None:
        hash1 = build_dedup_hash("source_a", "same text")
        hash2 = build_dedup_hash("source_b", "same text")
        assert hash1 != hash2


# -- C. Hard dedup --


class TestHardDedup:
    def test_hard_dedup_by_message_id(self) -> None:
        """Second insert of same source+message_id returns hard_duplicate."""
        now = datetime.now(tz=UTC)
        messages = [
            TelegramMessage(
                channel_name="financialjuice",
                message_id="5001",
                message_text="Oil rises on OPEC cut",
                published_at=now,
            ),
        ]

        source = StubMessageSource(messages=messages)
        repo = MagicMock()
        repo.check_global_context_dedup_hash.return_value = False
        # First call: newly inserted
        repo.insert_global_context_event.return_value = True

        service = GlobalContextIngestionService(
            source=source,
            repository=repo,
            logger=MagicMock(),
            channels=("financialjuice",),
        )

        result = service.ingest_channel("financialjuice")
        assert result.inserted == 1

        # Second call: hard duplicate
        repo.insert_global_context_event.return_value = False
        result2 = service.ingest_channel("financialjuice")
        assert result2.hard_duplicates == 1
        assert result2.inserted == 0


# -- D. Soft dedup --


class TestSoftDedup:
    def test_soft_dedup_by_hash(self) -> None:
        now = datetime.now(tz=UTC)
        messages = [
            TelegramMessage(
                channel_name="oilpricee",
                message_id="6001",
                message_text="Bitcoin rallies past $100k milestone",
                published_at=now,
            ),
        ]

        source = StubMessageSource(messages=messages)
        repo = MagicMock()
        # Dedup hash already exists
        repo.check_global_context_dedup_hash.return_value = True

        service = GlobalContextIngestionService(
            source=source,
            repository=repo,
            logger=MagicMock(),
            channels=("oilpricee",),
        )

        result = service.ingest_channel("oilpricee")
        assert result.soft_duplicates == 1
        assert result.inserted == 0
        # insert_global_context_event should not be called
        repo.insert_global_context_event.assert_not_called()


# -- E. Multi-source ingestion --


class TestMultiSourceIngestion:
    def test_multiple_channels(self) -> None:
        now = datetime.now(tz=UTC)
        messages = [
            TelegramMessage(
                channel_name="test",
                message_id="7001",
                message_text="Stocks rally on positive data",
                published_at=now,
            ),
        ]

        source = StubMessageSource(messages=messages)
        repo = MagicMock()
        repo.check_global_context_dedup_hash.return_value = False
        repo.insert_global_context_event.return_value = True
        repo.get_latest_global_context_message_id.return_value = None

        service = GlobalContextIngestionService(
            source=source,
            repository=repo,
            logger=MagicMock(),
            channels=("financialjuice", "oilpricee", "cointelegraph"),
            channel_pacing_seconds=0,  # no pacing in tests
        )

        result = service.ingest_all_detailed()
        assert result.sources_processed == 3
        assert result.messages_fetched == 3
        assert result.inserted == 3

    def test_one_source_failure_continues(self) -> None:
        """One failing source should not block others."""

        class FailingSource:
            def __init__(self):
                self._call_count = 0

            def fetch_recent_messages(self, channel_name, min_id=None):
                self._call_count += 1
                if channel_name == "failing_source":
                    raise ConnectionError("source unavailable")
                return [
                    TelegramMessage(
                        channel_name=channel_name,
                        message_id=f"8{self._call_count}01",
                        message_text="Oil rises 2%",
                        published_at=datetime.now(tz=UTC),
                    ),
                ]

        repo = MagicMock()
        repo.check_global_context_dedup_hash.return_value = False
        repo.insert_global_context_event.return_value = True
        repo.get_latest_global_context_message_id.return_value = None

        service = GlobalContextIngestionService(
            source=FailingSource(),
            repository=repo,
            logger=MagicMock(),
            channels=("good_source", "failing_source", "another_good"),
            channel_pacing_seconds=0,
        )

        result = service.ingest_all_detailed()
        assert result.sources_processed == 3
        assert result.inserted == 2
        assert "failing_source" in result.failed_sources
        assert len(result.failed_sources) == 1


# -- F. Unknown messages handled safely --


class TestUnknownMessages:
    def test_unclassified_message_stored(self) -> None:
        """Messages that don't match any rule are still stored."""
        now = datetime.now(tz=UTC)
        messages = [
            TelegramMessage(
                channel_name="financialjuice",
                message_id="9001",
                message_text="Interesting weather patterns today",
                published_at=now,
            ),
        ]

        source = StubMessageSource(messages=messages)
        repo = MagicMock()
        repo.check_global_context_dedup_hash.return_value = False
        repo.insert_global_context_event.return_value = True

        service = GlobalContextIngestionService(
            source=source,
            repository=repo,
            logger=MagicMock(),
            channels=("financialjuice",),
        )

        result = service.ingest_channel("financialjuice")
        assert result.inserted == 1
        # But classified_events should be 0 since it's unknown
        assert result.classified_events == 0

        # Verify stored with event_type=unknown
        call_args = repo.insert_global_context_event.call_args[0][0]
        assert call_args["event_type"] == "unknown"
        assert call_args["direction"] == "unknown"

    def test_empty_message_text(self) -> None:
        now = datetime.now(tz=UTC)
        messages = [
            TelegramMessage(
                channel_name="financialjuice",
                message_id="9002",
                message_text="",
                published_at=now,
            ),
        ]

        source = StubMessageSource(messages=messages)
        repo = MagicMock()
        repo.check_global_context_dedup_hash.return_value = False
        repo.insert_global_context_event.return_value = True

        service = GlobalContextIngestionService(
            source=source,
            repository=repo,
            logger=MagicMock(),
            channels=("financialjuice",),
        )

        result = service.ingest_channel("financialjuice")
        assert result.inserted == 1


# -- G. Background wiring --


class TestBackgroundWiring:
    def test_global_context_is_runnable(self) -> None:
        from tinvest_trader.app.config import BackgroundConfig
        from tinvest_trader.services.background_runner import BackgroundRunner

        config = BackgroundConfig(
            enabled=True,
            run_global_context=True,
        )
        service = MagicMock()
        runner = BackgroundRunner(
            config=config,
            logger=MagicMock(),
            global_context_service=service,
        )
        assert runner._global_context_is_runnable() is True

    def test_global_context_not_runnable_when_disabled(self) -> None:
        from tinvest_trader.app.config import BackgroundConfig
        from tinvest_trader.services.background_runner import BackgroundRunner

        config = BackgroundConfig(
            enabled=True,
            run_global_context=False,
        )
        service = MagicMock()
        runner = BackgroundRunner(
            config=config,
            logger=MagicMock(),
            global_context_service=service,
        )
        assert runner._global_context_is_runnable() is False

    def test_global_context_not_runnable_without_service(self) -> None:
        from tinvest_trader.app.config import BackgroundConfig
        from tinvest_trader.services.background_runner import BackgroundRunner

        config = BackgroundConfig(
            enabled=True,
            run_global_context=True,
        )
        runner = BackgroundRunner(
            config=config,
            logger=MagicMock(),
        )
        assert runner._global_context_is_runnable() is False

    def test_run_global_context_cycle(self) -> None:
        from tinvest_trader.app.config import BackgroundConfig
        from tinvest_trader.services.background_runner import BackgroundRunner

        config = BackgroundConfig(
            enabled=True,
            run_global_context=True,
        )
        service = MagicMock()
        service.ingest_all.return_value = 5
        runner = BackgroundRunner(
            config=config,
            logger=MagicMock(),
            global_context_service=service,
        )
        runner.run_global_context_cycle()
        service.ingest_all.assert_called_once()

    def test_cycle_exception_does_not_crash(self) -> None:
        from tinvest_trader.app.config import BackgroundConfig
        from tinvest_trader.services.background_runner import BackgroundRunner

        config = BackgroundConfig(
            enabled=True,
            run_global_context=True,
        )
        service = MagicMock()
        service.ingest_all.side_effect = RuntimeError("network error")
        runner = BackgroundRunner(
            config=config,
            logger=MagicMock(),
            global_context_service=service,
        )
        # Should not raise
        runner.run_global_context_cycle()


# -- H. Config --


class TestGlobalContextConfig:
    def test_default_config(self) -> None:
        from tinvest_trader.app.config import GlobalContextConfig

        cfg = GlobalContextConfig()
        assert cfg.enabled is False
        assert "financialjuice" in cfg.channels
        assert "oilpricee" in cfg.channels
        assert "cointelegraph" in cfg.channels
        assert cfg.poll_interval_seconds == 120
        assert cfg.fetch_limit_per_source == 20

    def test_config_in_app_config(self) -> None:
        from tinvest_trader.app.config import AppConfig

        app_cfg = AppConfig()
        assert hasattr(app_cfg, "global_context")
        assert app_cfg.global_context.enabled is False

    def test_background_config_has_run_flag(self) -> None:
        from tinvest_trader.app.config import BackgroundConfig

        bg_cfg = BackgroundConfig()
        assert bg_cfg.run_global_context is True


# -- I. No-repo mode --


class TestNoRepository:
    def test_ingest_without_repository(self) -> None:
        """Service works without DB (all messages 'inserted')."""
        now = datetime.now(tz=UTC)
        messages = [
            TelegramMessage(
                channel_name="test",
                message_id="1001",
                message_text="Oil rises 2%",
                published_at=now,
            ),
        ]

        source = StubMessageSource(messages=messages)
        service = GlobalContextIngestionService(
            source=source,
            repository=None,
            logger=MagicMock(),
            channels=("test",),
        )

        result = service.ingest_channel("test")
        assert result.inserted == 1
        assert result.classified_events == 1


# -- J. GlobalContextEvent model --


class TestGlobalContextEventModel:
    def test_model_creation(self) -> None:
        event = GlobalContextEvent(
            source_key="financialjuice",
            source_channel="financialjuice",
            telegram_message_id="12345",
            raw_text="Oil rises 3%",
            event_type="oil",
            direction="positive",
            confidence=0.7,
        )
        assert event.source_key == "financialjuice"
        assert event.event_type == "oil"
        assert event.direction == "positive"

    def test_model_defaults(self) -> None:
        event = GlobalContextEvent(
            source_key="test",
            source_channel="test",
        )
        assert event.event_type == "unknown"
        assert event.direction == "unknown"
        assert event.confidence == 0.0
        assert event.telegram_message_id is None


# -- K. ClassificationResult model --


class TestClassificationResultModel:
    def test_result_fields(self) -> None:
        result = ClassificationResult(
            event_type="oil",
            direction="positive",
            confidence=0.7,
        )
        assert result.event_type == "oil"
        assert result.direction == "positive"
        assert result.confidence == 0.7
