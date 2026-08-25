from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from tinvest_trader.app.config import (
    AppConfig,
    BackgroundConfig,
    CbrConfig,
    MarketActivityConfig,
    OperationalReadinessConfig,
    PaperPortfolioConfig,
    QuoteSyncConfig,
    SentimentConfig,
    SignalGenerationConfig,
)
from tinvest_trader.services.operational_readiness import (
    build_operational_report,
    format_operational_report,
)

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)


class FakeRepository:
    def __init__(self) -> None:
        recent = NOW - timedelta(minutes=5)
        self.readiness_data = {
            "source_latest_at": {
                "telegram": recent,
                "quotes": recent,
                "market_activity": recent,
                "cbr": None,
            },
            "activity_decisions": {"enter": 4, "skip": 6},
            "activity_skip_reasons": [
                {"reason": "score_below_minimum", "count": 3},
            ],
        }
        self.paper_summary = {
            "name": "shadow-v1",
            "open_positions": 1,
            "closed_positions": 10,
            "realized_pnl": 100.0,
            "wins": 6,
            "avg_net_return_pct": 0.01,
        }
        self.activity_summaries: dict[str, dict] = {}

    def get_operational_readiness_data(self, lookback_hours: int = 24) -> dict:
        assert lookback_hours == 24
        return self.readiness_data

    def get_paper_portfolio_summary(self, name: str) -> dict | None:
        assert name == "shadow-v1"
        return self.paper_summary

    def get_activity_paper_summary(self, name: str) -> dict | None:
        return self.activity_summaries.get(name)


def _config() -> AppConfig:
    return AppConfig(
        background=BackgroundConfig(enabled=True),
        sentiment=SentimentConfig(enabled=True),
        quote_sync=QuoteSyncConfig(enabled=True),
        market_activity=MarketActivityConfig(enabled=True),
        signal_generation=SignalGenerationConfig(enabled=True),
        paper_portfolio=PaperPortfolioConfig(enabled=True, name="shadow-v1"),
    )


def test_collecting_until_paper_sample_is_large_enough() -> None:
    report = build_operational_report(
        FakeRepository(),
        _config(),
        OperationalReadinessConfig(min_closed_positions=30),
        now=NOW,
    )

    assert report.status == "COLLECTING"
    assert report.blockers == []
    assert "paper sample 10/30" in report.collecting_reasons[0]
    assert report.execution_allowed is False


def test_ready_for_review_when_data_and_paper_thresholds_pass() -> None:
    repository = FakeRepository()
    repository.paper_summary = {
        **repository.paper_summary,
        "closed_positions": 30,
        "wins": 18,
    }

    report = build_operational_report(
        repository,
        _config(),
        OperationalReadinessConfig(),
        now=NOW,
    )

    assert report.status == "READY_FOR_REVIEW"
    assert report.blockers == []
    assert report.collecting_reasons == []


def test_stale_required_source_blocks_readiness() -> None:
    repository = FakeRepository()
    repository.readiness_data["source_latest_at"]["quotes"] = (
        NOW - timedelta(minutes=181)
    )
    repository.paper_summary = {
        **repository.paper_summary,
        "closed_positions": 30,
        "wins": 18,
    }

    report = build_operational_report(
        repository,
        _config(),
        OperationalReadinessConfig(max_data_age_minutes=180),
        now=NOW,
    )

    assert report.status == "NOT_READY"
    assert "quotes data is stale (181m)" in report.blockers


def test_poor_paper_performance_blocks_after_minimum_sample() -> None:
    repository = FakeRepository()
    repository.paper_summary = {
        **repository.paper_summary,
        "closed_positions": 30,
        "wins": 10,
        "avg_net_return_pct": -0.001,
    }

    report = build_operational_report(
        repository,
        _config(),
        OperationalReadinessConfig(),
        now=NOW,
    )

    assert report.status == "NOT_READY"
    assert any("win rate" in blocker for blocker in report.blockers)
    assert any("average net return" in blocker for blocker in report.blockers)


def test_sparse_event_source_is_informational_not_blocking() -> None:
    repository = FakeRepository()
    repository.paper_summary = {
        **repository.paper_summary,
        "closed_positions": 30,
        "wins": 18,
    }
    config = replace(_config(), cbr=CbrConfig(enabled=True))

    report = build_operational_report(
        repository,
        config,
        OperationalReadinessConfig(),
        now=NOW,
    )

    assert report.status == "READY_FOR_REVIEW"
    cbr = next(source for source in report.sources if source.name == "cbr")
    assert cbr.required is False
    assert cbr.latest_at is None


def test_broker_event_ttl_does_not_create_false_stale_blocker() -> None:
    repository = FakeRepository()
    repository.readiness_data["source_latest_at"]["broker_events"] = (
        NOW - timedelta(hours=12)
    )
    repository.paper_summary = {
        **repository.paper_summary,
        "closed_positions": 30,
        "wins": 18,
    }
    config = replace(
        _config(),
        broker_events=replace(_config().broker_events, enabled=True),
    )

    report = build_operational_report(
        repository,
        config,
        OperationalReadinessConfig(max_data_age_minutes=180),
        now=NOW,
    )

    assert report.status == "READY_FOR_REVIEW"
    broker = next(
        source for source in report.sources if source.name == "broker_events"
    )
    assert broker.required is False
    assert broker.fresh is False


def test_formatted_report_is_explicitly_informational() -> None:
    report = build_operational_report(
        FakeRepository(),
        _config(),
        OperationalReadinessConfig(),
        now=NOW,
    )

    text = format_operational_report(report)

    assert "Operational Readiness: COLLECTING" in text
    assert "Real orders: BLOCKED" in text
    assert "Activity 24h: enter 4 | skip 6" in text
    assert "score_below_minimum=3" in text
