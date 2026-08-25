from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest

from tinvest_trader.app.config import ActivityPaperConfig, load_config
from tinvest_trader.services.activity_paper_strategy_service import (
    ActivityPaperStrategyService,
    format_activity_paper_summary,
)

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


class FakeRepository:
    def __init__(self) -> None:
        self.portfolios: dict[str, dict] = {}
        self.candidates: list[dict] = []
        self.open_positions: dict[str, list[dict]] = {}
        self.resolved_positions: dict[str, list[dict]] = {}
        self.inserted_positions: list[dict] = []
        self.closed_positions: list[dict] = []
        self.decisions: list[dict] = []
        self.fail_portfolio: str | None = None

    def ensure_activity_paper_portfolio(self, **kwargs):
        if kwargs["name"] == self.fail_portfolio:
            raise RuntimeError("db")
        return self.portfolios.setdefault(kwargs["name"], {
            "name": kwargs["name"],
            "strategy": kwargs["strategy"],
            "horizon": kwargs["horizon"],
            "initial_cash": kwargs["initial_cash"],
            "currency": "RUB",
            "started_at": kwargs["now"],
        })

    def list_resolved_activity_paper_positions(self, name):
        return self.resolved_positions.get(name, [])

    def close_activity_paper_position(self, **kwargs):
        self.closed_positions.append(kwargs)
        return True

    def get_activity_paper_summary(self, name):
        portfolio = self.portfolios[name]
        open_items = self.open_positions.get(name, [])
        return {
            **portfolio,
            "open_positions": len(open_items),
            "closed_positions": 0,
            "open_notional": sum(item["notional"] for item in open_items),
            "realized_pnl": 0.0,
            "wins": 0,
            "avg_net_return_pct": None,
        }

    def list_open_activity_paper_positions(self, name):
        return self.open_positions.get(name, [])

    def list_activity_paper_entry_candidates(self, _name):
        return self.candidates

    def insert_activity_paper_position(self, position):
        self.inserted_positions.append(position)
        return len(self.inserted_positions)

    def insert_activity_paper_decision(self, **decision):
        self.decisions.append(decision)
        return True


def _candidate(**overrides) -> dict:
    candidate = {
        "spike_id": 7,
        "ticker": "SBER",
        "figi": "BBG004730N88",
        "entry_time": NOW - timedelta(minutes=1),
        "spike_type": "volume_price",
        "severity": "high",
        "score": 80.0,
        "entry_price": 100.0,
        "price_change_pct": 0.01,
        "confirmation_time": None,
        "confirmation_price": None,
        "confirmation_move_pct": None,
        "latest_entry_time": None,
    }
    candidate.update(overrides)
    return candidate


def _service(repo: FakeRepository, **overrides) -> ActivityPaperStrategyService:
    values = {
        "enabled": True,
        "initial_cash": 100_000.0,
        "position_fraction": 0.10,
        "max_open_positions": 5,
        "max_open_positions_per_ticker": 1,
        "commission_rate": 0.0005,
        "slippage_rate": 0.0005,
    }
    values.update(overrides)
    return ActivityPaperStrategyService(
        repository=repo,
        config=ActivityPaperConfig(**values),
        logger=logging.getLogger("test_activity_paper"),
        now_fn=lambda: NOW,
    )


def test_opens_equal_notional_with_opposite_ab_directions() -> None:
    repo = FakeRepository()
    repo.candidates = [_candidate()]

    result = _service(repo).run_cycle()

    assert result.opened == 2
    momentum, reversion = repo.inserted_positions
    assert momentum["strategy"] == "momentum"
    assert momentum["direction"] == "up"
    assert reversion["strategy"] == "reversion"
    assert reversion["direction"] == "down"
    assert momentum["notional"] == reversion["notional"] == 10_000.0
    assert {item["decision"] for item in repo.decisions} == {"enter"}


def test_volume_confirmed_arm_enters_at_following_candle_price() -> None:
    repo = FakeRepository()
    repo.candidates = [_candidate(
        spike_type="volume",
        confirmation_time=NOW - timedelta(seconds=15),
        confirmation_price=100.2,
        confirmation_move_pct=0.002,
    )]

    result = _service(repo, volume_confirmed_enabled=True).run_cycle()

    assert result.opened == 1
    assert result.skipped == 2
    position = repo.inserted_positions[0]
    assert position["portfolio_name"] == "activity-volume-confirmed-v1"
    assert position["strategy"] == "volume_confirmed"
    assert position["direction"] == "up"
    assert position["entry_price"] == 100.2
    assert position["entry_time"] == NOW - timedelta(seconds=15)


def test_volume_confirmation_pending_is_retried_without_terminal_decision() -> None:
    repo = FakeRepository()
    repo.candidates = [_candidate(spike_type="volume")]

    result = _service(repo, volume_confirmed_enabled=True).run_cycle()

    assert result.deferred == 1
    volume_decisions = [
        item for item in repo.decisions
        if item["portfolio_name"] == "activity-volume-confirmed-v1"
    ]
    assert volume_decisions == []


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        (
            {
                "confirmation_time": NOW - timedelta(seconds=15),
                "confirmation_price": 100.01,
                "confirmation_move_pct": 0.0001,
            },
            "confirmation_move_below_minimum",
        ),
        (
            {
                "entry_time": NOW - timedelta(minutes=5),
                "confirmation_time": NOW - timedelta(minutes=1),
                "confirmation_price": 100.2,
                "confirmation_move_pct": 0.002,
            },
            "confirmation_too_late",
        ),
    ],
)
def test_volume_confirmation_rejections_are_explainable(
    overrides: dict,
    reason: str,
) -> None:
    repo = FakeRepository()
    repo.candidates = [_candidate(spike_type="volume", **overrides)]

    result = _service(repo, volume_confirmed_enabled=True).run_cycle()

    assert result.opened == 0
    volume_decisions = [
        item for item in repo.decisions
        if item["portfolio_name"] == "activity-volume-confirmed-v1"
    ]
    assert [item["reason"] for item in volume_decisions] == [reason]


def test_closes_positions_with_round_trip_costs() -> None:
    repo = FakeRepository()
    for name in ("activity-momentum-v1", "activity-reversion-v1"):
        repo.resolved_positions[name] = [{
            "id": 1,
            "spike_id": 7,
            "ticker": "SBER",
            "direction": "up",
            "notional": 10_000.0,
            "exit_price": 101.0,
            "raw_return_pct": 0.01,
            "exit_time": NOW,
        }]

    result = _service(repo).run_cycle()

    assert result.closed == 2
    close = repo.closed_positions[0]
    assert close["gross_pnl"] == pytest.approx(100.0)
    assert close["costs"] == pytest.approx(20.0)
    assert close["net_pnl"] == pytest.approx(80.0)
    assert close["net_return_pct"] == pytest.approx(0.008)


@pytest.mark.parametrize(
    ("candidate", "reason"),
    [
        (_candidate(score=20.0), "score_below_minimum"),
        (_candidate(severity="low"), "severity_not_allowed"),
        (_candidate(spike_type="volume"), "spike_type_not_allowed"),
        (_candidate(price_change_pct=0.0), "flat_direction"),
        (
            _candidate(entry_time=NOW - timedelta(minutes=20)),
            "candidate_stale",
        ),
    ],
)
def test_quality_filters_record_skip_reason(candidate: dict, reason: str) -> None:
    repo = FakeRepository()
    repo.candidates = [candidate]

    result = _service(repo).run_cycle()

    assert result.skipped == 2
    assert repo.inserted_positions == []
    assert {item["reason"] for item in repo.decisions} == {reason}


def test_ticker_capacity_and_cooldown_are_recorded() -> None:
    repo = FakeRepository()
    repo.candidates = [_candidate()]
    for name in ("activity-momentum-v1", "activity-reversion-v1"):
        repo.open_positions[name] = [{
            "ticker": "SBER",
            "notional": 10_000.0,
        }]

    result = _service(repo).run_cycle()

    assert result.skipped == 2
    assert {item["reason"] for item in repo.decisions} == {"ticker_capacity"}

    repo = FakeRepository()
    repo.candidates = [_candidate(latest_entry_time=NOW - timedelta(minutes=5))]
    result = _service(repo, max_open_positions_per_ticker=2).run_cycle()

    assert result.skipped == 2
    assert {item["reason"] for item in repo.decisions} == {"ticker_cooldown"}


def test_cooldown_applies_to_position_opened_in_same_cycle() -> None:
    repo = FakeRepository()
    repo.candidates = [
        _candidate(spike_id=7, entry_time=NOW - timedelta(minutes=2)),
        _candidate(spike_id=8, entry_time=NOW - timedelta(minutes=1)),
    ]

    result = _service(repo, max_open_positions_per_ticker=2).run_cycle()

    assert result.opened == 2
    assert result.skipped == 2
    skipped = [item for item in repo.decisions if item["decision"] == "skip"]
    assert {item["reason"] for item in skipped} == {"ticker_cooldown"}


def test_one_failed_arm_does_not_stop_the_other() -> None:
    repo = FakeRepository()
    repo.fail_portfolio = "activity-momentum-v1"
    repo.candidates = [_candidate()]

    result = _service(repo).run_cycle()

    assert result.failed_portfolios == 1
    assert result.opened == 1
    assert repo.inserted_positions[0]["strategy"] == "reversion"


def test_activity_paper_config_defaults_disabled(monkeypatch) -> None:
    monkeypatch.delenv("TINVEST_ACTIVITY_PAPER_ENABLED", raising=False)

    config = load_config().activity_paper

    assert config == ActivityPaperConfig()
    assert config.enabled is False
    assert config.horizon == "15m"


def test_activity_paper_config_parses_environment(monkeypatch) -> None:
    monkeypatch.setenv("TINVEST_ACTIVITY_PAPER_ENABLED", "true")
    monkeypatch.setenv("TINVEST_ACTIVITY_PAPER_HORIZON", "60m")
    monkeypatch.setenv("TINVEST_ACTIVITY_PAPER_POSITION_FRACTION", "0.05")
    monkeypatch.setenv("TINVEST_ACTIVITY_PAPER_MAX_OPEN_POSITIONS", "8")
    monkeypatch.setenv("TINVEST_ACTIVITY_PAPER_ALLOWED_SEVERITIES", "high")
    monkeypatch.setenv("TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_ENABLED", "true")
    monkeypatch.setenv(
        "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMATION_MIN_MOVE_PCT",
        "0.001",
    )
    monkeypatch.setenv(
        "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMATION_MAX_DELAY_MINUTES",
        "2",
    )
    monkeypatch.setenv("TINVEST_BACKGROUND_RUN_ACTIVITY_PAPER_STRATEGY", "false")

    config = load_config()

    assert config.activity_paper.enabled is True
    assert config.activity_paper.horizon == "60m"
    assert config.activity_paper.position_fraction == 0.05
    assert config.activity_paper.max_open_positions == 8
    assert config.activity_paper.allowed_severities == ("high",)
    assert config.activity_paper.volume_confirmed_enabled is True
    assert config.activity_paper.volume_confirmation_min_move_pct == 0.001
    assert config.activity_paper.volume_confirmation_max_delay_minutes == 2
    assert config.background.run_activity_paper_strategy is False


def test_format_activity_paper_summary_compares_arms() -> None:
    rows = [
        {
            "name": "activity-momentum-v1",
            "strategy": "momentum",
            "horizon": "15m",
            "open_positions": 1,
            "closed_positions": 10,
            "wins": 6,
            "realized_pnl": 125.0,
        },
    ]

    rendered = format_activity_paper_summary(rows)

    assert "activity-momentum-v1" in rendered
    assert "60.0%" in rendered
    assert "125.00" in rendered
