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
        self.expired_positions: list[dict] = []
        self.decisions: list[dict] = []
        self.entries_since: dict[str, int] = {}
        self.entry_count_calls: list[tuple[str, datetime]] = []
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

    def expire_stale_activity_paper_positions(self, **kwargs):
        self.expired_positions.append(kwargs)
        return 1 if self.open_positions.get(kwargs["portfolio_name"]) else 0

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

    def count_activity_paper_entries_since(self, name, since):
        self.entry_count_calls.append((name, since))
        return self.entries_since.get(name, 0)

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
        "volume_ratio": 6.0,
        "confirmation_time": None,
        "confirmation_price": None,
        "confirmation_move_pct": None,
        "latest_entry_time": None,
        "candle_interval": "CANDLE_INTERVAL_1_MIN",
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


def test_volume_confirmed_v2_enters_with_strict_quality_gates() -> None:
    repo = FakeRepository()
    repo.candidates = [_candidate(
        spike_type="volume",
        confirmation_time=NOW - timedelta(seconds=15),
        confirmation_price=100.5,
        confirmation_move_pct=0.005,
        volume_ratio=6.0,
        score=85.0,
    )]

    result = _service(repo, volume_confirmed_v2_enabled=True).run_cycle()

    assert result.opened == 1
    position = repo.inserted_positions[0]
    assert position["portfolio_name"] == "activity-volume-confirmed-v2"
    assert position["strategy"] == "volume_confirmed_v2"
    assert position["entry_price"] == 100.5
    assert repo.entry_count_calls == [(
        "activity-volume-confirmed-v2",
        datetime(2026, 8, 23, 21, 0, tzinfo=UTC),
    )]


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"severity": "medium"}, "v2_severity_below_high"),
        ({"score": 79.0}, "v2_score_below_minimum"),
        ({"volume_ratio": None}, "v2_volume_ratio_unavailable"),
        ({"volume_ratio": 4.9}, "v2_volume_ratio_below_minimum"),
    ],
)
def test_volume_confirmed_v2_quality_rejections_are_explainable(
    overrides: dict,
    reason: str,
) -> None:
    repo = FakeRepository()
    repo.candidates = [_candidate(
        spike_type="volume",
        confirmation_time=NOW - timedelta(seconds=15),
        confirmation_price=100.5,
        confirmation_move_pct=0.005,
        **overrides,
    )]

    result = _service(repo, volume_confirmed_v2_enabled=True).run_cycle()

    assert result.opened == 0
    v2_decisions = [
        item for item in repo.decisions
        if item["portfolio_name"] == "activity-volume-confirmed-v2"
    ]
    assert [item["reason"] for item in v2_decisions] == [reason]


def test_volume_confirmed_v2_requires_larger_confirmation_move() -> None:
    repo = FakeRepository()
    repo.candidates = [_candidate(
        spike_type="volume",
        confirmation_time=NOW - timedelta(seconds=15),
        confirmation_price=100.3,
        confirmation_move_pct=0.003,
    )]

    result = _service(repo, volume_confirmed_v2_enabled=True).run_cycle()

    assert result.opened == 0
    v2_decisions = [
        item for item in repo.decisions
        if item["portfolio_name"] == "activity-volume-confirmed-v2"
    ]
    assert [item["reason"] for item in v2_decisions] == [
        "confirmation_move_below_minimum",
    ]


def test_volume_confirmed_v2_enforces_daily_limit() -> None:
    repo = FakeRepository()
    repo.entries_since["activity-volume-confirmed-v2"] = 20
    repo.candidates = [_candidate(
        spike_type="volume",
        confirmation_time=NOW - timedelta(seconds=15),
        confirmation_price=100.5,
        confirmation_move_pct=0.005,
    )]

    result = _service(repo, volume_confirmed_v2_enabled=True).run_cycle()

    assert result.opened == 0
    v2_decisions = [
        item for item in repo.decisions
        if item["portfolio_name"] == "activity-volume-confirmed-v2"
    ]
    assert [item["reason"] for item in v2_decisions] == [
        "v2_daily_entry_limit",
    ]


def test_volume_confirmed_v2_uses_its_longer_cooldown() -> None:
    repo = FakeRepository()
    repo.candidates = [_candidate(
        spike_type="volume",
        confirmation_time=NOW - timedelta(seconds=15),
        confirmation_price=100.5,
        confirmation_move_pct=0.005,
        latest_entry_time=NOW - timedelta(minutes=60),
    )]

    result = _service(
        repo,
        volume_confirmed_v2_enabled=True,
        max_open_positions_per_ticker=2,
    ).run_cycle()

    assert result.opened == 0
    v2_decisions = [
        item for item in repo.decisions
        if item["portfolio_name"] == "activity-volume-confirmed-v2"
    ]
    assert [item["reason"] for item in v2_decisions] == ["ticker_cooldown"]


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


def test_expires_unresolved_positions_without_fabricating_pnl() -> None:
    repo = FakeRepository()
    repo.open_positions["activity-momentum-v1"] = [{
        "ticker": "SBER",
        "notional": 10_000.0,
    }]

    result = _service(
        repo,
        unresolved_position_expiry_minutes=180,
    ).run_cycle()

    assert result.expired == 1
    call = repo.expired_positions[0]
    assert call["portfolio_name"] == "activity-momentum-v1"
    assert call["before"] == NOW - timedelta(minutes=180)
    assert call["expired_at"] == NOW
    assert repo.closed_positions == []


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
    monkeypatch.setenv(
        "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_ENABLED",
        "true",
    )
    monkeypatch.setenv(
        "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MIN_VOLUME_RATIO",
        "6",
    )
    monkeypatch.setenv(
        "TINVEST_ACTIVITY_PAPER_VOLUME_CONFIRMED_V2_MAX_ENTRIES_PER_DAY",
        "12",
    )
    monkeypatch.setenv("TINVEST_BACKGROUND_RUN_ACTIVITY_PAPER_STRATEGY", "false")
    monkeypatch.setenv("TINVEST_ACTIVITY_PAPER_UNRESOLVED_EXPIRY_MINUTES", "240")

    config = load_config()

    assert config.activity_paper.enabled is True
    assert config.activity_paper.horizon == "60m"
    assert config.activity_paper.position_fraction == 0.05
    assert config.activity_paper.max_open_positions == 8
    assert config.activity_paper.allowed_severities == ("high",)
    assert config.activity_paper.volume_confirmed_enabled is True
    assert config.activity_paper.volume_confirmation_min_move_pct == 0.001
    assert config.activity_paper.volume_confirmation_max_delay_minutes == 2
    assert config.activity_paper.volume_confirmed_v2_enabled is True
    assert config.activity_paper.volume_confirmed_v2_min_volume_ratio == 6.0
    assert config.activity_paper.volume_confirmed_v2_max_entries_per_day == 12
    assert config.activity_paper.unresolved_position_expiry_minutes == 240
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


def _strict_candidate(**overrides) -> dict:
    values = {
        "volume_ratio": 12.0,
        "entry_time": NOW - timedelta(minutes=3),
        "confirmation_time": NOW - timedelta(minutes=2),
        "confirmation_price": 100.5,
        "confirmation_move_pct": 0.005,
    }
    values.update(overrides)
    return _candidate(**values)


@pytest.mark.parametrize("strategy,spike_type", [
    ("momentum", "volume_price"), ("volume_confirmed_v2", "volume"),
])
@pytest.mark.parametrize("direction", ["up", "down"])
def test_strict_entries_confirm_both_long_and_short_at_closed_price(
    strategy, spike_type, direction,
):
    repo = FakeRepository()
    sign = 1 if direction == "up" else -1
    repo.candidates = [_strict_candidate(
        spike_type=spike_type, price_change_pct=sign * 0.01,
        confirmation_price=100 + sign * 0.5,
    )]

    _service(repo, strict_entries_enabled=True, volume_confirmed_v2_enabled=True).run_cycle()

    position = next(p for p in repo.inserted_positions if p["strategy"] == strategy)
    assert position["direction"] == direction
    assert position["entry_price"] == 100 + sign * 0.5
    assert position["entry_time"] == NOW - timedelta(minutes=1)
    assert any(d["reason"] == "strict_eligible" for d in repo.decisions)


@pytest.mark.parametrize("strategy,spike_type", [
    ("momentum", "volume_price"), ("volume_confirmed_v2", "volume"),
])
@pytest.mark.parametrize("overrides,reason", [
    ({"severity": "medium"}, "strict_severity_below_high"),
    ({"score": 79}, "strict_score_below_minimum"),
    ({"volume_ratio": 9.9}, "strict_volume_ratio_below_minimum"),
    ({"volume_ratio": None}, "strict_market_data_unavailable"),
    ({"score": float("nan")}, "strict_market_data_unavailable"),
    ({"price_change_pct": float("inf")}, "strict_market_data_unavailable"),
    ({"entry_price": 0}, "strict_market_data_unavailable"),
    ({"price_change_pct": 0}, "strict_flat_spike"),
    ({"price_change_pct": 0.021}, "strict_spike_overextended"),
    ({"spike_type": "price_momentum"}, "strict_spike_type_not_allowed"),
    ({"candle_interval": "CANDLE_INTERVAL_5_MIN"}, "strict_requires_minute_candles"),
    ({"confirmation_price": 99.5}, "strict_confirmation_direction_mismatch"),
    ({"confirmation_price": 100.1}, "strict_confirmation_move_below_minimum"),
    ({"confirmation_price": 101.1}, "strict_confirmation_overextended"),
    ({"confirmation_price": float("nan")}, "strict_confirmation_unavailable"),
    ({"confirmation_price": None}, "strict_confirmation_unavailable"),
    ({"confirmation_price": 0}, "strict_confirmation_unavailable"),
    ({"confirmation_time": NOW - timedelta(minutes=3)}, "strict_confirmation_too_late"),
    ({"confirmation_time": NOW}, "strict_confirmation_too_late"),
    ({"entry_time": NOW - timedelta(minutes=10), "confirmation_time": None},
     "strict_confirmation_missing"),
    ({"entry_time": NOW - timedelta(minutes=6),
      "confirmation_time": NOW - timedelta(minutes=5)}, "candidate_stale"),
])
def test_strict_rejections_are_explainable_and_isolated(strategy, spike_type, overrides, reason):
    repo = FakeRepository()
    values = {"spike_type": spike_type, **overrides}
    repo.candidates = [_strict_candidate(**values)]

    result = _service(
        repo, strict_entries_enabled=True, volume_confirmed_v2_enabled=True,
    ).run_cycle()

    assert result.failed_portfolios == 0
    assert not any(p["strategy"] == strategy for p in repo.inserted_positions)
    name = "activity-momentum-v1" if strategy == "momentum" else "activity-volume-confirmed-v2"
    assert [d["reason"] for d in repo.decisions if d["portfolio_name"] == name] == [reason]


@pytest.mark.parametrize("confirmation_time", [None, NOW - timedelta(seconds=30)])
def test_strict_pending_or_unclosed_candle_is_retried(confirmation_time):
    repo = FakeRepository()
    repo.candidates = [_strict_candidate(
        entry_time=NOW - timedelta(minutes=2), confirmation_time=confirmation_time,
    )]
    service = _service(repo, strict_entries_enabled=True)

    result = service.run_cycle()

    assert result.deferred == 1
    assert not any(d["portfolio_name"] == "activity-momentum-v1" for d in repo.decisions)
    repo.candidates[0]["confirmation_time"] = NOW - timedelta(minutes=1)
    service.run_cycle()
    assert any(p["strategy"] == "momentum" for p in repo.inserted_positions)


def test_strict_entry_must_precede_existing_outcome_horizon():
    repo = FakeRepository()
    repo.candidates = [_strict_candidate()]

    _service(repo, strict_entries_enabled=True, horizon="2m").run_cycle()

    assert "strict_confirmation_after_horizon" in {d["reason"] for d in repo.decisions}


def test_strict_cost_floor_uses_configured_round_trip_costs():
    repo = FakeRepository()
    repo.candidates = [_strict_candidate()]

    _service(repo, strict_entries_enabled=True, commission_rate=0.001).run_cycle()

    assert "strict_confirmation_below_cost_floor" in {d["reason"] for d in repo.decisions}


@pytest.mark.parametrize("price", [100.4, 101.0, 99.6, 99.0])
def test_strict_confirmation_accepts_exact_percentage_boundaries(price):
    repo = FakeRepository()
    repo.candidates = [_strict_candidate(
        confirmation_price=price, price_change_pct=0.01 if price > 100 else -0.01,
    )]

    _service(repo, strict_entries_enabled=True).run_cycle()

    assert any(p["strategy"] == "momentum" for p in repo.inserted_positions)


def test_strict_toggle_off_restores_original_momentum_entry():
    repo = FakeRepository()
    repo.candidates = [_candidate(volume_ratio=1, confirmation_time=None)]

    _service(repo, strict_entries_enabled=False, strict_min_score=100).run_cycle()

    assert [p["strategy"] for p in repo.inserted_positions] == ["momentum", "reversion"]
    assert {d["reason"] for d in repo.decisions} == {"eligible"}


def test_strict_entry_cannot_leak_previous_day_into_daily_budget():
    midnight = datetime(2026, 8, 24, 21, 0, tzinfo=UTC)
    repo = FakeRepository()
    repo.candidates = [_strict_candidate(
        entry_time=midnight - timedelta(minutes=3),
        confirmation_time=midnight - timedelta(minutes=2),
    )]
    service = ActivityPaperStrategyService(
        repo, ActivityPaperConfig(strict_entries_enabled=True),
        logging.getLogger("test_activity_paper"), now_fn=lambda: midnight,
    )

    service.run_cycle()

    assert "strict_entry_day_mismatch" in {d["reason"] for d in repo.decisions}
    assert ("activity-momentum-v1", midnight) in repo.entry_count_calls


@pytest.mark.parametrize("config,reason", [
    ({"volume_confirmed_v2_min_move_pct": 0.006}, "strict_confirmation_move_below_minimum"),
    ({"volume_confirmed_v2_min_volume_ratio": 15}, "v2_volume_ratio_below_minimum"),
    ({"volume_confirmed_v2_min_score": 90}, "v2_score_below_minimum"),
    ({"volume_confirmed_v2_max_entries_per_day": 0}, "strict_daily_entry_limit"),
])
def test_strict_profile_never_relaxes_existing_v2_thresholds(config, reason):
    repo = FakeRepository()
    repo.candidates = [_strict_candidate(spike_type="volume")]

    _service(
        repo, strict_entries_enabled=True, volume_confirmed_v2_enabled=True, **config,
    ).run_cycle()

    assert not any(p["strategy"] == "volume_confirmed_v2" for p in repo.inserted_positions)
    assert reason in {d["reason"] for d in repo.decisions}


@pytest.mark.parametrize("strategy,spike_type,name", [
    ("momentum", "volume_price", "activity-momentum-v1"),
    ("volume_confirmed_v2", "volume", "activity-volume-confirmed-v2"),
])
def test_strict_daily_limit_counts_persisted_and_same_cycle_entries(strategy, spike_type, name):
    repo = FakeRepository()
    repo.entries_since[name] = 4
    repo.candidates = [
        _strict_candidate(spike_id=i, ticker=f"T{i}", spike_type=spike_type)
        for i in range(3)
    ]

    _service(repo, strict_entries_enabled=True, volume_confirmed_v2_enabled=True).run_cycle()

    assert len([p for p in repo.inserted_positions if p["strategy"] == strategy]) == 1
    reasons = [d["reason"] for d in repo.decisions if d["portfolio_name"] == name]
    assert reasons == ["strict_eligible", "strict_daily_entry_limit", "strict_daily_entry_limit"]
    assert (name, datetime(2026, 8, 23, 21, tzinfo=UTC)) in repo.entry_count_calls


def test_strict_cooldown_leaves_reversion_unaffected():
    repo = FakeRepository()
    repo.candidates = [_strict_candidate(latest_entry_time=NOW - timedelta(minutes=150))]

    _service(repo, strict_entries_enabled=True).run_cycle()

    assert [p["strategy"] for p in repo.inserted_positions] == ["reversion"]
    assert "ticker_cooldown" in {d["reason"] for d in repo.decisions}


def test_strict_flag_does_not_change_original_volume_arm():
    repo = FakeRepository()
    repo.candidates = [_candidate(
        spike_type="volume", volume_ratio=2, price_change_pct=0,
        confirmation_time=NOW - timedelta(seconds=15),
        confirmation_price=99.8, confirmation_move_pct=-0.002,
    )]

    _service(repo, strict_entries_enabled=True, volume_confirmed_enabled=True).run_cycle()

    assert [p["strategy"] for p in repo.inserted_positions] == ["volume_confirmed"]
    assert repo.inserted_positions[0]["direction"] == "down"


@pytest.mark.parametrize("raw_return,gross,net", [(-0.01, 100, 80), (0.01, -100, -120)])
def test_short_pnl_sign_and_round_trip_costs(raw_return, gross, net):
    repo = FakeRepository()
    repo.resolved_positions["activity-momentum-v1"] = [{
        "id": 1, "direction": "down", "notional": 10_000,
        "raw_return_pct": raw_return, "exit_price": 100 * (1 + raw_return),
        "exit_time": NOW,
    }]

    _service(repo, strict_entries_enabled=True).run_cycle()

    assert repo.closed_positions[0]["gross_pnl"] == pytest.approx(gross)
    assert repo.closed_positions[0]["costs"] == 20
    assert repo.closed_positions[0]["net_pnl"] == pytest.approx(net)


@pytest.mark.parametrize("overrides", [
    {"strict_min_volume_ratio": -1}, {"strict_min_score": float("nan")},
    {"strict_min_confirmation_move_pct": 0}, {"strict_max_confirmation_move_pct": 0.001},
    {"strict_min_cost_multiple": 0.5}, {"commission_rate": -0.01},
    {"horizon": "eod"}, {"horizon": "0m"}, {"horizon": "invalid"},
])
def test_invalid_strict_config_fails_before_portfolio_work(overrides):
    repo = FakeRepository()
    with pytest.raises(ValueError):
        _service(repo, strict_entries_enabled=True, **overrides)
    assert repo.portfolios == {}


def test_strict_config_parses_environment(monkeypatch):
    values = {
        "STRICT_ENTRIES_ENABLED": "true", "STRICT_MIN_SCORE": "85",
        "STRICT_MIN_VOLUME_RATIO": "12", "STRICT_MAX_SPIKE_MOVE_PCT": "0.025",
        "STRICT_MIN_CONFIRMATION_MOVE_PCT": "0.003",
        "STRICT_MAX_CONFIRMATION_MOVE_PCT": "0.008", "STRICT_MIN_COST_MULTIPLE": "3",
        "STRICT_CONFIRMATION_MAX_DELAY_MINUTES": "1", "STRICT_MAX_ENTRY_AGE_MINUTES": "1",
        "STRICT_COOLDOWN_MINUTES": "240", "STRICT_MAX_ENTRIES_PER_DAY": "3",
    }
    for key, value in values.items():
        monkeypatch.setenv(f"TINVEST_ACTIVITY_PAPER_{key}", value)

    config = load_config().activity_paper

    for key, value in values.items():
        expected = True if value == "true" else float(value)
        assert getattr(config, key.lower()) == expected
