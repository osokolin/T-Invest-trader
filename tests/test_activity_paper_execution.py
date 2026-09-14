"""Offline lifecycle tests for causal fills, isolated from the broker engine."""

import logging
import math
from dataclasses import replace
from datetime import timedelta

import pytest

from tests.test_activity_paper_strategy import NOW, FakeRepository, _candidate
from tinvest_trader.app.config import ActivityPaperConfig, load_config
from tinvest_trader.services.activity_paper_execution import process_causal_execution
from tinvest_trader.services.activity_paper_strategy_service import ActivityPaperStrategyService

NAME = "activity-reversion-v2"


class ExecutionRepository(FakeRepository):
    def __init__(self):
        super().__init__()
        self.now = NOW
        self.requests = {}
        self.quotes = []

    def list_activity_paper_entry_candidates(self, name):
        if name != NAME:
            return []
        seen = {d["spike_id"] for d in self.decisions if d["portfolio_name"] == name}
        return [c for c in self.candidates if c["spike_id"] not in seen]

    def reserve_activity_paper_entry(self, position, **options):
        key = (position["portfolio_name"], position["spike_id"])
        if key in self.requests:
            return False
        self.requests[key] = {
            **position, "signal_price": position["entry_price"],
            "status": "pending", "decision_at": self.now,
            "entry_deadline": self.now+timedelta(seconds=options["wait_seconds"]),
            "quote_wait_seconds": options["wait_seconds"],
            "max_quote_age_seconds": options["max_quote_age"],
            "cost_rate": options["cost_rate"],
        }
        self.decisions.append({"portfolio_name": key[0], "spike_id": key[1],
                               "decision": "pending", "reason": "awaiting_quote"})
        return True

    def list_activity_paper_execution(self, name):
        return [dict(r) for r in self.requests.values()
                if r["portfolio_name"] == name and r["status"] in {"pending", "open"}]

    def first_activity_execution_quote(self, figi, *, after, until, as_of, max_age_seconds):
        candidates = [q for q in self.quotes if q["figi"] == figi
                      and math.isfinite(q["price"]) and q["price"] > 0
                      and after < q["source_time"] <= q["fetched_at"] <= min(until, as_of)
                      and (q["fetched_at"]-q["source_time"]).total_seconds() <= max_age_seconds]
        return min(candidates, key=lambda q: (q["fetched_at"], q["id"])) if candidates else None

    def fill_activity_paper_entry(self, request, quote, **times):
        r = self.requests[(request["portfolio_name"], request["spike_id"])]
        if r["status"] != "pending":
            return False
        r.update(status="open", entry_price=quote["price"], entry_time=quote["fetched_at"],
                 position_id=request["spike_id"], **times)
        self.inserted_positions.append(dict(r))
        self.open_positions.setdefault(r["portfolio_name"], []).append(dict(r))
        return True

    def finish_activity_paper_execution(self, request, **result):
        r = self.requests[(request["portfolio_name"], request["spike_id"])]
        if r["status"] not in {"pending", "open"}:
            return False
        r.update(result)
        self.open_positions[r["portfolio_name"]] = [
            p for p in self.open_positions.get(r["portfolio_name"], [])
            if p["spike_id"] != r["spike_id"]
        ]
        self.closed_positions.append(result)
        return True


def service(repo, **overrides):
    config = replace(ActivityPaperConfig(enabled=True, reversion_v2_enabled=True), **overrides)
    return ActivityPaperStrategyService(repo, config, logging.getLogger("causal-test"),
                                        now_fn=lambda: repo.now)


def quote(seconds=20, price=95, **overrides):
    return {"id": 1, "figi": "BBG004730N88", "price": price,
            "source_time": NOW+timedelta(seconds=seconds-1),
            "fetched_at": NOW+timedelta(seconds=seconds), **overrides}


def queued():
    repo = ExecutionRepository()
    repo.candidates = [_candidate(entry_time=NOW-timedelta(minutes=7))]
    result = service(repo).run_cycle()
    assert result.failed_portfolios == 0
    assert result.deferred == 1
    assert result.opened == 0
    assert not repo.inserted_positions
    return repo


def test_queue_does_not_fill_at_old_signal_price_and_resumes_after_restart():
    repo = queued()
    repo.quotes = [quote(-1, 100), quote(20, 95)]
    repo.now = NOW+timedelta(minutes=5)
    # Recreating the service does not lose a durable request or its deadline.
    result = service(repo).run_cycle()
    assert result.opened == 1
    position = repo.inserted_positions[0]
    assert position["entry_price"] == 95
    assert position["entry_time"] == NOW+timedelta(seconds=20)
    assert position["direction"] == "down"
    assert position["notional"] == 20000
    assert position["exit_due_at"] == NOW+timedelta(minutes=15, seconds=20)
    assert service(repo).run_cycle().opened == 0
    assert len(repo.requests) == 1


@pytest.mark.parametrize("bad", [
    quote(-1), quote(0), quote(121), quote(20, 0), quote(20, float("nan")),
    quote(20, float("inf")), quote(20, -1),
    quote(20, source_time=NOW-timedelta(seconds=1)),
    quote(20, source_time=NOW+timedelta(seconds=21)),
    quote(80, source_time=NOW+timedelta(seconds=1)),
    quote(20, figi="WRONG"),
])
def test_stale_future_invalid_or_late_quotes_cannot_fill(bad):
    repo = queued()
    repo.quotes = [bad]
    repo.now = NOW+timedelta(seconds=120)
    result = service(repo).run_cycle()
    assert result.cancelled == 1
    assert not repo.inserted_positions
    assert repo.closed_positions[-1]["reason"] == "entry_quote_timeout"
    assert "gross_return" not in repo.closed_positions[-1]


def test_future_received_quote_waits_until_available():
    repo = queued()
    repo.quotes = [quote(60)]
    repo.now = NOW+timedelta(seconds=30)
    assert service(repo).run_cycle().opened == 0
    repo.now += timedelta(seconds=30)
    assert service(repo).run_cycle().opened == 1


def test_first_eligible_received_quote_not_best_price_is_used():
    repo = queued()
    repo.quotes = [quote(100, 200, id=3), quote(10, 99, id=2), quote(20, 150)]
    repo.now = NOW+timedelta(seconds=110)
    assert service(repo).run_cycle().opened == 1
    assert repo.inserted_positions[0]["entry_price"] == 99


def test_exit_is_fifteen_minutes_after_received_fill_not_spike_outcome():
    repo = queued()
    repo.quotes = [quote(20, 95)]
    repo.now = NOW+timedelta(seconds=30)
    service(repo).run_cycle()
    repo.resolved_positions[NAME] = [{"exit_price": 500}]
    repo.quotes += [quote(14*60, 50), quote(15*60+30, 96, id=2)]
    repo.now = NOW+timedelta(minutes=15)
    assert service(repo).run_cycle().closed == 0
    repo.now += timedelta(minutes=1)
    assert service(repo).run_cycle().closed == 1
    result = repo.closed_positions[-1]
    assert result["quote"]["price"] == 96
    assert result["gross_return"] == pytest.approx(1-96/95)
    assert result["costs"] == 40
    assert service(repo).run_cycle().closed == 0


def test_costs_are_snapshotted_and_missing_exit_has_no_fake_pnl():
    repo = queued()
    repo.quotes = [quote(20)]
    repo.now = NOW+timedelta(seconds=30)
    service(repo).run_cycle()
    repo.now = NOW+timedelta(minutes=18)
    result = service(repo, commission_rate=0.1).run_cycle()
    assert result.expired == 1
    terminal = repo.closed_positions[-1]
    assert terminal["status"] == "expired"
    assert "gross_return" not in terminal and "quote" not in terminal
    assert not repo.open_positions[NAME]


def test_pending_reserves_capacity_across_cycles():
    repo = queued()
    repo.candidates += [_candidate(spike_id=8, ticker="GAZP", figi="GAZP")]
    result = service(repo, max_open_positions=1).run_cycle()
    assert result.deferred == 0
    assert len(repo.requests) == 1
    assert repo.decisions[-1]["reason"] == "portfolio_capacity"


def test_pending_reserves_cash_and_per_ticker_capacity():
    repo = queued()
    repo.candidates += [_candidate(spike_id=8)]
    service(repo).run_cycle()
    assert repo.decisions[-1]["reason"] == "ticker_capacity"
    repo.candidates += [_candidate(spike_id=9, ticker="GAZP", figi="GAZP")]
    repo.portfolios[NAME]["initial_cash"] = 20000
    service(repo).run_cycle()
    assert repo.decisions[-1]["reason"] == "insufficient_virtual_cash"


def test_signal_gates_and_disabled_baseline_remain_unchanged():
    repo = FakeRepository()
    repo.candidates = [_candidate()]
    result = ActivityPaperStrategyService(repo, ActivityPaperConfig(), logging.getLogger("test"),
                                         now_fn=lambda: NOW).run_cycle()
    assert result.opened == 2
    assert {p["entry_price"] for p in repo.inserted_positions} == {100}
    causal = ExecutionRepository()
    causal.candidates = [_candidate(score=1)]
    assert service(causal).run_cycle().deferred == 0
    assert not causal.requests


def test_disabled_flag_does_not_process_existing_requests():
    repo = queued()
    repo.quotes = [quote(20)]
    repo.now += timedelta(seconds=60)
    service(repo, reversion_v2_enabled=False).run_cycle()
    assert not repo.inserted_positions


@pytest.mark.parametrize("override", [
    {"horizon": "eod"}, {"horizon": "0m"}, {"reversion_v2_quote_wait_seconds": 0},
    {"reversion_v2_max_quote_age_seconds": -1}, {"reversion_v2_portfolio_name": ""},
    {"reversion_v2_portfolio_name": "activity-reversion-v1"},
    {"commission_rate": float("nan")}, {"slippage_rate": -0.1},
])
def test_invalid_enabled_configuration_is_rejected(override):
    with pytest.raises(ValueError):
        service(ExecutionRepository(), **override)


def test_reversion_v2_environment_and_defaults(monkeypatch):
    assert not ActivityPaperConfig().reversion_v2_enabled
    monkeypatch.setenv("TINVEST_ACTIVITY_PAPER_REVERSION_V2_ENABLED", "true")
    monkeypatch.setenv("TINVEST_ACTIVITY_PAPER_REVERSION_V2_NAME", "causal-test")
    monkeypatch.setenv("TINVEST_ACTIVITY_PAPER_REVERSION_V2_QUOTE_WAIT_SECONDS", "90")
    monkeypatch.setenv("TINVEST_ACTIVITY_PAPER_REVERSION_V2_MAX_QUOTE_AGE_SECONDS", "10")
    cfg = load_config().activity_paper
    assert cfg.reversion_v2_enabled
    assert cfg.reversion_v2_portfolio_name == "causal-test"
    assert cfg.reversion_v2_quote_wait_seconds == 90
    assert cfg.reversion_v2_max_quote_age_seconds == 10


def test_invalid_repository_contract_cannot_create_pnl(monkeypatch):
    repo = queued()
    repo.now += timedelta(seconds=30)
    monkeypatch.setattr(repo, "first_activity_execution_quote", lambda *a, **k: quote(-10))
    with pytest.raises(ValueError, match="invalid causal"):
        process_causal_execution(repo, NAME, repo.now)
    assert not repo.inserted_positions


def test_causal_failure_does_not_stop_legacy_portfolios():
    repo = FakeRepository()  # Deliberately lacks the new execution storage methods.
    repo.candidates = [_candidate()]
    result = ActivityPaperStrategyService(
        repo, ActivityPaperConfig(reversion_v2_enabled=True), logging.getLogger("test"),
        now_fn=lambda: NOW,
    ).run_cycle()
    assert result.failed_portfolios == 1
    assert result.opened == 2
    assert {p["portfolio_name"] for p in repo.inserted_positions} == {
        "activity-momentum-v1", "activity-reversion-v1",
    }


def test_long_fill_uses_request_cost_snapshot_after_config_change():
    repo = ExecutionRepository()
    repo.candidates = [_candidate(price_change_pct=-0.01)]
    service(repo).run_cycle()
    repo.quotes = [quote(120, 100)]  # The deadline is inclusive.
    repo.now = NOW+timedelta(seconds=120)
    assert service(repo).run_cycle().opened == 1
    assert repo.inserted_positions[0]["direction"] == "up"
    repo.quotes += [quote(17*60+10, 101, id=2)]
    repo.now = NOW+timedelta(minutes=18)
    assert service(repo, commission_rate=0.1).run_cycle().closed == 1
    assert repo.closed_positions[-1]["gross_return"] == pytest.approx(0.01)
    assert repo.closed_positions[-1]["costs"] == 40


@pytest.mark.parametrize("candidate_fields", [
    {"entry_time": NOW+timedelta(minutes=1)},
    {"entry_time": NOW-timedelta(seconds=30)},
    {"entry_price": float("nan")},
    {"candle_interval": "CANDLE_INTERVAL_5_MIN"},
])
def test_causal_request_requires_valid_closed_signal_data(candidate_fields):
    repo = ExecutionRepository()
    repo.candidates = [_candidate(**candidate_fields)]
    result = service(repo).run_cycle()
    assert result.skipped == 1 and not repo.requests
    assert repo.decisions[-1]["reason"].startswith("causal_")
