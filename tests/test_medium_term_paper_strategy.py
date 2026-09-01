from __future__ import annotations

import logging
from datetime import date, timedelta

import pytest

from tinvest_trader.app.config import MediumTermPaperConfig, load_config
from tinvest_trader.services.medium_term_paper_strategy_service import (
    MediumTermPaperStrategyService,
    calculate_atr,
    evaluate_medium_term_signal,
    format_medium_term_paper_summary,
)


def _bars(count: int = 56, *, signal_volume: int = 200) -> list[dict]:
    start = date(2026, 1, 1)
    result = []
    for index in range(count):
        close = 100 + index * 0.5
        result.append({
            "trade_date": start + timedelta(days=index),
            "open": close - 0.1,
            "high": close + 0.2,
            "low": close - 0.5,
            "close": close,
            "volume": 100,
        })
    result[-2]["volume"] = signal_volume
    return result


class FakeRepository:
    def __init__(self, bars: list[dict] | None = None) -> None:
        self.bars = bars or []
        self.portfolios: dict[str, dict] = {}
        self.positions: dict[str, list[dict]] = {}
        self.decisions: list[dict] = []
        self.decision_keys: set[tuple] = set()
        self.closed: list[dict] = []
        self.updates: list[dict] = []

    def ensure_medium_term_paper_portfolio(
        self, *, name: str, strategy: str, initial_cash: float,
    ) -> dict:
        return self.portfolios.setdefault(name, {
            "name": name,
            "strategy": strategy,
            "initial_cash": initial_cash,
            "currency": "RUB",
            "started_at": None,
        })

    def list_moex_daily_history(self, ticker: str, limit: int) -> list[dict]:
        return self.bars[-limit:]

    def list_open_medium_term_positions(self, portfolio_name: str) -> list[dict]:
        return list(self.positions.get(portfolio_name, []))

    def get_medium_term_paper_summary(self, portfolio_name: str) -> dict:
        open_items = self.positions.get(portfolio_name, [])
        return {
            "name": portfolio_name,
            "strategy": self.portfolios[portfolio_name]["strategy"],
            "initial_cash": 1_000_000.0,
            "realized_pnl": 0.0,
            "open_notional": sum(item["notional"] for item in open_items),
            "open_positions": len(open_items),
            "closed_positions": 0,
            "wins": 0,
            "avg_net_return_pct": None,
        }

    def insert_medium_term_decision(self, decision: dict) -> bool:
        key = (
            decision["portfolio_name"], decision["ticker"],
            decision["signal_date"],
        )
        if key in self.decision_keys:
            return False
        self.decision_keys.add(key)
        self.decisions.append(decision)
        return True

    def insert_medium_term_position(self, position: dict) -> int:
        position_id = sum(len(items) for items in self.positions.values()) + 1
        item = {
            "id": position_id,
            **position,
            "current_stop": position["initial_stop"],
            "highest_close": position["entry_price"],
            "last_evaluated_date": position["signal_date"],
            "held_sessions": 0,
        }
        self.positions.setdefault(position["portfolio_name"], []).append(item)
        return position_id

    def update_medium_term_position_state(self, **values) -> None:
        self.updates.append(values)

    def close_medium_term_position(self, **values) -> bool:
        self.closed.append(values)
        return True


def _service(repo: FakeRepository, **overrides) -> MediumTermPaperStrategyService:
    config = MediumTermPaperConfig(**overrides)
    return MediumTermPaperStrategyService(
        repository=repo,
        config=config,
        logger=logging.getLogger("test"),
        tracked_tickers=("SBER",),
    )


def test_eligible_signal_opens_all_three_virtual_arms() -> None:
    repo = FakeRepository(_bars())

    result = _service(repo).run_cycle()

    assert result.opened == 3
    assert {item["strategy"] for items in repo.positions.values() for item in items} == {
        "staircase", "atr", "hybrid",
    }
    assert {item["decision"] for item in repo.decisions} == {"enter"}
    staircase = repo.positions["medium-term-staircase-v1"][0]
    assert staircase["notional"] == pytest.approx(200_000)
    assert staircase["entry_date"] > staircase["signal_date"]


def test_entry_day_volume_is_not_used_to_create_signal() -> None:
    bars = _bars(signal_volume=100)
    bars[-1]["volume"] = 10_000
    repo = FakeRepository(bars)

    result = _service(repo).run_cycle()

    assert result.opened == 0
    assert {item["reason"] for item in repo.decisions} == {"volume_not_confirmed"}


def test_staircase_and_trailing_stops_never_move_down() -> None:
    service = _service(FakeRepository())

    staircase, staircase_reason = service._next_stop(
        strategy="staircase",
        entry_price=100,
        current_stop=98,
        highest_close=104,
        atr=1,
        latest_close=104,
    )
    atr, atr_reason = service._next_stop(
        strategy="atr",
        entry_price=100,
        current_stop=98,
        highest_close=104,
        atr=1,
        latest_close=103,
    )
    hybrid, hybrid_reason = service._next_stop(
        strategy="hybrid",
        entry_price=100,
        current_stop=98,
        highest_close=104,
        atr=1,
        latest_close=103,
    )

    assert staircase == pytest.approx(100)
    assert staircase_reason == "staircase"
    assert atr == pytest.approx(102)
    assert atr_reason == "atr_trail"
    assert hybrid == pytest.approx(101.5)
    assert hybrid_reason == "hybrid_breakeven_atr"


def test_gap_below_stop_exits_at_open_not_stop_price() -> None:
    bars = _bars()
    bar = bars[-1]
    bar["open"] = 95
    bar["low"] = 94
    repo = FakeRepository(bars)
    position = {
        "id": 7,
        "portfolio_name": "medium-term-staircase-v1",
        "strategy": "staircase",
        "ticker": "SBER",
        "signal_date": bars[-3]["trade_date"],
        "entry_date": bars[-2]["trade_date"],
        "entry_price": 100.0,
        "notional": 100_000.0,
        "atr_at_entry": 1.0,
        "initial_stop": 98.0,
        "current_stop": 98.0,
        "highest_close": 100.0,
        "last_evaluated_date": bars[-2]["trade_date"],
        "held_sessions": 0,
    }
    repo.positions["medium-term-staircase-v1"] = [position]

    closed, _ = _service(repo)._advance_open_positions(
        "medium-term-staircase-v1",
    )

    assert closed == 1
    assert repo.closed[0]["exit_price"] == 95
    assert repo.closed[0]["exit_reason"] == "stop_gap"


def test_atr_uses_true_range_with_previous_close() -> None:
    bars = [
        {"high": 101, "low": 99, "close": 100},
        {"high": 105, "low": 102, "close": 104},
        {"high": 106, "low": 103, "close": 105},
    ]

    assert calculate_atr(bars, 2) == pytest.approx(4)


def test_signal_requires_sufficient_history() -> None:
    signal = evaluate_medium_term_signal(_bars(10), MediumTermPaperConfig())

    assert signal.eligible is False
    assert signal.reason == "insufficient_history"


def test_medium_term_config_defaults_off_and_parses_env(monkeypatch) -> None:
    monkeypatch.setenv("TINVEST_MEDIUM_TERM_PAPER_ENABLED", "true")
    monkeypatch.setenv("TINVEST_MEDIUM_TERM_PAPER_RISK_PER_POSITION", "0.003")
    monkeypatch.setenv("TINVEST_MEDIUM_TERM_PAPER_TRACKED_TICKERS", "SBER,LKOH")
    monkeypatch.setenv("TINVEST_MEDIUM_TERM_PAPER_MAX_HOLDING_SESSIONS", "42")

    config = load_config().medium_term_paper

    assert MediumTermPaperConfig().enabled is False
    assert config.enabled is True
    assert config.risk_per_position == 0.003
    assert config.tracked_tickers_override == ("SBER", "LKOH")
    assert config.max_holding_sessions == 42


def test_format_medium_term_summary() -> None:
    output = format_medium_term_paper_summary([{
        "name": "medium-term-hybrid-v1",
        "strategy": "hybrid",
        "open_positions": 1,
        "closed_positions": 2,
        "wins": 1,
        "realized_pnl": 500.0,
        "avg_net_return_pct": 0.01,
    }])

    assert "virtual only" in output
    assert "medium-term-hybrid-v1" in output
    assert "win_rate=50.0%" in output
