from __future__ import annotations

import logging
from datetime import date, timedelta

import pytest

from tinvest_trader.app.config import MediumTermPaperConfig
from tinvest_trader.services.medium_term_replay_service import (
    BENCHMARK_ARM,
    MediumTermReplayService,
    adjust_bars_for_splits,
    format_medium_term_replay_result,
    simulate_medium_term_replay,
)


def _config(**overrides) -> MediumTermPaperConfig:
    defaults = {
        "sma_short_period": 2,
        "sma_long_period": 3,
        "breakout_period": 2,
        "volume_period": 2,
        "volume_multiplier": 1.5,
        "atr_period": 2,
        "history_bars": 10,
        "max_holding_sessions": 3,
    }
    defaults.update(overrides)
    return MediumTermPaperConfig(**defaults)


def _bars(*, signal_volume: int = 200, gap_after_entry: bool = False) -> list[dict]:
    start = date(2026, 1, 1)
    result = []
    for index in range(9):
        close = 100.0 + index
        result.append({
            "trade_date": start + timedelta(days=index),
            "open": close,
            "high": close + 0.4,
            "low": close - 0.4,
            "close": close,
            "volume": 100,
        })
    result[3]["volume"] = signal_volume
    if gap_after_entry:
        result[5].update({"open": 95.0, "high": 96.0, "low": 94.0, "close": 95.0})
    return result


def test_replay_uses_next_open_and_compares_three_arms_with_benchmark() -> None:
    bars = _bars()
    result = simulate_medium_term_replay(
        bars_by_ticker={"SBER": bars},
        config=_config(),
        start_date=bars[4]["trade_date"],
        end_date=bars[-1]["trade_date"],
    )

    assert {item["arm"] for item in result["summaries"]} == {
        "staircase", "atr", "hybrid", BENCHMARK_ARM,
    }
    assert len(result["trades"]) == 3
    assert {item["exit_reason"] for item in result["trades"]} == {"max_holding"}
    assert all(item["signal_date"] == bars[3]["trade_date"] for item in result["trades"])
    assert all(item["entry_date"] == bars[4]["trade_date"] for item in result["trades"])
    assert all(item["entry_price"] == bars[4]["open"] for item in result["trades"])
    assert len(result["equity"]) == 4 * 5


def test_replay_entry_bar_volume_cannot_create_same_day_entry() -> None:
    bars = _bars(signal_volume=100)
    bars[4]["volume"] = 10_000

    result = simulate_medium_term_replay(
        bars_by_ticker={"SBER": bars},
        config=_config(),
        start_date=bars[4]["trade_date"],
        end_date=bars[-1]["trade_date"],
    )

    assert not any(item["entry_date"] == bars[4]["trade_date"] for item in result["trades"])
    assert any(item["entry_date"] == bars[5]["trade_date"] for item in result["trades"])


def test_replay_gap_exit_uses_open_and_records_mark_to_market_drawdown() -> None:
    bars = _bars(gap_after_entry=True)

    result = simulate_medium_term_replay(
        bars_by_ticker={"SBER": bars},
        config=_config(),
        start_date=bars[4]["trade_date"],
        end_date=bars[-1]["trade_date"],
    )

    strategy_trades = result["trades"][:3]
    assert all(item["exit_reason"] == "stop_gap" for item in strategy_trades)
    assert all(item["exit_price"] == 95.0 for item in strategy_trades)
    strategy_equity = [
        item for item in result["equity"] if item["arm"] == "staircase"
    ]
    assert max(item["drawdown_pct"] for item in strategy_equity) > 0


class FakeReplayRepository:
    def __init__(self, bars: list[dict]) -> None:
        self.bars = bars
        self.persisted = None
        self.completed = None
        self.failed = None

    def create_medium_term_replay_run(self, **kwargs) -> int:
        self.created = kwargs
        return 42

    def list_moex_daily_history_range(self, *args, **kwargs) -> dict[str, list[dict]]:
        return {"SBER": self.bars}

    def list_moex_security_splits(self, *args, **kwargs) -> list[dict]:
        return []

    def list_broker_dividends_for_replay(self, *args, **kwargs) -> list[dict]:
        return []

    def insert_medium_term_replay_results(self, **kwargs) -> None:
        self.persisted = kwargs

    def complete_medium_term_replay_run(self, run_id: int) -> None:
        self.completed = run_id

    def fail_medium_term_replay_run(self, run_id: int, error: str) -> None:
        self.failed = (run_id, error)


def test_replay_service_persists_auditable_run() -> None:
    bars = _bars()
    repository = FakeReplayRepository(bars)
    service = MediumTermReplayService(
        repository=repository,
        config=_config(),
        logger=logging.getLogger("test"),
    )

    result = service.run(
        name="research-2026",
        start_date=bars[4]["trade_date"],
        end_date=bars[-1]["trade_date"],
        tickers=("sber",),
    )

    assert result.run_id == 42
    assert result.tickers == ("SBER",)
    assert result.trade_count == 3
    assert repository.persisted["run_id"] == 42
    assert repository.completed == 42
    assert repository.failed is None


def test_replay_service_marks_failed_run_for_missing_history() -> None:
    repository = FakeReplayRepository([])
    service = MediumTermReplayService(
        repository=repository,
        config=_config(),
        logger=logging.getLogger("test"),
    )

    with pytest.raises(ValueError, match="no MOEX daily bars"):
        service.run(
            name="missing-history",
            start_date=date(2021, 1, 1),
            end_date=date(2026, 1, 1),
            tickers=("SBER",),
        )

    assert repository.completed is None
    assert repository.failed is not None
    assert repository.failed[0] == 42


def test_replay_rejects_empty_range() -> None:
    with pytest.raises(ValueError, match="no MOEX daily bars"):
        simulate_medium_term_replay(
            bars_by_ticker={},
            config=_config(),
            start_date=date(2026, 1, 1),
            end_date=date(2026, 2, 1),
        )


def test_format_replay_result_includes_benchmark() -> None:
    bars = _bars()
    repository = FakeReplayRepository(bars)
    result = MediumTermReplayService(
        repository=repository,
        config=_config(),
        logger=logging.getLogger("test"),
    ).run(
        name="format-test",
        start_date=bars[4]["trade_date"],
        end_date=bars[-1]["trade_date"],
        tickers=("SBER",),
    )

    output = format_medium_term_replay_result(result)

    assert "medium-term replay: format-test" in output
    assert BENCHMARK_ARM in output
    assert "max_dd=" in output
    assert "dividends=" in output


def test_split_adjustment_removes_price_discontinuity() -> None:
    bars = {
        "GMKN": [
            {
                "trade_date": date(2024, 4, 5),
                "open": 15_000.0,
                "high": 15_100.0,
                "low": 14_900.0,
                "close": 15_000.0,
                "volume": 1_000,
            },
            {
                "trade_date": date(2024, 4, 8),
                "open": 150.0,
                "high": 151.0,
                "low": 149.0,
                "close": 150.0,
                "volume": 100_000,
            },
        ],
    }
    adjusted = adjust_bars_for_splits(bars, [{
        "ticker": "GMKN",
        "trade_date": date(2024, 4, 8),
        "before": 1,
        "after": 100,
    }])

    assert adjusted["GMKN"][0]["close"] == 150.0
    assert adjusted["GMKN"][0]["volume"] == 100_000
    assert adjusted["GMKN"][1]["close"] == 150.0


def test_benchmark_reinvests_dividends_as_total_return() -> None:
    start = date(2026, 1, 1)
    bars = [{
        "trade_date": start + timedelta(days=index),
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
        "volume": 100,
    } for index in range(3)]

    result = simulate_medium_term_replay(
        bars_by_ticker={"SBER": bars},
        config=_config(commission_rate=0.0, slippage_rate=0.0),
        start_date=start,
        end_date=start + timedelta(days=2),
        dividends=[{
            "ticker": "SBER",
            "entitlement_date": start,
            "amount": 10.0,
            "currency": "RUB",
        }],
    )

    benchmark = next(
        item for item in result["summaries"] if item["arm"] == BENCHMARK_ARM
    )
    assert benchmark["final_equity"] == pytest.approx(1_100_000.0)
    assert benchmark["dividend_income"] == pytest.approx(100_000.0)


def test_strategy_dividend_requires_open_position_on_entitlement_date() -> None:
    bars = _bars()
    entitlement_date = bars[4]["trade_date"]
    result = simulate_medium_term_replay(
        bars_by_ticker={"SBER": bars},
        config=_config(commission_rate=0.0, slippage_rate=0.0),
        start_date=entitlement_date,
        end_date=bars[-1]["trade_date"],
        dividends=[{
            "ticker": "SBER",
            "entitlement_date": entitlement_date,
            "amount": 10.0,
            "currency": "RUB",
        }],
    )

    assert all(item["dividend_income"] > 0 for item in result["trades"])
    assert all(
        item["dividend_income"] > 0
        for item in result["summaries"]
        if item["arm"] != BENCHMARK_ARM
    )
