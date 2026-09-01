from __future__ import annotations

import logging
from datetime import date, timedelta

import pytest

from tinvest_trader.app.config import MediumTermPaperConfig
from tinvest_trader.services.medium_term_replay_service import (
    BENCHMARK_ARM,
    MediumTermReplayService,
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
