from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest

from tinvest_trader.app.config import (
    BackgroundConfig,
    PaperPortfolioConfig,
    load_config,
)
from tinvest_trader.services.background_runner import BackgroundRunner
from tinvest_trader.services.paper_portfolio_service import (
    PaperPortfolioService,
    format_paper_portfolio_summary,
)


class FakeRepository:
    def __init__(self) -> None:
        self.closed: list[dict] = []
        self.inserted: list[dict] = []
        self.resolved: list[dict] = []
        self.candidates: list[dict] = []
        self.expiry_calls: list[dict] = []
        self.expired_count = 0
        self.summary = {
            "name": "shadow-v1",
            "initial_cash": 100_000.0,
            "currency": "RUB",
            "started_at": datetime(2026, 8, 23, tzinfo=UTC),
            "open_positions": 0,
            "closed_positions": 0,
            "open_notional": 0.0,
            "realized_pnl": 0.0,
            "wins": 0,
            "avg_net_return_pct": None,
        }

    def ensure_paper_portfolio(self, **_kwargs):
        return self.summary

    def list_resolved_open_paper_positions(self, _name):
        return self.resolved

    def close_paper_position(self, **kwargs):
        self.closed.append(kwargs)
        return True

    def expire_stale_paper_positions(self, **kwargs):
        self.expiry_calls.append(kwargs)
        return self.expired_count

    def get_paper_portfolio_summary(self, _name):
        return self.summary

    def list_paper_entry_candidates(self, _name, _stages):
        return self.candidates

    def insert_paper_position(self, **kwargs):
        self.inserted.append(kwargs)
        return len(self.inserted)


def _service(repo: FakeRepository, **config_overrides) -> PaperPortfolioService:
    defaults = {
        "enabled": True,
        "initial_cash": 100_000.0,
        "position_fraction": 0.10,
        "max_open_positions": 5,
        "commission_rate": 0.0005,
        "slippage_rate": 0.0005,
    }
    defaults.update(config_overrides)
    config = PaperPortfolioConfig(**defaults)
    return PaperPortfolioService(repo, config, logging.getLogger("test"))


def test_cycle_closes_resolved_short_and_opens_new_position():
    repo = FakeRepository()
    now = datetime(2026, 8, 23, tzinfo=UTC)
    repo.resolved = [
        {
            "id": 1,
            "prediction_id": 10,
            "ticker": "SBER",
            "direction": "down",
            "notional": 100_000.0,
            "exit_price": 250.0,
            "return_pct": -0.01,
            "resolved_at": now,
        },
    ]
    repo.candidates = [
        {
            "id": 11,
            "ticker": "GAZP",
            "direction": "up",
            "entry_price": 130.0,
            "entry_time": now,
        },
    ]

    result = _service(repo).run_cycle()

    assert result.opened == 1
    assert result.closed == 1
    assert result.expired == 0
    close = repo.closed[0]
    assert close["gross_return_pct"] == pytest.approx(0.01)
    assert close["costs"] == pytest.approx(200.0)
    assert close["net_pnl"] == pytest.approx(800.0)
    assert repo.inserted[0]["notional"] == pytest.approx(10_000.0)


def test_cycle_respects_position_capacity():
    repo = FakeRepository()
    repo.summary["open_positions"] = 1
    repo.summary["open_notional"] = 10_000.0
    now = datetime(2026, 8, 23, tzinfo=UTC)
    repo.candidates = [
        {
            "id": 11,
            "ticker": "GAZP",
            "direction": "up",
            "entry_price": 130.0,
            "entry_time": now,
        },
        {
            "id": 12,
            "ticker": "LKOH",
            "direction": "up",
            "entry_price": 7000.0,
            "entry_time": now,
        },
    ]

    result = _service(repo, max_open_positions=1).run_cycle()

    assert result.opened == 0
    assert result.skipped_capacity == 2
    assert repo.inserted == []


def test_cycle_expires_stale_unresolved_positions_without_pnl():
    repo = FakeRepository()
    repo.expired_count = 2
    now = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)

    result = _service(
        repo,
        unresolved_position_expiry_minutes=180,
    ).run_cycle(now=now)

    assert result.expired == 2
    assert repo.expiry_calls == [{
        "portfolio_name": "shadow-v1",
        "before": now - timedelta(minutes=180),
        "expired_at": now,
    }]


def test_format_summary_is_realized_only():
    summary = FakeRepository().summary
    summary.update(
        closed_positions=2,
        wins=1,
        realized_pnl=123.45,
        avg_net_return_pct=0.001,
    )

    rendered = format_paper_portfolio_summary(summary)

    assert "realized_equity: 100123.45 RUB" in rendered
    assert "win_rate: 50.0%" in rendered
    assert "avg_net_return: 0.100%" in rendered


def test_paper_portfolio_config_parses_environment(monkeypatch):
    monkeypatch.setenv("TINVEST_PAPER_PORTFOLIO_ENABLED", "true")
    monkeypatch.setenv("TINVEST_PAPER_PORTFOLIO_INITIAL_CASH", "250000")
    monkeypatch.setenv("TINVEST_PAPER_PORTFOLIO_ENTRY_STAGES", "delivered,generated")
    monkeypatch.setenv("TINVEST_PAPER_PORTFOLIO_UNRESOLVED_EXPIRY_MINUTES", "240")
    monkeypatch.setenv("TINVEST_SIGNAL_RESOLUTION_MAX_QUOTE_DELAY_SECONDS", "600")
    monkeypatch.setenv("TINVEST_BACKGROUND_RUN_PAPER_PORTFOLIO", "false")

    config = load_config()

    assert config.paper_portfolio.enabled is True
    assert config.paper_portfolio.initial_cash == 250_000.0
    assert config.paper_portfolio.entry_stages == ("delivered", "generated")
    assert config.paper_portfolio.unresolved_position_expiry_minutes == 240
    assert config.signal_resolution.max_quote_delay_seconds == 600
    assert config.background.run_paper_portfolio is False


def test_runner_skips_paper_portfolio_when_background_flag_is_disabled():
    service = FakeRepository()
    runner = BackgroundRunner(
        config=BackgroundConfig(enabled=True, run_paper_portfolio=False),
        logger=logging.getLogger("test"),
        paper_portfolio_config=PaperPortfolioConfig(enabled=True),
        paper_portfolio_service=service,
    )

    runner.run_paper_portfolio_cycle()

    assert service.closed == []
    assert service.inserted == []
