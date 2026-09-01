from datetime import UTC, datetime

import pytest

from tinvest_trader.app.config import PaperTariffComparisonConfig
from tinvest_trader.services.paper_tariff_comparison import (
    compare_tariffs,
    format_tariff_comparison,
)


def test_compare_tariffs_reprices_round_trip_and_monthly_fees() -> None:
    positions = [
        {
            "portfolio_name": "shadow-v1",
            "portfolio_type": "signal",
            "notional": 100_000,
            "gross_pnl": 1_000,
            "exit_time": datetime(2026, 8, 31, 20, 0, tzinfo=UTC),
        },
        {
            "portfolio_name": "shadow-v1",
            "portfolio_type": "signal",
            "notional": 50_000,
            "gross_pnl": -200,
            "exit_time": datetime(2026, 9, 1, 20, 0, tzinfo=UTC),
        },
    ]

    results = compare_tariffs(positions, PaperTariffComparisonConfig())
    by_tariff = {
        result.tariff: result
        for result in results
        if result.portfolio_name == "shadow-v1"
    }

    assert by_tariff["investor"].executed_turnover == 300_000
    assert by_tariff["investor"].active_months == 2
    assert by_tariff["investor"].broker_commission == pytest.approx(900)
    assert by_tariff["investor"].slippage_cost == pytest.approx(150)
    assert by_tariff["investor"].net_pnl == pytest.approx(-250)
    assert by_tariff["trader_paid"].subscription_cost == pytest.approx(780)
    assert by_tariff["trader_free"].net_pnl == pytest.approx(500)
    assert by_tariff["premium_paid"].subscription_cost == pytest.approx(5_980)
    assert by_tariff["premium_free"].net_pnl == pytest.approx(530)


def test_compare_tariffs_keeps_portfolios_isolated() -> None:
    positions = [
        {
            "portfolio_name": "shadow-v1",
            "portfolio_type": "signal",
            "notional": 100_000,
            "gross_pnl": 1_000,
            "exit_time": datetime(2026, 8, 31, 12, 0, tzinfo=UTC),
        },
        {
            "portfolio_name": "activity-v2",
            "portfolio_type": "activity",
            "notional": 20_000,
            "gross_pnl": 100,
            "exit_time": datetime(2026, 8, 31, 12, 0, tzinfo=UTC),
        },
    ]

    results = compare_tariffs(positions, PaperTariffComparisonConfig())

    assert len(results) == 15
    assert {result.portfolio_name for result in results} == {
        "activity-v2", "all_paper", "shadow-v1",
    }


def test_tariff_comparison_formats_empty_and_populated_reports() -> None:
    assert format_tariff_comparison([]) == (
        "no closed paper positions in selected period"
    )
    results = compare_tariffs(
        [{
            "portfolio_name": "shadow-v1",
            "portfolio_type": "signal",
            "notional": 100_000,
            "gross_pnl": 1_000,
            "exit_time": datetime(2026, 8, 31, 12, 0, tzinfo=UTC),
        }],
        PaperTariffComparisonConfig(),
    )

    output = format_tariff_comparison(results)

    assert "portfolio: shadow-v1 (signal)" in output
    assert "trader_paid" in output
    assert "premium_free" in output
