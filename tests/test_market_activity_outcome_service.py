import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from tinvest_trader.app.config import MarketActivityOutcomeConfig
from tinvest_trader.services.market_activity_outcome_service import (
    MarketActivityOutcomeService,
    format_market_activity_outcome_report,
)

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


def _spike(*, when: datetime | None = None, change: float = 0.01) -> dict:
    return {
        "id": 7,
        "ticker": "SBER",
        "figi": "BBG004730N88",
        "candle_time": when or NOW - timedelta(minutes=20),
        "candle_interval": "CANDLE_INTERVAL_1_MIN",
        "spike_type": "volume_price",
        "metrics": {"price_change_pct": change},
        "entry_price": 100.0,
    }


def _service(repository: MagicMock, **config_values) -> MarketActivityOutcomeService:
    config = MarketActivityOutcomeConfig(
        horizons_minutes=config_values.pop("horizons_minutes", (5, 15)),
        eod_enabled=config_values.pop("eod_enabled", False),
        **config_values,
    )
    return MarketActivityOutcomeService(
        repository=repository,
        config=config,
        logger=logging.getLogger("test_market_activity_outcomes"),
        now_fn=lambda: NOW,
    )


def test_resolves_elapsed_horizons_and_compares_momentum_reversion() -> None:
    repository = MagicMock()
    repository.list_market_activity_spikes_for_outcomes.return_value = [_spike()]
    repository.market_activity_outcome_exists.return_value = False
    repository.get_market_activity_price_after.return_value = {
        "price": 102.0,
        "candle_time": NOW - timedelta(minutes=5),
    }
    repository.insert_market_activity_spike_outcome.return_value = True

    result = _service(repository).resolve_all()

    assert result.outcomes_inserted == 2
    outcomes = [
        call.args[0]
        for call in repository.insert_market_activity_spike_outcome.call_args_list
    ]
    assert {item["horizon"] for item in outcomes} == {"5m", "15m"}
    assert all(item["momentum_return_pct"] == 0.02 for item in outcomes)
    assert all(item["momentum_outcome"] == "win" for item in outcomes)
    assert all(item["reversion_outcome"] == "loss" for item in outcomes)


def test_down_spike_favors_momentum_when_price_falls() -> None:
    repository = MagicMock()
    repository.list_market_activity_spikes_for_outcomes.return_value = [
        _spike(change=-0.01),
    ]
    repository.market_activity_outcome_exists.return_value = False
    repository.get_market_activity_price_after.return_value = {
        "price": 98.0,
        "candle_time": NOW - timedelta(minutes=5),
    }
    repository.insert_market_activity_spike_outcome.return_value = True

    _service(repository, horizons_minutes=(5,)).resolve_all()

    outcome = repository.insert_market_activity_spike_outcome.call_args.args[0]
    assert outcome["direction"] == "down"
    assert outcome["momentum_return_pct"] == 0.02
    assert outcome["momentum_outcome"] == "win"


def test_future_horizon_is_not_resolved() -> None:
    repository = MagicMock()
    repository.list_market_activity_spikes_for_outcomes.return_value = [
        _spike(when=NOW - timedelta(minutes=2)),
    ]

    result = _service(repository, horizons_minutes=(5,)).resolve_all()

    assert result.outcomes_inserted == 0
    repository.get_market_activity_price_after.assert_not_called()


def test_no_configured_horizons_skips_repository() -> None:
    repository = MagicMock()

    result = _service(
        repository,
        horizons_minutes=(),
        eod_enabled=False,
    ).resolve_all()

    assert result == result.__class__()
    repository.list_market_activity_spikes_for_outcomes.assert_not_called()


def test_missing_stored_candle_remains_unresolved() -> None:
    repository = MagicMock()
    repository.list_market_activity_spikes_for_outcomes.return_value = [_spike()]
    repository.market_activity_outcome_exists.return_value = False
    repository.get_market_activity_price_after.return_value = None

    result = _service(repository, horizons_minutes=(5,)).resolve_all()

    assert result.unresolved == 1
    repository.insert_market_activity_spike_outcome.assert_not_called()


def test_configured_price_delay_is_used_for_sparse_candles() -> None:
    repository = MagicMock()
    repository.list_market_activity_spikes_for_outcomes.return_value = [_spike()]
    repository.market_activity_outcome_exists.return_value = False
    repository.get_market_activity_price_after.return_value = {
        "price": 101.0,
        "candle_time": NOW - timedelta(minutes=1),
    }
    repository.insert_market_activity_spike_outcome.return_value = True

    result = _service(
        repository,
        horizons_minutes=(15,),
        max_price_delay_minutes=30,
    ).resolve_all()

    assert result.outcomes_inserted == 1
    call = repository.get_market_activity_price_after.call_args.kwargs
    assert call["latest_time"] == call["target_time"] + timedelta(minutes=30)


def test_eod_uses_last_stored_candle_before_moscow_close() -> None:
    repository = MagicMock()
    spike_time = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    repository.list_market_activity_spikes_for_outcomes.return_value = [
        _spike(when=spike_time),
    ]
    repository.market_activity_outcome_exists.return_value = False
    repository.get_market_activity_price_before.return_value = {
        "price": 101.0,
        "candle_time": datetime(2026, 8, 23, 20, 49, tzinfo=UTC),
    }
    repository.insert_market_activity_spike_outcome.return_value = True

    result = _service(
        repository,
        horizons_minutes=(),
        eod_enabled=True,
    ).resolve_all()

    assert result.outcomes_inserted == 1
    call = repository.get_market_activity_price_before.call_args.kwargs
    assert call["target_time"] == datetime(2026, 8, 23, 20, 50, tzinfo=UTC)


def test_future_eod_backlog_does_not_starve_intraday_horizons() -> None:
    repository = MagicMock()
    old_spike = _spike(when=NOW - timedelta(hours=2))
    old_spike["id"] = 1
    new_spike = _spike(when=NOW - timedelta(minutes=20))
    new_spike["id"] = 2
    repository.list_market_activity_spikes_for_outcomes.side_effect = [
        [new_spike],
        [new_spike],
        [old_spike],
    ]
    repository.market_activity_outcome_exists.side_effect = (
        lambda spike_id, _horizon: spike_id == 1
    )
    repository.get_market_activity_price_after.return_value = {
        "price": 101.0,
        "candle_time": NOW - timedelta(minutes=5),
    }
    repository.insert_market_activity_spike_outcome.return_value = True

    result = _service(repository, eod_enabled=True).resolve_all()

    horizon_groups = [
        call.kwargs["horizons"]
        for call in repository.list_market_activity_spikes_for_outcomes.call_args_list
    ]
    assert horizon_groups == [("5m",), ("15m",), ("eod",)]
    assert result.outcomes_inserted == 2
    assert {
        call.args[0]["spike_id"]
        for call in repository.insert_market_activity_spike_outcome.call_args_list
    } == {2}
    repository.get_market_activity_price_before.assert_not_called()


def test_existing_outcome_is_idempotently_skipped() -> None:
    repository = MagicMock()
    repository.list_market_activity_spikes_for_outcomes.return_value = [_spike()]
    repository.market_activity_outcome_exists.return_value = True

    result = _service(repository, horizons_minutes=(5,)).resolve_all()

    assert result.outcomes_inserted == 0
    repository.get_market_activity_price_after.assert_not_called()


def test_repository_failure_is_safe() -> None:
    repository = MagicMock()
    repository.list_market_activity_spikes_for_outcomes.side_effect = RuntimeError("db")

    result = _service(repository).resolve_all()

    assert result.failed == 1


def test_report_formats_momentum_and_reversion() -> None:
    report = format_market_activity_outcome_report([
        {
            "horizon": "5m",
            "sample_size": 10,
            "momentum_avg_return": 0.01,
            "reversion_avg_return": -0.01,
            "momentum_win_rate": 0.6,
            "reversion_win_rate": 0.3,
        },
    ])

    assert "5m" in report
    assert "60.0%" in report
    assert "30.0%" in report
