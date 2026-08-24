from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from tinvest_trader.app.config import MarketActivityConfig
from tinvest_trader.services.market_activity_service import MarketActivityService


def _candle(at: datetime, *, volume: int, close: float = 100.0) -> dict:
    return {
        "open": 100.0,
        "high": max(100.0, close),
        "low": 99.5,
        "close": close,
        "volume": volume,
        "time": at.isoformat(),
    }


def _service(*, candles: list[dict]) -> tuple[MarketActivityService, MagicMock, MagicMock]:
    client = MagicMock()
    client.get_candles.return_value = candles
    repository = MagicMock()
    repository.list_tracked_instruments.return_value = [
        {"ticker": "SBER", "figi": "BBG004730N88", "instrument_uid": "uid-sber"},
    ]
    repository.insert_market_activity_observation.return_value = True
    repository.insert_market_activity_spike.return_value = True
    now = datetime(2026, 8, 24, 10, 0, tzinfo=UTC)
    service = MarketActivityService(
        client=client,
        repository=repository,
        config=MarketActivityConfig(baseline_candles=3, min_volume=10),
        logger=logging.getLogger("test_market_activity"),
        now_fn=lambda: now,
    )
    return service, client, repository


def test_persists_observations_and_explainable_volume_price_spike() -> None:
    now = datetime(2026, 8, 24, 10, 0, tzinfo=UTC)
    candles = [
        _candle(now - timedelta(minutes=4 - index), volume=100)
        for index in range(4)
    ]
    candles.append(_candle(now, volume=400, close=102.0))
    service, client, repository = _service(candles=candles)

    result = service.observe_all()

    assert result.instruments_seen == 1
    assert result.observations_inserted == 5
    assert result.spikes_inserted == 1
    client.get_candles.assert_called_once()
    spike = repository.insert_market_activity_spike.call_args.args[0]
    assert spike["spike_type"] == "volume_price"
    assert spike["metrics"]["volume_ratio"] == 4.0
    assert "volume 4.00x baseline" in spike["reason"]


def test_skips_placeholder_instruments_without_api_call() -> None:
    service, client, repository = _service(candles=[])
    repository.list_tracked_instruments.return_value = [
        {"ticker": "SBER", "figi": "TICKER:SBER", "instrument_uid": ""},
    ]

    result = service.observe_all()

    assert result.instruments_seen == 0
    client.get_candles.assert_not_called()


def test_falls_back_to_figi_for_synthetic_instrument_uid() -> None:
    now = datetime(2026, 8, 24, 10, 0, tzinfo=UTC)
    service, client, _repository = _service(candles=[_candle(now, volume=100)])

    service.observe_all()

    assert client.get_candles.call_args.kwargs["instrument_id"] == "BBG004730N88"


def test_one_instrument_failure_does_not_abort_cycle() -> None:
    service, client, _repository = _service(candles=[])
    client.get_candles.side_effect = RuntimeError("api unavailable")

    result = service.observe_all()

    assert result.instruments_seen == 1
    assert result.failed == 1


def test_no_spike_without_baseline_data() -> None:
    now = datetime(2026, 8, 24, 10, 0, tzinfo=UTC)
    service, _client, repository = _service(
        candles=[_candle(now, volume=10_000, close=110.0)],
    )

    result = service.observe_all()

    assert result.observations_inserted == 1
    assert result.spikes_inserted == 0
    repository.insert_market_activity_spike.assert_not_called()


def test_skips_explicitly_incomplete_current_candle() -> None:
    now = datetime(2026, 8, 24, 10, 0, tzinfo=UTC)
    candle = _candle(now, volume=1_000, close=110.0)
    candle["is_complete"] = False
    service, _client, repository = _service(candles=[candle])

    result = service.observe_all()

    assert result.observations_inserted == 0
    repository.insert_market_activity_observation.assert_not_called()
