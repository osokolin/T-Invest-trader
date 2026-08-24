"""T-Bank candle-based market activity monitor.

This service is observational: it creates no signals, positions, or orders.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from statistics import median
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.app.config import MarketActivityConfig
    from tinvest_trader.infra.storage.repository import TradingRepository
    from tinvest_trader.infra.tbank.client import TBankClient


_PLACEHOLDER_FIGI_PREFIX = "TICKER:"


@dataclass(frozen=True)
class MarketActivityResult:
    """Summary of one safe, idempotent monitoring cycle."""

    instruments_seen: int = 0
    observations_inserted: int = 0
    spikes_inserted: int = 0
    failed: int = 0


class MarketActivityService:
    """Persist minute-candle activity features and explainable spikes."""

    def __init__(
        self,
        client: TBankClient,
        repository: TradingRepository,
        config: MarketActivityConfig,
        logger: logging.Logger,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._client = client
        self._repository = repository
        self._config = config
        self._logger = logger
        self._now_fn = now_fn or (lambda: datetime.now(UTC))

    def observe_all(self) -> MarketActivityResult:
        """Observe all tracked instruments without affecting trading pipelines."""
        result = MarketActivityResult()
        try:
            instruments = self._repository.list_tracked_instruments()
        except Exception:
            self._logger.exception(
                "market activity skipped: failed to list tracked instruments",
                extra={"component": "market_activity"},
            )
            return MarketActivityResult(failed=1)

        seen = inserted = spikes = failed = 0
        for instrument in instruments:
            figi = str(instrument.get("figi") or "").strip()
            ticker = str(instrument.get("ticker") or "").strip().upper()
            if not ticker or not figi or figi.startswith(_PLACEHOLDER_FIGI_PREFIX):
                continue
            seen += 1
            try:
                observation_count, spike_count = self._observe_instrument(
                    ticker=ticker,
                    figi=figi,
                    instrument_uid=str(instrument.get("instrument_uid") or ""),
                )
                inserted += observation_count
                spikes += spike_count
            except Exception:
                failed += 1
                self._logger.exception(
                    "market activity failed for instrument",
                    extra={"component": "market_activity", "ticker": ticker},
                )

        result = MarketActivityResult(
            instruments_seen=seen,
            observations_inserted=inserted,
            spikes_inserted=spikes,
            failed=failed,
        )
        self._logger.info(
            "market activity cycle complete",
            extra={
                "component": "market_activity",
                "instruments_seen": result.instruments_seen,
                "observations_inserted": result.observations_inserted,
                "spikes_inserted": result.spikes_inserted,
                "failed": result.failed,
            },
        )
        return result

    def _observe_instrument(
        self,
        *,
        ticker: str,
        figi: str,
        instrument_uid: str,
    ) -> tuple[int, int]:
        now = self._now_fn()
        instrument_id = (
            instrument_uid
            if instrument_uid and not instrument_uid.startswith("uid-")
            else figi
        )
        candles = self._client.get_candles(
            instrument_id=instrument_id,
            from_time=now - timedelta(minutes=max(1, self._config.lookback_minutes)),
            to_time=now,
            interval=self._config.candle_interval,
        )
        normalized = sorted(
            (candle for candle in candles if self._parse_time(candle.get("time"))),
            key=lambda candle: str(candle["time"]),
        )
        observations = spikes = 0
        prior_volumes: list[int] = []
        for candle in normalized:
            # Partial current candles would be overwritten on every poll.
            # Persist closed candles only, which keeps the append-only audit trail stable.
            if candle.get("is_complete") is False:
                continue
            volume = max(0, int(candle.get("volume", 0) or 0))
            baseline = self._baseline_volume(prior_volumes)
            observation = self._make_observation(
                ticker=ticker,
                figi=figi,
                candle=candle,
                baseline_volume=baseline,
            )
            if self._repository.insert_market_activity_observation(observation):
                observations += 1
            spike = self._make_spike(observation)
            if spike is not None and self._repository.insert_market_activity_spike(spike):
                spikes += 1
            prior_volumes.append(volume)
            prior_volumes = prior_volumes[-max(1, self._config.baseline_candles):]
        return observations, spikes

    def _baseline_volume(self, prior_volumes: list[int]) -> float | None:
        required = max(1, self._config.baseline_candles)
        if len(prior_volumes) < required:
            return None
        return float(median(prior_volumes[-required:]))

    def _make_observation(
        self,
        *,
        ticker: str,
        figi: str,
        candle: dict,
        baseline_volume: float | None,
    ) -> dict:
        open_price = float(candle["open"])
        high_price = float(candle["high"])
        low_price = float(candle["low"])
        close_price = float(candle["close"])
        volume = max(0, int(candle.get("volume", 0) or 0))
        return {
            "ticker": ticker,
            "figi": figi,
            "candle_time": self._parse_time(candle["time"]),
            "candle_interval": self._config.candle_interval,
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "volume": volume,
            "baseline_volume": baseline_volume,
            "volume_ratio": (volume / baseline_volume)
            if baseline_volume and baseline_volume > 0
            else None,
            "price_change_pct": ((close_price - open_price) / open_price)
            if open_price > 0
            else None,
            "range_pct": ((high_price - low_price) / open_price)
            if open_price > 0
            else None,
        }

    def _make_spike(self, observation: dict) -> dict | None:
        volume_ratio = observation["volume_ratio"]
        price_change_pct = observation["price_change_pct"]
        if volume_ratio is None or price_change_pct is None:
            return None
        if observation["volume"] < self._config.min_volume:
            return None

        volume_spike = volume_ratio >= self._config.volume_spike_multiplier
        price_spike = abs(price_change_pct) >= self._config.price_change_spike_pct
        if not volume_spike and not price_spike:
            return None

        if volume_spike and price_spike:
            spike_type = "volume_price"
        elif volume_spike:
            spike_type = "volume"
        else:
            spike_type = "price_momentum"
        score = min(100.0, 20.0 * volume_ratio + 2_000.0 * abs(price_change_pct))
        severity = "high" if score >= 80 else "medium" if score >= 45 else "low"
        direction = "up" if price_change_pct > 0 else "down"
        reason = (
            f"{spike_type}: volume {volume_ratio:.2f}x baseline, "
            f"price {direction} {abs(price_change_pct):.2%}"
        )
        return {
            "ticker": observation["ticker"],
            "figi": observation["figi"],
            "candle_time": observation["candle_time"],
            "candle_interval": observation["candle_interval"],
            "spike_type": spike_type,
            "severity": severity,
            "score": round(score, 2),
            "reason": reason,
            "metrics": {
                "volume": observation["volume"],
                "baseline_volume": observation["baseline_volume"],
                "volume_ratio": volume_ratio,
                "price_change_pct": price_change_pct,
                "range_pct": observation["range_pct"],
            },
        }

    @staticmethod
    def _parse_time(value: object) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        if not isinstance(value, str) or not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
