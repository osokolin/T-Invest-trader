"""Shadow evaluation of market-activity spikes using stored candles only."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from tinvest_trader.app.config import MarketActivityOutcomeConfig
    from tinvest_trader.infra.storage.repository import TradingRepository


MOSCOW = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True)
class MarketActivityOutcomeResult:
    """Summary of one idempotent outcome resolution cycle."""

    spikes_seen: int = 0
    outcomes_inserted: int = 0
    unresolved: int = 0
    failed: int = 0


class MarketActivityOutcomeService:
    """Compare momentum and reversion after observed activity spikes."""

    def __init__(
        self,
        repository: TradingRepository,
        config: MarketActivityOutcomeConfig,
        logger: logging.Logger,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._repository = repository
        self._config = config
        self._logger = logger
        self._now_fn = now_fn or (lambda: datetime.now(UTC))

    def resolve_all(self) -> MarketActivityOutcomeResult:
        """Resolve every elapsed configured horizon without external API calls."""
        now = self._normalized_now()
        horizons = tuple(
            [f"{minutes}m" for minutes in self._config.horizons_minutes]
            + (["eod"] if self._config.eod_enabled else [])
        )
        if not horizons:
            return MarketActivityOutcomeResult()
        try:
            spikes = self._repository.list_market_activity_spikes_for_outcomes(
                since=now - timedelta(days=max(1, self._config.lookback_days)),
                limit=max(1, self._config.resolution_limit),
                horizons=horizons,
            )
        except Exception:
            self._logger.exception(
                "market activity outcome cycle failed to load spikes",
                extra={"component": "market_activity_outcomes"},
            )
            return MarketActivityOutcomeResult(failed=1)

        inserted = unresolved = failed = 0
        for spike in spikes:
            for horizon, target_time, is_eod in self._targets(spike, now):
                try:
                    status = self._resolve_horizon(
                        spike=spike,
                        horizon=horizon,
                        target_time=target_time,
                        is_eod=is_eod,
                        now=now,
                    )
                    inserted += status == "inserted"
                    unresolved += status == "unresolved"
                except Exception:
                    failed += 1
                    self._logger.exception(
                        "market activity outcome resolution failed",
                        extra={
                            "component": "market_activity_outcomes",
                            "spike_id": spike.get("id"),
                            "horizon": horizon,
                        },
                    )

        result = MarketActivityOutcomeResult(
            spikes_seen=len(spikes),
            outcomes_inserted=inserted,
            unresolved=unresolved,
            failed=failed,
        )
        self._logger.info(
            "market activity outcome cycle complete",
            extra={"component": "market_activity_outcomes", **result.__dict__},
        )
        return result

    def _targets(
        self,
        spike: dict,
        now: datetime,
    ) -> list[tuple[str, datetime, bool]]:
        spike_time = self._as_aware(spike["candle_time"])
        targets = [
            (f"{minutes}m", spike_time + timedelta(minutes=minutes), False)
            for minutes in self._config.horizons_minutes
            if minutes > 0
        ]
        if self._config.eod_enabled:
            local_day = spike_time.astimezone(MOSCOW).date()
            local_close = datetime.combine(
                local_day,
                time(
                    hour=self._config.eod_hour_moscow,
                    minute=self._config.eod_minute_moscow,
                ),
                tzinfo=MOSCOW,
            ).astimezone(UTC)
            if local_close > spike_time:
                targets.append(("eod", local_close, True))
        return [target for target in targets if target[1] <= now]

    def _resolve_horizon(
        self,
        *,
        spike: dict,
        horizon: str,
        target_time: datetime,
        is_eod: bool,
        now: datetime,
    ) -> str:
        spike_id = int(spike["id"])
        if self._repository.market_activity_outcome_exists(spike_id, horizon):
            return "existing"

        if is_eod:
            price_row = self._repository.get_market_activity_price_before(
                figi=spike["figi"],
                candle_interval=spike["candle_interval"],
                after_time=self._as_aware(spike["candle_time"]),
                target_time=target_time,
            )
        else:
            price_row = self._repository.get_market_activity_price_after(
                figi=spike["figi"],
                candle_interval=spike["candle_interval"],
                target_time=target_time,
                latest_time=target_time + timedelta(minutes=5),
            )
        if price_row is None:
            return "unresolved"

        outcome = self._build_outcome(
            spike=spike,
            horizon=horizon,
            price_row=price_row,
            resolved_at=now,
        )
        return (
            "inserted"
            if self._repository.insert_market_activity_spike_outcome(outcome)
            else "existing"
        )

    def _build_outcome(
        self,
        *,
        spike: dict,
        horizon: str,
        price_row: dict,
        resolved_at: datetime,
    ) -> dict:
        entry_price = float(spike["entry_price"])
        outcome_price = float(price_row["price"])
        raw_return = (outcome_price - entry_price) / entry_price
        direction = self._direction(spike.get("metrics"))
        direction_factor = 1.0 if direction == "up" else -1.0 if direction == "down" else 0.0
        momentum_return = raw_return * direction_factor
        reversion_return = -momentum_return
        return {
            "spike_id": int(spike["id"]),
            "ticker": spike["ticker"],
            "figi": spike["figi"],
            "spike_time": self._as_aware(spike["candle_time"]),
            "horizon": horizon,
            "direction": direction,
            "entry_price": entry_price,
            "outcome_price": outcome_price,
            "raw_return_pct": raw_return,
            "momentum_return_pct": momentum_return,
            "reversion_return_pct": reversion_return,
            "momentum_outcome": self._classify(momentum_return),
            "reversion_outcome": self._classify(reversion_return),
            "outcome_time": self._as_aware(price_row["candle_time"]),
            "resolved_at": resolved_at,
        }

    def _classify(self, return_pct: float) -> str:
        threshold = max(0.0, self._config.neutral_threshold_pct)
        if return_pct > threshold:
            return "win"
        if return_pct < -threshold:
            return "loss"
        return "neutral"

    @staticmethod
    def _direction(metrics: object) -> str:
        if isinstance(metrics, str):
            metrics = json.loads(metrics)
        if not isinstance(metrics, dict):
            return "flat"
        change = float(metrics.get("price_change_pct") or 0.0)
        return "up" if change > 0 else "down" if change < 0 else "flat"

    def _normalized_now(self) -> datetime:
        return self._as_aware(self._now_fn()).astimezone(UTC)

    @staticmethod
    def _as_aware(value: datetime) -> datetime:
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def format_market_activity_outcome_report(rows: list[dict]) -> str:
    """Format a compact operator report for the CLI."""
    if not rows:
        return "No resolved market activity spike outcomes yet."
    lines = [
        "horizon  samples  momentum avg/win  reversion avg/win",
        "-------  -------  ----------------  -----------------",
    ]
    for row in rows:
        lines.append(
            f"{row['horizon']:>7}  {int(row['sample_size']):>7}  "
            f"{float(row['momentum_avg_return'] or 0):>7.3%}/"
            f"{float(row['momentum_win_rate'] or 0):>6.1%}  "
            f"{float(row['reversion_avg_return'] or 0):>7.3%}/"
            f"{float(row['reversion_win_rate'] or 0):>6.1%}"
        )
    return "\n".join(lines)
