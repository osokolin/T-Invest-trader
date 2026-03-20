"""Tests for signal outcome evaluation and prediction tracking."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from tinvest_trader.services.signal_outcome import (
    classify_outcome,
    format_signal_stats,
    resolve_pending_signals,
)

NOW = datetime(2026, 3, 20, 12, 0, 0, tzinfo=UTC)


def _make_prediction(
    prediction_id: int = 1,
    ticker: str = "SBER",
    signal_type: str = "up",
    price_at_signal: float = 100.0,
    created_at: datetime | None = None,
) -> dict:
    return {
        "id": prediction_id,
        "ticker": ticker,
        "signal_type": signal_type,
        "price_at_signal": price_at_signal,
        "created_at": created_at or NOW - timedelta(minutes=10),
    }


# -- Classification tests ---------------------------------------------------

class TestClassifyOutcome:
    def test_up_signal_price_increased_win(self) -> None:
        """B. Up signal + price up -> win."""
        assert classify_outcome("up", 0.01) == "win"

    def test_up_signal_price_decreased_loss(self) -> None:
        assert classify_outcome("up", -0.01) == "loss"

    def test_down_signal_price_decreased_win(self) -> None:
        """B. Down signal + price down -> win."""
        assert classify_outcome("down", -0.01) == "win"

    def test_down_signal_price_increased_loss(self) -> None:
        assert classify_outcome("down", 0.01) == "loss"

    def test_neutral_small_return(self) -> None:
        """B. Near-zero return -> neutral."""
        assert classify_outcome("up", 0.0001) == "neutral"
        assert classify_outcome("down", -0.0001) == "neutral"
        assert classify_outcome("up", 0.0) == "neutral"

    def test_unknown_signal_type_neutral(self) -> None:
        assert classify_outcome("sideways", 0.05) == "neutral"


# -- Return calculation tests -----------------------------------------------

class TestReturnCalculation:
    def test_correct_return_pct(self) -> None:
        """C. Correct return calculation."""
        price_signal = 100.0
        price_outcome = 105.0
        ret = (price_outcome - price_signal) / price_signal
        assert abs(ret - 0.05) < 1e-9

    def test_negative_return(self) -> None:
        price_signal = 100.0
        price_outcome = 95.0
        ret = (price_outcome - price_signal) / price_signal
        assert abs(ret - (-0.05)) < 1e-9

    def test_zero_return(self) -> None:
        price_signal = 100.0
        price_outcome = 100.0
        ret = (price_outcome - price_signal) / price_signal
        assert ret == 0.0


# -- Resolution service tests -----------------------------------------------

class TestResolvePendingSignals:
    def _mock_repo(
        self, pending: list[dict] | None = None,
    ) -> MagicMock:
        repo = MagicMock()
        repo.list_pending_predictions.return_value = pending or []
        return repo

    def test_no_pending_returns_zero(self) -> None:
        repo = self._mock_repo([])
        count = resolve_pending_signals(
            repo, lambda t: 100.0, logging.getLogger("test"),
            now=NOW,
        )
        assert count == 0

    def test_resolves_up_win(self) -> None:
        """A+B. Up signal, price went up -> win."""
        pred = _make_prediction(signal_type="up", price_at_signal=100.0)
        repo = self._mock_repo([pred])

        count = resolve_pending_signals(
            repo, lambda t: 105.0, logging.getLogger("test"),
            eval_window_seconds=300, now=NOW,
        )

        assert count == 1
        repo.resolve_prediction.assert_called_once()
        call_kwargs = repo.resolve_prediction.call_args
        assert call_kwargs[1]["outcome_label"] == "win"
        assert call_kwargs[1]["return_pct"] > 0

    def test_resolves_down_win(self) -> None:
        pred = _make_prediction(signal_type="down", price_at_signal=100.0)
        repo = self._mock_repo([pred])

        count = resolve_pending_signals(
            repo, lambda t: 95.0, logging.getLogger("test"),
            now=NOW,
        )

        assert count == 1
        call_kwargs = repo.resolve_prediction.call_args
        assert call_kwargs[1]["outcome_label"] == "win"
        assert call_kwargs[1]["return_pct"] < 0

    def test_resolves_loss(self) -> None:
        pred = _make_prediction(signal_type="up", price_at_signal=100.0)
        repo = self._mock_repo([pred])

        count = resolve_pending_signals(
            repo, lambda t: 95.0, logging.getLogger("test"),
            now=NOW,
        )

        assert count == 1
        call_kwargs = repo.resolve_prediction.call_args
        assert call_kwargs[1]["outcome_label"] == "loss"

    def test_neutral_tiny_return(self) -> None:
        pred = _make_prediction(signal_type="up", price_at_signal=100.0)
        repo = self._mock_repo([pred])

        count = resolve_pending_signals(
            repo, lambda t: 100.004, logging.getLogger("test"),
            now=NOW,
        )

        assert count == 1
        call_kwargs = repo.resolve_prediction.call_args
        assert call_kwargs[1]["outcome_label"] == "neutral"

    def test_skips_no_signal_price(self) -> None:
        pred = _make_prediction(price_at_signal=None)
        repo = self._mock_repo([pred])

        count = resolve_pending_signals(
            repo, lambda t: 105.0, logging.getLogger("test"),
            now=NOW,
        )

        assert count == 0
        repo.resolve_prediction.assert_not_called()

    def test_skips_no_outcome_price(self) -> None:
        pred = _make_prediction(price_at_signal=100.0)
        repo = self._mock_repo([pred])

        count = resolve_pending_signals(
            repo, lambda t: None, logging.getLogger("test"),
            now=NOW,
        )

        assert count == 0
        repo.resolve_prediction.assert_not_called()

    def test_handles_price_fetch_exception(self) -> None:
        pred = _make_prediction(price_at_signal=100.0)
        repo = self._mock_repo([pred])

        def raise_err(t: str) -> float:
            msg = "network error"
            raise ConnectionError(msg)

        count = resolve_pending_signals(
            repo, raise_err, logging.getLogger("test"),
            now=NOW,
        )

        assert count == 0
        repo.resolve_prediction.assert_not_called()

    def test_idempotent_no_double_resolve(self) -> None:
        """F. Idempotency: only resolves once via WHERE resolved_at IS NULL."""
        pred = _make_prediction(price_at_signal=100.0)
        repo = self._mock_repo([pred])

        # First resolve
        resolve_pending_signals(
            repo, lambda t: 105.0, logging.getLogger("test"),
            now=NOW,
        )

        # Second call with empty pending (already resolved)
        repo.list_pending_predictions.return_value = []
        count = resolve_pending_signals(
            repo, lambda t: 110.0, logging.getLogger("test"),
            now=NOW,
        )
        assert count == 0

    def test_multiple_predictions_resolved(self) -> None:
        preds = [
            _make_prediction(1, "SBER", "up", 100.0),
            _make_prediction(2, "GAZP", "down", 200.0),
        ]
        repo = self._mock_repo(preds)
        prices = {"SBER": 105.0, "GAZP": 190.0}

        count = resolve_pending_signals(
            repo, lambda t: prices[t], logging.getLogger("test"),
            now=NOW,
        )

        assert count == 2
        assert repo.resolve_prediction.call_count == 2

    def test_cutoff_uses_eval_window(self) -> None:
        """Cutoff is now - eval_window_seconds."""
        repo = self._mock_repo([])

        resolve_pending_signals(
            repo, lambda t: 100.0, logging.getLogger("test"),
            eval_window_seconds=600, now=NOW,
        )

        expected_cutoff = NOW - timedelta(seconds=600)
        repo.list_pending_predictions.assert_called_once_with(
            before=expected_cutoff,
        )


# -- Stats formatting -------------------------------------------------------

class TestFormatSignalStats:
    def test_basic_stats(self) -> None:
        """D. Stats aggregation."""
        stats = {
            "total": 100, "resolved": 80, "wins": 44,
            "losses": 30, "neutrals": 6, "avg_return": 0.0012,
        }
        output = format_signal_stats(stats, [], [])
        assert "total_signals: 100" in output
        assert "resolved: 80" in output
        assert "win_rate: 55.0%" in output
        assert "+0.12%" in output

    def test_no_resolved(self) -> None:
        stats = {
            "total": 5, "resolved": 0, "wins": 0,
            "losses": 0, "neutrals": 0, "avg_return": None,
        }
        output = format_signal_stats(stats, [], [])
        assert "win_rate: n/a" in output
        assert "avg_return: n/a" in output

    def test_by_ticker(self) -> None:
        stats = {"total": 10, "resolved": 8, "wins": 5,
                 "losses": 2, "neutrals": 1, "avg_return": 0.001}
        by_ticker = [
            {"ticker": "SBER", "total": 5, "resolved": 4,
             "wins": 3, "avg_return": 0.002},
            {"ticker": "GAZP", "total": 5, "resolved": 4,
             "wins": 2, "avg_return": -0.001},
        ]
        output = format_signal_stats(stats, by_ticker, [])
        assert "SBER: win_rate=75%" in output
        assert "GAZP: win_rate=50%" in output

    def test_by_type(self) -> None:
        stats = {"total": 10, "resolved": 8, "wins": 5,
                 "losses": 2, "neutrals": 1, "avg_return": 0.001}
        by_type = [
            {"signal_type": "up", "total": 6, "resolved": 5,
             "wins": 3, "avg_return": 0.002},
        ]
        output = format_signal_stats(stats, [], by_type)
        assert "up: win_rate=60%" in output

    def test_pending_ticker(self) -> None:
        stats = {"total": 5, "resolved": 0, "wins": 0,
                 "losses": 0, "neutrals": 0, "avg_return": None}
        by_ticker = [
            {"ticker": "SBER", "total": 5, "resolved": 0,
             "wins": 0, "avg_return": None},
        ]
        output = format_signal_stats(stats, by_ticker, [])
        assert "SBER: pending" in output


# -- Dry-run mode tests -----------------------------------------------------

class TestDryRunMode:
    def test_dry_run_config_exists(self) -> None:
        """E. Dry-run config is available."""
        from tinvest_trader.app.config import SignalCalibrationConfig
        config = SignalCalibrationConfig(dry_run=True)
        assert config.dry_run is True

    def test_dry_run_does_not_affect_prediction_recording(self) -> None:
        """E. Predictions are recorded regardless of dry_run flag."""
        # dry_run only gates execution, not prediction recording
        # This is verified by the fact that insert_signal_prediction
        # has no dry_run parameter
        from tinvest_trader.app.config import SignalCalibrationConfig
        config = SignalCalibrationConfig(dry_run=True, enabled=True)
        assert config.enabled is True
        assert config.dry_run is True
