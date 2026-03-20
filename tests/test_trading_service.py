"""Tests for TradingService execution gate via bind_signal()."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

from tinvest_trader.services.market_binding import BindingConfig, BindingStatus
from tinvest_trader.services.trading_service import TradingService


def _make_instrument(
    ticker: str,
    figi: str = "",
    name: str = "",
    enabled: bool = True,
    tracked: bool = True,
) -> dict:
    if not figi:
        figi = f"BBG00{ticker}001"
    if not name:
        name = f"{ticker} Corp"
    return {
        "ticker": ticker,
        "figi": figi,
        "name": name,
        "enabled": enabled,
        "tracked": tracked,
    }


INSTRUMENTS = [
    _make_instrument("SBER", figi="BBG004730N88", name="Sberbank"),
    _make_instrument("GAZP", figi="BBG004730RP0", name="Gazprom"),
    _make_instrument("YNDX", figi="BBG006L8G4H1", name="Yandex"),
]


def _service_with_instruments(instruments: list[dict]) -> TradingService:
    """Build TradingService with a mock repository returning given instruments."""
    repo = MagicMock()
    repo.list_tracked_instruments.return_value = instruments
    return TradingService(
        logger=logging.getLogger("test"),
        repository=repo,
    )


class TestResolveInstrument:
    """TradingService.resolve_instrument uses bind_signal(), not bind_market()."""

    def test_matched_single_instrument(self) -> None:
        """A. Execution proceeds when status == matched."""
        svc = _service_with_instruments(INSTRUMENTS)
        result = svc.resolve_instrument("SBER")
        assert result.status == BindingStatus.MATCHED
        assert result.selected_ticker == "SBER"
        assert result.selected_figi == "BBG004730N88"
        assert result.selected_candidate_id == "BBG004730N88"

    def test_no_match_blocks_execution(self) -> None:
        """B. No candidates -> execution blocked."""
        svc = _service_with_instruments(INSTRUMENTS)
        result = svc.resolve_instrument("NONEXISTENT")
        assert result.status == BindingStatus.NO_MATCH
        assert svc.is_execution_safe(result) is False

    def test_ambiguous_blocks_execution(self) -> None:
        """B. Multiple valid candidates -> execution blocked."""
        instruments = [
            _make_instrument("SBER", figi="BBG004730N88"),
            _make_instrument("SBER", figi="BBG004730N89"),
        ]
        svc = _service_with_instruments(instruments)
        result = svc.resolve_instrument("SBER")
        assert result.status == BindingStatus.AMBIGUOUS
        assert svc.is_execution_safe(result) is False

    def test_rejected_blocks_execution(self) -> None:
        """B. Placeholder FIGI -> no scored candidates -> no_match."""
        instruments = [
            {"ticker": "SBER", "figi": "TICKER:SBER", "name": "placeholder"},
        ]
        svc = _service_with_instruments(instruments)
        result = svc.resolve_instrument("SBER")
        assert result.status != BindingStatus.MATCHED
        assert svc.is_execution_safe(result) is False

    def test_prefix_match_rejected_by_default(self) -> None:
        """Prefix match has score 0.3 < threshold 0.5 -> rejected."""
        instruments = [_make_instrument("SBERP", figi="BBG0047315Y7")]
        svc = _service_with_instruments(instruments)
        result = svc.resolve_instrument("SBER")
        assert result.status == BindingStatus.REJECTED
        assert svc.is_execution_safe(result) is False

    def test_is_execution_safe_matched(self) -> None:
        """is_execution_safe returns True only for matched."""
        svc = _service_with_instruments(INSTRUMENTS)
        result = svc.resolve_instrument("GAZP")
        assert result.status == BindingStatus.MATCHED
        assert svc.is_execution_safe(result) is True

    def test_direction_and_window_passed(self) -> None:
        """Direction and window are accepted (no effect on v1 scoring)."""
        svc = _service_with_instruments(INSTRUMENTS)
        result = svc.resolve_instrument("YNDX", direction="buy", window="5min")
        assert result.status == BindingStatus.MATCHED
        assert result.selected_ticker == "YNDX"

    def test_case_insensitive(self) -> None:
        svc = _service_with_instruments(INSTRUMENTS)
        result = svc.resolve_instrument("sber")
        assert result.status == BindingStatus.MATCHED
        assert result.selected_ticker == "SBER"

    def test_empty_instruments_no_match(self) -> None:
        svc = _service_with_instruments([])
        result = svc.resolve_instrument("SBER")
        assert result.status == BindingStatus.NO_MATCH

    def test_repository_error_graceful(self) -> None:
        """If repository raises, binding gets empty candidates -> no_match."""
        repo = MagicMock()
        repo.list_tracked_instruments.side_effect = RuntimeError("db down")
        svc = TradingService(
            logger=logging.getLogger("test"),
            repository=repo,
        )
        result = svc.resolve_instrument("SBER")
        assert result.status == BindingStatus.NO_MATCH
        assert svc.is_execution_safe(result) is False

    def test_no_repository_no_match(self) -> None:
        """Without repository, binding gets empty candidates -> no_match."""
        svc = TradingService(logger=logging.getLogger("test"))
        result = svc.resolve_instrument("SBER")
        assert result.status == BindingStatus.NO_MATCH

    def test_result_has_candidates_and_validations(self) -> None:
        """Result includes full audit trail."""
        svc = _service_with_instruments(INSTRUMENTS)
        result = svc.resolve_instrument("SBER")
        assert len(result.candidates) >= 1
        assert len(result.validations) >= 1

    def test_custom_binding_config(self) -> None:
        """Custom config is respected."""
        instruments = [_make_instrument("SBERP", figi="BBG0047315Y7")]
        svc = TradingService(
            logger=logging.getLogger("test"),
            repository=MagicMock(
                list_tracked_instruments=MagicMock(return_value=instruments),
            ),
            binding_config=BindingConfig(
                min_score=0.2,
                require_exact_ticker=False,
            ),
        )
        result = svc.resolve_instrument("SBER")
        assert result.status == BindingStatus.MATCHED
        assert result.selected_ticker == "SBERP"


class TestExecutionGateContract:
    """Verify the execution gate contract holds."""

    def test_only_matched_has_figi(self) -> None:
        svc = _service_with_instruments(INSTRUMENTS)
        matched = svc.resolve_instrument("SBER")
        assert matched.selected_figi is not None

        no_match = svc.resolve_instrument("XXX")
        assert no_match.selected_figi is None

    def test_execution_gate_prevents_trade(self) -> None:
        """Simulate execution flow: gate blocks non-matched results."""
        svc = _service_with_instruments(INSTRUMENTS)

        # Good case: proceed
        result = svc.resolve_instrument("SBER")
        assert svc.is_execution_safe(result) is True

        # Bad case: block
        result = svc.resolve_instrument("NONEXISTENT")
        assert svc.is_execution_safe(result) is False
