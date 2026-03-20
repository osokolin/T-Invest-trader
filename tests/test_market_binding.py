"""Tests for market binding -- deterministic instrument selection."""

from __future__ import annotations

from tinvest_trader.services.market_binding import (
    BindingConfig,
    BindingStatus,
    bind_market,
    format_binding_debug,
    normalize_direction,
    normalize_ticker,
    normalize_window,
    score_candidates,
    validate_candidate,
)

# -- Fixtures ---------------------------------------------------------------

def _make_instrument(
    ticker: str,
    figi: str = "",
    name: str = "",
    enabled: bool = True,
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
        "tracked": True,
    }


INSTRUMENTS = [
    _make_instrument("SBER", figi="BBG004730N88", name="Sberbank"),
    _make_instrument("GAZP", figi="BBG004730RP0", name="Gazprom"),
    _make_instrument("YNDX", figi="BBG006L8G4H1", name="Yandex"),
    _make_instrument("LKOH", figi="BBG004731032", name="LUKOIL"),
    _make_instrument("OZON", figi="BBG00Y91R9T3", name="Ozon Holdings"),
]


# -- Normalization tests -----------------------------------------------------

class TestNormalizeTicker:
    def test_uppercase(self) -> None:
        assert normalize_ticker("sber") == "SBER"

    def test_strip(self) -> None:
        assert normalize_ticker("  GAZP  ") == "GAZP"

    def test_remove_spaces(self) -> None:
        assert normalize_ticker("S B E R") == "SBER"

    def test_empty(self) -> None:
        assert normalize_ticker("") == ""

    def test_already_normalized(self) -> None:
        assert normalize_ticker("YNDX") == "YNDX"


class TestNormalizeDirection:
    def test_up_variants(self) -> None:
        for word in ("up", "rise", "buy", "long", "bull", "bullish", "UP", "Rise"):
            assert normalize_direction(word) == "up", f"failed for {word}"

    def test_down_variants(self) -> None:
        for word in ("down", "fall", "sell", "short", "bear", "bearish", "DOWN"):
            assert normalize_direction(word) == "down", f"failed for {word}"

    def test_unknown_passthrough(self) -> None:
        assert normalize_direction("sideways") == "sideways"


class TestNormalizeWindow:
    def test_canonical(self) -> None:
        assert normalize_window("5m") == "5m"

    def test_alias_5min(self) -> None:
        assert normalize_window("5min") == "5m"

    def test_alias_60m(self) -> None:
        assert normalize_window("60m") == "1h"

    def test_alias_day(self) -> None:
        assert normalize_window("day") == "1d"

    def test_unknown_passthrough(self) -> None:
        assert normalize_window("3h") == "3h"

    def test_case_insensitive(self) -> None:
        assert normalize_window("Daily") == "1d"


# -- Scoring tests -----------------------------------------------------------

class TestScoreCandidates:
    def test_exact_match(self) -> None:
        scores = score_candidates("SBER", INSTRUMENTS)
        assert len(scores) == 1
        assert scores[0].ticker == "SBER"
        assert scores[0].score == 1.0
        assert "exact_ticker_match" in scores[0].reasons

    def test_no_match(self) -> None:
        scores = score_candidates("NONEXISTENT", INSTRUMENTS)
        assert scores == []

    def test_empty_query(self) -> None:
        scores = score_candidates("", INSTRUMENTS)
        assert scores == []

    def test_placeholder_figi_skipped(self) -> None:
        instruments = [
            _make_instrument("SBER", figi="TICKER:SBER"),
            _make_instrument("SBER", figi="BBG004730N88"),
        ]
        scores = score_candidates("SBER", instruments)
        assert len(scores) == 1
        assert scores[0].figi == "BBG004730N88"

    def test_prefix_match_lower_score(self) -> None:
        instruments = [
            _make_instrument("SBER", figi="BBG004730N88"),
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ]
        scores = score_candidates("SBER", instruments)
        assert len(scores) == 2
        assert scores[0].ticker == "SBER"
        assert scores[0].score == 1.0
        assert scores[1].ticker == "SBERP"
        assert scores[1].score == 0.3

    def test_deterministic_order(self) -> None:
        """Same scores produce stable ordering by ticker."""
        instruments = [
            _make_instrument("SBERP", figi="BBG0047315Y7"),
            _make_instrument("SBERA", figi="BBG0047315Y8"),
        ]
        scores = score_candidates("SBER", instruments)
        assert len(scores) == 2
        # Both prefix match at 0.3 -- sorted alphabetically
        assert scores[0].ticker == "SBERA"
        assert scores[1].ticker == "SBERP"


# -- Validation tests --------------------------------------------------------

class TestValidateCandidate:
    def test_valid_exact_match(self) -> None:
        scores = score_candidates("SBER", INSTRUMENTS)
        result = validate_candidate(
            scores[0], "SBER", INSTRUMENTS[0], BindingConfig(),
        )
        assert result.valid is True
        assert "exact_ticker" in result.checks_passed

    def test_ticker_mismatch_rejected(self) -> None:
        scores = score_candidates("SBER", [
            _make_instrument("SBER", figi="BBG004730N88"),
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ])
        # SBERP should fail exact ticker check
        result = validate_candidate(
            scores[1], "SBER", None, BindingConfig(),
        )
        assert result.valid is False
        assert "ticker_mismatch" in result.checks_failed

    def test_placeholder_figi_rejected(self) -> None:
        from tinvest_trader.services.market_binding import CandidateScore
        cand = CandidateScore(
            ticker="TEST", figi="TICKER:TEST", name="Test", score=1.0,
        )
        result = validate_candidate(cand, "TEST", None, BindingConfig())
        assert result.valid is False
        assert "placeholder_or_missing_figi" in result.checks_failed

    def test_low_score_rejected(self) -> None:
        from tinvest_trader.services.market_binding import CandidateScore
        cand = CandidateScore(
            ticker="SBER", figi="BBG004730N88", name="Sber", score=0.3,
        )
        result = validate_candidate(cand, "SBER", None, BindingConfig(min_score=0.5))
        assert result.valid is False
        assert any("score_below_threshold" in f for f in result.checks_failed)


# -- Binding engine tests ----------------------------------------------------

class TestBindMarket:
    def test_single_valid_match(self) -> None:
        """A. Single valid match -> matched."""
        result = bind_market("SBER", INSTRUMENTS)
        assert result.status == BindingStatus.MATCHED
        assert result.selected_ticker == "SBER"
        assert result.selected_figi == "BBG004730N88"
        assert len(result.reasons) > 0

    def test_no_candidates_no_match(self) -> None:
        """C. No candidates -> no_match."""
        result = bind_market("NONEXISTENT", INSTRUMENTS)
        assert result.status == BindingStatus.NO_MATCH
        assert result.selected_ticker is None
        assert "no_candidates_found" in result.reasons

    def test_empty_query_no_match(self) -> None:
        result = bind_market("", INSTRUMENTS)
        assert result.status == BindingStatus.NO_MATCH

    def test_placeholder_figi_no_match(self) -> None:
        """Placeholder FIGI instruments are filtered at scoring -> no_match."""
        instruments = [
            {"ticker": "SBER", "figi": "TICKER:SBER", "name": "Sber Placeholder"},
        ]
        result = bind_market("SBER", instruments)
        assert result.status == BindingStatus.NO_MATCH

    def test_candidate_fails_validation_rejected(self) -> None:
        """D. Candidate fails validation -> rejected (score below threshold)."""
        instruments = [
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ]
        # SBERP prefix-matches SBER with score 0.3, below default 0.5
        result = bind_market("SBER", instruments)
        assert result.status == BindingStatus.REJECTED

    def test_ticker_mismatch_rejection(self) -> None:
        """E. Ticker mismatch -> rejected when only prefix matches exist."""
        instruments = [
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ]
        result = bind_market("SBER", instruments)
        # SBERP has score 0.3 < 0.5 threshold -> rejected
        assert result.status == BindingStatus.REJECTED

    def test_multiple_valid_matches_ambiguous(self) -> None:
        """B. Multiple valid matches with close scores -> ambiguous."""
        # Two instruments with same ticker (simulating data quality issue)
        instruments = [
            _make_instrument("SBER", figi="BBG004730N88", name="Sber A"),
            _make_instrument("SBER", figi="BBG004730N89", name="Sber B"),
        ]
        result = bind_market("SBER", instruments, BindingConfig(min_gap=0.2))
        assert result.status == BindingStatus.AMBIGUOUS
        assert any("multiple_valid_candidates" in r for r in result.reasons)

    def test_gap_resolves_ambiguity(self) -> None:
        """Two candidates but gap is sufficient -> matched."""
        instruments = [
            _make_instrument("SBER", figi="BBG004730N88"),
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ]
        # SBER=1.0, SBERP=0.3 -- gap=0.7 > min_gap=0.2
        # But SBERP score 0.3 < min_score 0.5 -> only SBER valid
        result = bind_market("SBER", instruments)
        assert result.status == BindingStatus.MATCHED
        assert result.selected_ticker == "SBER"

    def test_confidence_threshold_behavior(self) -> None:
        """G. Low score candidates rejected by threshold."""
        instruments = [
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ]
        # SBERP prefix-matches SBER with score 0.3
        result = bind_market("SBER", instruments, BindingConfig(min_score=0.8))
        assert result.status == BindingStatus.REJECTED

    def test_result_contains_all_candidates(self) -> None:
        """Result always includes full candidate list for debugging."""
        instruments = [
            _make_instrument("SBER", figi="BBG004730N88"),
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ]
        result = bind_market("SBER", instruments)
        assert len(result.candidates) == 2

    def test_result_contains_validations(self) -> None:
        result = bind_market("SBER", INSTRUMENTS)
        assert len(result.validations) > 0

    def test_case_insensitive(self) -> None:
        result = bind_market("sber", INSTRUMENTS)
        assert result.status == BindingStatus.MATCHED
        assert result.selected_ticker == "SBER"

    def test_empty_instruments_no_match(self) -> None:
        result = bind_market("SBER", [])
        assert result.status == BindingStatus.NO_MATCH

    def test_no_exact_ticker_allows_prefix(self) -> None:
        """With require_exact_ticker=False, prefix matches can pass."""
        instruments = [
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ]
        config = BindingConfig(
            min_score=0.2,
            require_exact_ticker=False,
        )
        result = bind_market("SBER", instruments, config)
        assert result.status == BindingStatus.MATCHED
        assert result.selected_ticker == "SBERP"


# -- Debug output tests ------------------------------------------------------

class TestFormatBindingDebug:
    def test_output_shape_matched(self) -> None:
        """H. Debug output contains expected sections."""
        result = bind_market("SBER", INSTRUMENTS)
        output = format_binding_debug(result, "SBER")

        assert "market binding debug" in output
        assert "query: SBER" in output
        assert "status: matched" in output
        assert "selected_ticker: SBER" in output
        assert "selected_figi: BBG004730N88" in output
        assert "scoring:" in output
        assert "validation:" in output
        assert "reasons:" in output

    def test_output_shape_no_match(self) -> None:
        result = bind_market("NONEXISTENT", INSTRUMENTS)
        output = format_binding_debug(result, "NONEXISTENT")

        assert "status: no_match" in output
        assert "candidates: 0" in output

    def test_output_shape_rejected(self) -> None:
        instruments = [
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ]
        result = bind_market("SBER", instruments)
        output = format_binding_debug(result, "SBER")

        assert "status: rejected" in output
        assert "FAIL" in output

    def test_output_shape_ambiguous(self) -> None:
        instruments = [
            _make_instrument("SBER", figi="BBG004730N88"),
            _make_instrument("SBER", figi="BBG004730N89"),
        ]
        result = bind_market("SBER", instruments)
        output = format_binding_debug(result, "SBER")

        assert "status: ambiguous" in output


# -- Integration: binding is safe for execution ------------------------------

class TestBindingGateExecution:
    """Verify that only 'matched' status should proceed to execution."""

    def test_matched_is_safe(self) -> None:
        result = bind_market("SBER", INSTRUMENTS)
        assert result.status == BindingStatus.MATCHED
        # Only matched results have selected_figi
        assert result.selected_figi is not None

    def test_ambiguous_blocks_execution(self) -> None:
        instruments = [
            _make_instrument("SBER", figi="BBG004730N88"),
            _make_instrument("SBER", figi="BBG004730N89"),
        ]
        result = bind_market("SBER", instruments)
        assert result.status != BindingStatus.MATCHED

    def test_no_match_blocks_execution(self) -> None:
        result = bind_market("XXX", INSTRUMENTS)
        assert result.status != BindingStatus.MATCHED
        assert result.selected_figi is None

    def test_rejected_blocks_execution(self) -> None:
        instruments = [
            {"ticker": "SBER", "figi": "TICKER:SBER", "name": "placeholder"},
        ]
        result = bind_market("SBER", instruments)
        assert result.status != BindingStatus.MATCHED
        assert result.selected_figi is None
