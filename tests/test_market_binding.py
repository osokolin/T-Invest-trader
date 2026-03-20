"""Tests for market binding -- deterministic instrument selection."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from tinvest_trader.services.market_binding import (
    BindingConfig,
    BindingSignal,
    BindingStatus,
    CandidateScore,
    MarketCandidate,
    bind_market,
    bind_signal,
    build_signal,
    candidates_from_instruments,
    format_binding_debug,
    is_market_open,
    normalize_direction,
    normalize_ticker,
    normalize_window,
    require_matched,
    score_candidates,
    score_market_candidates,
    validate_candidate,
    validate_market_candidate,
)

# -- Fixtures ---------------------------------------------------------------

NOW = datetime(2026, 3, 20, 12, 0, 0, tzinfo=UTC)


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


def _make_market(
    ticker: str,
    market_id: str = "",
    name: str = "",
    status: str = "open",
    close_time: datetime | None = None,
) -> MarketCandidate:
    if not market_id:
        market_id = f"BBG00{ticker}001"
    if not name:
        name = f"{ticker} Corp"
    return MarketCandidate(
        id=market_id,
        ticker=ticker,
        name=name,
        status=status,
        close_time=close_time,
    )


INSTRUMENTS = [
    _make_instrument("SBER", figi="BBG004730N88", name="Sberbank"),
    _make_instrument("GAZP", figi="BBG004730RP0", name="Gazprom"),
    _make_instrument("YNDX", figi="BBG006L8G4H1", name="Yandex"),
    _make_instrument("LKOH", figi="BBG004731032", name="LUKOIL"),
    _make_instrument("OZON", figi="BBG00Y91R9T3", name="Ozon Holdings"),
]

MARKETS = [
    _make_market("SBER", market_id="BBG004730N88", name="Sberbank"),
    _make_market("GAZP", market_id="BBG004730RP0", name="Gazprom"),
    _make_market("YNDX", market_id="BBG006L8G4H1", name="Yandex"),
    _make_market("LKOH", market_id="BBG004731032", name="LUKOIL"),
    _make_market("OZON", market_id="BBG00Y91R9T3", name="Ozon Holdings"),
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


class TestBuildSignal:
    def test_normalizes_all_fields(self) -> None:
        signal = build_signal("sber", direction="Buy", window="60min")
        assert signal.ticker == "SBER"
        assert signal.direction == "up"
        assert signal.window == "1h"

    def test_optional_fields_none(self) -> None:
        signal = build_signal("GAZP")
        assert signal.direction is None
        assert signal.window is None
        assert signal.figi_hint is None


# -- Market candidate helpers -----------------------------------------------

class TestIsMarketOpen:
    def test_open_status(self) -> None:
        m = _make_market("SBER", status="open")
        assert is_market_open(m) is True

    def test_closed_status(self) -> None:
        m = _make_market("SBER", status="closed")
        assert is_market_open(m) is False

    def test_expired_status(self) -> None:
        m = _make_market("SBER", status="expired")
        assert is_market_open(m) is False

    def test_unknown_status(self) -> None:
        m = _make_market("SBER", status="unknown")
        assert is_market_open(m) is True

    def test_close_time_in_future(self) -> None:
        m = _make_market(
            "SBER", status="open",
            close_time=NOW + timedelta(hours=1),
        )
        assert is_market_open(m, now=NOW) is True

    def test_close_time_in_past(self) -> None:
        m = _make_market(
            "SBER", status="open",
            close_time=NOW - timedelta(hours=1),
        )
        assert is_market_open(m, now=NOW) is False


class TestCandidatesFromInstruments:
    def test_converts_instruments(self) -> None:
        candidates = candidates_from_instruments(INSTRUMENTS)
        assert len(candidates) == 5
        assert candidates[0].ticker == "SBER"
        assert candidates[0].id == "BBG004730N88"

    def test_skips_placeholder_figi(self) -> None:
        instruments = [
            _make_instrument("SBER", figi="TICKER:SBER"),
            _make_instrument("GAZP", figi="BBG004730RP0"),
        ]
        candidates = candidates_from_instruments(instruments)
        assert len(candidates) == 1
        assert candidates[0].ticker == "GAZP"

    def test_skips_empty_ticker(self) -> None:
        instruments = [{"ticker": "", "figi": "BBG001", "name": "empty"}]
        candidates = candidates_from_instruments(instruments)
        assert len(candidates) == 0


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
        assert scores[0].ticker == "SBERA"
        assert scores[1].ticker == "SBERP"

    def test_candidate_id_set(self) -> None:
        scores = score_candidates("SBER", INSTRUMENTS)
        assert scores[0].candidate_id == "BBG004730N88"


class TestScoreMarketCandidates:
    def test_exact_match(self) -> None:
        signal = build_signal("SBER")
        scores = score_market_candidates(signal, MARKETS)
        assert len(scores) == 1
        assert scores[0].ticker == "SBER"
        assert scores[0].score == 1.0

    def test_no_match(self) -> None:
        signal = build_signal("XXX")
        scores = score_market_candidates(signal, MARKETS)
        assert scores == []

    def test_figi_hint_bonus(self) -> None:
        signal = build_signal("SBER", figi_hint="BBG004730N88")
        scores = score_market_candidates(signal, MARKETS)
        assert len(scores) == 1
        assert "figi_hint_match" in scores[0].reasons


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
        result = validate_candidate(
            scores[1], "SBER", None, BindingConfig(),
        )
        assert result.valid is False
        assert "ticker_mismatch" in result.checks_failed

    def test_placeholder_figi_rejected(self) -> None:
        cand = CandidateScore(
            ticker="TEST", figi="TICKER:TEST", name="Test", score=1.0,
        )
        result = validate_candidate(cand, "TEST", None, BindingConfig())
        assert result.valid is False
        assert "placeholder_or_missing_figi" in result.checks_failed

    def test_low_score_rejected(self) -> None:
        cand = CandidateScore(
            ticker="SBER", figi="BBG004730N88", name="Sber", score=0.3,
        )
        result = validate_candidate(cand, "SBER", None, BindingConfig(min_score=0.5))
        assert result.valid is False
        assert any("score_below_threshold" in f for f in result.checks_failed)


class TestValidateMarketCandidate:
    def test_valid_open_market(self) -> None:
        signal = build_signal("SBER")
        cand = CandidateScore(
            ticker="SBER", figi="BBG004730N88", name="Sber",
            score=1.0, candidate_id="BBG004730N88",
        )
        market = _make_market("SBER", market_id="BBG004730N88", status="open")
        result = validate_market_candidate(
            cand, signal, market, BindingConfig(), now=NOW,
        )
        assert result.valid is True
        assert "market_open" in result.checks_passed

    def test_closed_market_rejected(self) -> None:
        """E. Closed market -> rejected."""
        signal = build_signal("SBER")
        cand = CandidateScore(
            ticker="SBER", figi="BBG004730N88", name="Sber",
            score=1.0, candidate_id="BBG004730N88",
        )
        market = _make_market("SBER", market_id="BBG004730N88", status="closed")
        result = validate_market_candidate(
            cand, signal, market, BindingConfig(), now=NOW,
        )
        assert result.valid is False
        assert any("market_closed" in f for f in result.checks_failed)

    def test_expired_close_time_rejected(self) -> None:
        signal = build_signal("SBER")
        cand = CandidateScore(
            ticker="SBER", figi="BBG004730N88", name="Sber",
            score=1.0, candidate_id="BBG004730N88",
        )
        market = _make_market(
            "SBER", market_id="BBG004730N88", status="open",
            close_time=NOW - timedelta(hours=1),
        )
        result = validate_market_candidate(
            cand, signal, market, BindingConfig(), now=NOW,
        )
        assert result.valid is False

    def test_no_market_data_passes_soft(self) -> None:
        signal = build_signal("SBER")
        cand = CandidateScore(
            ticker="SBER", figi="BBG004730N88", name="Sber",
            score=1.0, candidate_id="BBG004730N88",
        )
        result = validate_market_candidate(
            cand, signal, None, BindingConfig(), now=NOW,
        )
        assert result.valid is True
        assert "market_status_unknown_soft" in result.checks_passed

    def test_market_open_check_skipped_when_disabled(self) -> None:
        signal = build_signal("SBER")
        cand = CandidateScore(
            ticker="SBER", figi="BBG004730N88", name="Sber",
            score=1.0, candidate_id="BBG004730N88",
        )
        market = _make_market("SBER", market_id="BBG004730N88", status="closed")
        config = BindingConfig(require_market_open=False)
        result = validate_market_candidate(
            cand, signal, market, config, now=NOW,
        )
        assert result.valid is True


# -- Legacy bind_market tests ------------------------------------------------

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
        result = bind_market("SBER", instruments)
        assert result.status == BindingStatus.REJECTED

    def test_ticker_mismatch_rejection(self) -> None:
        """E. Ticker mismatch -> rejected when only prefix matches exist."""
        instruments = [
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ]
        result = bind_market("SBER", instruments)
        assert result.status == BindingStatus.REJECTED

    def test_multiple_valid_matches_ambiguous(self) -> None:
        """B. Multiple valid matches with close scores -> ambiguous."""
        instruments = [
            _make_instrument("SBER", figi="BBG004730N88", name="Sber A"),
            _make_instrument("SBER", figi="BBG004730N89", name="Sber B"),
        ]
        result = bind_market("SBER", instruments, BindingConfig(min_gap=0.2))
        assert result.status == BindingStatus.AMBIGUOUS
        assert any("multiple_valid_candidates" in r for r in result.reasons)

    def test_gap_resolves_ambiguity(self) -> None:
        instruments = [
            _make_instrument("SBER", figi="BBG004730N88"),
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ]
        result = bind_market("SBER", instruments)
        assert result.status == BindingStatus.MATCHED
        assert result.selected_ticker == "SBER"

    def test_confidence_threshold_behavior(self) -> None:
        """G. Low score candidates rejected by threshold."""
        instruments = [
            _make_instrument("SBERP", figi="BBG0047315Y7"),
        ]
        result = bind_market("SBER", instruments, BindingConfig(min_score=0.8))
        assert result.status == BindingStatus.REJECTED

    def test_result_contains_all_candidates(self) -> None:
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


# -- Signal-based binding tests ----------------------------------------------

class TestBindSignal:
    def test_single_valid_match(self) -> None:
        """A. Single valid match -> matched."""
        signal = build_signal("SBER")
        result = bind_signal(signal, MARKETS, now=NOW)
        assert result.status == BindingStatus.MATCHED
        assert result.selected_ticker == "SBER"
        assert result.selected_figi == "BBG004730N88"
        assert result.selected_candidate_id == "BBG004730N88"

    def test_no_candidates_no_match(self) -> None:
        """C. No candidates -> no_match."""
        signal = build_signal("XXX")
        result = bind_signal(signal, MARKETS, now=NOW)
        assert result.status == BindingStatus.NO_MATCH
        assert "no_candidates_found" in result.reasons

    def test_empty_signal_ticker(self) -> None:
        signal = BindingSignal(ticker="")
        result = bind_signal(signal, MARKETS, now=NOW)
        assert result.status == BindingStatus.NO_MATCH

    def test_multiple_same_ticker_ambiguous(self) -> None:
        """B. Multiple valid candidates -> ambiguous."""
        markets = [
            _make_market("SBER", market_id="id1"),
            _make_market("SBER", market_id="id2"),
        ]
        signal = build_signal("SBER")
        result = bind_signal(signal, markets, now=NOW)
        assert result.status == BindingStatus.AMBIGUOUS

    def test_closed_market_rejected(self) -> None:
        """E. Closed market -> rejected."""
        markets = [
            _make_market("SBER", market_id="BBG004730N88", status="closed"),
        ]
        signal = build_signal("SBER")
        result = bind_signal(signal, markets, now=NOW)
        assert result.status == BindingStatus.REJECTED
        assert any("market_closed" in r for r in result.reasons)

    def test_expired_market_rejected(self) -> None:
        markets = [
            _make_market(
                "SBER", market_id="BBG004730N88", status="open",
                close_time=NOW - timedelta(hours=1),
            ),
        ]
        signal = build_signal("SBER")
        result = bind_signal(signal, markets, now=NOW)
        assert result.status == BindingStatus.REJECTED

    def test_score_below_threshold_rejected(self) -> None:
        """F. Score below threshold -> rejected."""
        markets = [_make_market("SBERP", market_id="BBG0047315Y7")]
        signal = build_signal("SBER")
        config = BindingConfig(min_score=0.8)
        result = bind_signal(signal, markets, config=config, now=NOW)
        assert result.status == BindingStatus.REJECTED

    def test_one_open_one_closed_matches_open(self) -> None:
        """Only open market passes validation."""
        markets = [
            _make_market("SBER", market_id="id_closed", status="closed"),
            _make_market("SBER", market_id="id_open", status="open"),
        ]
        signal = build_signal("SBER")
        result = bind_signal(signal, markets, now=NOW)
        assert result.status == BindingStatus.MATCHED
        assert result.selected_candidate_id == "id_open"

    def test_case_insensitive(self) -> None:
        signal = build_signal("sber")
        result = bind_signal(signal, MARKETS, now=NOW)
        assert result.status == BindingStatus.MATCHED

    def test_result_has_candidate_id(self) -> None:
        signal = build_signal("GAZP")
        result = bind_signal(signal, MARKETS, now=NOW)
        assert result.status == BindingStatus.MATCHED
        assert result.selected_candidate_id is not None

    def test_market_open_check_skipped(self) -> None:
        """With require_market_open=False, closed markets pass."""
        markets = [_make_market("SBER", market_id="id1", status="closed")]
        signal = build_signal("SBER")
        config = BindingConfig(require_market_open=False)
        result = bind_signal(signal, markets, config=config, now=NOW)
        assert result.status == BindingStatus.MATCHED


# -- Execution gate tests ---------------------------------------------------

class TestRequireMatched:
    def test_matched_returns_true(self) -> None:
        result = bind_market("SBER", INSTRUMENTS)
        assert require_matched(result) is True

    def test_no_match_returns_false(self) -> None:
        result = bind_market("XXX", INSTRUMENTS)
        assert require_matched(result) is False

    def test_ambiguous_returns_false(self) -> None:
        instruments = [
            _make_instrument("SBER", figi="BBG004730N88"),
            _make_instrument("SBER", figi="BBG004730N89"),
        ]
        result = bind_market("SBER", instruments)
        assert require_matched(result) is False

    def test_rejected_returns_false(self) -> None:
        instruments = [_make_instrument("SBERP", figi="BBG0047315Y7")]
        result = bind_market("SBER", instruments)
        assert require_matched(result) is False


class TestBindingGateExecution:
    """Verify that only 'matched' status should proceed to execution."""

    def test_matched_is_safe(self) -> None:
        result = bind_market("SBER", INSTRUMENTS)
        assert result.status == BindingStatus.MATCHED
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

    def test_signal_bind_closed_blocks_execution(self) -> None:
        """G. Execution is blocked when market is closed."""
        markets = [_make_market("SBER", market_id="id1", status="closed")]
        signal = build_signal("SBER")
        result = bind_signal(signal, markets, now=NOW)
        assert require_matched(result) is False


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

    def test_output_includes_candidate_id(self) -> None:
        """H. CLI debug output includes validation per candidate."""
        result = bind_market("SBER", INSTRUMENTS)
        output = format_binding_debug(result, "SBER")
        assert "selected_candidate_id:" in output

    def test_signal_bind_debug_output(self) -> None:
        signal = build_signal("SBER")
        result = bind_signal(signal, MARKETS, now=NOW)
        output = format_binding_debug(result, "SBER")
        assert "status: matched" in output
        assert "PASS" in output
