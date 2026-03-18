"""Tests for sentiment/instrument_mapper.py -- ticker resolution and filtering."""

from tinvest_trader.sentiment.instrument_mapper import InstrumentMapper
from tinvest_trader.sentiment.models import TickerMention


def _make_mapper(
    ticker_to_figi: dict | None = None,
    tracked_tickers: frozenset | None = None,
) -> InstrumentMapper:
    return InstrumentMapper(
        ticker_to_figi=ticker_to_figi or {},
        tracked_tickers=tracked_tickers or frozenset(),
    )


def _mention(ticker: str = "SBER") -> TickerMention:
    return TickerMention(ticker=ticker, mention_type="hashtag")


def test_resolve_enriches_with_figi():
    mapper = _make_mapper(ticker_to_figi={"SBER": "BBG004730N88"})
    resolved = mapper.resolve(_mention("SBER"))
    assert resolved.figi == "BBG004730N88"
    assert resolved.ticker == "SBER"


def test_resolve_unknown_ticker_keeps_none_figi():
    mapper = _make_mapper(ticker_to_figi={})
    resolved = mapper.resolve(_mention("UNKNOWN"))
    assert resolved.figi is None


def test_is_relevant_tracked_ticker():
    mapper = _make_mapper(tracked_tickers=frozenset({"SBER", "GAZP"}))
    assert mapper.is_relevant(_mention("SBER")) is True
    assert mapper.is_relevant(_mention("GAZP")) is True


def test_is_relevant_untracked_ticker():
    mapper = _make_mapper(tracked_tickers=frozenset({"SBER"}))
    assert mapper.is_relevant(_mention("UNKNOWN")) is False


def test_is_relevant_empty_tracked_means_all():
    mapper = _make_mapper(tracked_tickers=frozenset())
    assert mapper.is_relevant(_mention("ANYTHING")) is True


def test_resolve_case_insensitive():
    mapper = _make_mapper(ticker_to_figi={"SBER": "BBG004730N88"})
    resolved = mapper.resolve(TickerMention(ticker="sber", mention_type="hashtag"))
    assert resolved.figi == "BBG004730N88"


def test_is_relevant_case_insensitive():
    mapper = _make_mapper(tracked_tickers=frozenset({"SBER"}))
    assert mapper.is_relevant(TickerMention(ticker="sber", mention_type="hashtag")) is True
