"""Tests for sentiment/parser.py -- ticker extraction."""

from tinvest_trader.sentiment.parser import extract_tickers


def test_extract_hashtag():
    mentions = extract_tickers("#SBER растет на 5%")
    assert len(mentions) == 1
    assert mentions[0].ticker == "SBER"
    assert mentions[0].mention_type == "hashtag"


def test_extract_cashtag():
    mentions = extract_tickers("$GAZP падает после новостей")
    assert len(mentions) == 1
    assert mentions[0].ticker == "GAZP"
    assert mentions[0].mention_type == "cashtag"


def test_extract_multiple_tickers():
    mentions = extract_tickers("#SBER и #LKOH растут, $GAZP в боковике")
    tickers = {m.ticker for m in mentions}
    assert tickers == {"SBER", "LKOH", "GAZP"}


def test_extract_no_tickers():
    mentions = extract_tickers("Рынок сегодня без изменений")
    assert mentions == []


def test_normalize_to_uppercase():
    mentions = extract_tickers("#sber and $gazp")
    assert all(m.ticker == m.ticker.upper() for m in mentions)


def test_deduplicate_same_ticker():
    mentions = extract_tickers("#SBER отлично, еще раз #SBER")
    assert len(mentions) == 1
    assert mentions[0].ticker == "SBER"


def test_deduplicate_hash_and_cash():
    """Same ticker as hashtag and cashtag should appear only once."""
    mentions = extract_tickers("#SBER и потом $SBER")
    assert len(mentions) == 1
    assert mentions[0].ticker == "SBER"


def test_ticker_length_bounds():
    """Tickers must be 2-6 characters."""
    mentions = extract_tickers("#A too short, #TOOLONGXX too long, #OK fine")
    tickers = {m.ticker for m in mentions}
    assert "A" not in tickers
    assert "TOOLONGXX" not in tickers
    assert "OK" in tickers
