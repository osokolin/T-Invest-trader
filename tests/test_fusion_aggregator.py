from datetime import UTC, datetime

from tinvest_trader.fusion.aggregator import fuse_signals
from tinvest_trader.fusion.models import FusedSignalFeature
from tinvest_trader.observation.models import SignalObservation

NOW = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)


def _make_observation(**overrides):
    defaults = {
        "ticker": "SBER",
        "figi": None,
        "window": "5m",
        "observation_time": NOW,
        "message_count": 10,
        "positive_count": 5,
        "negative_count": 3,
        "neutral_count": 2,
        "positive_score_avg": 0.75,
        "negative_score_avg": 0.60,
        "neutral_score_avg": 0.50,
        "sentiment_balance": 0.15,
    }
    defaults.update(overrides)
    return SignalObservation(**defaults)


def _make_broker_event(source_method, event_time=None, event_value=None, currency=None):
    return {
        "source_method": source_method,
        "event_type": source_method,
        "event_direction": None,
        "event_value": event_value,
        "currency": currency,
        "event_time": event_time,
    }


def test_fuse_both_sides():
    obs = _make_observation()
    events = [
        _make_broker_event("dividends", NOW, 10.5, "RUB"),
        _make_broker_event("reports", NOW),
    ]
    result = fuse_signals(obs, events, "SBER", None, "5m", NOW)
    assert isinstance(result, FusedSignalFeature)
    assert result.sentiment_message_count == 10
    assert result.sentiment_positive_count == 5
    assert result.sentiment_balance == 0.15
    assert result.broker_dividends_count == 1
    assert result.broker_reports_count == 1
    assert result.broker_insider_deals_count == 0
    assert result.broker_total_event_count == 2
    assert result.broker_latest_dividend_value == 10.5
    assert result.broker_latest_dividend_currency == "RUB"


def test_fuse_no_observation():
    events = [
        _make_broker_event("insider_deals", NOW),
    ]
    result = fuse_signals(None, events, "GAZP", "FIGI1", "1h", NOW)
    assert result.sentiment_message_count is None
    assert result.sentiment_balance is None
    assert result.broker_insider_deals_count == 1
    assert result.broker_total_event_count == 1


def test_fuse_no_broker_events():
    obs = _make_observation(ticker="YNDX")
    result = fuse_signals(obs, [], "YNDX", None, "15m", NOW)
    assert result.sentiment_message_count == 10
    assert result.broker_dividends_count == 0
    assert result.broker_reports_count == 0
    assert result.broker_insider_deals_count == 0
    assert result.broker_total_event_count == 0
    assert result.broker_latest_dividend_value is None


def test_fuse_empty_both_sides():
    result = fuse_signals(None, [], "LKOH", None, "5m", NOW)
    assert result.sentiment_message_count is None
    assert result.broker_total_event_count == 0


def test_fuse_multiple_broker_events_latest_wins():
    t1 = datetime(2025, 6, 1, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 6, 1, 11, 0, 0, tzinfo=UTC)
    events = [
        _make_broker_event("dividends", t1, 5.0, "RUB"),
        _make_broker_event("dividends", t2, 8.0, "USD"),
    ]
    result = fuse_signals(None, events, "SBER", None, "1h", NOW)
    assert result.broker_dividends_count == 2
    assert result.broker_latest_dividend_value == 8.0
    assert result.broker_latest_dividend_currency == "USD"


def test_fuse_preserves_ticker_figi_window():
    result = fuse_signals(None, [], "VTBR", "FIGI_VTBR", "15m", NOW)
    assert result.ticker == "VTBR"
    assert result.figi == "FIGI_VTBR"
    assert result.window == "15m"
    assert result.observation_time == NOW
