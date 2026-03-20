from datetime import UTC, date, datetime, timedelta

from tinvest_trader.fusion.aggregator import _enrich_moex, fuse_signals
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


# --- Long windows ---


def test_fuse_long_window_with_broker_events():
    """Broker events within a 30d window should be counted."""
    t_old = NOW - timedelta(days=10)
    events = [
        _make_broker_event("dividends", t_old, 7.5, "RUB"),
        _make_broker_event("reports", t_old - timedelta(days=5)),
    ]
    result = fuse_signals(None, events, "SBER", None, "30d", NOW)
    assert result.broker_dividends_count == 1
    assert result.broker_reports_count == 1
    assert result.broker_total_event_count == 2
    assert result.broker_latest_dividend_value == 7.5


def test_fuse_long_window_sentiment_only():
    """Long window with sentiment but no broker events still works."""
    obs = _make_observation(window="1d")
    result = fuse_signals(obs, [], "SBER", None, "1d", NOW)
    assert result.sentiment_message_count == 10
    assert result.broker_total_event_count == 0


# --- Real DB source_method values ---


def test_fuse_getdividends_method():
    """Aggregator should recognize GetDividends (actual DB source_method)."""
    events = [_make_broker_event("GetDividends", NOW, 12.0, "RUB")]
    result = fuse_signals(None, events, "SBER", None, "7d", NOW)
    assert result.broker_dividends_count == 1
    assert result.broker_latest_dividend_value == 12.0


def test_fuse_getassetreports_method():
    events = [_make_broker_event("GetAssetReports", NOW)]
    result = fuse_signals(None, events, "SBER", None, "7d", NOW)
    assert result.broker_reports_count == 1


def test_fuse_getinsiderdeals_method():
    events = [_make_broker_event("GetInsiderDeals", NOW)]
    result = fuse_signals(None, events, "SBER", None, "7d", NOW)
    assert result.broker_insider_deals_count == 1


# --- Recency fields ---


def test_fuse_recency_fields_populated():
    """When recency dict is provided, recency fields should be set."""
    div_time = NOW - timedelta(days=5)
    rep_time = NOW - timedelta(days=30)
    recency = {
        "last_dividend_at": div_time,
        "last_report_at": rep_time,
        "last_insider_deal_at": None,
    }
    result = fuse_signals(None, [], "SBER", None, "5m", NOW, recency=recency)
    assert result.last_dividend_at == div_time
    assert result.last_report_at == rep_time
    assert result.last_insider_deal_at is None
    assert result.days_since_last_dividend == 5.0
    assert result.days_since_last_report == 30.0
    assert result.days_since_last_insider_deal is None


def test_fuse_recency_none_when_no_data():
    """Without recency dict, all recency fields should be None."""
    result = fuse_signals(None, [], "SBER", None, "5m", NOW)
    assert result.last_dividend_at is None
    assert result.last_report_at is None
    assert result.last_insider_deal_at is None
    assert result.days_since_last_dividend is None
    assert result.days_since_last_report is None
    assert result.days_since_last_insider_deal is None


def test_fuse_recency_empty_dict():
    """Empty recency dict should produce None recency fields."""
    result = fuse_signals(None, [], "SBER", None, "5m", NOW, recency={})
    assert result.last_dividend_at is None
    assert result.days_since_last_dividend is None


def test_fuse_days_since_fractional():
    """days_since_* should be fractional when event is partial-day old."""
    div_time = NOW - timedelta(hours=12)
    recency = {
        "last_dividend_at": div_time,
        "last_report_at": None,
        "last_insider_deal_at": None,
    }
    result = fuse_signals(None, [], "SBER", None, "1d", NOW, recency=recency)
    assert result.days_since_last_dividend == 0.5


def test_fuse_recency_independent_of_window_events():
    """Recency should be populated even when window broker events are empty."""
    div_time = NOW - timedelta(days=100)
    recency = {
        "last_dividend_at": div_time,
        "last_report_at": None,
        "last_insider_deal_at": None,
    }
    result = fuse_signals(None, [], "SBER", None, "5m", NOW, recency=recency)
    assert result.broker_dividends_count == 0  # no events in window
    assert result.last_dividend_at == div_time  # but recency is set
    assert result.days_since_last_dividend == 100.0


# --- MOEX market context ---


def _make_market_context(
    close=300.5,
    volume=1000000,
    num_trades=5000,
    trade_date=date(2025, 5, 30),
    high=305.0,
    low=295.0,
    previous_close=298.0,
):
    return {
        "latest": {
            "close": close,
            "volume": volume,
            "num_trades": num_trades,
            "trade_date": trade_date,
            "high": high,
            "low": low,
        },
        "previous_close": previous_close,
    }


def test_enrich_moex_full():
    """All MOEX fields should be populated from market context."""
    ctx = _make_market_context()
    result = _enrich_moex(ctx, NOW)
    assert result["moex_latest_close"] == 300.5
    assert result["moex_latest_volume"] == 1000000
    assert result["moex_latest_numtrades"] == 5000
    assert result["moex_last_trade_date"] == date(2025, 5, 30)
    assert result["moex_days_since_last_trade"] == 2.0  # June 1 - May 30
    # price change: (300.5 - 298.0) / 298.0 * 100 = 0.8389...
    assert abs(result["moex_price_change_1d_pct"] - 0.8389) < 0.01
    # range: (305.0 - 295.0) / 295.0 * 100 = 3.3898...
    assert abs(result["moex_range_pct"] - 3.3898) < 0.01


def test_enrich_moex_none():
    """None market_context should produce all-None MOEX fields."""
    result = _enrich_moex(None, NOW)
    assert result["moex_latest_close"] is None
    assert result["moex_latest_volume"] is None
    assert result["moex_last_trade_date"] is None
    assert result["moex_days_since_last_trade"] is None
    assert result["moex_price_change_1d_pct"] is None
    assert result["moex_range_pct"] is None


def test_enrich_moex_empty_latest():
    """Empty latest dict should produce all-None MOEX fields."""
    result = _enrich_moex({"latest": None, "previous_close": None}, NOW)
    assert result["moex_latest_close"] is None


def test_enrich_moex_no_previous_close():
    """Without previous_close, price_change_1d_pct should be None."""
    ctx = _make_market_context(previous_close=None)
    result = _enrich_moex(ctx, NOW)
    assert result["moex_latest_close"] == 300.5
    assert result["moex_price_change_1d_pct"] is None


def test_enrich_moex_zero_previous_close():
    """Zero previous_close should not cause division by zero."""
    ctx = _make_market_context(previous_close=0)
    result = _enrich_moex(ctx, NOW)
    assert result["moex_price_change_1d_pct"] is None


def test_enrich_moex_zero_low():
    """Zero low should not cause division by zero for range_pct."""
    ctx = _make_market_context(low=0, high=10.0)
    result = _enrich_moex(ctx, NOW)
    assert result["moex_range_pct"] is None


def test_fuse_with_market_context():
    """fuse_signals should populate MOEX fields from market_context."""
    ctx = _make_market_context()
    result = fuse_signals(
        None, [], "SBER", None, "5m", NOW, market_context=ctx,
    )
    assert result.moex_latest_close == 300.5
    assert result.moex_latest_volume == 1000000
    assert result.moex_latest_numtrades == 5000
    assert result.moex_last_trade_date == date(2025, 5, 30)
    assert result.moex_days_since_last_trade == 2.0


def test_fuse_without_market_context():
    """Without market_context, MOEX fields should be None."""
    result = fuse_signals(None, [], "SBER", None, "5m", NOW)
    assert result.moex_latest_close is None
    assert result.moex_latest_volume is None
    assert result.moex_last_trade_date is None
    assert result.moex_days_since_last_trade is None
    assert result.moex_price_change_1d_pct is None
    assert result.moex_range_pct is None


def test_fuse_moex_independent_of_sentiment_and_broker():
    """MOEX fields should be set even without sentiment or broker data."""
    ctx = _make_market_context(close=100.0, volume=500, num_trades=10)
    result = fuse_signals(None, [], "GAZP", None, "1h", NOW, market_context=ctx)
    assert result.sentiment_message_count is None
    assert result.broker_total_event_count == 0
    assert result.moex_latest_close == 100.0
    assert result.moex_latest_volume == 500
