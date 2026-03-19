"""Tests for broker event mapping into normalized broker event features."""

from datetime import UTC, datetime

from tinvest_trader.domain.models import BrokerEventFeature
from tinvest_trader.infra.tbank.mapper import map_broker_event_feature


def test_map_dividend_event_feature():
    raw = {
        "record_date": "2026-03-15T00:00:00+00:00",
        "payment_date": "2026-03-20T00:00:00+00:00",
        "dividend_type": "Regular Cash",
        "dividend_net": {"currency": "RUB", "units": 10, "nano": 500_000_000},
    }

    feature = map_broker_event_feature(
        source_method="GetDividends",
        raw=raw,
        figi="FIGI1",
        ticker="sber",
        account_id="acc-1",
    )

    assert isinstance(feature, BrokerEventFeature)
    assert feature.account_id == "acc-1"
    assert feature.event_type == "dividend"
    assert feature.ticker == "SBER"
    assert feature.currency == "RUB"
    assert feature.event_value == 10.5
    assert feature.event_direction is None
    assert feature.event_uid.startswith("GetDividends:")


def test_map_asset_report_feature():
    raw = {
        "instrument_id": "uid-1",
        "report_date": "2026-03-18T00:00:00+00:00",
        "period_year": 2025,
        "period_num": 4,
        "period_type": "ASSET_REPORT_PERIOD_TYPE_Q4",
    }

    feature = map_broker_event_feature(
        source_method="GetAssetReports",
        raw=raw,
        figi="FIGI1",
        ticker="GAZP",
    )

    assert feature.event_type == "report"
    assert feature.event_time == datetime(2026, 3, 18, 0, 0, tzinfo=UTC)
    assert feature.event_value is None
    assert feature.currency is None
    assert feature.event_uid.startswith("GetAssetReports:")


def test_map_insider_deal_feature():
    raw = {
        "trade_id": 1001,
        "direction": "TRADE_DIRECTION_SELL",
        "currency": "USD",
        "date": "2026-03-19T00:00:00+00:00",
        "price": {"units": 180, "nano": 250_000_000},
    }

    feature = map_broker_event_feature(
        source_method="GetInsiderDeals",
        raw=raw,
        figi="FIGI2",
        ticker="AAPL",
    )

    assert feature.event_type == "insider_deal"
    assert feature.event_direction == "sell"
    assert feature.event_value == 180.25
    assert feature.currency == "USD"
    assert feature.event_uid.startswith("GetInsiderDeals:")
