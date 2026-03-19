"""Maps T-Bank API response dicts into internal domain models.

All broker-specific structure knowledge is isolated here.
Nothing outside infra/ should depend on broker payload shapes.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from tinvest_trader.domain.enums import CandleInterval, OrderSide, OrderStatus, TradingStatus
from tinvest_trader.domain.models import (
    BrokerEventFeature,
    BrokerOrder,
    Candle,
    Instrument,
    MarketSnapshot,
    MoneyValue,
)


def map_money_value(raw: dict) -> MoneyValue:
    """Map a broker money-value dict to MoneyValue."""
    return MoneyValue(
        currency=raw.get("currency", "RUB"),
        units=int(raw.get("units", 0)),
        nano=int(raw.get("nano", 0)),
    )


def map_instrument(raw: dict) -> Instrument:
    """Map a broker instrument dict to Instrument."""
    return Instrument(
        figi=raw["figi"],
        ticker=raw["ticker"],
        name=raw.get("name", ""),
    )


def map_trading_status(raw: str) -> TradingStatus:
    """Map a broker trading-status string to TradingStatus enum."""
    mapping = {
        "SECURITY_TRADING_STATUS_NORMAL_TRADING": TradingStatus.OPEN,
        "SECURITY_TRADING_STATUS_NOT_AVAILABLE_FOR_TRADING": TradingStatus.CLOSED,
        "SECURITY_TRADING_STATUS_OPENING_PERIOD": TradingStatus.PREMARKET,
    }
    return mapping.get(raw, TradingStatus.UNKNOWN)


def map_candle(raw: dict, interval: CandleInterval) -> Candle:
    """Map a broker candle dict to Candle."""
    return Candle(
        open=map_money_value(raw["open"]),
        high=map_money_value(raw["high"]),
        low=map_money_value(raw["low"]),
        close=map_money_value(raw["close"]),
        volume=int(raw.get("volume", 0)),
        time=_parse_timestamp(raw.get("time")),
        interval=interval,
    )


def map_market_snapshot(
    instrument: Instrument,
    last_price_raw: dict,
    trading_status_raw: str,
) -> MarketSnapshot:
    """Combine instrument, price, and status into a MarketSnapshot."""
    return MarketSnapshot(
        instrument=instrument,
        last_price=map_money_value(last_price_raw),
        trading_status=map_trading_status(trading_status_raw),
        time=datetime.now(UTC),
    )


def _parse_timestamp(value: str | None) -> datetime:
    """Parse an ISO timestamp string, falling back to now(UTC)."""
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if value is None:
        return datetime.now(UTC)
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return datetime.now(UTC)


def _parse_optional_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _quotation_to_float(raw: object) -> float | None:
    if not isinstance(raw, dict):
        return None
    units = raw.get("units")
    nano = raw.get("nano")
    if units is None and nano is None:
        return None
    return float(int(units or 0) + int(nano or 0) / 1_000_000_000)


def _resolve_event_time(raw: dict, fields: tuple[str, ...]) -> datetime | None:
    for field in fields:
        parsed = _parse_optional_timestamp(raw.get(field))
        if parsed is not None:
            return parsed
    return None


def _normalize_event_direction(source_method: str, raw: dict) -> str | None:
    if source_method != "GetInsiderDeals":
        return None

    raw_direction = str(raw.get("direction", "")).upper()
    mapping = {
        "TRADE_DIRECTION_BUY": "buy",
        "TRADE_DIRECTION_SELL": "sell",
    }
    return mapping.get(raw_direction)


def _build_event_uid(source_method: str, figi: str | None, raw: dict) -> str:
    if source_method == "GetDividends":
        components = {
            "figi": figi,
            "record_date": raw.get("record_date"),
            "payment_date": raw.get("payment_date"),
            "dividend_type": raw.get("dividend_type"),
            "dividend_net": raw.get("dividend_net"),
        }
    elif source_method == "GetAssetReports":
        components = {
            "figi": figi,
            "instrument_id": raw.get("instrument_id"),
            "report_date": raw.get("report_date"),
            "period_year": raw.get("period_year"),
            "period_num": raw.get("period_num"),
            "period_type": raw.get("period_type"),
        }
    elif source_method == "GetInsiderDeals":
        components = {
            "figi": figi,
            "instrument_uid": raw.get("instrument_uid"),
            "trade_id": raw.get("trade_id"),
            "date": raw.get("date"),
            "disclosure_date": raw.get("disclosure_date"),
            "direction": raw.get("direction"),
            "quantity": raw.get("quantity"),
            "price": raw.get("price"),
        }
    else:
        components = {"figi": figi, "raw": raw}

    digest = hashlib.sha256(
        json.dumps(components, sort_keys=True, default=str).encode("utf-8"),
    ).hexdigest()
    return f"{source_method}:{digest}"


def map_broker_event_feature(
    source_method: str,
    raw: dict,
    figi: str | None,
    ticker: str | None,
    account_id: str = "",
) -> BrokerEventFeature:
    """Map broker event payload into a normalized broker event feature."""
    if source_method == "GetDividends":
        event_time = _resolve_event_time(
            raw, ("record_date", "payment_date", "declared_date", "created_at"),
        )
        event_type = "dividend"
        event_value = _quotation_to_float(raw.get("dividend_net"))
        currency = (
            raw.get("dividend_net", {}).get("currency")
            if isinstance(raw.get("dividend_net"), dict)
            else None
        )
    elif source_method == "GetAssetReports":
        event_time = _resolve_event_time(raw, ("report_date", "created_at"))
        event_type = "report"
        event_value = None
        currency = None
    elif source_method == "GetInsiderDeals":
        event_time = _resolve_event_time(raw, ("date", "disclosure_date"))
        event_type = "insider_deal"
        event_value = _quotation_to_float(raw.get("price"))
        currency = raw.get("currency")
    else:
        raise ValueError(f"unsupported broker event source method: {source_method}")

    return BrokerEventFeature(
        account_id=account_id,
        source_method=source_method,
        figi=figi,
        ticker=ticker.upper() if ticker else None,
        event_uid=_build_event_uid(source_method, figi, raw),
        event_time=event_time,
        event_type=event_type,
        event_direction=_normalize_event_direction(source_method, raw),
        event_value=event_value,
        currency=currency,
    )


# -- Order mapping --


def map_order_status(raw: str) -> OrderStatus:
    """Map a broker execution-report status string to OrderStatus."""
    mapping = {
        "EXECUTION_REPORT_STATUS_NEW": OrderStatus.NEW,
        "EXECUTION_REPORT_STATUS_PARTIALLYFILL": OrderStatus.PARTIALLY_FILLED,
        "EXECUTION_REPORT_STATUS_FILL": OrderStatus.FILLED,
        "EXECUTION_REPORT_STATUS_CANCELLED": OrderStatus.CANCELLED,
        "EXECUTION_REPORT_STATUS_REJECTED": OrderStatus.REJECTED,
    }
    return mapping.get(raw, OrderStatus.REJECTED)


def map_order_direction(raw: str) -> OrderSide:
    """Map a broker order-direction string to OrderSide."""
    mapping = {
        "ORDER_DIRECTION_BUY": OrderSide.BUY,
        "ORDER_DIRECTION_SELL": OrderSide.SELL,
    }
    return mapping.get(raw, OrderSide.BUY)


def map_broker_order(raw: dict) -> BrokerOrder:
    """Map a broker order response dict to BrokerOrder."""
    return BrokerOrder(
        order_id=raw["order_id"],
        figi=raw.get("figi", ""),
        direction=map_order_direction(raw.get("direction", "")),
        quantity=int(raw.get("requested_quantity", 0)),
        filled_quantity=int(raw.get("filled_quantity", 0)),
        status=map_order_status(raw.get("status", "")),
        message=raw.get("message", ""),
    )
