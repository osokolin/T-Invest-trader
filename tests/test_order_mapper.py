"""Tests for order-related mapping in infra/tbank/mapper.py."""

from tinvest_trader.domain.enums import OrderSide, OrderStatus
from tinvest_trader.domain.models import BrokerOrder
from tinvest_trader.infra.tbank.mapper import (
    map_broker_order,
    map_order_direction,
    map_order_status,
)


def test_map_order_status_fill():
    assert map_order_status("EXECUTION_REPORT_STATUS_FILL") == OrderStatus.FILLED


def test_map_order_status_new():
    assert map_order_status("EXECUTION_REPORT_STATUS_NEW") == OrderStatus.NEW


def test_map_order_status_partial():
    assert (
        map_order_status("EXECUTION_REPORT_STATUS_PARTIALLYFILL") == OrderStatus.PARTIALLY_FILLED
    )


def test_map_order_status_cancelled():
    assert map_order_status("EXECUTION_REPORT_STATUS_CANCELLED") == OrderStatus.CANCELLED


def test_map_order_status_rejected():
    assert map_order_status("EXECUTION_REPORT_STATUS_REJECTED") == OrderStatus.REJECTED


def test_map_order_status_unknown_defaults_to_rejected():
    assert map_order_status("SOMETHING_UNKNOWN") == OrderStatus.REJECTED


def test_map_order_direction_buy():
    assert map_order_direction("ORDER_DIRECTION_BUY") == OrderSide.BUY


def test_map_order_direction_sell():
    assert map_order_direction("ORDER_DIRECTION_SELL") == OrderSide.SELL


def test_map_broker_order():
    raw = {
        "order_id": "abc-123",
        "figi": "BBG000B9XRY4",
        "direction": "ORDER_DIRECTION_BUY",
        "requested_quantity": 10,
        "filled_quantity": 10,
        "status": "EXECUTION_REPORT_STATUS_FILL",
        "message": "",
    }
    result = map_broker_order(raw)
    assert isinstance(result, BrokerOrder)
    assert result.order_id == "abc-123"
    assert result.direction == OrderSide.BUY
    assert result.quantity == 10
    assert result.filled_quantity == 10
    assert result.status == OrderStatus.FILLED
