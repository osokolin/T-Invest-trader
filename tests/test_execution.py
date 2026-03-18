"""Tests for execution/engine.py -- order submission and error classification."""

import logging

from tinvest_trader.app.config import BrokerConfig
from tinvest_trader.domain.enums import OrderSide, OrderStatus, OrderType
from tinvest_trader.domain.models import ExecutionResult, OrderIntent
from tinvest_trader.execution.engine import ExecutionEngine
from tinvest_trader.infra.tbank.client import TBankClient


def _make_engine() -> ExecutionEngine:
    """Create an ExecutionEngine with a stub client."""
    client = TBankClient(
        config=BrokerConfig(),
        logger=logging.getLogger("test"),
    )
    return ExecutionEngine(
        client=client,
        logger=logging.getLogger("test"),
    )


def _make_intent(**kwargs) -> OrderIntent:
    """Create a test OrderIntent with defaults."""
    defaults = {
        "figi": "BBG000B9XRY4",
        "direction": OrderSide.BUY,
        "quantity": 5,
        "order_type": OrderType.MARKET,
    }
    defaults.update(kwargs)
    return OrderIntent(**defaults)


def test_submit_order_returns_execution_result():
    engine = _make_engine()
    intent = _make_intent()
    result = engine.submit_order(intent)
    assert isinstance(result, ExecutionResult)


def test_submit_order_success():
    engine = _make_engine()
    intent = _make_intent()
    result = engine.submit_order(intent)
    assert result.success is True
    assert result.broker_order is not None
    assert result.broker_order.status == OrderStatus.FILLED
    assert result.error == ""


def test_submit_order_preserves_figi():
    engine = _make_engine()
    intent = _make_intent(figi="BBG00TEST123")
    result = engine.submit_order(intent)
    assert result.broker_order is not None
    assert result.broker_order.figi == "BBG00TEST123"


def test_submit_order_preserves_quantity():
    engine = _make_engine()
    intent = _make_intent(quantity=42)
    result = engine.submit_order(intent)
    assert result.broker_order is not None
    assert result.broker_order.quantity == 42


def test_idempotency_key_present_in_intent():
    intent = _make_intent()
    assert intent.idempotency_key
    assert len(intent.idempotency_key) > 0


def test_idempotency_keys_are_unique():
    intent1 = _make_intent()
    intent2 = _make_intent()
    assert intent1.idempotency_key != intent2.idempotency_key


def test_is_retryable_success_is_false():
    result = ExecutionResult(success=True)
    assert ExecutionEngine.is_retryable(result) is False


def test_is_retryable_network_error_is_true():
    result = ExecutionResult(success=False, error="connection timeout")
    assert ExecutionEngine.is_retryable(result) is True


def test_is_retryable_rejected_is_false():
    from tinvest_trader.domain.models import BrokerOrder

    broker_order = BrokerOrder(
        order_id="x",
        figi="BBG000B9XRY4",
        direction=OrderSide.BUY,
        quantity=1,
        filled_quantity=0,
        status=OrderStatus.REJECTED,
        message="insufficient funds",
    )
    result = ExecutionResult(success=False, broker_order=broker_order, error="rejected")
    assert ExecutionEngine.is_retryable(result) is False
