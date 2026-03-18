"""Tests for instruments/registry.py -- tracking and trading eligibility."""

import logging

from tinvest_trader.instruments.registry import InstrumentRegistry


def _make_registry(
    tracked=("FIGI1", "FIGI2", "FIGI3"),
    enabled=("FIGI1",),
) -> InstrumentRegistry:
    return InstrumentRegistry(
        tracked=tracked,
        enabled=enabled,
        logger=logging.getLogger("test"),
    )


def test_is_tracked_returns_true():
    reg = _make_registry()
    assert reg.is_tracked("FIGI1") is True
    assert reg.is_tracked("FIGI2") is True


def test_is_tracked_returns_false():
    reg = _make_registry()
    assert reg.is_tracked("UNKNOWN") is False


def test_is_trade_enabled_returns_true():
    reg = _make_registry()
    assert reg.is_trade_enabled("FIGI1") is True


def test_is_trade_enabled_returns_false():
    reg = _make_registry()
    assert reg.is_trade_enabled("FIGI2") is False
    assert reg.is_trade_enabled("UNKNOWN") is False


def test_list_tracked():
    reg = _make_registry()
    assert reg.list_tracked() == ["FIGI1", "FIGI2", "FIGI3"]


def test_list_enabled():
    reg = _make_registry()
    assert reg.list_enabled() == ["FIGI1"]


def test_empty_registry():
    reg = _make_registry(tracked=(), enabled=())
    assert reg.list_tracked() == []
    assert reg.list_enabled() == []
    assert reg.is_tracked("FIGI1") is False
    assert reg.is_trade_enabled("FIGI1") is False


def test_enabled_not_subset_logs_warning(caplog):
    with caplog.at_level(logging.WARNING):
        _make_registry(tracked=("FIGI1",), enabled=("FIGI1", "FIGI_EXTRA"))
    assert "FIGI_EXTRA" in caplog.text


def test_enabled_not_subset_does_not_raise():
    """Registry should warn but not raise when enabled is not a subset of tracked."""
    reg = _make_registry(tracked=("FIGI1",), enabled=("FIGI1", "FIGI_EXTRA"))
    assert reg.is_trade_enabled("FIGI_EXTRA") is True
