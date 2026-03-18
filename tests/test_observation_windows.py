"""Tests for observation/windows.py -- window parsing."""

import pytest

from tinvest_trader.observation.windows import parse_window, parse_windows


def test_parse_5m():
    w = parse_window("5m")
    assert w.label == "5m"
    assert w.seconds == 300


def test_parse_15m():
    w = parse_window("15m")
    assert w.label == "15m"
    assert w.seconds == 900


def test_parse_1h():
    w = parse_window("1h")
    assert w.label == "1h"
    assert w.seconds == 3600


def test_parse_2d():
    w = parse_window("2d")
    assert w.label == "2d"
    assert w.seconds == 172800


def test_parse_with_whitespace():
    w = parse_window("  5m  ")
    assert w.label == "5m"
    assert w.seconds == 300


def test_parse_uppercase():
    w = parse_window("5M")
    assert w.label == "5m"


def test_parse_invalid_raises():
    with pytest.raises(ValueError, match="invalid window format"):
        parse_window("abc")


def test_parse_invalid_unit_raises():
    with pytest.raises(ValueError, match="invalid window format"):
        parse_window("5x")


def test_parse_windows_multiple():
    windows = parse_windows(("5m", "15m", "1h"))
    assert len(windows) == 3
    assert windows[0].seconds == 300
    assert windows[1].seconds == 900
    assert windows[2].seconds == 3600


def test_parse_windows_empty():
    windows = parse_windows(())
    assert windows == []
