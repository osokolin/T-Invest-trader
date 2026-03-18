"""Tests for portfolio/state.py -- position tracking."""

from tinvest_trader.domain.enums import OrderSide
from tinvest_trader.domain.models import Position
from tinvest_trader.portfolio.state import PortfolioState


def test_get_position_unknown_figi():
    state = PortfolioState()
    pos = state.get_position("BBG000B9XRY4")
    assert isinstance(pos, Position)
    assert pos.quantity == 0


def test_update_position_buy():
    state = PortfolioState()
    pos = state.update_position("BBG000B9XRY4", OrderSide.BUY, 10)
    assert pos.quantity == 10


def test_update_position_sell():
    state = PortfolioState()
    state.update_position("BBG000B9XRY4", OrderSide.BUY, 10)
    pos = state.update_position("BBG000B9XRY4", OrderSide.SELL, 3)
    assert pos.quantity == 7


def test_update_position_multiple_buys():
    state = PortfolioState()
    state.update_position("BBG000B9XRY4", OrderSide.BUY, 5)
    state.update_position("BBG000B9XRY4", OrderSide.BUY, 3)
    pos = state.get_position("BBG000B9XRY4")
    assert pos.quantity == 8


def test_all_positions_empty():
    state = PortfolioState()
    assert state.all_positions() == []


def test_all_positions_tracks_multiple():
    state = PortfolioState()
    state.update_position("FIGI_A", OrderSide.BUY, 10)
    state.update_position("FIGI_B", OrderSide.BUY, 5)
    positions = state.all_positions()
    assert len(positions) == 2
    figis = {p.figi for p in positions}
    assert figis == {"FIGI_A", "FIGI_B"}
