"""Portfolio state -- minimal local position tracking.

No persistence yet. Only what is required for execution validation
and position awareness.
"""

from __future__ import annotations

from tinvest_trader.domain.enums import OrderSide
from tinvest_trader.domain.models import Position


class PortfolioState:
    """In-memory portfolio state tracking positions by FIGI."""

    def __init__(self) -> None:
        self._positions: dict[str, Position] = {}

    def get_position(self, figi: str) -> Position:
        """Get current position for a FIGI. Returns zero-quantity if unknown."""
        return self._positions.get(figi, Position(figi=figi, quantity=0))

    def update_position(self, figi: str, side: OrderSide, filled_quantity: int) -> Position:
        """Update position after a fill. Returns the new position."""
        current = self.get_position(figi)
        if side == OrderSide.BUY:
            new_qty = current.quantity + filled_quantity
        else:
            new_qty = current.quantity - filled_quantity
        updated = Position(figi=figi, quantity=new_qty)
        self._positions[figi] = updated
        return updated

    def all_positions(self) -> list[Position]:
        """Return all tracked positions."""
        return list(self._positions.values())
