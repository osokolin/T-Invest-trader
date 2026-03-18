"""Window parsing -- converts config strings like '5m', '1h' to durations."""

from __future__ import annotations

import re

from tinvest_trader.observation.models import ObservationWindow

_WINDOW_PATTERN = re.compile(r"^(\d+)([mhd])$")

_UNIT_SECONDS = {
    "m": 60,
    "h": 3600,
    "d": 86400,
}


def parse_window(label: str) -> ObservationWindow:
    """Parse a window string like '5m', '1h', '2d' into an ObservationWindow."""
    match = _WINDOW_PATTERN.match(label.strip().lower())
    if not match:
        raise ValueError(f"invalid window format: {label!r} (expected e.g. '5m', '1h', '2d')")
    amount = int(match.group(1))
    unit = match.group(2)
    return ObservationWindow(label=label.strip().lower(), seconds=amount * _UNIT_SECONDS[unit])


def parse_windows(labels: tuple[str, ...]) -> list[ObservationWindow]:
    """Parse multiple window strings."""
    return [parse_window(label) for label in labels]
