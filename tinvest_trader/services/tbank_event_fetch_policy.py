"""TTL-based selective fetch policy for broker event ingestion.

Decides which (figi, event_type) pairs are eligible for fetching based on
time since last successful fetch, error cooldown, and per-cycle caps.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tinvest_trader.infra.storage.repository import TradingRepository


@dataclass(frozen=True)
class FetchPolicyConfig:
    """Configuration for event fetch policy."""

    enabled: bool = True

    # TTL per event type: seconds since last successful fetch before re-fetch.
    dividends_ttl_seconds: int = 86400        # 24h
    reports_ttl_seconds: int = 86400          # 24h
    insider_deals_ttl_seconds: int = 86400    # 24h

    # After N consecutive failures, apply cooldown before retrying.
    failure_cooldown_seconds: int = 3600      # 1h
    max_consecutive_failures: int = 5

    # Max fetches per single ingest_all() cycle (0 = unlimited).
    max_fetches_per_cycle: int = 0


def _ttl_for_event_type(config: FetchPolicyConfig, event_type: str) -> int:
    """Return TTL in seconds for the given event type."""
    ttls = {
        "dividends": config.dividends_ttl_seconds,
        "reports": config.reports_ttl_seconds,
        "insider_deals": config.insider_deals_ttl_seconds,
    }
    return ttls.get(event_type, config.dividends_ttl_seconds)


def should_fetch(
    config: FetchPolicyConfig,
    event_type: str,
    fetch_state: dict | None,
    now: datetime,
) -> bool:
    """Decide whether a single (figi, event_type) pair should be fetched.

    Returns True if:
    - No prior fetch state exists (never fetched before).
    - TTL has expired since last successful fetch.
    - Last fetch was a failure but cooldown has passed and max failures
      not exceeded.

    Returns False otherwise.
    """
    if fetch_state is None:
        return True

    ttl_seconds = _ttl_for_event_type(config, event_type)

    # Check TTL against last successful fetch
    last_success = fetch_state.get("last_success_at")
    if last_success is not None:
        elapsed = (now - last_success).total_seconds()
        return elapsed >= ttl_seconds

    # Never succeeded before. Check failure cooldown.
    error_count = fetch_state.get("error_count", 0)
    if error_count >= config.max_consecutive_failures:
        return False

    last_error = fetch_state.get("last_error_at")
    if last_error is not None:
        cooldown_elapsed = (now - last_error).total_seconds()
        if cooldown_elapsed < config.failure_cooldown_seconds:
            return False

    return True


@dataclass(frozen=True)
class EligibleFetch:
    """A (figi, event_type) pair that passed the policy check."""

    figi: str
    event_type: str


def select_eligible_fetches(
    config: FetchPolicyConfig,
    figis: tuple[str, ...],
    event_types: tuple[str, ...],
    repository: TradingRepository | None,
    now: datetime,
    logger: logging.Logger,
) -> list[EligibleFetch]:
    """Return the list of (figi, event_type) pairs eligible for fetching.

    Loads all fetch states from DB in one query, applies TTL checks,
    respects max_fetches_per_cycle cap.
    """
    # Load all fetch states in bulk
    states_by_key: dict[tuple[str, str], dict] = {}
    if repository is not None:
        try:
            all_states = repository.get_all_fetch_states()
            for state in all_states:
                key = (state["figi"], state["event_type"])
                states_by_key[key] = state
        except Exception:
            logger.exception(
                "fetch policy: failed to load fetch states, allowing all",
                extra={"component": "fetch_policy"},
            )

    eligible: list[EligibleFetch] = []
    skipped_ttl = 0
    skipped_cooldown = 0

    for event_type in event_types:
        for figi in figis:
            state = states_by_key.get((figi, event_type))
            if should_fetch(config, event_type, state, now):
                eligible.append(EligibleFetch(figi=figi, event_type=event_type))
            else:
                # Distinguish TTL skip from cooldown skip for logging
                if state and state.get("error_count", 0) >= config.max_consecutive_failures:
                    skipped_cooldown += 1
                else:
                    skipped_ttl += 1

    # Apply per-cycle cap
    cap = config.max_fetches_per_cycle
    capped = 0
    if cap > 0 and len(eligible) > cap:
        capped = len(eligible) - cap
        eligible = eligible[:cap]

    total_pairs = len(figis) * len(event_types)
    logger.info(
        "fetch policy selection complete",
        extra={
            "component": "fetch_policy",
            "total_pairs": total_pairs,
            "eligible": len(eligible),
            "skipped_ttl": skipped_ttl,
            "skipped_cooldown": skipped_cooldown,
            "capped": capped,
        },
    )
    return eligible
