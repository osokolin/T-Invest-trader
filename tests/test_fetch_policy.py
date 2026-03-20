"""Tests for T-Bank event fetch policy."""

import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from tinvest_trader.services.tbank_event_fetch_policy import (
    EligibleFetch,
    FetchPolicyConfig,
    select_eligible_fetches,
    should_fetch,
)


def _config(**overrides) -> FetchPolicyConfig:
    defaults = {
        "enabled": True,
        "dividends_ttl_seconds": 86400,
        "reports_ttl_seconds": 86400,
        "insider_deals_ttl_seconds": 86400,
        "failure_cooldown_seconds": 3600,
        "max_consecutive_failures": 5,
        "max_fetches_per_cycle": 0,
    }
    defaults.update(overrides)
    return FetchPolicyConfig(**defaults)


NOW = datetime(2026, 3, 20, 12, 0, 0, tzinfo=UTC)


# -- should_fetch tests --


def test_should_fetch_no_prior_state():
    """First fetch ever -- always allowed."""
    assert should_fetch(_config(), "dividends", None, NOW) is True


def test_should_fetch_ttl_not_expired():
    """Last success within TTL -- skip."""
    state = {
        "last_success_at": NOW - timedelta(hours=12),
        "last_error_at": None,
        "error_count": 0,
    }
    assert should_fetch(_config(), "dividends", state, NOW) is False


def test_should_fetch_ttl_expired():
    """Last success older than TTL -- allow."""
    state = {
        "last_success_at": NOW - timedelta(hours=25),
        "last_error_at": None,
        "error_count": 0,
    }
    assert should_fetch(_config(), "dividends", state, NOW) is True


def test_should_fetch_custom_ttl_per_event_type():
    """Each event type can have its own TTL."""
    cfg = _config(insider_deals_ttl_seconds=3600)
    state = {
        "last_success_at": NOW - timedelta(hours=2),
        "last_error_at": None,
        "error_count": 0,
    }
    assert should_fetch(cfg, "insider_deals", state, NOW) is True


def test_should_fetch_failure_cooldown_active():
    """Error with cooldown not yet elapsed -- skip."""
    state = {
        "last_success_at": None,
        "last_error_at": NOW - timedelta(minutes=30),
        "error_count": 2,
    }
    assert should_fetch(_config(), "dividends", state, NOW) is False


def test_should_fetch_failure_cooldown_elapsed():
    """Error with cooldown elapsed -- allow retry."""
    state = {
        "last_success_at": None,
        "last_error_at": NOW - timedelta(hours=2),
        "error_count": 2,
    }
    assert should_fetch(_config(), "dividends", state, NOW) is True


def test_should_fetch_max_failures_exceeded():
    """Max consecutive failures reached -- block permanently until reset."""
    state = {
        "last_success_at": None,
        "last_error_at": NOW - timedelta(hours=24),
        "error_count": 5,
    }
    assert should_fetch(_config(), "dividends", state, NOW) is False


def test_should_fetch_success_after_failures_resets():
    """When last_success_at exists and TTL expired, error_count is irrelevant."""
    state = {
        "last_success_at": NOW - timedelta(hours=25),
        "last_error_at": NOW - timedelta(minutes=10),
        "error_count": 3,
    }
    assert should_fetch(_config(), "dividends", state, NOW) is True


# -- select_eligible_fetches tests --


def test_select_eligible_no_repository():
    """Without repository, all pairs are eligible (no state to check)."""
    result = select_eligible_fetches(
        config=_config(),
        figis=("FIGI1", "FIGI2"),
        event_types=("dividends",),
        repository=None,
        now=NOW,
        logger=logging.getLogger("test"),
    )
    assert len(result) == 2
    assert result[0] == EligibleFetch(figi="FIGI1", event_type="dividends")
    assert result[1] == EligibleFetch(figi="FIGI2", event_type="dividends")


def test_select_eligible_filters_by_ttl():
    """Pairs with recent success are excluded."""
    repo = MagicMock()
    repo.get_all_fetch_states.return_value = [
        {
            "figi": "FIGI1",
            "event_type": "dividends",
            "last_checked_at": NOW - timedelta(hours=1),
            "last_success_at": NOW - timedelta(hours=1),
            "last_error_at": None,
            "error_count": 0,
        },
    ]

    result = select_eligible_fetches(
        config=_config(),
        figis=("FIGI1", "FIGI2"),
        event_types=("dividends",),
        repository=repo,
        now=NOW,
        logger=logging.getLogger("test"),
    )

    assert len(result) == 1
    assert result[0].figi == "FIGI2"


def test_select_eligible_max_fetches_cap():
    """Per-cycle cap limits total eligible fetches."""
    repo = MagicMock()
    repo.get_all_fetch_states.return_value = []

    result = select_eligible_fetches(
        config=_config(max_fetches_per_cycle=2),
        figis=("FIGI1", "FIGI2", "FIGI3"),
        event_types=("dividends",),
        repository=repo,
        now=NOW,
        logger=logging.getLogger("test"),
    )

    assert len(result) == 2


def test_select_eligible_db_error_allows_all():
    """If DB fetch fails, all pairs are eligible (fail-open)."""
    repo = MagicMock()
    repo.get_all_fetch_states.side_effect = RuntimeError("db down")

    result = select_eligible_fetches(
        config=_config(),
        figis=("FIGI1",),
        event_types=("dividends", "reports"),
        repository=repo,
        now=NOW,
        logger=logging.getLogger("test"),
    )

    assert len(result) == 2


# -- Integration with ingestion service --


def test_ingestion_service_records_fetch_state():
    """Verify ingestion service records success/failure in DB."""
    from tinvest_trader.services.broker_event_ingestion_service import (
        BrokerEventIngestionService,
    )

    client = MagicMock()
    client.get_instrument.return_value = {
        "figi": "FIGI1",
        "ticker": "SBER",
        "uid": "uid-1",
    }
    client.get_dividends.return_value = [
        {
            "record_date": "2026-03-15T00:00:00+00:00",
            "payment_date": "2026-03-20T00:00:00+00:00",
            "dividend_type": "Regular Cash",
            "dividend_net": {"currency": "RUB", "units": 10, "nano": 0},
        },
    ]
    repo = MagicMock()
    repo.insert_broker_event_raw.return_value = True
    repo.insert_broker_event_feature.return_value = True
    repo.get_all_fetch_states.return_value = []

    policy = _config()
    service = BrokerEventIngestionService(
        client=client,
        repository=repo,
        logger=logging.getLogger("test"),
        account_id="acc-1",
        tracked_figis=("FIGI1",),
        event_types=("dividends",),
        lookback_days_by_event_type={"dividends": 30},
        fetch_policy_config=policy,
    )

    service.ingest_all(as_of=NOW)

    repo.record_fetch_success.assert_called_once_with("FIGI1", "dividends", NOW)
    repo.record_fetch_failure.assert_not_called()


def test_ingestion_service_records_failure_on_exception():
    """When ingestion raises, record_fetch_failure is called."""
    from tinvest_trader.services.broker_event_ingestion_service import (
        BrokerEventIngestionService,
    )

    client = MagicMock()
    client.get_instrument.return_value = {
        "figi": "FIGI1",
        "ticker": "SBER",
        "uid": "uid-1",
    }
    client.get_dividends.side_effect = RuntimeError("api down")
    repo = MagicMock()
    repo.get_all_fetch_states.return_value = []

    policy = _config()
    service = BrokerEventIngestionService(
        client=client,
        repository=repo,
        logger=logging.getLogger("test"),
        account_id="acc-1",
        tracked_figis=("FIGI1",),
        event_types=("dividends",),
        lookback_days_by_event_type={"dividends": 30},
        fetch_policy_config=policy,
    )

    service.ingest_all(as_of=NOW)

    repo.record_fetch_failure.assert_called_once_with("FIGI1", "dividends", NOW)
    repo.record_fetch_success.assert_not_called()


def test_ingestion_service_skips_ineligible_pairs():
    """When policy says skip, no API call is made for that pair."""
    from tinvest_trader.services.broker_event_ingestion_service import (
        BrokerEventIngestionService,
    )

    client = MagicMock()
    client.get_instrument.return_value = {
        "figi": "FIGI1",
        "ticker": "SBER",
        "uid": "uid-1",
    }
    repo = MagicMock()
    # FIGI1/dividends was fetched 1 hour ago (TTL=24h) -> skip
    repo.get_all_fetch_states.return_value = [
        {
            "figi": "FIGI1",
            "event_type": "dividends",
            "last_checked_at": NOW - timedelta(hours=1),
            "last_success_at": NOW - timedelta(hours=1),
            "last_error_at": None,
            "error_count": 0,
        },
    ]

    policy = _config()
    service = BrokerEventIngestionService(
        client=client,
        repository=repo,
        logger=logging.getLogger("test"),
        account_id="acc-1",
        tracked_figis=("FIGI1",),
        event_types=("dividends",),
        lookback_days_by_event_type={"dividends": 30},
        fetch_policy_config=policy,
    )

    processed = service.ingest_all(as_of=NOW)

    assert processed == 0
    client.get_dividends.assert_not_called()


def test_ingestion_without_policy_fetches_everything():
    """Without policy config, all pairs are fetched (backward compat)."""
    from tinvest_trader.services.broker_event_ingestion_service import (
        BrokerEventIngestionService,
    )

    client = MagicMock()
    client.get_instrument.return_value = {
        "figi": "FIGI1",
        "ticker": "SBER",
        "uid": "uid-1",
    }
    client.get_dividends.return_value = [
        {
            "record_date": "2026-03-15T00:00:00+00:00",
            "payment_date": "2026-03-20T00:00:00+00:00",
            "dividend_type": "Regular Cash",
            "dividend_net": {"currency": "RUB", "units": 10, "nano": 0},
        },
    ]
    repo = MagicMock()
    repo.insert_broker_event_raw.return_value = True
    repo.insert_broker_event_feature.return_value = True

    service = BrokerEventIngestionService(
        client=client,
        repository=repo,
        logger=logging.getLogger("test"),
        account_id="acc-1",
        tracked_figis=("FIGI1",),
        event_types=("dividends",),
        lookback_days_by_event_type={"dividends": 30},
        # No fetch_policy_config
    )

    processed = service.ingest_all(as_of=NOW)

    assert processed == 1
    client.get_dividends.assert_called_once()
    # No state recording calls without policy
    repo.record_fetch_success.assert_not_called()
