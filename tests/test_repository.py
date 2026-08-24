"""Tests for infra/storage/repository.py -- audit trail write path."""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

from tinvest_trader.domain.enums import OrderSide, OrderStatus, OrderType, TradingStatus
from tinvest_trader.domain.models import (
    BrokerEventFeature,
    BrokerEventRaw,
    BrokerOrder,
    ExecutionResult,
    Instrument,
    MarketSnapshot,
    MoneyValue,
    OrderIntent,
)
from tinvest_trader.infra.storage.repository import TradingRepository


def _make_repo():
    """Create a TradingRepository with a mocked PostgresPool."""
    pool = MagicMock()
    conn = MagicMock()
    pool.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
    pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)
    logger = logging.getLogger("test")
    repo = TradingRepository(pool=pool, logger=logger)
    return repo, conn


def _make_snapshot() -> MarketSnapshot:
    return MarketSnapshot(
        instrument=Instrument(figi="BBG000B9XRY4", ticker="AAPL", name="Apple"),
        last_price=MoneyValue(currency="USD", units=150, nano=500_000_000),
        trading_status=TradingStatus.OPEN,
        time=datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC),
    )


def _make_intent(**kwargs) -> OrderIntent:
    defaults = {
        "figi": "BBG000B9XRY4",
        "direction": OrderSide.BUY,
        "quantity": 10,
        "order_type": OrderType.MARKET,
        "idempotency_key": "test-key-123",
    }
    defaults.update(kwargs)
    return OrderIntent(**defaults)


def test_insert_market_snapshot():
    repo, conn = _make_repo()
    snap = _make_snapshot()
    repo.insert_market_snapshot(snap)
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()
    args = conn.execute.call_args
    assert "market_snapshots" in args[0][0]
    params = args[0][1]
    assert params[0] == "BBG000B9XRY4"
    assert params[1] == "AAPL"


def test_insert_order_intent():
    repo, conn = _make_repo()
    intent = _make_intent()
    repo.insert_order_intent(intent, account_id="acc-1")
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()
    args = conn.execute.call_args
    assert "order_intents" in args[0][0]
    params = args[0][1]
    assert params[0] == "acc-1"
    assert params[1] == "BBG000B9XRY4"


def test_insert_order_intent_idempotent():
    repo, conn = _make_repo()
    intent = _make_intent()
    repo.insert_order_intent(intent)
    sql = conn.execute.call_args[0][0]
    assert "ON CONFLICT" in sql


def test_insert_paper_position_is_idempotent():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = (42,)
    now = datetime.now(tz=UTC)

    result = repo.insert_paper_position(
        portfolio_name="shadow-v1",
        prediction_id=1,
        ticker="SBER",
        direction="up",
        entry_price=250.0,
        entry_time=now,
        notional=100_000.0,
    )

    assert result == 42
    sql = conn.execute.call_args[0][0]
    assert "paper_portfolio_positions" in sql
    assert "ON CONFLICT" in sql


def test_insert_execution_event_success():
    repo, conn = _make_repo()
    intent = _make_intent()
    broker_order = BrokerOrder(
        order_id="ord-1",
        figi="BBG000B9XRY4",
        direction=OrderSide.BUY,
        quantity=10,
        filled_quantity=10,
        status=OrderStatus.FILLED,
    )
    result = ExecutionResult(success=True, broker_order=broker_order)
    repo.insert_execution_event(intent, result, account_id="acc-1")
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()
    args = conn.execute.call_args
    assert "execution_events" in args[0][0]
    params = args[0][1]
    assert params[0] == "acc-1"
    assert params[1] == "submission"
    assert params[3] is True  # success


def test_insert_execution_event_failure():
    repo, conn = _make_repo()
    intent = _make_intent()
    result = ExecutionResult(success=False, error="connection timeout")
    repo.insert_execution_event(intent, result, event_type="retry")
    args = conn.execute.call_args
    params = args[0][1]
    assert params[1] == "retry"
    assert params[3] is False  # success


def test_upsert_instrument():
    repo, conn = _make_repo()
    inst = Instrument(figi="BBG000B9XRY4", ticker="AAPL", name="Apple")
    repo.upsert_instrument(inst, tracked=True, enabled=False, instrument_uid="uid-1")
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()
    sql = conn.execute.call_args[0][0]
    assert "ON CONFLICT" in sql


def test_insert_broker_operation():
    repo, conn = _make_repo()
    repo.insert_broker_operation(
        account_id="acc-1",
        operation_type="buy",
        figi="BBG000B9XRY4",
        quantity=5,
        price=150.5,
        currency="USD",
        broker_operation_id="bop-1",
    )
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()
    assert "broker_operations" in conn.execute.call_args[0][0]


def test_insert_portfolio_snapshot():
    repo, conn = _make_repo()
    now = datetime.now(tz=UTC)
    repo.insert_portfolio_snapshot(
        account_id="acc-1",
        snapshot_time=now,
        total_value=100_000.0,
        currency="RUB",
    )
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()
    assert "portfolio_snapshots" in conn.execute.call_args[0][0]


def test_insert_position_snapshot():
    repo, conn = _make_repo()
    now = datetime.now(tz=UTC)
    repo.insert_position_snapshot(
        account_id="acc-1",
        figi="BBG000B9XRY4",
        quantity=10,
        snapshot_time=now,
        average_price=150.5,
    )
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()
    assert "position_snapshots" in conn.execute.call_args[0][0]


def test_insert_broker_event_raw():
    repo, conn = _make_repo()
    cur = MagicMock()
    cur.fetchone.return_value = (1,)
    conn.execute.return_value = cur

    inserted = repo.insert_broker_event_raw(
        BrokerEventRaw(
            account_id="acc-1",
            source_method="GetDividends",
            figi="FIGI1",
            ticker="SBER",
            event_uid="event-1",
            event_time=datetime(2026, 3, 19, tzinfo=UTC),
            payload={"record_date": "2026-03-19T00:00:00+00:00"},
        ),
    )

    assert inserted is True
    conn.commit.assert_called_once()
    assert "broker_event_raw" in conn.execute.call_args[0][0]


def test_insert_broker_event_feature():
    repo, conn = _make_repo()
    cur = MagicMock()
    cur.fetchone.return_value = (1,)
    conn.execute.return_value = cur

    inserted = repo.insert_broker_event_feature(
        BrokerEventFeature(
            account_id="acc-1",
            source_method="GetDividends",
            figi="FIGI1",
            ticker="SBER",
            event_uid="event-1",
            event_time=datetime(2026, 3, 19, tzinfo=UTC),
            event_type="dividend",
            event_value=10.5,
            currency="RUB",
        ),
    )

    assert inserted is True
    conn.commit.assert_called_once()
    assert "broker_event_features" in conn.execute.call_args[0][0]


def test_fetch_latest_broker_event_time():
    repo, conn = _make_repo()
    now = datetime(2026, 3, 19, tzinfo=UTC)
    cur = MagicMock()
    cur.fetchone.return_value = (now,)
    conn.execute.return_value = cur

    latest = repo.fetch_latest_broker_event_time(
        source_method="GetDividends",
        figi="FIGI1",
        account_id="acc-1",
    )

    assert latest == now
    assert "broker_event_features" in conn.execute.call_args[0][0]


def test_find_primary_sentiment_source():
    repo, conn = _make_repo()
    cur = MagicMock()
    cur.fetchone.return_value = ("markettwits", "123", 456)
    conn.execute.return_value = cur
    observation_time = datetime(2026, 3, 23, 10, 0, tzinfo=UTC)

    result = repo.find_primary_sentiment_source(
        ticker="SBER",
        figi="BBG004730N88",
        observation_time=observation_time,
        window="15m",
    )

    assert result == {
        "source_channel": "markettwits",
        "source_message_id": "123",
        "source_message_db_id": 456,
    }
    sql, params = conn.execute.call_args.args
    assert "telegram_sentiment_events" in sql
    assert "ranked_channels" in sql
    assert "scored_at >=" in sql
    assert params == ("BBG004730N88", observation_time, 900, observation_time)


def test_find_primary_sentiment_source_returns_none_without_events():
    repo, conn = _make_repo()
    cur = MagicMock()
    cur.fetchone.return_value = None
    conn.execute.return_value = cur

    result = repo.find_primary_sentiment_source(
        ticker="SBER",
        figi=None,
        observation_time=datetime(2026, 3, 23, 10, 0, tzinfo=UTC),
        window="1h",
    )

    assert result is None
    sql, params = conn.execute.call_args.args
    assert "ticker = %s" in sql
    assert params[0] == "SBER"


def test_fetch_operational_summary():
    repo, conn = _make_repo()
    cur = MagicMock()
    cur.fetchone.return_value = (1, 2, 3, 4, 5)
    conn.execute.return_value = cur

    summary = repo.fetch_operational_summary()

    conn.execute.assert_called_once()
    assert summary == {
        "telegram_messages_raw": 1,
        "telegram_message_mentions": 2,
        "telegram_sentiment_events": 3,
        "signal_observations": 4,
        "market_snapshots": 5,
    }


def test_list_market_activity_spikes_filters_completed_horizons():
    repo, conn = _make_repo()
    cur = MagicMock()
    cur.fetchall.return_value = []
    conn.execute.return_value = cur
    since = datetime(2026, 8, 1, tzinfo=UTC)

    result = repo.list_market_activity_spikes_for_outcomes(
        since=since,
        limit=100,
        horizons=("5m", "15m", "eod"),
    )

    assert result == []
    sql, params = conn.execute.call_args.args
    assert "market_activity_spike_outcomes" in sql
    assert "count(DISTINCT r.horizon)" in sql
    assert params == (since, ["5m", "15m", "eod"], 3, 100)


def test_insert_market_activity_spike_outcome_is_idempotent():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = (42,)
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)

    inserted = repo.insert_market_activity_spike_outcome({
        "spike_id": 7,
        "ticker": "SBER",
        "figi": "BBG004730N88",
        "spike_time": now,
        "horizon": "5m",
        "direction": "up",
        "entry_price": 100.0,
        "outcome_price": 101.0,
        "raw_return_pct": 0.01,
        "momentum_return_pct": 0.01,
        "reversion_return_pct": -0.01,
        "momentum_outcome": "win",
        "reversion_outcome": "loss",
        "outcome_time": now,
        "resolved_at": now,
    })

    assert inserted is True
    sql = conn.execute.call_args.args[0]
    assert "market_activity_spike_outcomes" in sql
    assert "ON CONFLICT (spike_id, horizon) DO NOTHING" in sql
