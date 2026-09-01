"""Tests for infra/storage/repository.py -- audit trail write path."""

import logging
from datetime import UTC, datetime, timedelta
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


def test_lists_both_paper_position_types_for_tariff_comparison():
    repo, conn = _make_repo()
    now = datetime.now(tz=UTC)
    conn.execute.return_value.fetchall.return_value = [
        ("shadow-v1", "signal", 100_000, 1_000, now),
        ("activity-v2", "activity", 20_000, -50, now),
    ]

    rows = repo.list_closed_paper_positions_for_tariff_comparison(now)

    sql, params = conn.execute.call_args.args
    assert "paper_portfolio_positions" in sql
    assert "activity_paper_positions" in sql
    assert params == (now, now)
    assert rows[0]["portfolio_type"] == "signal"
    assert rows[1]["portfolio_type"] == "activity"


def test_first_quote_after_uses_bounded_source_time_window():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = (101.5, datetime.now(tz=UTC))
    after = datetime(2026, 8, 24, 12, 5, tzinfo=UTC)
    not_after = after + timedelta(minutes=15)

    quote = repo.get_first_quote_after(
        "SBER",
        after,
        not_after=not_after,
    )

    assert quote is not None
    sql, params = conn.execute.call_args.args
    assert "source_time >= %s" in sql
    assert "source_time <= %s" in sql
    assert params == ("SBER", after, not_after)


def test_expire_stale_paper_positions_preserves_pnl():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchall.return_value = [(2,), (3,)]
    before = datetime(2026, 8, 24, 9, 0, tzinfo=UTC)
    expired_at = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)

    expired = repo.expire_stale_paper_positions(
        portfolio_name="shadow-v1",
        before=before,
        expired_at=expired_at,
    )

    assert expired == 2
    sql, params = conn.execute.call_args.args
    assert "status = 'expired'" in sql
    assert "NOT EXISTS" in sql
    assert "net_pnl" not in sql
    assert params == (expired_at, expired_at, "shadow-v1", before)


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


def test_insert_activity_paper_position_is_virtual_and_idempotent():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = (55,)
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)

    position_id = repo.insert_activity_paper_position({
        "portfolio_name": "activity-momentum-v1",
        "spike_id": 7,
        "strategy": "momentum",
        "horizon": "15m",
        "ticker": "SBER",
        "figi": "BBG004730N88",
        "spike_type": "volume_price",
        "severity": "high",
        "score": 80.0,
        "direction": "up",
        "entry_price": 100.0,
        "entry_time": now,
        "notional": 20_000.0,
    })

    assert position_id == 55
    sql = conn.execute.call_args.args[0]
    assert "activity_paper_positions" in sql
    assert "ON CONFLICT (portfolio_name, spike_id) DO NOTHING" in sql
    assert "order_intents" not in sql


def test_list_activity_paper_candidates_excludes_prior_decisions():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchall.return_value = []

    result = repo.list_activity_paper_entry_candidates("activity-momentum-v1")

    assert result == []
    sql, params = conn.execute.call_args.args
    assert "activity_paper_decisions" in sql
    assert "activity_paper_positions" in sql
    assert "LEFT JOIN LATERAL" in sql
    assert "confirmation_move_pct" in sql
    assert "s.candle_time >= portfolio.started_at" in sql
    assert params == ("activity-momentum-v1",)


def test_activity_paper_candidate_maps_following_confirmation_candle():
    repo, conn = _make_repo()
    spike_time = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    confirmation_time = spike_time + timedelta(minutes=1)
    conn.execute.return_value.fetchall.return_value = [(
        7, "SBER", "BBG004730N88", spike_time, "volume", "high", 80,
        100, 0.0, 6.0, confirmation_time, 100.2, 0.002, None,
    )]

    result = repo.list_activity_paper_entry_candidates(
        "activity-volume-confirmed-v1",
    )

    assert result == [{
        "spike_id": 7,
        "ticker": "SBER",
        "figi": "BBG004730N88",
        "entry_time": spike_time,
        "spike_type": "volume",
        "severity": "high",
        "score": 80.0,
        "entry_price": 100.0,
        "price_change_pct": 0.0,
        "volume_ratio": 6.0,
        "confirmation_time": confirmation_time,
        "confirmation_price": 100.2,
        "confirmation_move_pct": 0.002,
        "latest_entry_time": None,
    }]


def test_count_activity_paper_entries_since_uses_entry_time_boundary():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = (12,)
    since = datetime(2026, 8, 23, 21, 0, tzinfo=UTC)

    result = repo.count_activity_paper_entries_since(
        "activity-volume-confirmed-v2",
        since,
    )

    assert result == 12
    sql, params = conn.execute.call_args.args
    assert "activity_paper_positions" in sql
    assert "entry_time >= %s" in sql
    assert params == ("activity-volume-confirmed-v2", since)


def test_resolved_activity_positions_use_virtual_entry_price():
    repo, conn = _make_repo()
    now = datetime(2026, 8, 24, 12, 15, tzinfo=UTC)
    conn.execute.return_value.fetchall.return_value = [(
        9, 7, "SBER", "up", 10_000, 101.0, 0.008, now,
    )]

    result = repo.list_resolved_activity_paper_positions(
        "activity-volume-confirmed-v1",
    )

    assert result[0]["raw_return_pct"] == 0.008
    sql, params = conn.execute.call_args.args
    assert "o.outcome_price - p.entry_price" in sql
    assert params == ("activity-volume-confirmed-v1",)


def test_close_activity_paper_position_records_net_result():
    repo, conn = _make_repo()
    conn.execute.return_value.rowcount = 1
    now = datetime(2026, 8, 24, 12, 15, tzinfo=UTC)

    closed = repo.close_activity_paper_position(
        position_id=9,
        exit_price=101.0,
        exit_time=now,
        gross_return_pct=0.01,
        net_return_pct=0.008,
        gross_pnl=100.0,
        costs=20.0,
        net_pnl=80.0,
    )

    assert closed is True
    sql, params = conn.execute.call_args.args
    assert "activity_paper_positions" in sql
    assert "status = 'closed'" in sql
    assert params[-1] == 9


def test_expire_stale_activity_positions_preserves_pnl() -> None:
    repo, conn = _make_repo()
    conn.execute.return_value.fetchall.return_value = [(9,), (10,)]
    before = datetime(2026, 8, 24, 9, 0, tzinfo=UTC)
    expired_at = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)

    expired = repo.expire_stale_activity_paper_positions(
        portfolio_name="activity-momentum-v1",
        before=before,
        expired_at=expired_at,
    )

    assert expired == 2
    sql, params = conn.execute.call_args.args
    assert "status = 'expired'" in sql
    assert "NOT EXISTS" in sql
    assert "net_pnl" not in sql
    assert params == (
        expired_at,
        expired_at,
        "activity-momentum-v1",
        before,
    )


def test_activity_paper_summary_reports_expired_separately() -> None:
    repo, conn = _make_repo()
    started_at = datetime(2026, 8, 24, 9, 0, tzinfo=UTC)
    conn.execute.return_value.fetchone.return_value = (
        "activity-momentum-v1",
        "momentum",
        "15m",
        1_000_000,
        "RUB",
        started_at,
        1,
        10,
        2,
        20_000,
        125.0,
        6,
        0.001,
    )

    summary = repo.get_activity_paper_summary("activity-momentum-v1")

    assert summary is not None
    sql = conn.execute.call_args.args[0]
    assert "activity_paper_positions" in sql
    assert "position.status = 'expired'" in sql
    assert summary["open_positions"] == 1
    assert summary["closed_positions"] == 10
    assert summary["expired_positions"] == 2
    assert summary["realized_pnl"] == 125.0


def test_list_moex_daily_history_is_complete_and_chronological() -> None:
    repo, conn = _make_repo()
    first = datetime(2026, 8, 21, tzinfo=UTC).date()
    second = datetime(2026, 8, 24, tzinfo=UTC).date()
    conn.execute.return_value.fetchall.return_value = [
        (first, 100, 103, 99, 102, 1000),
        (second, 102, 105, 101, 104, 1500),
    ]

    bars = repo.list_moex_daily_history("sber", 120)

    sql, params = conn.execute.call_args.args
    assert "moex_market_history" in sql
    assert "open IS NOT NULL" in sql
    assert "ORDER BY trade_date ASC" in sql
    assert params == ("SBER", 120)
    assert bars[0]["trade_date"] == first
    assert bars[-1]["close"] == 104.0


def test_insert_medium_term_position_is_virtual_and_idempotent() -> None:
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = (73,)
    signal_date = datetime(2026, 8, 21, tzinfo=UTC).date()
    entry_date = datetime(2026, 8, 24, tzinfo=UTC).date()

    position_id = repo.insert_medium_term_position({
        "portfolio_name": "medium-term-staircase-v1",
        "strategy": "staircase",
        "ticker": "SBER",
        "signal_date": signal_date,
        "entry_date": entry_date,
        "entry_price": 100.0,
        "notional": 200_000.0,
        "atr_at_entry": 1.5,
        "initial_stop": 98.0,
    })

    assert position_id == 73
    sql = conn.execute.call_args.args[0]
    assert "medium_term_paper_positions" in sql
    assert "ON CONFLICT (portfolio_name, ticker, signal_date) DO NOTHING" in sql
    assert "order_intents" not in sql
    assert "execution_events" not in sql


def test_update_medium_term_position_audits_only_stop_raise() -> None:
    repo, conn = _make_repo()
    trade_date = datetime(2026, 8, 25, tzinfo=UTC).date()

    repo.update_medium_term_position_state(
        position_id=73,
        trade_date=trade_date,
        held_sessions=2,
        highest_close=104.0,
        previous_stop=98.0,
        new_stop=99.0,
        stop_reason="staircase",
    )

    assert conn.execute.call_count == 2
    assert "medium_term_stop_history" in conn.execute.call_args_list[1].args[0]


def test_close_medium_term_position_is_guarded_by_open_status() -> None:
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = (73,)
    exit_date = datetime(2026, 9, 10, tzinfo=UTC).date()

    closed = repo.close_medium_term_position(
        position_id=73,
        exit_date=exit_date,
        exit_price=110.0,
        exit_reason="max_holding",
        held_sessions=63,
        gross_return_pct=0.10,
        net_return_pct=0.098,
        gross_pnl=20_000.0,
        costs=400.0,
        net_pnl=19_600.0,
    )

    assert closed is True
    sql = conn.execute.call_args.args[0]
    assert "medium_term_paper_positions" in sql
    assert "WHERE id = %s AND status = 'open'" in sql


def test_list_moex_replay_range_includes_bounded_warmup() -> None:
    repo, conn = _make_repo()
    warmup_date = datetime(2020, 12, 30, tzinfo=UTC).date()
    replay_date = datetime(2021, 1, 4, tzinfo=UTC).date()
    conn.execute.return_value.fetchall.return_value = [
        ("SBER", warmup_date, 99, 101, 98, 100, 900),
        ("SBER", replay_date, 100, 103, 99, 102, 1200),
    ]

    result = repo.list_moex_daily_history_range(
        ("sber",),
        start_date=datetime(2021, 1, 1, tzinfo=UTC).date(),
        end_date=datetime(2021, 12, 31, tzinfo=UTC).date(),
        warmup_bars=60,
    )

    sql, params = conn.execute.call_args.args
    assert "CROSS JOIN LATERAL" in sql
    assert "history.trade_date < %s" in sql
    assert params[-1] == 60
    assert result["SBER"][0]["trade_date"] == warmup_date
    assert result["SBER"][1]["close"] == 102.0


def test_create_medium_term_replay_run_is_immutable_by_name() -> None:
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = (91,)

    run_id = repo.create_medium_term_replay_run(
        name="five-year-research",
        start_date=datetime(2021, 1, 1, tzinfo=UTC).date(),
        end_date=datetime(2026, 1, 1, tzinfo=UTC).date(),
        tickers=("SBER", "GAZP"),
        config={"initial_stop_pct": 0.02},
    )

    assert run_id == 91
    sql = conn.execute.call_args.args[0]
    assert "medium_term_replay_runs" in sql
    assert "ON CONFLICT (name) DO NOTHING" in sql


def test_insert_medium_term_replay_results_is_virtual_only() -> None:
    repo, conn = _make_repo()
    trade_date = datetime(2026, 1, 5, tzinfo=UTC).date()
    repo.insert_medium_term_replay_results(
        run_id=91,
        trades=[{
            "arm": "staircase",
            "ticker": "SBER",
            "signal_date": trade_date - timedelta(days=1),
            "entry_date": trade_date,
            "exit_date": trade_date + timedelta(days=10),
            "entry_price": 100.0,
            "exit_price": 105.0,
            "notional": 100_000.0,
            "initial_stop": 98.0,
            "exit_reason": "max_holding",
            "held_sessions": 10,
            "gross_return_pct": 0.05,
            "net_return_pct": 0.048,
            "gross_pnl": 5000.0,
            "costs": 200.0,
            "net_pnl": 4800.0,
        }],
        equity_rows=[{
            "arm": "staircase",
            "trade_date": trade_date,
            "cash": 900_000.0,
            "position_value": 99_800.0,
            "total_equity": 999_800.0,
            "drawdown_pct": 0.0002,
            "open_positions": 1,
        }],
    )

    assert conn.executemany.call_count == 2
    sql_text = "\n".join(call.args[0] for call in conn.executemany.call_args_list)
    assert "medium_term_replay_trades" in sql_text
    assert "medium_term_replay_equity" in sql_text
    assert "order_intents" not in sql_text
    assert "execution_events" not in sql_text
