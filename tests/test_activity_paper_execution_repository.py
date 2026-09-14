"""Offline SQL contracts; no Postgres or broker connectivity is required."""

from datetime import timedelta
from decimal import Decimal

import pytest

from tests.test_activity_paper_strategy import NOW
from tests.test_repository import _make_repo


def position():
    return dict(portfolio_name="reversion-v2", spike_id=7, ticker="SBER", figi="FIGI",
                direction="down", horizon="15m", notional=20000,
                entry_time=NOW-timedelta(minutes=7), entry_price=100)


def reserve(repo):
    return repo.reserve_activity_paper_entry(position(), wait_seconds=120,
                                            max_quote_age=30, cost_rate=0.002,
                                            max_positions=10, max_per_ticker=1)


def test_reservation_serializes_capacity_and_atomically_records_decision():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.side_effect = [
        (Decimal(100000),), (0, 0, Decimal(0)), (Decimal(0),), (NOW,),
    ]
    assert reserve(repo)
    calls = conn.execute.call_args_list
    assert "FOR UPDATE" in calls[0].args[0]
    assert "status = 'pending'" in calls[1].args[0]
    assert "status = 'open'" in calls[1].args[0]
    assert "clock_timestamp()" in calls[3].args[0]
    assert "ON CONFLICT (portfolio_name, spike_id) DO NOTHING" in calls[3].args[0]
    assert calls[3].args[1][-4:] == (120, 120, 30, 0.002)
    assert "'pending', 'awaiting_quote'" in calls[4].args[0]
    assert calls[4].args[1][-1] == NOW
    assert repo._pool.get_connection.call_count == 1


@pytest.mark.parametrize("exposure", [(10, 0, 0), (1, 1, 0), (1, 0, 90000)])
def test_reservation_rechecks_global_and_ticker_slots_and_cash(exposure):
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.side_effect = [(100000,), exposure, (0,)]
    assert not reserve(repo)
    assert all("INSERT" not in c.args[0] for c in conn.execute.call_args_list)


def test_duplicate_request_does_not_write_another_decision():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.side_effect = [(100000,), (0, 0, 0), (0,), None]
    assert not reserve(repo)
    assert conn.execute.call_count == 4


def test_quote_selection_enforces_reception_time_freshness_and_identity():
    repo, conn = _make_repo()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.execute.return_value.fetchone.return_value = None
    until = NOW+timedelta(seconds=120)
    assert repo.first_activity_execution_quote(
        "FIGI", after=NOW, until=until, as_of=until, max_age_seconds=30,
    ) is None
    sql, args = cur.execute.call_args.args
    assert "figi = %s" in sql
    assert "fetched_at > %s" in sql and "source_time > %s" in sql
    assert "source_time <= fetched_at" in sql
    assert "ORDER BY fetched_at ASC, id ASC" in sql
    assert "price < 'Infinity'::numeric" in sql
    assert args == ("FIGI", NOW, until, until, NOW, 30)


def test_fill_locks_and_copies_the_quote_not_the_signal_price():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.side_effect = [("pending",), (42,)]
    q = dict(id=77, price=95, source_time=NOW+timedelta(seconds=9),
             fetched_at=NOW+timedelta(seconds=10))
    due = q["fetched_at"]+timedelta(minutes=15)
    assert repo.fill_activity_paper_entry(position(), q, exit_due_at=due,
                                         processed_at=NOW+timedelta(minutes=1))
    calls = conn.execute.call_args_list
    assert "FOR UPDATE" in calls[0].args[0] and "FOR UPDATE" in calls[1].args[0]
    assert calls[2].args[1][:2] == (95, q["fetched_at"])
    assert calls[3].args[1][:4] == (42, 77, q["source_time"], q["fetched_at"])
    assert "causal_quote_fill" in calls[4].args[0]
    assert "recorded_at=" not in calls[4].args[0]
    assert repo._pool.get_connection.call_count == 1


def test_repeated_fill_does_not_duplicate_position():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = ("open",)
    assert not repo.fill_activity_paper_entry(position(), {}, exit_due_at=NOW, processed_at=NOW)
    assert conn.execute.call_count == 2


def test_cancel_does_not_create_or_price_a_position():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = ("pending",)
    assert repo.finish_activity_paper_execution(position(), status="cancelled",
                                                reason="entry_quote_timeout", now=NOW)
    text = "\n".join(c.args[0] for c in conn.execute.call_args_list)
    assert "activity_paper_positions" not in text
    assert "decision='cancel'" in text


def test_expired_exit_keeps_pnl_and_prices_unknown():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = ("open",)
    request = {**position(), "position_id": 42}
    assert repo.finish_activity_paper_execution(request, status="expired",
                                                reason="exit_quote_timeout", now=NOW)
    sql = conn.execute.call_args_list[1].args[0]
    assert "status='expired'" in sql
    assert "net_pnl" not in sql and "exit_price" not in sql and "exit_time" not in sql


def test_close_is_guarded_and_audited_in_one_transaction():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = ("open",)
    request = {**position(), "position_id": 42}
    q = dict(id=78, price=96, source_time=NOW, fetched_at=NOW+timedelta(seconds=1))
    assert repo.finish_activity_paper_execution(
        request, status="closed", reason="timed_quote_exit", now=NOW+timedelta(seconds=10),
        quote=q, gross_return=-0.01, costs=40,
    )
    values = conn.execute.call_args_list[1].args[1]
    assert values[0:2] == (96, q["fetched_at"])
    assert values[6] == -240
    assert values[7] == NOW+timedelta(seconds=10)
    assert repo._pool.get_connection.call_count == 1


def test_repeated_close_is_noop():
    repo, conn = _make_repo()
    conn.execute.return_value.fetchone.return_value = ("closed",)
    assert not repo.finish_activity_paper_execution(
        position(), status="closed", reason="timed_quote_exit", now=NOW,
    )
    assert conn.execute.call_count == 1
