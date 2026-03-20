"""Tests for broker fetch policy observability."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from tinvest_trader.services.broker_fetch_policy_observability import (
    FetchPolicyReport,
    build_fetch_policy_report,
    format_report,
    send_fetch_policy_alert,
)
from tinvest_trader.services.tbank_event_fetch_policy import FetchPolicyConfig

NOW = datetime(2026, 3, 20, 12, 0, 0, tzinfo=UTC)


def _policy(**overrides) -> FetchPolicyConfig:
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


def _mock_repo(
    *,
    tracked: list[dict] | None = None,
    fetch_states: list[dict] | None = None,
    failures: list[dict] | None = None,
    never_succeeded: list[dict] | None = None,
    stale: list[dict] | None = None,
    summary: dict | None = None,
) -> MagicMock:
    repo = MagicMock()
    repo.list_tracked_instruments.return_value = tracked or []
    repo.get_all_fetch_states.return_value = fetch_states or []
    repo.list_broker_fetch_failures.return_value = failures or []
    repo.list_broker_fetch_never_succeeded.return_value = never_succeeded or []
    repo.list_broker_fetch_stale.return_value = stale or []
    repo.get_broker_fetch_policy_summary.return_value = summary or {
        "total_states": 0,
        "succeeded_ever": 0,
        "never_succeeded": 0,
        "recent_failures": 0,
        "max_error_count": 0,
    }
    return repo


# -- build_fetch_policy_report tests --


def test_report_empty_state():
    """No tracked instruments, empty state."""
    repo = _mock_repo()
    report = build_fetch_policy_report(repo, _policy(), now=NOW)
    assert report.tracked_total == 0
    assert report.total_pairs == 0
    assert report.eligible_now == 0


def test_report_all_eligible_no_state():
    """All tracked instruments eligible when no fetch state exists."""
    tracked = [
        {"ticker": "SBER", "figi": "FIGI1", "instrument_uid": "uid-1",
         "name": "Sber", "isin": "", "moex_secid": "", "lot": 10,
         "currency": "RUB", "enabled": True, "updated_at": NOW},
        {"ticker": "GAZP", "figi": "FIGI2", "instrument_uid": "uid-2",
         "name": "Gazprom", "isin": "", "moex_secid": "", "lot": 10,
         "currency": "RUB", "enabled": True, "updated_at": NOW},
    ]
    repo = _mock_repo(tracked=tracked)
    report = build_fetch_policy_report(repo, _policy(), now=NOW)

    assert report.tracked_total == 2
    assert report.total_pairs == 6  # 2 figis x 3 event types
    assert report.eligible_now == 6
    assert report.skipped_ttl == 0


def test_report_skipped_by_ttl():
    """Pairs with recent success are counted as skipped_ttl."""
    tracked = [
        {"ticker": "SBER", "figi": "FIGI1", "instrument_uid": "uid-1",
         "name": "Sber", "isin": "", "moex_secid": "", "lot": 10,
         "currency": "RUB", "enabled": True, "updated_at": NOW},
    ]
    fetch_states = [
        {
            "figi": "FIGI1", "event_type": "dividends",
            "last_checked_at": NOW - timedelta(hours=1),
            "last_success_at": NOW - timedelta(hours=1),
            "last_error_at": None, "error_count": 0,
        },
        {
            "figi": "FIGI1", "event_type": "reports",
            "last_checked_at": NOW - timedelta(hours=1),
            "last_success_at": NOW - timedelta(hours=1),
            "last_error_at": None, "error_count": 0,
        },
    ]
    repo = _mock_repo(tracked=tracked, fetch_states=fetch_states)
    report = build_fetch_policy_report(repo, _policy(), now=NOW)

    assert report.skipped_ttl == 2
    assert report.eligible_now == 1  # insider_deals has no state


def test_report_blocked_max_failures():
    """Pairs with max failures are counted as blocked."""
    tracked = [
        {"ticker": "SBER", "figi": "FIGI1", "instrument_uid": "uid-1",
         "name": "Sber", "isin": "", "moex_secid": "", "lot": 10,
         "currency": "RUB", "enabled": True, "updated_at": NOW},
    ]
    fetch_states = [
        {
            "figi": "FIGI1", "event_type": "dividends",
            "last_checked_at": NOW - timedelta(hours=2),
            "last_success_at": None,
            "last_error_at": NOW - timedelta(hours=2),
            "error_count": 5,
        },
    ]
    repo = _mock_repo(tracked=tracked, fetch_states=fetch_states)
    report = build_fetch_policy_report(
        repo, _policy(max_consecutive_failures=5), now=NOW,
    )

    assert report.blocked_max_failures == 1


def test_report_skipped_cooldown():
    """Pairs with recent error within cooldown are counted as skipped_cooldown."""
    tracked = [
        {"ticker": "SBER", "figi": "FIGI1", "instrument_uid": "uid-1",
         "name": "Sber", "isin": "", "moex_secid": "", "lot": 10,
         "currency": "RUB", "enabled": True, "updated_at": NOW},
    ]
    fetch_states = [
        {
            "figi": "FIGI1", "event_type": "dividends",
            "last_checked_at": NOW - timedelta(minutes=30),
            "last_success_at": None,
            "last_error_at": NOW - timedelta(minutes=30),
            "error_count": 2,  # Below max_consecutive_failures=5
        },
    ]
    repo = _mock_repo(tracked=tracked, fetch_states=fetch_states)
    report = build_fetch_policy_report(repo, _policy(), now=NOW)

    assert report.skipped_cooldown == 1


def test_report_placeholder_figi_excluded():
    """Placeholder FIGIs counted separately, not included in pairs."""
    tracked = [
        {"ticker": "SBER", "figi": "FIGI1", "instrument_uid": "uid-1",
         "name": "Sber", "isin": "", "moex_secid": "", "lot": 10,
         "currency": "RUB", "enabled": True, "updated_at": NOW},
        {"ticker": "UNKNOWN", "figi": "TICKER:UNKNOWN", "instrument_uid": None,
         "name": "", "isin": "", "moex_secid": "", "lot": None,
         "currency": None, "enabled": False, "updated_at": NOW},
    ]
    repo = _mock_repo(tracked=tracked)
    report = build_fetch_policy_report(repo, _policy(), now=NOW)

    assert report.tracked_total == 2
    assert report.placeholder_figi_count == 1
    assert report.total_pairs == 3  # Only FIGI1 x 3 event types


def test_report_never_succeeded_from_db():
    """Never succeeded count comes from DB summary."""
    repo = _mock_repo(
        summary={
            "total_states": 10,
            "succeeded_ever": 6,
            "never_succeeded": 4,
            "recent_failures": 3,
            "max_error_count": 7,
        },
    )
    report = build_fetch_policy_report(repo, _policy(), now=NOW)

    assert report.never_succeeded == 4
    assert report.recent_failures == 3


def test_report_stale_from_db():
    """Stale count comes from DB query."""
    stale_rows = [
        {
            "figi": "FIGI1", "event_type": "dividends",
            "last_checked_at": NOW - timedelta(days=5),
            "last_success_at": NOW - timedelta(days=5),
            "last_error_at": None, "error_count": 0, "ticker": "SBER",
        },
    ]
    repo = _mock_repo(stale=stale_rows)
    report = build_fetch_policy_report(repo, _policy(), now=NOW)

    assert report.stale == 1
    assert len(report.stale_examples) == 1


# -- format_report tests --


def test_format_report_basic_shape():
    """Output has expected key lines."""
    report = FetchPolicyReport(
        tracked_total=10,
        placeholder_figi_count=1,
        total_pairs=27,
        eligible_now=5,
        skipped_ttl=20,
        skipped_cooldown=1,
        blocked_max_failures=1,
        never_succeeded=2,
        stale=3,
        recent_failures=4,
    )
    output = format_report(report)

    assert "broker fetch policy status" in output
    assert "tracked_total: 10" in output
    assert "eligible_now: 5" in output
    assert "skipped_ttl: 20" in output
    assert "blocked_max_failures: 1" in output
    assert "never_succeeded: 2" in output
    assert "stale: 3" in output
    assert "recent_failures: 4" in output


def test_format_report_with_examples():
    """Examples sections appear when data exists."""
    report = FetchPolicyReport(
        blocked_examples=[
            {"ticker": "YNDX", "event_type": "dividends",
             "figi": "F1", "error_count": 5},
        ],
        never_succeeded_examples=[
            {"ticker": "SBER", "event_type": "insider_deals", "figi": "F2"},
        ],
        stale_examples=[
            {"ticker": "GAZP", "event_type": "reports",
             "figi": "F3", "last_success_at": NOW},
        ],
    )
    output = format_report(report)

    assert "blocked:" in output
    assert "YNDX dividends" in output
    assert "never_succeeded:" in output
    assert "SBER insider_deals" in output
    assert "stale:" in output
    assert "GAZP reports" in output


def test_format_report_limit_examples():
    """Only limited number of examples shown."""
    blocked = [
        {"ticker": f"T{i}", "event_type": "dividends",
         "figi": f"F{i}", "error_count": 5}
        for i in range(20)
    ]
    report = FetchPolicyReport(blocked_examples=blocked)
    output = format_report(report)

    # _MAX_EXAMPLES = 10
    assert output.count("dividends: errors=5") == 10


# -- has_issues tests --


def test_has_issues_true_on_blocked():
    report = FetchPolicyReport(blocked_max_failures=1)
    assert report.has_issues is True


def test_has_issues_true_on_stale():
    report = FetchPolicyReport(stale=1)
    assert report.has_issues is True


def test_has_issues_true_on_never_succeeded():
    report = FetchPolicyReport(never_succeeded=1)
    assert report.has_issues is True


def test_has_issues_false_when_clean():
    report = FetchPolicyReport(
        tracked_total=10,
        eligible_now=5,
        skipped_ttl=25,
    )
    assert report.has_issues is False


# -- alerting tests --


def test_alert_not_sent_when_no_issues():
    report = FetchPolicyReport()
    result = send_fetch_policy_alert(report)
    assert result is False


def test_alert_logged_when_no_telegram():
    report = FetchPolicyReport(blocked_max_failures=1)
    with patch.dict("os.environ", {}, clear=False):
        result = send_fetch_policy_alert(report, bot_token="", chat_id="")
    assert result is True


def test_alert_sent_via_telegram():
    report = FetchPolicyReport(
        tracked_total=10,
        eligible_now=5,
        blocked_max_failures=2,
        blocked_examples=[
            {"ticker": "YNDX", "event_type": "dividends",
             "figi": "F1", "error_count": 5},
        ],
    )

    with patch(
        "tinvest_trader.services.broker_fetch_policy_observability"
        ".urllib.request.urlopen",
    ) as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = send_fetch_policy_alert(
            report, bot_token="test-token", chat_id="123",
        )

    assert result is True
    mock_urlopen.assert_called_once()


def test_alert_message_contains_examples():
    """Alert message includes example entries."""
    from tinvest_trader.services.broker_fetch_policy_observability import (
        _build_alert_message,
    )

    report = FetchPolicyReport(
        tracked_total=10,
        eligible_now=5,
        blocked_max_failures=1,
        stale=1,
        never_succeeded=1,
        recent_failures=2,
        blocked_examples=[
            {"ticker": "YNDX", "event_type": "dividends",
             "figi": "F1", "error_count": 5},
        ],
        stale_examples=[
            {"ticker": "GAZP", "event_type": "reports",
             "figi": "F3", "last_success_at": NOW},
        ],
        never_succeeded_examples=[
            {"ticker": "SBER", "event_type": "insider_deals", "figi": "F2"},
        ],
    )
    msg = _build_alert_message(report)

    assert "YNDX dividends: max_failures" in msg
    assert "GAZP reports: stale" in msg
    assert "SBER insider_deals: never succeeded" in msg


# -- CLI tests --


def test_cli_parser_has_command():
    """CLI parser accepts broker-fetch-policy-status command."""
    from tinvest_trader.cli import build_parser

    parser = build_parser()
    args = parser.parse_args(["broker-fetch-policy-status"])
    assert args.command == "broker-fetch-policy-status"
    assert args.limit == 10
    assert args.stale_seconds == 172800
    assert args.alert is False
    assert args.fail_on_issues is False


def test_cli_parser_custom_flags():
    """CLI parser accepts custom flags."""
    from tinvest_trader.cli import build_parser

    parser = build_parser()
    args = parser.parse_args([
        "broker-fetch-policy-status",
        "--limit", "5",
        "--stale-seconds", "3600",
        "--alert",
        "--fail-on-issues",
    ])
    assert args.limit == 5
    assert args.stale_seconds == 3600
    assert args.alert is True
    assert args.fail_on_issues is True
