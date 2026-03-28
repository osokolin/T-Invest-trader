"""Tests for the alerting service."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from tinvest_trader.app.config import AlertingConfig
from tinvest_trader.services.alerting import (
    Alert,
    check_cooldown,
    evaluate_alerts,
    format_alert_telegram_message,
    run_alert_check,
)


@pytest.fixture()
def alerting_config() -> AlertingConfig:
    return AlertingConfig(
        enabled=True,
        check_interval_seconds=300,
        cooldown_seconds=3600,
        signal_gap_minutes=120,
        telegram_gap_minutes=60,
        quote_gap_minutes=30,
        global_context_gap_minutes=60,
        pending_signals_alert_enabled=True,
        pending_signals_max=50,
        win_rate_min=0.3,
        win_rate_lookback_days=7,
        win_rate_min_resolved=10,
    )


@pytest.fixture()
def mock_repo():
    repo = MagicMock()
    repo.get_alerting_health_data.return_value = {}
    repo.get_last_alert_fired_at.return_value = None
    repo.insert_alert_event.return_value = 1
    return repo


@pytest.fixture()
def mock_logger():
    return MagicMock()


def _now():
    return datetime.now(UTC)


# Fixed datetimes for deterministic tests (Monday=0 .. Sunday=6)
# Wednesday 12:00 UTC = 15:00 MSK — within market hours (09:50–18:50 MSK)
_WEDNESDAY = datetime(2026, 3, 18, 12, 0, 0, tzinfo=UTC)  # weekday=2
_SATURDAY = datetime(2026, 3, 21, 12, 0, 0, tzinfo=UTC)  # weekday=5
# Wednesday 04:00 UTC = 07:00 MSK — before market open (09:50 MSK)
_WEDNESDAY_NIGHT = datetime(2026, 3, 18, 4, 0, 0, tzinfo=UTC)  # weekday=2
# Wednesday 07:00 UTC = 10:00 MSK — within grace period (09:50–10:50 MSK)
_WEDNESDAY_GRACE = datetime(2026, 3, 18, 7, 0, 0, tzinfo=UTC)  # weekday=2


@patch("tinvest_trader.services.alerting.datetime")
class TestEvaluateAlerts:
    """All gap tests use a patched Wednesday datetime for determinism."""

    def _setup_mock_dt(self, mock_dt):
        mock_dt.now.return_value = _WEDNESDAY

    def test_no_alerts_when_all_healthy(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY - timedelta(minutes=10),
            "pending_signals": 5,
            "latest_telegram_at": _WEDNESDAY - timedelta(minutes=5),
            "latest_quote_at": _WEDNESDAY - timedelta(minutes=2),
            "latest_global_context_at": _WEDNESDAY - timedelta(minutes=10),
            "win_rate_7d_resolved": 20,
            "win_rate_7d": 0.55,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        assert alerts == []

    def test_signal_gap_alert(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY - timedelta(minutes=200),
            "pending_signals": 0,
            "latest_telegram_at": _WEDNESDAY,
            "latest_quote_at": _WEDNESDAY,
            "latest_global_context_at": _WEDNESDAY,
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "signal_gap" in keys
        alert = next(a for a in alerts if a.key == "signal_gap")
        assert alert.category == "signal_pipeline"
        assert alert.severity == "warning"

    def test_pending_signals_alert(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY,
            "pending_signals": 100,
            "latest_telegram_at": _WEDNESDAY,
            "latest_quote_at": _WEDNESDAY,
            "latest_global_context_at": _WEDNESDAY,
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "pending_signals_high" in keys

    def test_pending_signals_alert_disabled(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        disabled_config = replace(
            alerting_config, pending_signals_alert_enabled=False,
        )
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY,
            "pending_signals": 100,
            "latest_telegram_at": _WEDNESDAY,
            "latest_quote_at": _WEDNESDAY,
            "latest_global_context_at": _WEDNESDAY,
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        alerts = evaluate_alerts(disabled_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "pending_signals_high" not in keys

    def test_win_rate_alert(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY,
            "pending_signals": 0,
            "latest_telegram_at": _WEDNESDAY,
            "latest_quote_at": _WEDNESDAY,
            "latest_global_context_at": _WEDNESDAY,
            "win_rate_7d_resolved": 15,
            "win_rate_7d": 0.2,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "win_rate_low" in keys
        alert = next(a for a in alerts if a.key == "win_rate_low")
        assert alert.severity == "critical"

    def test_win_rate_skipped_when_few_resolved(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY,
            "pending_signals": 0,
            "latest_telegram_at": _WEDNESDAY,
            "latest_quote_at": _WEDNESDAY,
            "latest_global_context_at": _WEDNESDAY,
            "win_rate_7d_resolved": 3,
            "win_rate_7d": 0.1,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "win_rate_low" not in keys

    def test_telegram_gap_alert(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY,
            "pending_signals": 0,
            "latest_telegram_at": _WEDNESDAY - timedelta(minutes=120),
            "latest_quote_at": _WEDNESDAY,
            "latest_global_context_at": _WEDNESDAY,
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "telegram_gap" in keys

    def test_quote_gap_alert(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY,
            "pending_signals": 0,
            "latest_telegram_at": _WEDNESDAY,
            "latest_quote_at": _WEDNESDAY - timedelta(minutes=60),
            "latest_global_context_at": _WEDNESDAY,
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "quote_gap" in keys

    def test_global_context_gap_alert(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY,
            "pending_signals": 0,
            "latest_telegram_at": _WEDNESDAY,
            "latest_quote_at": _WEDNESDAY,
            "latest_global_context_at": _WEDNESDAY - timedelta(minutes=120),
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "global_context_gap" in keys
        alert = next(a for a in alerts if a.key == "global_context_gap")
        assert alert.severity == "info"

    def test_no_alerts_when_no_health_data(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {}
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        assert alerts == []

    def test_multiple_alerts_can_fire(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY - timedelta(minutes=200),
            "pending_signals": 100,
            "latest_telegram_at": _WEDNESDAY - timedelta(minutes=120),
            "latest_quote_at": _WEDNESDAY - timedelta(minutes=60),
            "latest_global_context_at": _WEDNESDAY - timedelta(minutes=120),
            "win_rate_7d_resolved": 20,
            "win_rate_7d": 0.2,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        assert len(alerts) >= 4

    def test_gap_alerts_suppressed_on_weekend(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        mock_dt.now.return_value = _SATURDAY
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _SATURDAY - timedelta(minutes=200),
            "pending_signals": 100,
            "latest_telegram_at": _SATURDAY - timedelta(minutes=120),
            "latest_quote_at": _SATURDAY - timedelta(minutes=60),
            "latest_global_context_at": _SATURDAY - timedelta(minutes=120),
            "win_rate_7d_resolved": 20,
            "win_rate_7d": 0.2,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        # Gap alerts suppressed on weekend
        assert "signal_gap" not in keys
        assert "telegram_gap" not in keys
        assert "quote_gap" not in keys
        assert "global_context_gap" not in keys
        # Pending still fires; win rate suppressed on weekends
        assert "pending_signals_high" in keys
        assert "win_rate_low" not in keys

    def test_gap_alerts_suppressed_outside_market_hours(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        mock_dt.now.return_value = _WEDNESDAY_NIGHT
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY_NIGHT - timedelta(minutes=200),
            "pending_signals": 100,
            "latest_telegram_at": _WEDNESDAY_NIGHT - timedelta(minutes=120),
            "latest_quote_at": _WEDNESDAY_NIGHT - timedelta(minutes=60),
            "latest_global_context_at": _WEDNESDAY_NIGHT - timedelta(minutes=120),
            "win_rate_7d_resolved": 20,
            "win_rate_7d": 0.2,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        # Gap alerts suppressed outside market hours
        assert "signal_gap" not in keys
        assert "telegram_gap" not in keys
        assert "quote_gap" not in keys
        assert "global_context_gap" not in keys
        # Non-gap alerts still fire
        assert "pending_signals_high" in keys
        assert "win_rate_low" in keys

    def test_gap_alerts_suppressed_during_grace_period(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        """Gap alerts suppressed in first 60 min after market open (09:50–10:50 MSK)."""
        mock_dt.now.return_value = _WEDNESDAY_GRACE
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY_GRACE - timedelta(minutes=2000),
            "pending_signals": 0,
            "latest_telegram_at": _WEDNESDAY_GRACE - timedelta(minutes=120),
            "latest_quote_at": _WEDNESDAY_GRACE - timedelta(minutes=60),
            "latest_global_context_at": _WEDNESDAY_GRACE - timedelta(minutes=120),
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        # Gap alerts suppressed during grace period
        assert "signal_gap" not in keys
        assert "telegram_gap" not in keys
        assert "quote_gap" not in keys
        assert "global_context_gap" not in keys


class TestCooldown:
    def test_no_cooldown_when_never_fired(self, mock_repo):
        alert = Alert(
            key="test", category="test",
            severity="warning", title="t", message="m",
        )
        mock_repo.get_last_alert_fired_at.return_value = None
        assert check_cooldown(alert, mock_repo, 3600) is False

    def test_cooldown_active_when_recent(self, mock_repo):
        alert = Alert(
            key="test", category="test",
            severity="warning", title="t", message="m",
        )
        mock_repo.get_last_alert_fired_at.return_value = (
            _now() - timedelta(minutes=10)
        )
        assert check_cooldown(alert, mock_repo, 3600) is True

    def test_cooldown_expired(self, mock_repo):
        alert = Alert(
            key="test", category="test",
            severity="warning", title="t", message="m",
        )
        mock_repo.get_last_alert_fired_at.return_value = (
            _now() - timedelta(hours=2)
        )
        assert check_cooldown(alert, mock_repo, 3600) is False


class TestFormatMessage:
    def test_format_contains_key_fields(self):
        alert = Alert(
            key="signal_gap",
            category="signal_pipeline",
            severity="warning",
            title="No new signals for 200m",
            message="Last signal 200 minutes ago.",
        )
        text = format_alert_telegram_message(alert)
        assert "WARNING" in text
        assert "signal_gap" in text
        assert "200m" in text
        assert "signal_pipeline" in text

    def test_critical_severity_format(self):
        alert = Alert(
            key="win_rate_low",
            category="analytics",
            severity="critical",
            title="Win rate dropped",
            message="Details here.",
        )
        text = format_alert_telegram_message(alert)
        assert "CRITICAL" in text


@patch("tinvest_trader.services.alerting.datetime")
class TestRunAlertCheck:
    def _setup_mock_dt(self, mock_dt):
        mock_dt.now.return_value = _WEDNESDAY

    def test_dry_run_no_persist_no_send(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY - timedelta(minutes=200),
            "pending_signals": 0,
            "latest_telegram_at": _WEDNESDAY,
            "latest_quote_at": _WEDNESDAY,
            "latest_global_context_at": _WEDNESDAY,
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        result = run_alert_check(
            alerting_config, None, mock_repo, mock_logger,
            send=False, dry_run=True,
        )
        assert result.alerts_fired >= 1
        assert result.alerts_sent == 0
        mock_repo.insert_alert_event.assert_not_called()

    def test_alerts_persisted_when_not_dry_run(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY - timedelta(minutes=200),
            "pending_signals": 0,
            "latest_telegram_at": _WEDNESDAY,
            "latest_quote_at": _WEDNESDAY,
            "latest_global_context_at": _WEDNESDAY,
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        result = run_alert_check(
            alerting_config, None, mock_repo, mock_logger,
            send=False, dry_run=False,
        )
        assert result.alerts_fired >= 1
        assert mock_repo.insert_alert_event.called

    def test_cooldown_prevents_duplicate(
        self, mock_dt, alerting_config, mock_repo, mock_logger,
    ):
        self._setup_mock_dt(mock_dt)
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _WEDNESDAY - timedelta(minutes=200),
            "pending_signals": 0,
            "latest_telegram_at": _WEDNESDAY,
            "latest_quote_at": _WEDNESDAY,
            "latest_global_context_at": _WEDNESDAY,
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        # All alerts were recently fired
        mock_repo.get_last_alert_fired_at.return_value = (
            _WEDNESDAY - timedelta(minutes=10)
        )
        result = run_alert_check(
            alerting_config, None, mock_repo, mock_logger,
            send=False, dry_run=False,
        )
        assert result.alerts_fired == 0
        assert result.alerts_cooled_down >= 1


class TestAlertingConfig:
    def test_default_config(self):
        cfg = AlertingConfig()
        assert cfg.enabled is False
        assert cfg.cooldown_seconds == 3600
        assert cfg.signal_gap_minutes == 120
        assert cfg.pending_signals_alert_enabled is False
        assert cfg.win_rate_min == 0.3
