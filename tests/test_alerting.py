"""Tests for the alerting service."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

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


class TestEvaluateAlerts:
    def test_no_alerts_when_all_healthy(
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now() - timedelta(minutes=10),
            "pending_signals": 5,
            "latest_telegram_at": _now() - timedelta(minutes=5),
            "latest_quote_at": _now() - timedelta(minutes=2),
            "latest_global_context_at": _now() - timedelta(minutes=10),
            "win_rate_7d_resolved": 20,
            "win_rate_7d": 0.55,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        assert alerts == []

    def test_signal_gap_alert(
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now() - timedelta(minutes=200),
            "pending_signals": 0,
            "latest_telegram_at": _now(),
            "latest_quote_at": _now(),
            "latest_global_context_at": _now(),
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
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now(),
            "pending_signals": 100,
            "latest_telegram_at": _now(),
            "latest_quote_at": _now(),
            "latest_global_context_at": _now(),
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "pending_signals_high" in keys

    def test_win_rate_alert(
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now(),
            "pending_signals": 0,
            "latest_telegram_at": _now(),
            "latest_quote_at": _now(),
            "latest_global_context_at": _now(),
            "win_rate_7d_resolved": 15,
            "win_rate_7d": 0.2,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "win_rate_low" in keys
        alert = next(a for a in alerts if a.key == "win_rate_low")
        assert alert.severity == "critical"

    def test_win_rate_skipped_when_few_resolved(
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now(),
            "pending_signals": 0,
            "latest_telegram_at": _now(),
            "latest_quote_at": _now(),
            "latest_global_context_at": _now(),
            "win_rate_7d_resolved": 3,
            "win_rate_7d": 0.1,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "win_rate_low" not in keys

    def test_telegram_gap_alert(
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now(),
            "pending_signals": 0,
            "latest_telegram_at": _now() - timedelta(minutes=120),
            "latest_quote_at": _now(),
            "latest_global_context_at": _now(),
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "telegram_gap" in keys

    def test_quote_gap_alert(
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now(),
            "pending_signals": 0,
            "latest_telegram_at": _now(),
            "latest_quote_at": _now() - timedelta(minutes=60),
            "latest_global_context_at": _now(),
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "quote_gap" in keys

    def test_global_context_gap_alert(
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now(),
            "pending_signals": 0,
            "latest_telegram_at": _now(),
            "latest_quote_at": _now(),
            "latest_global_context_at": _now() - timedelta(minutes=120),
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        keys = [a.key for a in alerts]
        assert "global_context_gap" in keys
        alert = next(a for a in alerts if a.key == "global_context_gap")
        assert alert.severity == "info"

    def test_no_alerts_when_no_health_data(
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {}
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        assert alerts == []

    def test_multiple_alerts_can_fire(
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now() - timedelta(minutes=200),
            "pending_signals": 100,
            "latest_telegram_at": _now() - timedelta(minutes=120),
            "latest_quote_at": _now() - timedelta(minutes=60),
            "latest_global_context_at": _now() - timedelta(minutes=120),
            "win_rate_7d_resolved": 20,
            "win_rate_7d": 0.2,
        }
        alerts = evaluate_alerts(alerting_config, mock_repo, mock_logger)
        assert len(alerts) >= 4


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


class TestRunAlertCheck:
    def test_dry_run_no_persist_no_send(
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now() - timedelta(minutes=200),
            "pending_signals": 0,
            "latest_telegram_at": _now(),
            "latest_quote_at": _now(),
            "latest_global_context_at": _now(),
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
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now() - timedelta(minutes=200),
            "pending_signals": 0,
            "latest_telegram_at": _now(),
            "latest_quote_at": _now(),
            "latest_global_context_at": _now(),
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
        self, alerting_config, mock_repo, mock_logger,
    ):
        mock_repo.get_alerting_health_data.return_value = {
            "latest_signal_at": _now() - timedelta(minutes=200),
            "pending_signals": 0,
            "latest_telegram_at": _now(),
            "latest_quote_at": _now(),
            "latest_global_context_at": _now(),
            "win_rate_7d_resolved": 0,
            "win_rate_7d": None,
        }
        # All alerts were recently fired
        mock_repo.get_last_alert_fired_at.return_value = (
            _now() - timedelta(minutes=10)
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
        assert cfg.win_rate_min == 0.3
