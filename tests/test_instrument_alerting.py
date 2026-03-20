"""Tests for instrument health alerting."""

import logging
from unittest.mock import MagicMock, patch

from tinvest_trader.services.instrument_alerting import (
    _build_alert_message,
    send_instrument_health_alert,
)
from tinvest_trader.services.instrument_health import (
    InstrumentHealthReport,
    InstrumentIssue,
)

# ================================================================
# Message formatting
# ================================================================


def test_message_contains_header():
    report = InstrumentHealthReport(
        total_tracked=10,
        complete=7,
        placeholder_figi_count=2,
        missing_metadata_count=1,
        instruments_with_issues=[
            InstrumentIssue(ticker="SBER", issues=["placeholder_figi"]),
        ],
    )
    msg = _build_alert_message(report)
    assert "Instrument Health Issues" in msg


def test_message_contains_counts():
    report = InstrumentHealthReport(
        total_tracked=73,
        complete=65,
        placeholder_figi_count=3,
        missing_metadata_count=5,
        stale_count=2,
        instruments_with_issues=[
            InstrumentIssue(ticker="SBER", issues=["placeholder_figi"]),
        ],
    )
    msg = _build_alert_message(report)
    assert "tracked: 73" in msg
    assert "complete: 65" in msg
    assert "placeholder_figi: 3" in msg
    assert "missing_metadata: 5" in msg
    assert "stale: 2" in msg


def test_message_contains_examples():
    report = InstrumentHealthReport(
        total_tracked=10,
        complete=8,
        placeholder_figi_count=1,
        missing_metadata_count=1,
        instruments_with_issues=[
            InstrumentIssue(ticker="SBER", issues=["placeholder_figi"]),
            InstrumentIssue(ticker="YNDX", issues=["missing_uid"]),
        ],
    )
    msg = _build_alert_message(report)
    assert "SBER: placeholder_figi" in msg
    assert "YNDX: missing_uid" in msg


def test_message_limits_examples():
    issues = [
        InstrumentIssue(ticker=f"T{i:03d}", issues=["stale"])
        for i in range(20)
    ]
    report = InstrumentHealthReport(
        total_tracked=20,
        stale_count=20,
        instruments_with_issues=issues,
    )
    msg = _build_alert_message(report)
    # Should only show first 10
    assert "T009" in msg
    assert "T010" not in msg


def test_message_omits_zero_categories():
    report = InstrumentHealthReport(
        total_tracked=10,
        complete=9,
        placeholder_figi_count=1,
        missing_metadata_count=0,
        stale_count=0,
        instruments_with_issues=[
            InstrumentIssue(ticker="SBER", issues=["placeholder_figi"]),
        ],
    )
    msg = _build_alert_message(report)
    assert "placeholder_figi: 1" in msg
    assert "missing_metadata" not in msg
    assert "stale" not in msg


# ================================================================
# Alert not sent when no issues
# ================================================================


def test_no_alert_when_clean():
    report = InstrumentHealthReport(total_tracked=10, complete=10)
    result = send_instrument_health_alert(report)
    assert result is False


# ================================================================
# Alert sent when issues exist
# ================================================================


def test_alert_sent_via_logging_fallback(caplog):
    report = InstrumentHealthReport(
        total_tracked=10,
        complete=8,
        placeholder_figi_count=2,
        instruments_with_issues=[
            InstrumentIssue(ticker="SBER", issues=["placeholder_figi"]),
            InstrumentIssue(ticker="GAZP", issues=["placeholder_figi"]),
        ],
    )
    with caplog.at_level(logging.WARNING, logger="tinvest_trader"):
        result = send_instrument_health_alert(report)

    assert result is True
    assert any("Instrument Health Issues" in r.message for r in caplog.records)


def test_alert_sent_via_telegram(monkeypatch):
    report = InstrumentHealthReport(
        total_tracked=5,
        complete=4,
        placeholder_figi_count=1,
        instruments_with_issues=[
            InstrumentIssue(ticker="SBER", issues=["placeholder_figi"]),
        ],
    )
    monkeypatch.setenv("TINVEST_ALERT_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TINVEST_ALERT_CHAT_ID", "12345")

    with patch(
        "tinvest_trader.services.instrument_alerting._send_telegram_message",
    ) as mock_send:
        mock_send.return_value = True
        result = send_instrument_health_alert(report)

    assert result is True
    mock_send.assert_called_once()
    args = mock_send.call_args[0]
    assert args[0] == "test-token"
    assert args[1] == "12345"
    assert "Instrument Health Issues" in args[2]


def test_alert_uses_explicit_params_over_env(monkeypatch):
    report = InstrumentHealthReport(
        total_tracked=5,
        complete=4,
        placeholder_figi_count=1,
        instruments_with_issues=[
            InstrumentIssue(ticker="SBER", issues=["placeholder_figi"]),
        ],
    )
    monkeypatch.setenv("TINVEST_ALERT_BOT_TOKEN", "env-token")
    monkeypatch.setenv("TINVEST_ALERT_CHAT_ID", "env-chat")

    with patch(
        "tinvest_trader.services.instrument_alerting._send_telegram_message",
    ) as mock_send:
        mock_send.return_value = True
        send_instrument_health_alert(
            report, bot_token="explicit-token", chat_id="explicit-chat",
        )

    args = mock_send.call_args[0]
    assert args[0] == "explicit-token"
    assert args[1] == "explicit-chat"


def test_telegram_failure_falls_back_to_logging(monkeypatch, caplog):
    report = InstrumentHealthReport(
        total_tracked=5,
        complete=4,
        placeholder_figi_count=1,
        instruments_with_issues=[
            InstrumentIssue(ticker="SBER", issues=["placeholder_figi"]),
        ],
    )
    monkeypatch.setenv("TINVEST_ALERT_BOT_TOKEN", "tok")
    monkeypatch.setenv("TINVEST_ALERT_CHAT_ID", "123")

    with (
        patch(
            "tinvest_trader.services.instrument_alerting._send_telegram_message",
        ) as mock_send,
        caplog.at_level(logging.WARNING, logger="tinvest_trader"),
    ):
        mock_send.return_value = False
        result = send_instrument_health_alert(report)

    assert result is True
    # Should fall back to logging
    assert any("Instrument Health Issues" in r.message for r in caplog.records)


# ================================================================
# CLI integration
# ================================================================


def test_cli_health_alert_flag():
    from tinvest_trader.cli import main

    mock_repo = MagicMock()
    mock_repo.list_tracked_instruments.return_value = [
        {
            "ticker": "SBER", "figi": "TICKER:SBER",
            "instrument_uid": None, "name": "", "isin": "",
            "moex_secid": "", "updated_at": None,
        },
    ]

    with (
        patch("tinvest_trader.cli.load_config"),
        patch("tinvest_trader.cli.build_container") as mock_build,
        patch(
            "tinvest_trader.services.instrument_alerting"
            ".send_instrument_health_alert",
        ) as mock_alert,
    ):
        container = MagicMock()
        container.repository = mock_repo
        mock_build.return_value = container
        main(["instrument-health", "--alert"])

    mock_alert.assert_called_once()


def test_cli_enrich_alert_flag():
    from tinvest_trader.cli import main

    mock_repo = MagicMock()
    mock_repo.list_tracked_instruments.return_value = []

    with (
        patch("tinvest_trader.cli.load_config"),
        patch("tinvest_trader.cli.build_container") as mock_build,
        patch(
            "tinvest_trader.services.instrument_alerting"
            ".send_instrument_health_alert",
        ) as mock_alert,
    ):
        container = MagicMock()
        container.repository = mock_repo
        container.tbank_client = MagicMock()
        container.logger = logging.getLogger("test")
        mock_build.return_value = container
        main(["enrich-instruments", "--alert"])

    mock_alert.assert_called_once()
