"""Tests for quote sync service -- T-Bank bulk last-price ingestion."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from tinvest_trader.app.config import BackgroundConfig, QuoteSyncConfig
from tinvest_trader.services.background_runner import BackgroundRunner
from tinvest_trader.services.quote_sync import (
    QuoteSyncResult,
    _parse_timestamp,
    sync_quotes,
)

# -- Fixtures --


@pytest.fixture()
def logger():
    return logging.getLogger("test_quote_sync")


@pytest.fixture()
def mock_client():
    return MagicMock()


@pytest.fixture()
def mock_repository():
    return MagicMock()


def _make_instrument(
    ticker: str,
    figi: str,
    instrument_uid: str = "",
    name: str = "",
) -> dict:
    return {
        "ticker": ticker,
        "figi": figi,
        "instrument_uid": instrument_uid,
        "name": name or ticker,
        "moex_secid": "",
    }


# -- sync_quotes tests --


class TestSyncQuotes:
    """Core sync flow tests."""

    def test_empty_tracked(self, mock_client, mock_repository, logger):
        """No tracked instruments -> no API calls."""
        mock_repository.list_tracked_instruments.return_value = []
        result = sync_quotes(
            client=mock_client,
            repository=mock_repository,
            logger=logger,
        )
        assert result.requested == 0
        assert result.inserted == 0
        mock_client.get_last_prices.assert_not_called()

    def test_all_placeholders_skipped(self, mock_client, mock_repository, logger):
        """Instruments with placeholder FIGIs are skipped."""
        mock_repository.list_tracked_instruments.return_value = [
            _make_instrument("SBER", "TICKER:SBER"),
            _make_instrument("GAZP", "TICKER:GAZP"),
        ]
        result = sync_quotes(
            client=mock_client,
            repository=mock_repository,
            logger=logger,
        )
        assert result.requested == 0
        assert result.skipped == 2
        mock_client.get_last_prices.assert_not_called()

    def test_bulk_fetch_and_persist(self, mock_client, mock_repository, logger):
        """Normal flow: bulk fetch -> persist -> count."""
        mock_repository.list_tracked_instruments.return_value = [
            _make_instrument("SBER", "BBG004730N88", "uid-sber"),
            _make_instrument("GAZP", "BBG004730RP0", "uid-gazp"),
            _make_instrument("VTBR", "TICKER:VTBR"),  # placeholder
        ]
        mock_client.get_last_prices.return_value = [
            {
                "instrument_uid": "uid-sber",
                "price": 250.50,
                "source_time": "2026-03-20T10:30:00Z",
            },
            {
                "instrument_uid": "uid-gazp",
                "price": 155.25,
                "source_time": "2026-03-20T10:30:00Z",
            },
        ]
        mock_repository.insert_market_quotes_bulk.return_value = 2

        result = sync_quotes(
            client=mock_client,
            repository=mock_repository,
            logger=logger,
        )

        assert result.requested == 2
        assert result.received == 2
        assert result.inserted == 2
        assert result.skipped == 1  # VTBR placeholder
        assert result.failed == 0

        # Verify bulk insert called with correct data
        call_args = mock_repository.insert_market_quotes_bulk.call_args[0][0]
        assert len(call_args) == 2
        assert call_args[0]["ticker"] == "SBER"
        assert call_args[0]["figi"] == "BBG004730N88"
        assert call_args[0]["price"] == 250.50
        assert call_args[1]["ticker"] == "GAZP"

    def test_prefers_instrument_uid_for_api_call(
        self, mock_client, mock_repository, logger,
    ):
        """When instrument_uid is available, use it for the API call."""
        mock_repository.list_tracked_instruments.return_value = [
            _make_instrument("SBER", "BBG004730N88", "uid-sber"),
        ]
        mock_client.get_last_prices.return_value = [
            {"instrument_uid": "uid-sber", "price": 250.0, "source_time": ""},
        ]
        mock_repository.insert_market_quotes_bulk.return_value = 1

        sync_quotes(
            client=mock_client,
            repository=mock_repository,
            logger=logger,
        )

        # API should be called with uid, not figi
        called_ids = mock_client.get_last_prices.call_args[0][0]
        assert called_ids == ["uid-sber"]

    def test_falls_back_to_figi_when_no_uid(
        self, mock_client, mock_repository, logger,
    ):
        """When no instrument_uid, fall back to FIGI for API call."""
        mock_repository.list_tracked_instruments.return_value = [
            _make_instrument("SBER", "BBG004730N88", ""),
        ]
        mock_client.get_last_prices.return_value = []
        mock_repository.insert_market_quotes_bulk.return_value = 0

        sync_quotes(
            client=mock_client,
            repository=mock_repository,
            logger=logger,
        )

        called_ids = mock_client.get_last_prices.call_args[0][0]
        assert called_ids == ["BBG004730N88"]

    def test_limit_restricts_instruments(
        self, mock_client, mock_repository, logger,
    ):
        """--limit restricts how many instruments are fetched."""
        mock_repository.list_tracked_instruments.return_value = [
            _make_instrument("SBER", "BBG004730N88", "uid-sber"),
            _make_instrument("GAZP", "BBG004730RP0", "uid-gazp"),
            _make_instrument("LKOH", "BBG006L8G4H1", "uid-lkoh"),
        ]
        mock_client.get_last_prices.return_value = [
            {"instrument_uid": "uid-sber", "price": 250.0, "source_time": ""},
        ]
        mock_repository.insert_market_quotes_bulk.return_value = 1

        result = sync_quotes(
            client=mock_client,
            repository=mock_repository,
            logger=logger,
            limit=1,
        )

        assert result.requested == 1
        called_ids = mock_client.get_last_prices.call_args[0][0]
        assert len(called_ids) == 1

    def test_api_failure_is_safe(self, mock_client, mock_repository, logger):
        """API failure doesn't crash, reports error."""
        mock_repository.list_tracked_instruments.return_value = [
            _make_instrument("SBER", "BBG004730N88", "uid-sber"),
        ]
        mock_client.get_last_prices.side_effect = RuntimeError("API down")

        result = sync_quotes(
            client=mock_client,
            repository=mock_repository,
            logger=logger,
        )

        assert result.failed == 1
        assert "bulk fetch failed" in result.errors[0]

    def test_db_failure_is_safe(self, mock_client, mock_repository, logger):
        """DB write failure doesn't crash, reports error."""
        mock_repository.list_tracked_instruments.return_value = [
            _make_instrument("SBER", "BBG004730N88", "uid-sber"),
        ]
        mock_client.get_last_prices.return_value = [
            {"instrument_uid": "uid-sber", "price": 250.0, "source_time": ""},
        ]
        mock_repository.insert_market_quotes_bulk.side_effect = RuntimeError("DB down")

        result = sync_quotes(
            client=mock_client,
            repository=mock_repository,
            logger=logger,
        )

        assert result.failed > 0
        assert "persist failed" in result.errors[0]

    def test_db_list_failure_is_safe(self, mock_client, mock_repository, logger):
        """DB read failure doesn't crash, reports error."""
        mock_repository.list_tracked_instruments.side_effect = RuntimeError("DB down")

        result = sync_quotes(
            client=mock_client,
            repository=mock_repository,
            logger=logger,
        )

        assert "failed to list tracked instruments" in result.errors[0]

    def test_unmatched_prices_skipped(self, mock_client, mock_repository, logger):
        """Prices for unknown UIDs are skipped."""
        mock_repository.list_tracked_instruments.return_value = [
            _make_instrument("SBER", "BBG004730N88", "uid-sber"),
        ]
        mock_client.get_last_prices.return_value = [
            {"instrument_uid": "uid-unknown", "price": 100.0, "source_time": ""},
        ]
        mock_repository.insert_market_quotes_bulk.return_value = 0

        result = sync_quotes(
            client=mock_client,
            repository=mock_repository,
            logger=logger,
        )

        assert result.received == 1
        assert result.skipped == 1  # unmatched
        mock_repository.insert_market_quotes_bulk.assert_not_called()

    def test_repeated_sync_is_safe(self, mock_client, mock_repository, logger):
        """Running sync twice doesn't break -- append-only table."""
        mock_repository.list_tracked_instruments.return_value = [
            _make_instrument("SBER", "BBG004730N88", "uid-sber"),
        ]
        mock_client.get_last_prices.return_value = [
            {"instrument_uid": "uid-sber", "price": 250.0, "source_time": ""},
        ]
        mock_repository.insert_market_quotes_bulk.return_value = 1

        r1 = sync_quotes(
            client=mock_client, repository=mock_repository, logger=logger,
        )
        r2 = sync_quotes(
            client=mock_client, repository=mock_repository, logger=logger,
        )

        assert r1.inserted == 1
        assert r2.inserted == 1
        assert mock_repository.insert_market_quotes_bulk.call_count == 2

    def test_mixed_placeholder_and_real(self, mock_client, mock_repository, logger):
        """Mix of placeholder and real instruments handled correctly."""
        mock_repository.list_tracked_instruments.return_value = [
            _make_instrument("SBER", "BBG004730N88", "uid-sber"),
            _make_instrument("VTBR", "TICKER:VTBR"),
            _make_instrument("GAZP", "BBG004730RP0", "uid-gazp"),
            _make_instrument("NOFI", ""),  # empty figi
        ]
        mock_client.get_last_prices.return_value = [
            {"instrument_uid": "uid-sber", "price": 250.0, "source_time": ""},
            {"instrument_uid": "uid-gazp", "price": 155.0, "source_time": ""},
        ]
        mock_repository.insert_market_quotes_bulk.return_value = 2

        result = sync_quotes(
            client=mock_client,
            repository=mock_repository,
            logger=logger,
        )

        assert result.requested == 2  # only SBER and GAZP
        assert result.received == 2
        assert result.inserted == 2
        assert result.skipped == 2  # VTBR placeholder + NOFI empty


# -- Timestamp parsing --


class TestParseTimestamp:
    def test_iso_with_z(self):
        ts = _parse_timestamp("2026-03-20T10:30:00Z")
        assert ts is not None
        assert ts.year == 2026
        assert ts.month == 3
        assert ts.day == 20

    def test_iso_with_offset(self):
        ts = _parse_timestamp("2026-03-20T10:30:00+00:00")
        assert ts is not None

    def test_empty_string(self):
        assert _parse_timestamp("") is None

    def test_none_safe(self):
        assert _parse_timestamp(None) is None

    def test_invalid_format(self):
        assert _parse_timestamp("not-a-date") is None


# -- Repository method tests (using mocks) --


class TestRepositoryMethods:
    """Test repository quote methods via mock pool."""

    def test_insert_market_quote_returns_id(self, mock_repository, logger):
        """insert_market_quote returns row id."""
        mock_repository.insert_market_quote.return_value = 42
        result = mock_repository.insert_market_quote(
            ticker="SBER", figi="BBG004730N88", price=250.0,
        )
        assert result == 42

    def test_insert_market_quotes_bulk_returns_count(self, mock_repository, logger):
        """insert_market_quotes_bulk returns inserted count."""
        mock_repository.insert_market_quotes_bulk.return_value = 3
        result = mock_repository.insert_market_quotes_bulk([
            {"ticker": "SBER", "figi": "F1", "price": 250.0},
            {"ticker": "GAZP", "figi": "F2", "price": 155.0},
            {"ticker": "LKOH", "figi": "F3", "price": 7000.0},
        ])
        assert result == 3

    def test_get_latest_quote_by_ticker(self, mock_repository, logger):
        """get_latest_quote_by_ticker returns dict or None."""
        mock_repository.get_latest_quote_by_ticker.return_value = {
            "id": 1, "ticker": "SBER", "figi": "BBG004730N88",
            "price": 250.0, "source_time": None, "fetched_at": datetime.now(UTC),
        }
        result = mock_repository.get_latest_quote_by_ticker("SBER")
        assert result["ticker"] == "SBER"
        assert result["price"] == 250.0

    def test_get_latest_quote_by_figi(self, mock_repository, logger):
        """get_latest_quote_by_figi returns dict or None."""
        mock_repository.get_latest_quote_by_figi.return_value = None
        result = mock_repository.get_latest_quote_by_figi("BBG-UNKNOWN")
        assert result is None


# -- Background runner integration --


class TestBackgroundRunnerQuoteSync:
    """Test background runner respects quote sync config."""

    def test_quote_sync_not_runnable_when_disabled(self):
        config = BackgroundConfig(enabled=True, run_quote_sync=True)
        qs_config = QuoteSyncConfig(enabled=False)
        runner = BackgroundRunner(
            config=config,
            logger=logging.getLogger("test"),
            quote_sync_config=qs_config,
            quote_sync_fn=lambda: None,
        )
        assert not runner._quote_sync_is_runnable()

    def test_quote_sync_not_runnable_when_no_fn(self):
        config = BackgroundConfig(enabled=True, run_quote_sync=True)
        qs_config = QuoteSyncConfig(enabled=True)
        runner = BackgroundRunner(
            config=config,
            logger=logging.getLogger("test"),
            quote_sync_config=qs_config,
            quote_sync_fn=None,
        )
        assert not runner._quote_sync_is_runnable()

    def test_quote_sync_not_runnable_when_background_flag_off(self):
        config = BackgroundConfig(enabled=True, run_quote_sync=False)
        qs_config = QuoteSyncConfig(enabled=True)
        runner = BackgroundRunner(
            config=config,
            logger=logging.getLogger("test"),
            quote_sync_config=qs_config,
            quote_sync_fn=lambda: None,
        )
        assert not runner._quote_sync_is_runnable()

    def test_quote_sync_runnable_when_all_enabled(self):
        config = BackgroundConfig(enabled=True, run_quote_sync=True)
        qs_config = QuoteSyncConfig(enabled=True)
        runner = BackgroundRunner(
            config=config,
            logger=logging.getLogger("test"),
            quote_sync_config=qs_config,
            quote_sync_fn=lambda: None,
        )
        assert runner._quote_sync_is_runnable()

    def test_quote_sync_cycle_calls_fn(self):
        called = []
        config = BackgroundConfig(enabled=True, run_quote_sync=True)
        qs_config = QuoteSyncConfig(enabled=True)
        runner = BackgroundRunner(
            config=config,
            logger=logging.getLogger("test"),
            quote_sync_config=qs_config,
            quote_sync_fn=lambda: called.append(1),
        )
        runner.run_quote_sync_cycle()
        assert len(called) == 1

    def test_quote_sync_cycle_handles_exception(self):
        config = BackgroundConfig(enabled=True, run_quote_sync=True)
        qs_config = QuoteSyncConfig(enabled=True)
        runner = BackgroundRunner(
            config=config,
            logger=logging.getLogger("test"),
            quote_sync_config=qs_config,
            quote_sync_fn=MagicMock(side_effect=RuntimeError("boom")),
        )
        # Should not raise
        runner.run_quote_sync_cycle()

    def test_has_runnable_tasks_includes_quote_sync(self):
        config = BackgroundConfig(enabled=True, run_quote_sync=True)
        qs_config = QuoteSyncConfig(enabled=True)
        runner = BackgroundRunner(
            config=config,
            logger=logging.getLogger("test"),
            quote_sync_config=qs_config,
            quote_sync_fn=lambda: None,
        )
        assert runner._has_runnable_tasks()


# -- CLI test --


class TestCLI:
    def test_sync_quotes_cli_dispatch(self):
        from tinvest_trader.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(["sync-quotes"])
        assert args.command == "sync-quotes"
        assert args.limit == 0

    def test_sync_quotes_cli_with_limit(self):
        from tinvest_trader.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(["sync-quotes", "--limit", "10"])
        assert args.limit == 10


# -- TBankClient.get_last_prices tests --


class TestTBankClientGetLastPrices:
    def test_empty_list_returns_empty(self, logger):
        from tinvest_trader.app.config import BrokerConfig
        from tinvest_trader.infra.tbank.client import TBankClient

        client = TBankClient(BrokerConfig(token="test"), logger)
        result = client.get_last_prices([])
        assert result == []

    def test_no_token_returns_empty(self, logger):
        from tinvest_trader.app.config import BrokerConfig
        from tinvest_trader.infra.tbank.client import TBankClient

        client = TBankClient(BrokerConfig(token=""), logger)
        result = client.get_last_prices(["uid-1", "uid-2"])
        assert result == []

    @patch("tinvest_trader.infra.tbank.client.urllib_request.urlopen")
    def test_parses_response(self, mock_urlopen, logger):
        import json

        from tinvest_trader.app.config import BrokerConfig
        from tinvest_trader.infra.tbank.client import TBankClient

        response_data = {
            "lastPrices": [
                {
                    "instrumentUid": "uid-sber",
                    "price": {"units": "250", "nano": 500000000},
                    "time": "2026-03-20T10:30:00Z",
                },
                {
                    "instrumentUid": "uid-gazp",
                    "price": {"units": "155", "nano": 250000000},
                    "time": "2026-03-20T10:30:00Z",
                },
            ],
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        client = TBankClient(BrokerConfig(token="test-token"), logger)
        result = client.get_last_prices(["uid-sber", "uid-gazp"])

        assert len(result) == 2
        assert result[0]["instrument_uid"] == "uid-sber"
        assert result[0]["price"] == pytest.approx(250.5)
        assert result[1]["instrument_uid"] == "uid-gazp"
        assert result[1]["price"] == pytest.approx(155.25)

    @patch("tinvest_trader.infra.tbank.client.urllib_request.urlopen")
    def test_skips_zero_prices(self, mock_urlopen, logger):
        import json

        from tinvest_trader.app.config import BrokerConfig
        from tinvest_trader.infra.tbank.client import TBankClient

        response_data = {
            "lastPrices": [
                {
                    "instrumentUid": "uid-1",
                    "price": {"units": "0", "nano": 0},
                    "time": "",
                },
            ],
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        client = TBankClient(BrokerConfig(token="test-token"), logger)
        result = client.get_last_prices(["uid-1"])

        assert len(result) == 0  # zero price skipped

    @patch("tinvest_trader.infra.tbank.client.urllib_request.urlopen")
    def test_api_error_returns_empty(self, mock_urlopen, logger):
        from urllib.error import HTTPError

        from tinvest_trader.app.config import BrokerConfig
        from tinvest_trader.infra.tbank.client import TBankClient

        mock_urlopen.side_effect = HTTPError(
            url="test", code=429, msg="Too Many Requests",
            hdrs=None, fp=MagicMock(read=MagicMock(return_value=b"rate limited")),
        )

        client = TBankClient(BrokerConfig(token="test-token"), logger)
        result = client.get_last_prices(["uid-1"])

        assert result == []


# -- QuoteSyncResult dataclass --


class TestQuoteSyncResult:
    def test_defaults(self):
        r = QuoteSyncResult()
        assert r.requested == 0
        assert r.received == 0
        assert r.inserted == 0
        assert r.skipped == 0
        assert r.failed == 0
        assert r.errors == []
