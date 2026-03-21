"""Tests for global market data integration (structured price pipeline)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tinvest_trader.app.config import BackgroundConfig, GlobalMarketDataConfig
from tinvest_trader.infra.market_data.global_api_client import (
    DEFAULT_SYMBOLS,
    MarketSnapshot,
    fetch_all_instruments,
    normalize_response,
)
from tinvest_trader.services.global_market_data_sync import (
    GlobalMarketDataSyncResult,
    build_global_market_data_report,
    sync_global_market_data,
)

# ================================================================
# A. Normalization of API responses
# ================================================================


class TestNormalizeResponse:
    """Test normalize_response with various Yahoo Finance payloads."""

    def _build_raw(
        self,
        *,
        price: float = 5234.1,
        prev_close: float = 5192.0,
        market_time: int = 1711036800,
    ) -> dict:
        return {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "regularMarketPrice": price,
                            "chartPreviousClose": prev_close,
                            "regularMarketTime": market_time,
                        },
                    },
                ],
            },
        }

    def test_basic_normalization(self):
        raw = self._build_raw()
        snap = normalize_response("^GSPC", raw)

        assert snap is not None
        assert snap.symbol == "SPX"
        assert snap.category == "index"
        assert snap.price == 5234.1
        assert snap.change_pct is not None
        assert snap.source_name == "yahoo_finance"

    def test_change_pct_calculated(self):
        raw = self._build_raw(price=100.0, prev_close=95.0)
        snap = normalize_response("^GSPC", raw)

        expected_pct = round((100.0 - 95.0) / 95.0 * 100, 2)
        assert snap.change_pct == expected_pct

    def test_negative_change_pct(self):
        raw = self._build_raw(price=90.0, prev_close=100.0)
        snap = normalize_response("^GSPC", raw)

        assert snap.change_pct < 0

    def test_source_time_parsed(self):
        raw = self._build_raw(market_time=1711036800)
        snap = normalize_response("^GSPC", raw)

        assert snap.source_time is not None
        assert snap.source_time.tzinfo == UTC

    def test_brent_symbol(self):
        raw = self._build_raw()
        snap = normalize_response("BZ=F", raw)

        assert snap.symbol == "BRENT"
        assert snap.category == "oil"

    def test_vix_symbol(self):
        raw = self._build_raw()
        snap = normalize_response("^VIX", raw)

        assert snap.symbol == "VIX"
        assert snap.category == "volatility"

    def test_dxy_symbol(self):
        raw = self._build_raw()
        snap = normalize_response("DX-Y.NYB", raw)

        assert snap.symbol == "DXY"
        assert snap.category == "fx"

    def test_unknown_symbol_uses_raw(self):
        raw = self._build_raw()
        snap = normalize_response("UNKNOWN_SYM", raw)

        assert snap.symbol == "UNKNOWN_SYM"
        assert snap.category == "unknown"

    def test_malformed_response_returns_none(self):
        assert normalize_response("^GSPC", {}) is None
        assert normalize_response("^GSPC", {"chart": {}}) is None
        assert normalize_response("^GSPC", {"chart": {"result": []}}) is None

    def test_missing_prev_close(self):
        raw = {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "regularMarketPrice": 100.0,
                            "regularMarketTime": 1711036800,
                        },
                    },
                ],
            },
        }
        snap = normalize_response("^GSPC", raw)

        assert snap is not None
        assert snap.price == 100.0
        assert snap.change_pct is None

    def test_zero_prev_close_no_division_error(self):
        raw = self._build_raw(prev_close=0.0)
        snap = normalize_response("^GSPC", raw)

        assert snap is not None
        assert snap.change_pct is None

    def test_custom_symbol_map(self):
        raw = self._build_raw()
        custom = {"^GSPC": {"name": "SP500", "category": "equity_index"}}
        snap = normalize_response("^GSPC", raw, symbol_map=custom)

        assert snap.symbol == "SP500"
        assert snap.category == "equity_index"


# ================================================================
# B. Partial failure handling
# ================================================================


class TestFetchAllInstruments:
    """Test partial failure behavior in fetch_all_instruments."""

    @patch(
        "tinvest_trader.infra.market_data.global_api_client.fetch_instrument",
    )
    def test_partial_failure(self, mock_fetch):
        """If one symbol fails, others still succeed."""
        good_raw = {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "regularMarketPrice": 5000.0,
                            "chartPreviousClose": 4950.0,
                            "regularMarketTime": 1711036800,
                        },
                    },
                ],
            },
        }
        # Only first symbol succeeds
        mock_fetch.side_effect = [good_raw, None, None, None, None]

        logger = MagicMock()
        results = fetch_all_instruments(logger, symbols=DEFAULT_SYMBOLS)

        assert len(results) == 1
        assert results[0].symbol == "SPX"

    @patch(
        "tinvest_trader.infra.market_data.global_api_client.fetch_instrument",
    )
    def test_all_fail(self, mock_fetch):
        mock_fetch.return_value = None

        logger = MagicMock()
        results = fetch_all_instruments(logger, symbols=DEFAULT_SYMBOLS)

        assert results == []

    @patch(
        "tinvest_trader.infra.market_data.global_api_client.fetch_instrument",
    )
    def test_malformed_response_skipped(self, mock_fetch):
        """Malformed JSON is skipped gracefully."""
        mock_fetch.return_value = {"chart": {}}

        logger = MagicMock()
        results = fetch_all_instruments(logger, symbols=DEFAULT_SYMBOLS)

        assert results == []


# ================================================================
# C. Sync service (DB inserts)
# ================================================================


class TestSyncGlobalMarketData:
    """Test sync_global_market_data service."""

    @patch(
        "tinvest_trader.services.global_market_data_sync"
        ".fetch_all_instruments",
    )
    def test_sync_inserts_snapshots(self, mock_fetch):
        mock_fetch.return_value = [
            MarketSnapshot(
                symbol="SPX", category="index", price=5234.1,
                change_pct=0.8, source_time=datetime.now(tz=UTC),
            ),
            MarketSnapshot(
                symbol="BRENT", category="oil", price=84.7,
                change_pct=1.4, source_time=datetime.now(tz=UTC),
            ),
        ]

        repo = MagicMock()
        repo.insert_global_market_snapshot.return_value = True
        logger = MagicMock()

        result = sync_global_market_data(repo, logger)

        assert result.received == 2
        assert result.inserted == 2
        assert result.failed == 0

    @patch(
        "tinvest_trader.services.global_market_data_sync"
        ".fetch_all_instruments",
    )
    def test_sync_handles_db_failure(self, mock_fetch):
        mock_fetch.return_value = [
            MarketSnapshot(
                symbol="SPX", category="index", price=5234.1,
                change_pct=0.8, source_time=datetime.now(tz=UTC),
            ),
        ]

        repo = MagicMock()
        repo.insert_global_market_snapshot.return_value = False
        logger = MagicMock()

        result = sync_global_market_data(repo, logger)

        assert result.received == 1
        assert result.inserted == 0
        assert result.failed == 1

    @patch(
        "tinvest_trader.services.global_market_data_sync"
        ".fetch_all_instruments",
    )
    def test_sync_no_repo(self, mock_fetch):
        mock_fetch.return_value = [
            MarketSnapshot(
                symbol="SPX", category="index", price=5234.1,
                change_pct=0.8, source_time=datetime.now(tz=UTC),
            ),
        ]

        logger = MagicMock()
        result = sync_global_market_data(None, logger)

        assert result.received == 1
        assert result.inserted == 0

    @patch(
        "tinvest_trader.services.global_market_data_sync"
        ".fetch_all_instruments",
    )
    def test_sync_detects_missing_symbols(self, mock_fetch):
        mock_fetch.return_value = [
            MarketSnapshot(
                symbol="SPX", category="index", price=5234.1,
                change_pct=0.8, source_time=datetime.now(tz=UTC),
            ),
        ]

        repo = MagicMock()
        repo.insert_global_market_snapshot.return_value = True
        logger = MagicMock()

        result = sync_global_market_data(repo, logger)

        assert "BRENT" in result.missing_symbols
        assert "VIX" in result.missing_symbols
        assert "SPX" not in result.missing_symbols

    @patch(
        "tinvest_trader.services.global_market_data_sync"
        ".fetch_all_instruments",
    )
    def test_sync_empty_response(self, mock_fetch):
        mock_fetch.return_value = []

        repo = MagicMock()
        logger = MagicMock()
        result = sync_global_market_data(repo, logger)

        assert result.received == 0
        assert result.inserted == 0
        repo.insert_global_market_snapshot.assert_not_called()


# ================================================================
# D. Latest snapshot retrieval / reporting
# ================================================================


class TestBuildReport:
    """Test build_global_market_data_report."""

    def test_empty_report(self):
        repo = MagicMock()
        repo.get_latest_global_market_snapshots.return_value = []

        output = build_global_market_data_report(repo)

        assert "no data yet" in output

    def test_report_with_data(self):
        repo = MagicMock()
        repo.get_latest_global_market_snapshots.return_value = [
            {
                "symbol": "SPX", "category": "index",
                "price": 5234.1, "change_pct": 0.8,
                "source_time": datetime(2024, 3, 21, 16, 0, tzinfo=UTC),
                "fetched_at": datetime.now(tz=UTC),
                "source_name": "yahoo_finance",
            },
            {
                "symbol": "BRENT", "category": "oil",
                "price": 84.7, "change_pct": 1.4,
                "source_time": datetime(2024, 3, 21, 16, 0, tzinfo=UTC),
                "fetched_at": datetime.now(tz=UTC),
                "source_name": "yahoo_finance",
            },
            {
                "symbol": "VIX", "category": "volatility",
                "price": 14.2, "change_pct": -3.0,
                "source_time": datetime(2024, 3, 21, 16, 0, tzinfo=UTC),
                "fetched_at": datetime.now(tz=UTC),
                "source_name": "yahoo_finance",
            },
        ]

        output = build_global_market_data_report(repo)

        assert "SPX" in output
        assert "BRENT" in output
        assert "VIX" in output
        assert "indices:" in output
        assert "oil:" in output
        assert "volatility:" in output

    def test_report_missing_change_pct(self):
        repo = MagicMock()
        repo.get_latest_global_market_snapshots.return_value = [
            {
                "symbol": "DXY", "category": "fx",
                "price": 104.2, "change_pct": None,
                "source_time": None,
                "fetched_at": datetime.now(tz=UTC),
                "source_name": "yahoo_finance",
            },
        ]

        output = build_global_market_data_report(repo)

        assert "DXY: 104.20" in output
        assert "%" not in output.split("DXY")[1].split("\n")[0]


# ================================================================
# E. Background wiring
# ================================================================


class TestBackgroundWiring:
    """Test background runner integration."""

    def test_global_market_data_is_runnable(self):
        from tinvest_trader.services.background_runner import BackgroundRunner

        cfg = BackgroundConfig(enabled=True, run_global_market_data=True)
        runner = BackgroundRunner(
            config=cfg,
            logger=MagicMock(),
            global_market_data_fn=lambda: None,
        )

        assert runner._global_market_data_is_runnable()

    def test_global_market_data_not_runnable_when_disabled(self):
        from tinvest_trader.services.background_runner import BackgroundRunner

        cfg = BackgroundConfig(enabled=True, run_global_market_data=False)
        runner = BackgroundRunner(
            config=cfg,
            logger=MagicMock(),
            global_market_data_fn=lambda: None,
        )

        assert not runner._global_market_data_is_runnable()

    def test_global_market_data_not_runnable_without_fn(self):
        from tinvest_trader.services.background_runner import BackgroundRunner

        cfg = BackgroundConfig(enabled=True, run_global_market_data=True)
        runner = BackgroundRunner(
            config=cfg,
            logger=MagicMock(),
        )

        assert not runner._global_market_data_is_runnable()

    def test_cycle_exception_safe(self):
        from tinvest_trader.services.background_runner import BackgroundRunner

        def _failing_fn():
            msg = "API timeout"
            raise TimeoutError(msg)

        cfg = BackgroundConfig(enabled=True, run_global_market_data=True)
        runner = BackgroundRunner(
            config=cfg,
            logger=MagicMock(),
            global_market_data_fn=_failing_fn,
        )

        # Should not raise
        runner.run_global_market_data_cycle()


# ================================================================
# F. CLI sync shape
# ================================================================


class TestGlobalMarketDataSyncResult:
    """Test GlobalMarketDataSyncResult model."""

    def test_defaults(self):
        result = GlobalMarketDataSyncResult()

        assert result.requested == 0
        assert result.received == 0
        assert result.inserted == 0
        assert result.failed == 0
        assert result.missing_symbols == []

    def test_with_data(self):
        result = GlobalMarketDataSyncResult(
            requested=5, received=4, inserted=4,
            failed=0, missing_symbols=["VIX"],
        )

        assert result.requested == 5
        assert result.missing_symbols == ["VIX"]


# ================================================================
# G. Unknown / missing symbols handled safely
# ================================================================


class TestMissingSymbols:
    """Test graceful handling of unknown and missing symbols."""

    def test_unknown_yahoo_symbol_normalized(self):
        raw = {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "regularMarketPrice": 42.0,
                            "regularMarketTime": 1711036800,
                        },
                    },
                ],
            },
        }

        snap = normalize_response("NONEXISTENT", raw)

        assert snap is not None
        assert snap.symbol == "NONEXISTENT"
        assert snap.category == "unknown"

    @patch(
        "tinvest_trader.services.global_market_data_sync"
        ".fetch_all_instruments",
    )
    def test_sync_with_custom_symbols(self, mock_fetch):
        """Custom symbol map works correctly."""
        mock_fetch.return_value = [
            MarketSnapshot(
                symbol="CUSTOM", category="test", price=1.0,
                change_pct=0.0, source_time=datetime.now(tz=UTC),
            ),
        ]

        custom_symbols = {"TEST=F": {"name": "CUSTOM", "category": "test"}}
        repo = MagicMock()
        repo.insert_global_market_snapshot.return_value = True
        logger = MagicMock()

        result = sync_global_market_data(
            repo, logger, symbols=custom_symbols,
        )

        assert result.requested == 1
        assert result.received == 1


# ================================================================
# H. MarketSnapshot model
# ================================================================


class TestMarketSnapshot:
    """Test MarketSnapshot model."""

    def test_creation(self):
        snap = MarketSnapshot(
            symbol="SPX", category="index", price=5234.1,
            change_pct=0.8,
            source_time=datetime(2024, 3, 21, 16, 0, tzinfo=UTC),
        )

        assert snap.symbol == "SPX"
        assert snap.source_name == "yahoo_finance"

    def test_default_source_name(self):
        snap = MarketSnapshot(
            symbol="TEST", category="test", price=1.0,
            change_pct=None, source_time=None,
        )

        assert snap.source_name == "yahoo_finance"


# ================================================================
# I. Config defaults
# ================================================================


class TestGlobalMarketDataConfig:
    """Test config defaults."""

    def test_defaults(self):
        cfg = GlobalMarketDataConfig()

        assert cfg.enabled is False
        assert cfg.poll_interval_seconds == 300
        assert "^GSPC" in cfg.symbols
        assert "BZ=F" in cfg.symbols

    def test_background_config_has_run_flag(self):
        cfg = BackgroundConfig()

        assert cfg.run_global_market_data is True

    def test_default_symbols_cover_all_categories(self):
        categories = {info["category"] for info in DEFAULT_SYMBOLS.values()}

        assert "index" in categories
        assert "oil" in categories
        assert "fx" in categories
        assert "volatility" in categories
