"""Tests for signal generation service."""

from __future__ import annotations

from unittest.mock import MagicMock

from tinvest_trader.services.signal_generation import (
    SignalCandidate,
    SignalGenerationConfig,
    SignalGenerationResult,
    evaluate_fused_row,
    format_signal_generation_result,
    generate_signals,
    select_best_per_ticker,
)

# ── evaluate_fused_row ──


class TestEvaluateFusedRow:
    def _config(self, **kw):
        return SignalGenerationConfig(**kw)

    def test_passes_threshold_positive(self):
        row = {
            "ticker": "SBER",
            "window": "15m",
            "observation_time": "2026-03-23T10:00:00",
            "sentiment_message_count": 5,
            "sentiment_balance": 0.65,
            "sentiment_positive_avg": 0.7,
            "sentiment_negative_avg": 0.1,
            "id": 123,
        }
        result = evaluate_fused_row(row, self._config())
        assert result is not None
        assert result.direction == "up"
        assert result.ticker == "SBER"
        assert result.confidence == 0.65

    def test_passes_threshold_negative(self):
        row = {
            "ticker": "GAZP",
            "window": "1h",
            "observation_time": "2026-03-23T10:00:00",
            "sentiment_message_count": 4,
            "sentiment_balance": -0.5,
            "sentiment_positive_avg": 0.2,
            "sentiment_negative_avg": 0.6,
            "id": 456,
        }
        result = evaluate_fused_row(row, self._config())
        assert result is not None
        assert result.direction == "down"
        assert result.confidence == 0.5

    def test_below_message_count_threshold(self):
        row = {
            "ticker": "SBER",
            "window": "5m",
            "observation_time": "2026-03-23T10:00:00",
            "sentiment_message_count": 2,
            "sentiment_balance": 0.8,
            "id": 789,
        }
        result = evaluate_fused_row(row, self._config(min_message_count=3))
        assert result is None

    def test_below_balance_threshold(self):
        row = {
            "ticker": "VTBR",
            "window": "15m",
            "observation_time": "2026-03-23T10:00:00",
            "sentiment_message_count": 5,
            "sentiment_balance": 0.1,
            "id": 101,
        }
        result = evaluate_fused_row(row, self._config(min_sentiment_balance=0.3))
        assert result is None

    def test_none_balance_skipped(self):
        row = {
            "ticker": "LKOH",
            "window": "15m",
            "observation_time": "2026-03-23T10:00:00",
            "sentiment_message_count": 5,
            "sentiment_balance": None,
            "id": 102,
        }
        result = evaluate_fused_row(row, self._config())
        assert result is None

    def test_zero_message_count_skipped(self):
        row = {
            "ticker": "ROSN",
            "window": "15m",
            "observation_time": "2026-03-23T10:00:00",
            "sentiment_message_count": 0,
            "sentiment_balance": 0.8,
            "id": 103,
        }
        result = evaluate_fused_row(row, self._config())
        assert result is None

    def test_confidence_capped_at_one(self):
        row = {
            "ticker": "SBER",
            "window": "15m",
            "observation_time": "2026-03-23T10:00:00",
            "sentiment_message_count": 10,
            "sentiment_balance": 1.5,
            "sentiment_positive_avg": None,
            "sentiment_negative_avg": None,
            "id": 104,
        }
        result = evaluate_fused_row(row, self._config())
        assert result is not None
        assert result.confidence == 1.0

    def test_features_json_populated(self):
        row = {
            "ticker": "SBER",
            "window": "1h",
            "observation_time": "2026-03-23T10:00:00",
            "sentiment_message_count": 5,
            "sentiment_balance": 0.6,
            "sentiment_positive_avg": 0.7,
            "sentiment_negative_avg": 0.1,
            "id": 105,
        }
        result = evaluate_fused_row(row, self._config())
        assert result is not None
        assert result.features_json["fused_feature_id"] == 105
        assert result.features_json["window"] == "1h"


# ── generate_signals ──


class TestGenerateSignals:
    def _mock_repo(self, rows=None, exists=False):
        repo = MagicMock()
        repo.list_recent_fused_features.return_value = rows or []
        repo.signal_exists_recent.return_value = exists
        repo.insert_signal_prediction.return_value = 42
        return repo

    def _fused_row(self, ticker="SBER", balance=0.6, msgs=5, **kw):
        row = {
            "id": kw.get("id", 1),
            "ticker": ticker,
            "window": kw.get("window", "15m"),
            "observation_time": kw.get(
                "observation_time", "2026-03-23T10:00:00",
            ),
            "sentiment_message_count": msgs,
            "sentiment_balance": balance,
            "sentiment_positive_avg": 0.7,
            "sentiment_negative_avg": 0.1,
        }
        return row

    def test_generates_signal(self):
        repo = self._mock_repo(rows=[self._fused_row()])
        logger = MagicMock()
        result = generate_signals(repo, logger)
        assert result.inserted == 1
        assert result.candidates == 1
        repo.insert_signal_prediction.assert_called_once()

    def test_skips_below_threshold(self):
        repo = self._mock_repo(rows=[self._fused_row(balance=0.1)])
        logger = MagicMock()
        result = generate_signals(repo, logger)
        assert result.skipped_threshold == 1
        assert result.inserted == 0

    def test_skips_duplicate(self):
        repo = self._mock_repo(
            rows=[self._fused_row()], exists=True,
        )
        logger = MagicMock()
        result = generate_signals(repo, logger)
        assert result.skipped_duplicate == 1
        assert result.inserted == 0

    def test_dry_run_no_insert(self):
        repo = self._mock_repo(rows=[self._fused_row()])
        logger = MagicMock()
        result = generate_signals(repo, logger, dry_run=True)
        assert result.inserted == 1
        repo.insert_signal_prediction.assert_not_called()
        assert result.signals[0]["dry_run"] is True

    def test_handles_insert_failure(self):
        repo = self._mock_repo(rows=[self._fused_row()])
        repo.insert_signal_prediction.return_value = None
        logger = MagicMock()
        result = generate_signals(repo, logger)
        assert result.failed == 1
        assert result.inserted == 0

    def test_empty_rows(self):
        repo = self._mock_repo(rows=[])
        logger = MagicMock()
        result = generate_signals(repo, logger)
        assert result.rows_seen == 0
        assert result.candidates == 0

    def test_multiple_rows_mixed(self):
        rows = [
            self._fused_row(ticker="SBER", balance=0.6, id=1),
            self._fused_row(ticker="GAZP", balance=0.1, id=2),
            self._fused_row(ticker="LKOH", balance=-0.5, id=3),
        ]
        repo = self._mock_repo(rows=rows)
        logger = MagicMock()
        result = generate_signals(repo, logger)
        assert result.rows_seen == 3
        assert result.candidates == 2
        assert result.inserted == 2
        assert result.skipped_threshold == 1

    def test_config_passed_to_repo(self):
        repo = self._mock_repo(rows=[])
        logger = MagicMock()
        cfg = SignalGenerationConfig(lookback_minutes=15, limit=100)
        generate_signals(repo, logger, config=cfg)
        repo.list_recent_fused_features.assert_called_once_with(
            lookback_minutes=15, limit=100,
        )


# ── format_signal_generation_result ──


class TestFormatResult:
    def test_format_basic(self):
        result = SignalGenerationResult(
            rows_seen=10, candidates_before_dedup=5,
            candidates=3, skipped_ticker_dedup=2,
            inserted=2, skipped_threshold=5,
            skipped_duplicate=1, failed=0,
        )
        text = format_signal_generation_result(result)
        assert "rows_seen: 10" in text
        assert "candidates_before_dedup: 5" in text
        assert "candidates: 3" in text
        assert "skipped_ticker_dedup: 2" in text
        assert "inserted: 2" in text

    def test_format_with_signals(self):
        result = SignalGenerationResult(
            rows_seen=1, candidates=1, inserted=1,
            signals=[{
                "ticker": "SBER", "direction": "up",
                "confidence": 0.65, "signal_id": 42,
            }],
        )
        text = format_signal_generation_result(result)
        assert "SBER" in text
        assert "up" in text

    def test_format_dry_run_signal(self):
        result = SignalGenerationResult(
            rows_seen=1, candidates=1, inserted=1,
            signals=[{
                "ticker": "GAZP", "direction": "down",
                "confidence": 0.5, "dry_run": True,
            }],
        )
        text = format_signal_generation_result(result)
        assert "[DRY_RUN]" in text


# ── select_best_per_ticker ──


class TestSelectBestPerTicker:
    def _candidate(self, ticker="SBER", confidence=0.6, obs="2026-03-23T10:00:00",
                   window="15m"):
        return SignalCandidate(
            ticker=ticker,
            direction="up" if confidence > 0 else "down",
            confidence=abs(confidence),
            window=window,
            observation_time=obs,
            features_json={},
        )

    def test_multiple_windows_one_signal(self):
        candidates = [
            self._candidate(ticker="LKOH", confidence=0.5, window="5m"),
            self._candidate(ticker="LKOH", confidence=0.7, window="15m"),
            self._candidate(ticker="LKOH", confidence=0.4, window="1h"),
        ]
        result = select_best_per_ticker(candidates)
        assert len(result) == 1
        assert result[0].confidence == 0.7
        assert result[0].window == "15m"

    def test_highest_balance_selected(self):
        candidates = [
            self._candidate(ticker="SBER", confidence=0.3),
            self._candidate(ticker="SBER", confidence=0.9),
            self._candidate(ticker="SBER", confidence=0.6),
        ]
        result = select_best_per_ticker(candidates)
        assert len(result) == 1
        assert result[0].confidence == 0.9

    def test_equal_balance_latest_wins(self):
        candidates = [
            self._candidate(ticker="GAZP", confidence=0.5,
                            obs="2026-03-23T09:00:00"),
            self._candidate(ticker="GAZP", confidence=0.5,
                            obs="2026-03-23T10:00:00"),
        ]
        result = select_best_per_ticker(candidates)
        assert len(result) == 1
        assert result[0].observation_time == "2026-03-23T10:00:00"

    def test_single_candidate_unchanged(self):
        candidates = [self._candidate(ticker="VTBR", confidence=0.4)]
        result = select_best_per_ticker(candidates)
        assert len(result) == 1
        assert result[0].ticker == "VTBR"

    def test_different_tickers_all_kept(self):
        candidates = [
            self._candidate(ticker="SBER", confidence=0.5),
            self._candidate(ticker="GAZP", confidence=0.6),
            self._candidate(ticker="LKOH", confidence=0.7),
        ]
        result = select_best_per_ticker(candidates)
        assert len(result) == 3

    def test_empty_list(self):
        assert select_best_per_ticker([]) == []


class TestGenerateSignalsPerTickerDedup:
    def _mock_repo(self, rows=None, exists=False):
        repo = MagicMock()
        repo.list_recent_fused_features.return_value = rows or []
        repo.signal_exists_recent.return_value = exists
        repo.insert_signal_prediction.return_value = 42
        return repo

    def _fused_row(self, ticker="SBER", balance=0.6, msgs=5, **kw):
        return {
            "id": kw.get("id", 1),
            "ticker": ticker,
            "window": kw.get("window", "15m"),
            "observation_time": kw.get(
                "observation_time", "2026-03-23T10:00:00",
            ),
            "sentiment_message_count": msgs,
            "sentiment_balance": balance,
            "sentiment_positive_avg": 0.7,
            "sentiment_negative_avg": 0.1,
        }

    def test_dedup_across_windows(self):
        rows = [
            self._fused_row(ticker="LKOH", balance=0.5, window="5m", id=1),
            self._fused_row(ticker="LKOH", balance=0.8, window="15m", id=2),
            self._fused_row(ticker="LKOH", balance=0.4, window="1h", id=3),
        ]
        repo = self._mock_repo(rows=rows)
        logger = MagicMock()
        result = generate_signals(repo, logger)
        assert result.candidates_before_dedup == 3
        assert result.candidates == 1
        assert result.skipped_ticker_dedup == 2
        assert result.inserted == 1

    def test_result_summary_includes_dedup(self):
        result = SignalGenerationResult(
            rows_seen=5,
            candidates_before_dedup=4,
            candidates=2,
            skipped_ticker_dedup=2,
            inserted=2,
        )
        s = result.summary()
        assert "candidates_before_dedup=4" in s
        assert "skipped_ticker_dedup=2" in s


# ── BackgroundRunner integration ──


class TestBackgroundRunnerIntegration:
    def test_signal_generation_cycle_calls_fn(self):
        from tinvest_trader.services.background_runner import BackgroundRunner

        config = MagicMock()
        config.enabled = True
        config.run_signal_generation = True
        gen_fn = MagicMock(return_value="result")
        gen_config = MagicMock()
        gen_config.enabled = True

        runner = BackgroundRunner(
            config=config,
            logger=MagicMock(),
            signal_generation_fn=gen_fn,
            signal_generation_config=gen_config,
        )
        runner.run_signal_generation_cycle()
        gen_fn.assert_called_once()

    def test_signal_generation_disabled(self):
        from tinvest_trader.services.background_runner import BackgroundRunner

        config = MagicMock()
        config.run_signal_generation = False
        gen_fn = MagicMock()

        runner = BackgroundRunner(
            config=config,
            logger=MagicMock(),
            signal_generation_fn=gen_fn,
        )
        runner.run_signal_generation_cycle()
        gen_fn.assert_not_called()

    def test_signal_generation_exception_safe(self):
        from tinvest_trader.services.background_runner import BackgroundRunner

        config = MagicMock()
        config.run_signal_generation = True
        gen_fn = MagicMock(side_effect=RuntimeError("boom"))
        gen_config = MagicMock()
        gen_config.enabled = True

        runner = BackgroundRunner(
            config=config,
            logger=MagicMock(),
            signal_generation_fn=gen_fn,
            signal_generation_config=gen_config,
        )
        # Must not raise
        runner.run_signal_generation_cycle()
