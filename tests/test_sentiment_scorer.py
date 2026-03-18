"""Tests for sentiment/scorer.py -- stub sentiment scoring."""

from tinvest_trader.sentiment.scorer import StubSentimentScorer


def _scorer() -> StubSentimentScorer:
    return StubSentimentScorer(model_name="test-stub")


def test_positive_sentiment():
    result = _scorer().score("#SBER растет на фоне прибыли")
    assert result.label == "positive"
    assert result.score_positive > result.score_negative


def test_negative_sentiment():
    result = _scorer().score("$GAZP падение на фоне санкций и обвала")
    assert result.label == "negative"
    assert result.score_negative > result.score_positive


def test_neutral_sentiment():
    result = _scorer().score("Рынок в боковике, без изменений")
    assert result.label == "neutral"
    assert result.score_neutral >= result.score_positive
    assert result.score_neutral >= result.score_negative


def test_scores_sum_approximately_to_one():
    result = _scorer().score("любой текст")
    total = result.score_positive + result.score_negative + result.score_neutral
    assert abs(total - 1.0) < 0.01


def test_model_name_preserved():
    result = _scorer().score("текст")
    assert result.model_name == "test-stub"


def test_scored_at_is_set():
    result = _scorer().score("текст")
    assert result.scored_at is not None
