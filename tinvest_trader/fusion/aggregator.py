"""Fusion aggregator -- combines sentiment observations and broker event features."""

from __future__ import annotations

from datetime import datetime

from tinvest_trader.fusion.models import FusedSignalFeature
from tinvest_trader.observation.models import SignalObservation


def fuse_signals(
    observation: SignalObservation | None,
    broker_events: list[dict],
    ticker: str,
    figi: str | None,
    window: str,
    observation_time: datetime,
) -> FusedSignalFeature:
    """Combine a sentiment observation with broker event feature rows.

    Parameters
    ----------
    observation:
        SignalObservation for this ticker/window, or None if no sentiment data.
    broker_events:
        List of dicts with keys: source_method, event_type, event_direction,
        event_value, currency, event_time. Typically fetched from
        broker_event_features for the same ticker and window.
    ticker, figi, window, observation_time:
        Identifiers for the fused row.
    """
    # Sentiment side
    if observation is not None:
        s_msg = observation.message_count
        s_pos = observation.positive_count
        s_neg = observation.negative_count
        s_neu = observation.neutral_count
        s_pos_avg = observation.positive_score_avg
        s_neg_avg = observation.negative_score_avg
        s_neu_avg = observation.neutral_score_avg
        s_balance = observation.sentiment_balance
    else:
        s_msg = s_pos = s_neg = s_neu = None
        s_pos_avg = s_neg_avg = s_neu_avg = s_balance = None

    # Broker event side -- count by source_method
    div_count = 0
    rep_count = 0
    ins_count = 0
    latest_div_value: float | None = None
    latest_div_currency: str | None = None
    latest_div_time: datetime | None = None
    latest_rep_time: datetime | None = None
    latest_ins_time: datetime | None = None

    for ev in broker_events:
        method = ev.get("source_method", "")
        ev_time = ev.get("event_time")

        if method == "dividends":
            div_count += 1
            if ev_time is not None and (latest_div_time is None or ev_time > latest_div_time):
                latest_div_time = ev_time
                latest_div_value = ev.get("event_value")
                latest_div_currency = ev.get("currency")
        elif method == "reports":
            rep_count += 1
            if ev_time is not None and (latest_rep_time is None or ev_time > latest_rep_time):
                latest_rep_time = ev_time
        elif method == "insider_deals":
            ins_count += 1
            if ev_time is not None and (latest_ins_time is None or ev_time > latest_ins_time):
                latest_ins_time = ev_time

    total_count = div_count + rep_count + ins_count

    return FusedSignalFeature(
        ticker=ticker,
        figi=figi,
        window=window,
        observation_time=observation_time,
        sentiment_message_count=s_msg,
        sentiment_positive_count=s_pos,
        sentiment_negative_count=s_neg,
        sentiment_neutral_count=s_neu,
        sentiment_positive_avg=s_pos_avg,
        sentiment_negative_avg=s_neg_avg,
        sentiment_neutral_avg=s_neu_avg,
        sentiment_balance=s_balance,
        broker_dividends_count=div_count,
        broker_reports_count=rep_count,
        broker_insider_deals_count=ins_count,
        broker_total_event_count=total_count,
        broker_latest_dividend_value=latest_div_value,
        broker_latest_dividend_currency=latest_div_currency,
        broker_latest_report_time=latest_rep_time,
        broker_latest_insider_deal_time=latest_ins_time,
    )
