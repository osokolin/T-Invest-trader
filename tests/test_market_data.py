import logging

from tinvest_trader.app.config import BrokerConfig
from tinvest_trader.domain.enums import CandleInterval, TradingStatus
from tinvest_trader.domain.models import Candle, Instrument, MarketSnapshot
from tinvest_trader.infra.tbank.client import TBankClient
from tinvest_trader.market_data.service import MarketDataService


def _make_service() -> MarketDataService:
    client = TBankClient(config=BrokerConfig(), logger=logging.getLogger("test"))
    return MarketDataService(client=client, logger=logging.getLogger("test"))


def test_get_instrument():
    svc = _make_service()
    inst = svc.get_instrument("BBG000B9XRY4")
    assert isinstance(inst, Instrument)
    assert inst.figi == "BBG000B9XRY4"


def test_get_snapshot():
    svc = _make_service()
    snap = svc.get_snapshot("BBG000B9XRY4")
    assert isinstance(snap, MarketSnapshot)
    assert snap.instrument.figi == "BBG000B9XRY4"
    assert snap.trading_status == TradingStatus.OPEN
    assert snap.last_price.as_float > 0


def test_get_recent_candles():
    svc = _make_service()
    candles = svc.get_recent_candles("BBG000B9XRY4", CandleInterval.MIN_5)
    assert len(candles) >= 1
    assert isinstance(candles[0], Candle)
    assert candles[0].interval == CandleInterval.MIN_5
