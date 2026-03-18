from __future__ import annotations

import logging
from dataclasses import dataclass, field

from tinvest_trader.app.config import AppConfig
from tinvest_trader.infra.logging.journal import setup_logging
from tinvest_trader.infra.tbank.client import TBankClient
from tinvest_trader.market_data.service import MarketDataService
from tinvest_trader.services.trading_service import TradingService


@dataclass
class Container:
    """Dependency container. Wires all components."""

    config: AppConfig
    logger: logging.Logger = field(init=False)
    tbank_client: TBankClient = field(init=False)
    market_data: MarketDataService = field(init=False)
    trading_service: TradingService = field(init=False)

    def __post_init__(self) -> None:
        self.logger = setup_logging(self.config.logging)
        self.tbank_client = TBankClient(config=self.config.broker, logger=self.logger)
        self.market_data = MarketDataService(client=self.tbank_client, logger=self.logger)
        self.trading_service = TradingService(logger=self.logger)


def build_container(config: AppConfig) -> Container:
    return Container(config=config)
