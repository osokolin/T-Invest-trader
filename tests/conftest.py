import pytest

from tinvest_trader.app.config import AppConfig, load_config
from tinvest_trader.app.container import Container, build_container


@pytest.fixture()
def config() -> AppConfig:
    return load_config()


@pytest.fixture()
def container(config: AppConfig) -> Container:
    return build_container(config)
