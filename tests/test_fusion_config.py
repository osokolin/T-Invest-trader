from tinvest_trader.app.config import FusionConfig, load_config


def test_fusion_disabled_by_default():
    cfg = load_config()
    assert cfg.fusion.enabled is False


def test_fusion_default_windows():
    cfg = FusionConfig()
    assert cfg.windows == ("5m", "15m", "1h", "1d", "7d", "30d")


def test_fusion_default_persist():
    cfg = FusionConfig()
    assert cfg.persist is True


def test_fusion_env_vars(monkeypatch):
    monkeypatch.setenv("TINVEST_FUSION_ENABLED", "true")
    monkeypatch.setenv("TINVEST_FUSION_WINDOWS", "10m,2h")
    monkeypatch.setenv("TINVEST_FUSION_PERSIST", "false")
    monkeypatch.setenv("TINVEST_FUSION_TRACKED_TICKERS", "SBER,GAZP")
    cfg = load_config()
    assert cfg.fusion.enabled is True
    assert cfg.fusion.windows == ("10m", "2h")
    assert cfg.fusion.persist is False
    assert cfg.fusion.tracked_tickers == ("SBER", "GAZP")


def test_background_fusion_interval(monkeypatch):
    monkeypatch.setenv("TINVEST_BACKGROUND_FUSION_INTERVAL_SECONDS", "120")
    cfg = load_config()
    assert cfg.background.fusion_interval_seconds == 120


def test_background_run_fusion_default():
    cfg = load_config()
    assert cfg.background.run_fusion is True


def test_background_run_fusion_disabled(monkeypatch):
    monkeypatch.setenv("TINVEST_BACKGROUND_RUN_FUSION", "false")
    cfg = load_config()
    assert cfg.background.run_fusion is False
