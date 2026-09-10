"""Docker healthcheck: loop liveness plus independent storage readiness."""

from __future__ import annotations

import json
import time

from tinvest_trader.app.config import load_config
from tinvest_trader.infra.storage.health import inspect_storage
from tinvest_trader.services.background_runner import HEARTBEAT_PATH


def main() -> int:
    config = load_config()
    issues = []
    if config.background.enabled:
        try:
            age = time.time() - HEARTBEAT_PATH.stat().st_mtime
            if not 0 <= age < 300:
                issues.append("background heartbeat stale")
        except OSError:
            issues.append("background heartbeat missing")
    report = inspect_storage(config, persist=True) if config.storage_health.enabled else {}
    if report.get("status") == "critical":
        issues.extend(report["issues"])
    print(json.dumps({"healthy": not issues, "issues": issues, "storage": report}))
    return int(bool(issues))


if __name__ == "__main__":
    raise SystemExit(main())
