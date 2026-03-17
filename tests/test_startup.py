import subprocess
import sys


def test_app_runs_without_crashing():
    """Run the app entrypoint and verify it exits cleanly."""
    result = subprocess.run(
        [sys.executable, "-m", "tinvest_trader.app.main"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert "tinvest_trader started successfully" in result.stderr
    assert "shutdown complete" in result.stderr
