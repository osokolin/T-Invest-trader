import os
import signal
import subprocess
import sys
import time


def test_app_runs_without_crashing():
    """Run the app entrypoint, verify startup, then send SIGTERM for clean shutdown."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "tinvest_trader.app.main"],
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        text=True,
    )

    # Wait for startup to complete (up to 5s)
    deadline = time.monotonic() + 5
    output_lines = []
    started = False
    while time.monotonic() < deadline:
        line = proc.stderr.readline()
        if not line and proc.poll() is not None:
            break
        output_lines.append(line)
        if "started successfully" in line:
            started = True
            break

    assert started, f"App did not start. Output: {''.join(output_lines)}"

    # Send SIGTERM for clean shutdown
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=5)

    # Read remaining output
    remaining = proc.stderr.read()
    full_output = "".join(output_lines) + remaining

    assert proc.returncode == 0
    assert "shutdown complete" in full_output


def test_app_runs_with_background_enabled_without_optional_services():
    """Background runner should not block startup when pipelines are disabled."""
    env = os.environ.copy()
    env["TINVEST_BACKGROUND_ENABLED"] = "true"

    proc = subprocess.Popen(
        [sys.executable, "-m", "tinvest_trader.app.main"],
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        text=True,
        env=env,
    )

    deadline = time.monotonic() + 5
    output_lines = []
    started = False
    while time.monotonic() < deadline:
        line = proc.stderr.readline()
        if not line and proc.poll() is not None:
            break
        output_lines.append(line)
        if "started successfully" in line:
            started = True
            break

    assert started, f"App did not start. Output: {''.join(output_lines)}"

    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=5)

    remaining = proc.stderr.read()
    full_output = "".join(output_lines) + remaining

    assert proc.returncode == 0
    assert "background runner" in full_output
