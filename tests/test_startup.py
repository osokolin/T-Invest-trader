import os
import signal
import subprocess
import sys
import time


def _wait_for_startup(proc: subprocess.Popen[str]) -> tuple[bool, list[str]]:
    deadline = time.monotonic() + 5
    output_lines: list[str] = []
    started = False
    while time.monotonic() < deadline:
        line = proc.stderr.readline()
        if not line and proc.poll() is not None:
            break
        output_lines.append(line)
        if "started successfully" in line:
            started = True
            break
    return started, output_lines


def _shutdown_and_collect(
    proc: subprocess.Popen[str],
    output_lines: list[str],
) -> tuple[int, str]:
    proc.send_signal(signal.SIGINT)
    try:
        _stdout, remaining = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        _stdout, remaining = proc.communicate(timeout=5)

    full_output = "".join(output_lines) + remaining
    assert proc.returncode is not None
    return proc.returncode, full_output


def _kill_and_collect(
    proc: subprocess.Popen[str],
    output_lines: list[str],
) -> tuple[int, str]:
    proc.kill()
    proc.wait(timeout=5)
    remaining = proc.stderr.read()
    full_output = "".join(output_lines) + remaining
    assert proc.returncode is not None
    return proc.returncode, full_output


def test_app_runs_without_crashing():
    """Run the app entrypoint, verify startup, then send SIGTERM for clean shutdown."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "tinvest_trader.app.main"],
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        text=True,
    )

    started, output_lines = _wait_for_startup(proc)

    assert started, f"App did not start. Output: {''.join(output_lines)}"

    returncode, full_output = _shutdown_and_collect(proc, output_lines)
    assert returncode == 0
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

    started, output_lines = _wait_for_startup(proc)

    assert started, f"App did not start. Output: {''.join(output_lines)}"

    returncode, full_output = _kill_and_collect(proc, output_lines)
    assert returncode != 0
    assert "background runner" in full_output
