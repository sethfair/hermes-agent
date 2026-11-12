import json
import pytest
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

_BOOT_CEILING_SECS = 120.0


def _stdout_queue(proc: subprocess.Popen) -> queue.Queue[dict]:
    out: queue.Queue[dict] = queue.Queue()
    assert proc.stdout is not None

    def drain() -> None:
        for line in proc.stdout or []:
            out.put(json.loads(line))

    threading.Thread(target=drain, daemon=True).start()
    return out


def _read_json_line(out: queue.Queue[dict], timeout: float = 2.0) -> dict:
    try:
        return out.get(timeout=timeout)
    except queue.Empty as exc:
        raise AssertionError("timed out waiting for compute host JSON") from exc


def _read_hello(out: queue.Queue[dict], proc: subprocess.Popen) -> dict:
    """Wait for the first frame, which is emitted only after the child's cold import of
    ``tui_gateway.server`` (2-10s on a loaded 4-worker runner). Boot is a different budget
    from the 2s frame round trip, so wait for it event-based: fail as soon as the child
    exits, with a generous ceiling only to bound a hang."""
    deadline = time.monotonic() + _BOOT_CEILING_SECS
    while time.monotonic() < deadline:
        try:
            return out.get(timeout=0.2)
        except queue.Empty:
            assert proc.poll() is None, f"compute host exited before hello: rc={proc.returncode}"
    raise AssertionError("timed out waiting for compute host hello")


@pytest.mark.platforms("linux")
def test_compute_host_line_json_hello_and_shutdown():
    repo = Path(__file__).resolve().parents[2]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(repo) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [sys.executable, "-m", "tui_gateway.compute_host"],
        cwd=str(repo),
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    assert proc.stdin is not None
    out = _stdout_queue(proc)
    try:
        hello = _read_hello(out, proc)
        assert hello["type"] == "hello"
        assert hello["host_pid"] == proc.pid

        proc.stdin.write(json.dumps({"type": "bogus", "request_id": "b"}) + "\n")
        proc.stdin.flush()
        error = _read_json_line(out)
        assert error["type"] == "error"
        assert error["message"] == "unknown frame type: bogus"

        proc.stdin.write(json.dumps({"type": "shutdown", "request_id": "stop"}) + "\n")
        proc.stdin.flush()
        assert _read_json_line(out)["type"] == "shutdown.ack"
        proc.wait(timeout=2)
    finally:
        if proc.poll() is None:
            proc.kill()
