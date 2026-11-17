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
    """Next non-heartbeat frame within ``timeout``; ``hb`` frames are not replies."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            frame = out.get(timeout=max(deadline - time.monotonic(), 0.0))
        except queue.Empty as exc:
            raise AssertionError("timed out waiting for compute host JSON") from exc
        if frame.get("type") != "hb":
            return frame


def _read_hello(out: queue.Queue[dict], proc: subprocess.Popen) -> dict:
    """Wait for the ``hello`` frame, which is emitted only after the child's cold import of
    ``tui_gateway.server`` (2-10s+ on a loaded 4-worker runner). Boot is a different budget
    from the 2s frame round trip, so wait for it event-based: fail as soon as the child
    exits, with a generous ceiling only to bound a hang. The host heartbeats (``hb``) every
    15s from construction, before that import finishes, so a slow boot sees ``hb`` frames
    first; the supervisor ignores them while waiting and so do we."""
    deadline = time.monotonic() + _BOOT_CEILING_SECS
    while time.monotonic() < deadline:
        try:
            frame = out.get(timeout=0.2)
        except queue.Empty:
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr else ""
                raise AssertionError(
                    f"compute host exited before hello: rc={proc.returncode}\n{stderr[-2000:]}")
            continue
        if frame.get("type") != "hb":
            return frame
    raise AssertionError("timed out waiting for compute host hello")


class _AliveProc:
    def poll(self) -> None:
        return None


def test_read_hello_skips_heartbeats_that_precede_hello():
    out: queue.Queue[dict] = queue.Queue()
    for frame in ({"type": "hb"}, {"type": "hb"}, {"type": "hello", "host_pid": 1}):
        out.put(frame)
    assert _read_hello(out, _AliveProc())["type"] == "hello"  # type: ignore[arg-type]


def test_read_json_line_skips_heartbeats_before_reply():
    out: queue.Queue[dict] = queue.Queue()
    out.put({"type": "hb"})
    out.put({"type": "shutdown.ack"})
    assert _read_json_line(out)["type"] == "shutdown.ack"


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
