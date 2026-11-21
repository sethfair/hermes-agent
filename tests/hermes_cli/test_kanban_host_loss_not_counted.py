"""A worker lost to its host must not spend the card's failure budget.

On MacPro a stalled gateway answered managed workers late; every worker waiting at that moment
died together (``worker_receipt_timeout`` / ``EOFError`` / an owner admission settled
``interrupted``) and each card, with ``DEFAULT_FAILURE_LIMIT = 2``, was parked ``blocked`` after
two such losses although nothing about the card was wrong. These are booked through the same
``infrastructure`` path a refused spawn uses: the run and event say so, ``consecutive_failures``
stays put, the breaker never trips, and the guard spaces the retry so a stalled gateway is not
hammered. A genuine worker error still counts.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from agent import managed_worker
from gateway.session_managed_worker import _worker_error_reason
from hermes_cli import kanban_db_dispatch as kbd
from tests.hermes_cli.test_kanban_owner_receipt_reclaim import NOW, _Board

SETTLED = ("terminal", "interrupted")


@pytest.fixture
def board(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    scratch = _Board(tmp_path)
    yield scratch
    scratch.close()


def _lose(board, tid, *, reason=None, admission=SETTLED, failures=1):
    """A dead managed attempt whose owner recorded ``reason`` (or nothing), ``failures`` already spent."""
    run = board.dead_attempt(tid, worker_bound=True, admission=admission)
    if reason is not None:
        board.event(tid, run, "worker_lost", {"reason": reason, "run_id": run}, NOW - 60)
    board.conn.execute("UPDATE tasks SET consecutive_failures = ? WHERE id = ?", (failures, tid))
    board.conn.commit()
    return run


def _booked(board, tid):
    row = board.state(tid)
    run = board.conn.execute(
        "SELECT outcome, metadata FROM task_runs WHERE task_id = ? ORDER BY id DESC LIMIT 1", (tid,)
    ).fetchone()
    return row, run["outcome"], json.loads(run["metadata"] or "{}")


def _worker_emitted_reason(monkeypatch, exc):
    """Run the worker error path and consume its frame through the gateway seam."""
    sent = []

    class Channel:
        stream = SimpleNamespace(close=lambda: None)

        def send(self, kind, **payload):
            sent.append({"type": kind, **payload})

    monkeypatch.setattr(managed_worker, "WorkerChannel", lambda _stream: Channel())
    monkeypatch.setattr(managed_worker.os, "fdopen", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(managed_worker.os, "dup2", lambda *_args: None)
    monkeypatch.setattr(managed_worker, "hello", dict)
    monkeypatch.setattr(managed_worker, "read_frame", lambda _stream: {})
    monkeypatch.setattr(managed_worker, "validate_bootstrap", lambda _frame: {})
    monkeypatch.setattr(
        managed_worker,
        "execute",
        lambda _frame, _channel: (_ for _ in ()).throw(exc),
    )

    assert managed_worker.main() == 1
    return _worker_error_reason(sent[-1])


@pytest.mark.parametrize(
    "tid, exc, expected_reason, expected_failures, infrastructure",
    [
        ("t_timeout", TimeoutError("worker_receipt_timeout"), "worker_receipt_timeout", 1, True),
        ("t_generic", RuntimeError("worker_receipt_timeout"), "managed_worker_failed", 2, False),
    ],
    ids=["host-receipt-timeout", "generic-worker-failure"],
)
def test_worker_frame_gateway_reason_controls_dispatcher_failure_budget(
    board, monkeypatch, tid, exc, expected_reason, expected_failures, infrastructure
):
    """The real worker frame must survive the gateway allowlist before crash accounting."""
    reason = _worker_emitted_reason(monkeypatch, exc)
    assert reason == expected_reason
    _lose(board, tid, reason=reason, admission=None, failures=1)

    kbd.detect_crashed_workers(board.conn)

    row, outcome, meta = _booked(board, tid)
    assert outcome == "crashed"
    assert row["consecutive_failures"] == expected_failures
    assert meta.get("infrastructure", False) is infrastructure


@pytest.mark.parametrize("reason, admission", [
    ("worker_receipt_timeout", SETTLED),
    ("EOFError", SETTLED),
    ("TimeoutError", SETTLED),
    ("managed_worker_pipe_closed", SETTLED),
    (None, SETTLED),  # the note alone: "owner admission settled interrupted"
], ids=["receipt-timeout", "eof", "timeout-type", "pipe-closed", "admission-interrupted"])
def test_host_side_loss_does_not_spend_the_failure_budget(board, reason, admission):
    _lose(board, "t_host", reason=reason, admission=admission, failures=1)  # one failure from the limit of 2

    assert kbd.detect_crashed_workers(board.conn) == ["t_host"]
    assert kbd.detect_crashed_workers._last_auto_blocked == []

    row, outcome, meta = _booked(board, "t_host")
    assert (row["status"], row["consecutive_failures"]) == ("ready", 1)
    assert outcome == "crashed" and meta["infrastructure"] is True
    assert "settled interrupted" in row["last_failure_error"] or reason in row["last_failure_error"]


def test_a_genuine_worker_error_still_trips_the_limit(board):
    _lose(board, "t_real", reason="managed_worker_failed", admission=None, failures=1)
    _lose(board, "t_real2", reason=None, admission=None, failures=1)

    kbd.detect_crashed_workers(board.conn)
    assert sorted(kbd.detect_crashed_workers._last_auto_blocked) == ["t_real", "t_real2"]
    for tid in ("t_real", "t_real2"):
        row, outcome, meta = _booked(board, tid)
        assert outcome == "crashed"
        assert (row["status"], row["consecutive_failures"]) == ("blocked", 2)
        assert "infrastructure" not in meta


def test_a_burst_of_identical_host_losses_is_not_a_systemic_trip(board):
    """Three same-fingerprint crashes in one sweep hold cards ``blocked`` (sticky); a gateway stall kills
    every running worker at once with the same error, which is exactly that shape."""
    tids = [f"t_burst{i}" for i in range(3)]
    for tid in tids:
        _lose(board, tid, reason="worker_receipt_timeout", failures=0)

    kbd.detect_crashed_workers(board.conn)
    assert kbd.detect_crashed_workers._last_auto_blocked == []
    assert {board.state(t)["status"] for t in tids} == {"ready"}


def test_host_loss_is_spaced_by_the_respawn_guard(board, monkeypatch):
    _lose(board, "t_host", reason="EOFError")
    _lose(board, "t_real", reason=None, admission=None)
    kbd.detect_crashed_workers(board.conn)

    assert kbd.check_respawn_guard(board.conn, "t_host") == "infrastructure_cooldown"
    assert kbd.check_respawn_guard(board.conn, "t_real") is None  # an ordinary crash retries at once

    board.conn.execute("UPDATE task_runs SET ended_at = ? WHERE task_id = 't_host'", (NOW - 121,))
    board.conn.commit()
    assert kbd.check_respawn_guard(board.conn, "t_host") is None
