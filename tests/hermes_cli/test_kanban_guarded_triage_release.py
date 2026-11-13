"""Guarded Triage release: guard and release share one write transaction."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc

Result = kb.TriageReleaseGuardResult


@pytest.fixture
def conn(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    with kbc.connect() as c:
        yield c


def _triage(c):
    return kb.create_task(c, title="rough idea", triage=True)


def test_allowed_guard_releases_and_applies_fields(conn):
    tid = _triage(conn)
    receipt = kb.guarded_specify_triage_task(
        conn, tid, guard=lambda c, t: Result(True), title="clear", author="op",
    )
    assert (receipt.released, receipt.outcome, receipt.status) == (True, "released", "ready")
    assert receipt.changed_fields == ("title",)
    assert kb.get_task(conn, tid).title == "clear"


def test_denied_guard_rolls_back_with_reason(conn):
    tid = _triage(conn)
    receipt = kb.guarded_specify_triage_task(
        conn, tid, guard=lambda c, t: Result(False, " held "), title="x", author="op",
    )
    assert receipt == kb.TriageReleaseReceipt(False, "precondition_failed", "triage",
                                              guard_reason="held")
    task = kb.get_task(conn, tid)
    assert (task.status, task.title) == ("triage", "rough idea")


def test_denial_without_reason_is_an_error_and_leaves_triage(conn):
    tid = _triage(conn)
    with pytest.raises(ValueError, match="reason"):
        kb.guarded_specify_triage_task(conn, tid, guard=lambda c, t: Result(False, "  "))
    assert kb.get_task(conn, tid).status == "triage"


def test_not_in_triage_skips_guard(conn):
    tid = kb.create_task(conn, title="already todo")
    called = []
    receipt = kb.guarded_specify_triage_task(
        conn, tid, guard=lambda c, t: called.append(t) or Result(True),
    )
    assert receipt.outcome == "not_in_triage" and not receipt.released
    assert called == []


def test_guard_cannot_write(conn):
    tid = _triage(conn)

    def guard(c, t):
        c.execute("UPDATE tasks SET title = 'pwned'")
        return Result(True)

    with pytest.raises(RuntimeError, match="read-only"):
        kb.guarded_specify_triage_task(conn, tid, guard=guard)
    assert kb.get_task(conn, tid).title == "rough idea"


def test_guard_sees_hold_committed_before_lock_acquired(conn):
    """A writer that holds the lock cannot land a hold between guard and release."""
    tid = _triage(conn)
    started = threading.Event()
    out = {}

    def guard(c, t):
        held = c.execute("SELECT 1 FROM task_comments WHERE task_id = ?", (t,)).fetchone()
        return Result(held is None, None if held is None else "hold")

    def release():
        with kbc.connect() as c2:
            started.set()
            out["r"] = kb.guarded_specify_triage_task(c2, tid, guard=guard)

    with kbc.connect() as writer, kb.write_txn(writer):
        writer.execute(
            "INSERT INTO task_comments (task_id, author, body, created_at) VALUES (?, 'sec', 'hold', 1)",
            (tid,),
        )
        thread = threading.Thread(target=release)
        thread.start()
        assert started.wait(2)
    thread.join(5)
    assert out["r"].outcome == "precondition_failed"
    assert kb.get_task(conn, tid).status == "triage"


def test_unguarded_specify_triage_task_unchanged(conn):
    tid = _triage(conn)
    assert kb.specify_triage_task(conn, tid, title="t") is True
    assert kb.specify_triage_task(conn, tid, title="t") is False
