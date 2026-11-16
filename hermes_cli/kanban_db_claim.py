"""Atomic Ready and Review claim operations for the Kanban database.

The public facade remains :mod:`hermes_cli.kanban_db`; bodies late-bind that
facade so its established monkeypatch seams continue to govern behavior.
"""

from __future__ import annotations

import sqlite3
import time
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from hermes_cli.kanban_db import Task


def _parents_satisfied(conn: sqlite3.Connection, task_id: str) -> bool:
    """Return whether every direct parent is terminal for dependency gating."""
    return conn.execute(
        "SELECT 1 FROM task_links l "
        "JOIN tasks p ON p.id = l.parent_id "
        "WHERE l.child_id = ? "
        "AND p.status NOT IN ('done', 'archived') LIMIT 1", (task_id,),
    ).fetchone() is None


def unsatisfied_parents(conn: sqlite3.Connection, task_id: str) -> list[tuple[str, str]]:
    """``(parent_id, status)`` for every direct parent still open, in id order."""
    rows = conn.execute(
        "SELECT p.id, p.status FROM task_links l "
        "JOIN tasks p ON p.id = l.parent_id "
        "WHERE l.child_id = ? AND p.status NOT IN ('done', 'archived') "
        "ORDER BY p.id", (task_id,),
    ).fetchall()
    return [(row["id"], row["status"]) for row in rows]


def _claim_and_open_run(
    conn: sqlite3.Connection, task_id: str, source_status: str, lock: str, expires: int, now: int,
    *, event_extra: Optional[dict] = None, admission_id: Optional[str] = None,
) -> Optional[int]:
    """CAS ``source_status -> running``, open a run row, emit ``claimed``."""
    if admission_id is not None and source_status == "ready":
        from hermes_cli import kanban_ready_monitor as monitor
        task = conn.execute("SELECT assignee FROM tasks WHERE id=?", (task_id,)).fetchone()
        previous_run = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM task_runs WHERE task_id=?", (task_id,)
        ).fetchone()[0]
        generation = monitor.ready_generation(conn, task_id)
        if not task or not generation or not monitor.validate_claim_admission(
                conn, admission_id, task_id, task["assignee"] or "", generation[0], int(previous_run)):
            return None
    cur = conn.execute(
        f"""
        UPDATE tasks
           SET status        = 'running',
               claim_lock    = ?,
               claim_expires = ?,
                   started_at    = COALESCE(started_at, ?),
                   dispatch_admission_id = ?
         WHERE id = ?
           AND status = '{source_status}'
           AND claim_lock IS NULL
        """,
        (lock, expires, now, admission_id, task_id),
    )
    if cur.rowcount != 1:
        return None
    trow = conn.execute(
        "SELECT assignee, max_runtime_seconds, current_step_key "
        "FROM tasks WHERE id = ?", (task_id,),
    ).fetchone()
    run_cur = conn.execute(
        """
        INSERT INTO task_runs (
            task_id, profile, step_key, status,
            claim_lock, claim_expires, max_runtime_seconds,
            started_at
        ) VALUES (?, ?, ?, 'running', ?, ?, ?, ?)
        """,
        (
            task_id, trow["assignee"] if trow else None, trow["current_step_key"] if trow else None,
            lock, expires, trow["max_runtime_seconds"] if trow else None, now,
        ),
    )
    if run_cur.lastrowid is None:
        raise RuntimeError("claim run insert did not return a row id")
    run_id = int(run_cur.lastrowid)
    conn.execute("UPDATE tasks SET current_run_id = ? WHERE id = ?", (run_id, task_id))
    _kb._append_event(
        conn, task_id, "claimed",
        {"lock": lock, "expires": expires, "run_id": run_id, **(event_extra or {})}, run_id=run_id,
    )
    if source_status == "ready":
        from hermes_cli import kanban_ready_monitor as monitor
        monitor.close_for_native_ready_claim(conn, task_id, run_id, now=now)
    return run_id


def claim_task(
    conn: sqlite3.Connection, task_id: str, *, ttl_seconds: Optional[int] = None,
    claimer: Optional[str] = None, admission_id: Optional[str] = None,
) -> Optional[Task]:
    """Atomically transition ``ready -> running``."""
    now = int(time.time())
    lock = claimer or _kb._claimer_id()
    expires = now + _kb._resolve_claim_ttl_seconds(ttl_seconds)
    with _kb.write_txn(conn):
        if not _kb._parents_satisfied(conn, task_id):
            conn.execute(
                "UPDATE tasks SET status = 'todo' "
                "WHERE id = ? AND status = 'ready'", (task_id,),
            )
            _kb._append_event(conn, task_id, "claim_rejected", {"reason": "parents_not_done"})
            return None
        _kb._reclaim_dangling_run(
            conn, task_id, statuses=("ready",), now=now, note="invariant recovery on re-claim",
        )
        run_id = _kb._claim_and_open_run(
            conn, task_id, "ready", lock, expires, now, admission_id=admission_id,
        )
        if run_id is None:
            return None
        claimed = _kb.get_task(conn, task_id)
    _kb._fire_task_hook("kanban_task_claimed", claimed, task_id, run_id)
    return claimed


def claim_review_task(
    conn: sqlite3.Connection, task_id: str, *, ttl_seconds: Optional[int] = None,
    claimer: Optional[str] = None,
) -> Optional[Task]:
    """Atomic ``review -> running``; a new run tracks the reviewer."""
    now = int(time.time())
    lock = claimer or _kb._claimer_id()
    expires = now + _kb._resolve_claim_ttl_seconds(ttl_seconds)
    with _kb.write_txn(conn):
        if not _kb._parents_satisfied(conn, task_id):
            demoted = conn.execute(
                "UPDATE tasks SET status = 'todo' "
                "WHERE id = ? AND status = 'review' AND claim_lock IS NULL", (task_id,),
            )
            if demoted.rowcount == 1:
                _kb._append_event(
                    conn, task_id, "dependency_wait",
                    {"reason": "parent_reopened", "source_status": "review"},
                )
            return None
        run_id = _kb._claim_and_open_run(
            conn, task_id, "review", lock, expires, now,
            event_extra={"source_status": "review"},
        )
        if run_id is None:
            return None
        return _kb.get_task(conn, task_id)


from hermes_cli import kanban_db as _kb
