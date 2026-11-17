"""Atomic Ready and Review claim operations for the Kanban database.

The public facade remains :mod:`hermes_cli.kanban_db`; bodies late-bind that
facade so its established monkeypatch seams continue to govern behavior.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Literal, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from hermes_cli.kanban_db import Task


ClaimReason = Literal[
    "kanban_paused",
    "kanban_control_unknown",
    "parents_not_done",
    "admission_refused",
    "claim_lost",
]


@dataclass(frozen=True)
class ClaimAttemptResult:
    task: Optional[Task]
    reason: Optional[ClaimReason] = None


class _ClaimRollback(RuntimeError):
    def __init__(self, reason: ClaimReason):
        super().__init__(reason)
        self.reason: ClaimReason = reason


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
    admission_validated: bool = False, expect_assignee: Optional[str] = None,
) -> Optional[int]:
    """CAS ``source_status -> running``, open a run row, emit ``claimed``."""
    if admission_id is not None and source_status == "ready" and not admission_validated:
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
           AND (? IS NULL OR assignee = ?)
        """,
        (lock, expires, now, admission_id, task_id, expect_assignee, expect_assignee),
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


def _control_refusal(conn: sqlite3.Connection) -> Optional[ClaimReason]:
    from hermes_cli.kanban_control import read_control

    state = read_control(conn)
    if state.state == "paused":
        return "kanban_paused"
    if state.state != "running":
        return "kanban_control_unknown"
    return None


def claim_task_result(
    conn: sqlite3.Connection, task_id: str, *, ttl_seconds: Optional[int] = None,
    claimer: Optional[str] = None, monitor_attempt_id: Optional[str] = None,
    admission_id: Optional[str] = None, expect_assignee: Optional[str] = None,
) -> ClaimAttemptResult:
    """Return the immutable outcome of the deciding Ready claim transaction."""
    if monitor_attempt_id is not None and admission_id is not None:
        raise ValueError("monitor_attempt_id and admission_id are mutually exclusive")
    now = int(time.time())
    lock = claimer or _kb._claimer_id()
    expires = now + _kb._resolve_claim_ttl_seconds(ttl_seconds)
    try:
        with _kb.write_txn(conn):
            refusal = _control_refusal(conn)
            if refusal is not None:
                return ClaimAttemptResult(task=None, reason=refusal)
            if not _kb._parents_satisfied(conn, task_id):
                conn.execute(
                    "UPDATE tasks SET status = 'todo' "
                    "WHERE id = ? AND status = 'ready'", (task_id,),
                )
                _kb._append_event(
                    conn, task_id, "claim_rejected", {"reason": "parents_not_done"},
                )
                return ClaimAttemptResult(task=None, reason="parents_not_done")
            _kb._reclaim_dangling_run(
                conn, task_id, statuses=("ready",), now=now,
                note="invariant recovery on re-claim",
            )
            admission_validated = False
            if monitor_attempt_id is not None:
                from hermes_cli import kanban_ready_monitor as monitor

                task = conn.execute(
                    "SELECT assignee FROM tasks WHERE id=?", (task_id,),
                ).fetchone()
                generation = monitor.ready_generation(conn, task_id)
                previous_run = conn.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM task_runs WHERE task_id=?",
                    (task_id,),
                ).fetchone()[0]
                if task is None or generation is None:
                    return ClaimAttemptResult(task=None, reason="claim_lost")
                try:
                    admission_id = monitor.arm_claim_admission(
                        conn, attempt_id=monitor_attempt_id, task_id=task_id,
                        ready_generation=generation[0], run_generation=int(previous_run),
                    )
                except ValueError:
                    return ClaimAttemptResult(task=None, reason="admission_refused")
                if not monitor.validate_claim_admission(
                    conn, admission_id, task_id, task["assignee"] or "",
                    generation[0], int(previous_run),
                ):
                    raise _ClaimRollback("admission_refused")
                admission_validated = True
            run_id = _kb._claim_and_open_run(
                conn, task_id, "ready", lock, expires, now,
                admission_id=admission_id, admission_validated=admission_validated,
                expect_assignee=expect_assignee,
            )
            if run_id is None:
                if admission_id is not None:
                    raise _ClaimRollback("claim_lost")
                return ClaimAttemptResult(task=None, reason="claim_lost")
            claimed = _kb.get_task(conn, task_id)
    except _ClaimRollback as exc:
        return ClaimAttemptResult(task=None, reason=exc.reason)
    _kb._fire_task_hook("kanban_task_claimed", claimed, task_id, run_id)
    return ClaimAttemptResult(task=claimed)


def claim_task(
    conn: sqlite3.Connection, task_id: str, *, ttl_seconds: Optional[int] = None,
    claimer: Optional[str] = None, admission_id: Optional[str] = None,
    expect_assignee: Optional[str] = None,
) -> Optional[Task]:
    """Compatibility wrapper returning only the claimed Ready task."""
    return claim_task_result(
        conn, task_id, ttl_seconds=ttl_seconds, claimer=claimer,
        admission_id=admission_id, expect_assignee=expect_assignee,
    ).task


def claim_review_task_result(
    conn: sqlite3.Connection, task_id: str, *, ttl_seconds: Optional[int] = None,
    claimer: Optional[str] = None, expect_assignee: Optional[str] = None,
) -> ClaimAttemptResult:
    """Return the immutable outcome of the deciding Review claim transaction."""
    now = int(time.time())
    lock = claimer or _kb._claimer_id()
    expires = now + _kb._resolve_claim_ttl_seconds(ttl_seconds)
    with _kb.write_txn(conn):
        refusal = _control_refusal(conn)
        if refusal is not None:
            return ClaimAttemptResult(task=None, reason=refusal)
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
            return ClaimAttemptResult(task=None, reason="parents_not_done")
        run_id = _kb._claim_and_open_run(
            conn, task_id, "review", lock, expires, now,
            event_extra={"source_status": "review"},
            expect_assignee=expect_assignee,
        )
        if run_id is None:
            return ClaimAttemptResult(task=None, reason="claim_lost")
        return ClaimAttemptResult(task=_kb.get_task(conn, task_id))


def claim_review_task(
    conn: sqlite3.Connection, task_id: str, *, ttl_seconds: Optional[int] = None,
    claimer: Optional[str] = None, expect_assignee: Optional[str] = None,
) -> Optional[Task]:
    """Compatibility wrapper returning only the claimed Review task."""
    return claim_review_task_result(
        conn, task_id, ttl_seconds=ttl_seconds, claimer=claimer,
        expect_assignee=expect_assignee,
    ).task


from hermes_cli import kanban_db as _kb
