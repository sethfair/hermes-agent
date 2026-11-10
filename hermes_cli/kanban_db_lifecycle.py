"""Explicit operator lifecycle transitions for Kanban tasks."""

from __future__ import annotations

import sqlite3
from typing import Optional


def release_triage_task(
    conn: sqlite3.Connection, task_id: str, *, actor: str, reason: Optional[str] = None,
    dry_run: bool = False,
) -> tuple[bool, Optional[str]]:
    """Release a specified task from ``triage`` without rewriting its spec."""
    from hermes_cli import kanban_db as kb

    with kb.write_txn(conn):
        cur_status = kb._task_status(conn, task_id)
        if cur_status is None:
            return False, f"task {task_id} not found"
        if cur_status != "triage":
            return False, (
                f"task {task_id} is {cur_status!r}; release-triage only applies "
                f"to 'triage'"
            )

        parents = conn.execute(
            "SELECT t.id, t.status FROM tasks t "
            "JOIN task_links l ON l.parent_id = t.id "
            "WHERE l.child_id = ?", (task_id,),
        ).fetchall()
        unsatisfied = [p["id"] for p in parents if p["status"] not in ("done", "archived")]
        if unsatisfied:
            return False, f"unsatisfied parent dependencies: {', '.join(unsatisfied)}"
        if dry_run:
            return True, None

        upd = conn.execute(
            "UPDATE tasks SET status = 'ready' WHERE id = ? AND status = 'triage'",
            (task_id,),
        )
        if upd.rowcount != 1:
            return False, f"task {task_id} status changed during triage release"
        kb._append_event(
            conn,
            task_id,
            "triage_released",
            {"actor": actor, "reason": reason, "from_status": "triage"},
        )

    return True, None