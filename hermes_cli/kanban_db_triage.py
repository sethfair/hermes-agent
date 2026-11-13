"""Guarded Triage release: a precondition check and the release in one write transaction."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class TriageReleaseGuardResult:
    """Decision from a guard. A denial must carry a non-empty ``reason``."""

    allowed: bool
    reason: Optional[str] = None


@dataclass(frozen=True)
class TriageReleaseReceipt:
    released: bool
    outcome: str  # "released" | "not_in_triage" | "precondition_failed"
    status: Optional[str]
    changed_fields: tuple[str, ...] = ()
    guard_reason: Optional[str] = None


class ReadOnlyGuardConnection:
    """SELECT-only view of the release transaction handed to a guard.

    Guards are trusted first-party code; this stops accidental writes and
    transaction control, it is not a sandbox.
    """

    __slots__ = ("_conn",)

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute(self, sql: str, parameters: Any = ()) -> sqlite3.Cursor:
        head = sql.lstrip().split(None, 1)
        if not head or head[0].upper() != "SELECT":
            raise RuntimeError("Triage release guard connection is read-only")
        return self._conn.execute(sql, parameters)


class _Denied(Exception):
    def __init__(self, receipt: TriageReleaseReceipt) -> None:
        self.receipt = receipt


def guarded_specify_triage_task(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    guard: Optional[Callable[[ReadOnlyGuardConnection, str], TriageReleaseGuardResult]] = None,
    title: Optional[str] = None,
    body: Optional[str] = None,
    assignee: Optional[str] = None,
    author: Optional[str] = None,
) -> TriageReleaseReceipt:
    """Run ``guard`` (if any) and release ``task_id`` from triage in one BEGIN IMMEDIATE txn.

    A denial or a guard exception rolls back; a task not in triage returns
    ``not_in_triage`` without calling the guard.
    """
    from hermes_cli import kanban_db as kb

    if title is not None and not title.strip():
        raise ValueError("title cannot be blank")
    assignee = kb._canonical_assignee(assignee)
    try:
        with kb.write_txn(conn):
            existing = conn.execute(
                "SELECT title, body, assignee FROM tasks WHERE id = ? AND status = 'triage'",
                (task_id,),
            ).fetchone()
            if existing is None:
                return TriageReleaseReceipt(False, "not_in_triage", kb._task_status(conn, task_id))
            if guard is not None:
                changes = conn.total_changes
                decision = guard(ReadOnlyGuardConnection(conn), task_id)
                if not isinstance(decision, TriageReleaseGuardResult):
                    raise TypeError("guard must return TriageReleaseGuardResult")
                if not conn.in_transaction or conn.total_changes != changes:
                    raise RuntimeError("Triage release guard must be read-only")
                if not decision.allowed:
                    reason = (decision.reason or "").strip()
                    if not reason:
                        raise ValueError("denied Triage release guard requires a reason")
                    raise _Denied(TriageReleaseReceipt(False, "precondition_failed", "triage",
                                                       guard_reason=reason))

            sets: list[str] = ["status = 'todo'"]
            params: list[Any] = []
            changed: list[str] = []
            for field, value in (("title", title.strip() if title is not None else None),
                                 ("body", body), ("assignee", assignee)):
                if value is not None and value != (existing[field] or ("" if field != "assignee" else None)):
                    sets.append(f"{field} = ?")
                    params.append(value)
                    changed.append(field)
            params.append(task_id)
            cur = conn.execute(
                f"UPDATE tasks SET {', '.join(sets)} WHERE id = ? AND status = 'triage'",
                tuple(params),
            )
            if cur.rowcount != 1:
                return TriageReleaseReceipt(False, "not_in_triage", kb._task_status(conn, task_id))
            if changed and author and author.strip():
                # Not add_comment (own txn + 'commented' event); 'specified' below records it.
                kb._insert_comment(
                    conn, task_id, author.strip(),
                    "Specified — updated " + ", ".join(changed) + " and promoted to todo.",
                    int(time.time()),
                )
            kb._append_event(conn, task_id, "specified",
                             {"changed_fields": changed} if changed else None)
    except _Denied as denied:
        return denied.receipt
    # Own IMMEDIATE txn: a parent-free released task flips to 'ready' now.
    kb.recompute_ready(conn)
    return TriageReleaseReceipt(True, "released", kb._task_status(conn, task_id), tuple(changed))
