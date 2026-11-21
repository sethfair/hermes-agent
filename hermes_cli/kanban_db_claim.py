"""Ready and Review claim compatibility adapters.

Protected writes live in :mod:`hermes_cli.kanban_db_task_state`; this module
preserves the established public result types and monkeypatch seams.
"""

from __future__ import annotations

import secrets
import sqlite3
import time
from dataclasses import dataclass
from typing import cast, Literal, Optional, TYPE_CHECKING

from hermes_cli import kanban_db_task_state as task_state

if TYPE_CHECKING:
    from hermes_cli.kanban_db import Task


ClaimReason = Literal[
    "block_identity_unknown",
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


def _parents_satisfied(conn: sqlite3.Connection, task_id: str) -> bool:
    """Return whether every direct parent is terminal for dependency gating."""
    return conn.execute(
        "SELECT 1 FROM task_links l JOIN tasks p ON p.id=l.parent_id "
        "WHERE l.child_id=? AND p.status NOT IN ('done','archived') LIMIT 1",
        (task_id,),
    ).fetchone() is None


def unsatisfied_parents(conn: sqlite3.Connection, task_id: str) -> list[tuple[str, str]]:
    """``(parent_id, status)`` for each open direct parent, ordered by id."""
    rows = conn.execute(
        "SELECT p.id,p.status FROM task_links l JOIN tasks p ON p.id=l.parent_id "
        "WHERE l.child_id=? AND p.status NOT IN ('done','archived') ORDER BY p.id",
        (task_id,),
    ).fetchall()
    return [(row["id"], row["status"]) for row in rows]


def _operation_id(lane: str, task_id: str) -> str:
    return f"legacy-claim-{lane}:{task_id}:{secrets.token_hex(12)}"


def claim_task_result(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    ttl_seconds: Optional[int] = None,
    claimer: Optional[str] = None,
    monitor_attempt_id: Optional[str] = None,
    admission_id: Optional[str] = None,
    expect_assignee: Optional[str] = None,
) -> ClaimAttemptResult:
    """Return the immutable outcome of one deciding Ready claim operation."""
    if monitor_attempt_id is not None and admission_id is not None:
        raise ValueError("monitor_attempt_id and admission_id are mutually exclusive")
    now = int(time.time())
    outcome = task_state.claim_task_transition(
        conn,
        task_id,
        actor_scope="legacy-claim",
        operation_id=_operation_id("ready", task_id),
        source_status="ready",
        lock=claimer or _kb._claimer_id(),
        expires=now + _kb._resolve_claim_ttl_seconds(ttl_seconds),
        now=now,
        monitor_attempt_id=monitor_attempt_id,
        admission_id=admission_id,
        expect_assignee=expect_assignee,
    )
    return ClaimAttemptResult(
        task=cast("Optional[Task]", outcome.task),
        reason=cast("Optional[ClaimReason]", outcome.reason),
    )


def claim_task(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    ttl_seconds: Optional[int] = None,
    claimer: Optional[str] = None,
    admission_id: Optional[str] = None,
    expect_assignee: Optional[str] = None,
) -> Optional[Task]:
    """Compatibility wrapper returning only the claimed Ready task."""
    return claim_task_result(
        conn,
        task_id,
        ttl_seconds=ttl_seconds,
        claimer=claimer,
        admission_id=admission_id,
        expect_assignee=expect_assignee,
    ).task


def claim_review_task_result(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    ttl_seconds: Optional[int] = None,
    claimer: Optional[str] = None,
    expect_assignee: Optional[str] = None,
) -> ClaimAttemptResult:
    """Return the immutable outcome of one deciding Review claim operation."""
    now = int(time.time())
    outcome = task_state.claim_task_transition(
        conn,
        task_id,
        actor_scope="legacy-claim",
        operation_id=_operation_id("review", task_id),
        source_status="review",
        lock=claimer or _kb._claimer_id(),
        expires=now + _kb._resolve_claim_ttl_seconds(ttl_seconds),
        now=now,
        expect_assignee=expect_assignee,
    )
    return ClaimAttemptResult(
        task=cast("Optional[Task]", outcome.task),
        reason=cast("Optional[ClaimReason]", outcome.reason),
    )


def claim_review_task(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    ttl_seconds: Optional[int] = None,
    claimer: Optional[str] = None,
    expect_assignee: Optional[str] = None,
) -> Optional[Task]:
    """Compatibility wrapper returning only the claimed Review task."""
    return claim_review_task_result(
        conn,
        task_id,
        ttl_seconds=ttl_seconds,
        claimer=claimer,
        expect_assignee=expect_assignee,
    ).task


# Stable public test seam; the implementation remains in the privileged module.
_claim_and_open_run = task_state._claim_and_open_run


from hermes_cli import kanban_db as _kb
