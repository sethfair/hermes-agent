"""Receipt-backed dashboard direct-status transitions."""
# ruff: noqa: TC004 -- owner primitives are injected by bind() after initialization

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from hermes_cli.kanban_db_task_state import (
        TransitionOutcome,
        _AppliedTransition,
        _KernelResult,
        _canonical_hash,
        _compatibility_lifecycle_operation_id,
        _run_operation,
    )


@dataclass(frozen=True)
class DirectStatusRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    new_status: str
    operation_kind: str = "direct_status"
    authority_kind: str = "direct_status"


def bind(owner) -> None:
    """Bind initialized owner primitives without duplicating storage authority."""
    globals().update({
        name: value for name, value in vars(owner).items()
        if not name.startswith("__")
    })


def request_hash(request: DirectStatusRequest) -> str:
    return _canonical_hash({
        "task_id": request.task_id,
        "operation_kind": request.operation_kind,
        "actor_scope": request.actor_scope,
        "new_status": request.new_status,
    })


def apply(
    private: sqlite3.Connection,
    request: DirectStatusRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    row = private.execute(
        "SELECT status,current_run_id,worker_pid,claim_lock,worker_started_at,"
        "block_identity_state FROM tasks WHERE id=?",
        (request.task_id,),
    ).fetchone()
    if row is None:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="task_not_found"
        ))
    if row["block_identity_state"] == "unknown":
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="block_identity_unknown"
        ))
    effective_status = request.new_status
    if row["status"] == "running" and request.new_status == "ready":
        resume_status = kb._retry_status_for_run(
            private, request.task_id, row["current_run_id"]
        )
        if resume_status == "review":
            effective_status = (
                "review" if kb._parents_satisfied(private, request.task_id) else "todo"
            )
    if effective_status == "ready" and not kb._parents_satisfied(
        private, request.task_id
    ):
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="parents_not_done"
        ))
    was_running = row["status"] == "running"
    reopening_parent = (
        row["status"] in {"done", "archived"}
        and effective_status not in {"done", "archived"}
    )
    updated = private.execute(
        "UPDATE tasks SET status=?, "
        "claim_lock=CASE WHEN ?='running' THEN claim_lock ELSE NULL END, "
        "claim_expires=CASE WHEN ?='running' THEN claim_expires ELSE NULL END, "
        "worker_pid=CASE WHEN ?='running' THEN worker_pid ELSE NULL END "
        "WHERE id=?",
        (effective_status,) * 4 + (request.task_id,),
    )
    if updated.rowcount != 1:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="compare_and_set"
        ))
    run_id = None
    terminations: list[tuple[Optional[int], Optional[str], object]] = []
    if was_running and effective_status != "running" and row["current_run_id"]:
        run_id = kb._end_run(
            private,
            request.task_id,
            outcome="reclaimed",
            status="reclaimed",
            summary=f"status changed to {effective_status} (dashboard/direct)",
        )
        terminations.append(
            (row["worker_pid"], row["claim_lock"], row["worker_started_at"])
        )
    kb._append_event(
        private,
        request.task_id,
        "status",
        {"status": effective_status, "requested_status": request.new_status},
        run_id=run_id,
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    if reopening_parent:
        invalidated = kb.invalidate_descendants_for_parent_reopen(
            private, request.task_id, author=request.actor_scope
        )
        terminations.extend(invalidated["terminations"])
    return _AppliedTransition(
        TransitionOutcome(
            True,
            request.operation_id,
            event_id,
            row["block_identity_state"],
        ),
        hook_run_id=run_id,
        terminations=tuple(terminations),
    )


def transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    new_status: str,
) -> TransitionOutcome:
    """Apply a dashboard-compatible direct status move through the receipt kernel."""
    from hermes_cli import kanban_db as kb

    if new_status not in {"todo", "ready", "triage"}:
        raise ValueError("direct status must be 'todo', 'ready', or 'triage'")
    result: _KernelResult = _run_operation(
        conn,
        DirectStatusRequest(task_id, actor_scope, operation_id, new_status),
    )
    if not result.applied:
        return result.outcome
    for pid, claim_lock, started_at in result.terminations:
        kb._terminate_reclaimed_worker(pid, claim_lock, started_at=started_at)
    if new_status == "ready":
        kb.recompute_ready(conn)
    return result.outcome


def compatibility(
    conn: sqlite3.Connection,
    task_id: str,
    new_status: str,
    *,
    actor_scope: str = "dashboard",
) -> bool:
    """Preserve the dashboard adapter's literal-bool result."""
    return transition(
        conn,
        task_id,
        actor_scope=actor_scope,
        operation_id=_compatibility_lifecycle_operation_id("direct-status", task_id),
        new_status=new_status,
    ).committed
