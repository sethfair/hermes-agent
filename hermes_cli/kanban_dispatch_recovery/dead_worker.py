"""Typed storage operation for reclaiming a dead dispatcher worker."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class DeadWorkerRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    expected_run_id: int
    worker_pid: int
    claim_lock: Optional[str]
    assignee: Optional[str]
    retry_status: str
    run_outcome: str
    event_kind: str
    event_payload: dict[str, Any]
    error: str
    rate_limited: bool
    operation_kind: str = "dead_worker"
    authority_kind: str = "recovery"


def _state():
    from hermes_cli import kanban_db_task_state

    return kanban_db_task_state


def dead_worker_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    expected_run_id: int,
    worker_pid: int,
    claim_lock: Optional[str],
    assignee: Optional[str],
    retry_status: str,
    run_outcome: str,
    event_kind: str,
    event_payload: dict[str, Any],
    error: str,
    rate_limited: bool,
):
    request = DeadWorkerRequest(
        task_id, actor_scope, operation_id, expected_run_id, worker_pid,
        claim_lock, assignee, retry_status, run_outcome, event_kind,
        event_payload, error[:500], rate_limited,
    )
    return _state()._run_operation(conn, request).outcome


def _apply_dead_worker(private: sqlite3.Connection, request: DeadWorkerRequest):
    from hermes_cli import kanban_db as kb

    next_status = request.retry_status if request.rate_limited else "recovering"
    updated = private.execute(
        "UPDATE tasks SET status=?,claim_lock=NULL,claim_expires=NULL,worker_pid=NULL,"
        "worker_started_at=NULL,last_failure_error=? "
        "WHERE id=? AND status='running' AND current_run_id=? "
        "AND worker_pid=? AND claim_lock IS ?",
        (
            next_status,
            request.error if request.rate_limited else None,
            request.task_id,
            request.expected_run_id,
            request.worker_pid,
            request.claim_lock,
        ),
    )
    if updated.rowcount != 1:
        outcome = _state().TransitionOutcome(
            False, request.operation_id, identity_state="unknown", refusal="compare_and_set",
        )
        return _state()._AppliedTransition(outcome)
    run_id = kb._end_run(
        private,
        request.task_id,
        outcome=request.run_outcome,
        status=request.run_outcome,
        error=request.error,
        metadata=dict(request.event_payload),
    )
    kb._append_event(
        private, request.task_id, request.event_kind, request.event_payload, run_id=run_id,
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    outcome = _state().TransitionOutcome(
        True, request.operation_id, event_id, identity_state="unknown",
    )
    return _state()._AppliedTransition(outcome, hook_run_id=run_id)


def reconcile_pending_dead_workers(
    conn: sqlite3.Connection,
    *,
    failure_limit: int,
    protocol_failure_limit: int,
) -> list[str]:
    """Finalize dead-worker failure accounting interrupted after the reclaim receipt."""
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_dispatch as dispatch

    rows = conn.execute(
        "SELECT t.id AS task_id,t.max_retries,r.operation_id,e.run_id,e.kind,e.payload,tr.error "
        "FROM tasks t JOIN task_transition_receipts r ON r.task_id=t.id "
        "JOIN task_events e ON e.id=r.event_id LEFT JOIN task_runs tr ON tr.id=e.run_id "
        "WHERE t.status='recovering' AND r.operation_kind='dead_worker' "
        "AND r.outcome_code='committed' "
        "AND r.event_id=(SELECT MAX(r2.event_id) FROM task_transition_receipts r2 "
        " WHERE r2.task_id=t.id AND r2.operation_kind='dead_worker' AND r2.outcome_code='committed')"
    ).fetchall()
    finalized: list[str] = []
    for row in rows:
        run_id = row["run_id"]
        if run_id is None:
            continue
        payload = json.loads(row["payload"] or "{}")
        retry_status = payload.get("retry_status")
        if retry_status not in {"ready", "review"}:
            continue
        protocol = bool(payload.get("protocol_violation"))
        terminal = bool(payload.get("terminal_provider"))
        count_failure = True
        force_trip = terminal
        effective_limit = failure_limit
        extra = {"pid": payload.get("pid"), "claimer": payload.get("claimer")}
        if protocol:
            streak = dispatch._protocol_violation_streak(conn, row["task_id"])
            override = kb._row_get(row, "max_retries")
            effective_limit = int(override) if override is not None else protocol_failure_limit
            count_failure = streak >= effective_limit
            force_trip = count_failure
            extra.update({
                "protocol_violations": streak,
                "protocol_violation_limit": effective_limit,
            })
        if terminal:
            extra["terminal_provider"] = True
        result = _state().dispatch_failure_transition(
            conn,
            row["task_id"],
            actor_scope="dispatcher",
            operation_id=f"dispatch-failure:crashed:{row['task_id']}:{run_id}",
            trigger_operation_id=row["operation_id"],
            trigger_operation_kind="dead_worker",
            expected_run_id=int(run_id),
            error=row["error"] or "worker exited",
            outcome="crashed",
            retry_status=retry_status,
            failure_limit=effective_limit,
            count_failure=count_failure,
            force_trip=force_trip,
            event_payload_extra=extra,
        )
        if result.committed:
            finalized.append(row["task_id"])
    return finalized


def record_recovered_worker_failure(
    conn: sqlite3.Connection,
    tid: str,
    pid: int,
    claimer: str,
    run_id: int,
    dead: Any,
    *,
    default_failure_limit: int,
    failure_limit: Optional[int] = None,
    force_trip: bool = False,
    count_failure: bool = True,
    event_payload_extra: Optional[dict] = None,
) -> bool:
    """Receipt-backed failure accounting for one committed dead-worker reclaim."""
    extra = {"pid": pid, "claimer": claimer}
    if event_payload_extra:
        extra.update(event_payload_extra)
    result = _state().dispatch_failure_transition(
        conn,
        tid,
        actor_scope="dispatcher",
        operation_id=f"dispatch-failure:crashed:{tid}:{run_id}",
        trigger_operation_id=f"dead-worker:{tid}:{run_id}",
        trigger_operation_kind="dead_worker",
        expected_run_id=run_id,
        error=dead.error_text,
        outcome="crashed",
        retry_status=dead.event_payload["retry_status"],
        failure_limit=default_failure_limit if failure_limit is None else failure_limit,
        count_failure=count_failure,
        force_trip=force_trip,
        event_payload_extra=extra,
    )
    task = __import__("hermes_cli.kanban_db", fromlist=["get_task"]).get_task(conn, tid)
    return result.committed and task is not None and task.status == "blocked"
