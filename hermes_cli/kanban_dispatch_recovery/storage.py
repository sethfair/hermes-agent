"""Receipt-backed dispatcher recovery storage operations."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class RuntimeTimeoutRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    expected_run_id: int
    worker_pid: int
    claim_lock: str
    elapsed_seconds: int
    limit_seconds: int
    sigkill: bool
    retry_status: str
    error: str
    operation_kind: str = "runtime_timeout"
    authority_kind: str = "recovery"


@dataclass(frozen=True)
class DispatchFailureRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    trigger_operation_id: str
    expected_run_id: int
    error: str
    outcome: str
    retry_status: str
    failure_limit: int
    force_trip: bool
    event_payload_extra: Optional[dict[str, Any]]
    operation_kind: str = "dispatch_failure"
    authority_kind: str = "recovery"


@dataclass(frozen=True)
class StaleRunningRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    expected_run_id: int
    claim_lock: Optional[str]
    worker_pid: Optional[int]
    elapsed_seconds: int
    last_heartbeat_at: Optional[int]
    heartbeat_age_seconds: Optional[int]
    timeout_seconds: int
    termination: dict[str, Any]
    retry_status: str
    error: str
    operation_kind: str = "stale_running"
    authority_kind: str = "recovery"


def _state():
    from hermes_cli import kanban_db_task_state

    return kanban_db_task_state


def _outcome(request, *, committed: bool, event_id=None, refusal=None):
    return _state().TransitionOutcome(
        committed, request.operation_id, event_id, "unknown", refusal
    )


def _applied(outcome, **kwargs):
    return _state()._AppliedTransition(outcome, **kwargs)


def runtime_timeout_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    expected_run_id: int,
    worker_pid: int,
    claim_lock: str,
    elapsed_seconds: int,
    limit_seconds: int,
    sigkill: bool,
    retry_status: str,
    error: str,
):
    request = RuntimeTimeoutRequest(
        task_id, actor_scope, operation_id, expected_run_id, worker_pid,
        claim_lock, elapsed_seconds, limit_seconds, sigkill, retry_status, error,
    )
    return _state()._run_operation(conn, request).outcome


def dispatch_failure_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    trigger_operation_id: str,
    expected_run_id: int,
    error: str,
    outcome: str,
    retry_status: str,
    failure_limit: int,
    force_trip: bool = False,
    event_payload_extra: Optional[dict[str, Any]] = None,
):
    request = DispatchFailureRequest(
        task_id, actor_scope, operation_id, trigger_operation_id,
        expected_run_id, error[:500], outcome, retry_status, failure_limit, force_trip,
        event_payload_extra,
    )
    return _state()._run_operation(conn, request).outcome


def stale_running_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    expected_run_id: int,
    claim_lock: Optional[str],
    worker_pid: Optional[int],
    elapsed_seconds: int,
    last_heartbeat_at: Optional[int],
    heartbeat_age_seconds: Optional[int],
    timeout_seconds: int,
    termination: dict[str, Any],
    retry_status: str,
    error: str,
):
    request = StaleRunningRequest(
        task_id, actor_scope, operation_id, expected_run_id, claim_lock,
        worker_pid, elapsed_seconds, last_heartbeat_at, heartbeat_age_seconds,
        timeout_seconds, termination, retry_status, error,
    )
    return _state()._run_operation(conn, request).outcome


def reconcile_pending_runtime_timeouts(
    conn: sqlite3.Connection, *, failure_limit: int,
) -> list[str]:
    """Finish receipt-backed failure accounting left pending after timeout recovery."""
    rows = conn.execute(
        "SELECT t.id AS task_id,r.operation_id,e.run_id,e.payload,tr.error "
        "FROM tasks t "
        "JOIN task_transition_receipts r ON r.task_id=t.id "
        "JOIN task_events e ON e.id=r.event_id "
        "LEFT JOIN task_runs tr ON tr.id=e.run_id "
        "WHERE t.status='recovering' AND r.operation_kind='runtime_timeout' "
        "AND r.outcome_code='committed' "
        "AND r.event_id=(SELECT MAX(r2.event_id) FROM task_transition_receipts r2 "
        "                WHERE r2.task_id=t.id AND r2.operation_kind='runtime_timeout' "
        "                  AND r2.outcome_code='committed') "
        "ORDER BY r.committed_at,r.operation_id"
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
        result = dispatch_failure_transition(
            conn,
            row["task_id"],
            actor_scope="dispatcher",
            operation_id=f"dispatch-failure:timed-out:{row['task_id']}:{run_id}",
            trigger_operation_id=row["operation_id"],
            expected_run_id=int(run_id),
            error=row["error"] or "runtime timeout",
            outcome="timed_out",
            retry_status=retry_status,
            failure_limit=failure_limit,
            event_payload_extra={
                "pid": payload.get("pid"),
                "sigkill": bool(payload.get("sigkill")),
                "retry_status": retry_status,
            },
        )
        if result.committed:
            finalized.append(row["task_id"])
    return finalized


def _apply_runtime_timeout(private: sqlite3.Connection, request):
    from hermes_cli import kanban_db as kb

    current = private.execute(
        "SELECT status,current_run_id,worker_pid,claim_lock FROM tasks WHERE id=?",
        (request.task_id,),
    ).fetchone()
    if current is None:
        return _applied(_outcome(request, committed=False, refusal="task_not_found"))
    if (
        current["status"] != "running"
        or current["current_run_id"] != request.expected_run_id
        or current["worker_pid"] != request.worker_pid
        or current["claim_lock"] != request.claim_lock
    ):
        return _applied(_outcome(request, committed=False, refusal="compare_and_set"))
    updated = private.execute(
        "UPDATE tasks SET status='recovering',claim_lock=NULL,claim_expires=NULL,worker_pid=NULL,"
        "worker_started_at=NULL,last_heartbeat_at=NULL "
        "WHERE id=? AND status='running' AND current_run_id=? AND worker_pid=? AND claim_lock=?",
        (
            request.task_id, request.expected_run_id,
            request.worker_pid, request.claim_lock,
        ),
    )
    if updated.rowcount != 1:
        return _applied(_outcome(request, committed=False, refusal="compare_and_set"))
    payload = {
        "pid": request.worker_pid,
        "elapsed_seconds": request.elapsed_seconds,
        "limit_seconds": request.limit_seconds,
        "sigkill": request.sigkill,
        "retry_status": request.retry_status,
    }
    run_id = kb._end_run(
        private, request.task_id, outcome="timed_out", status="timed_out",
        error=request.error, metadata=payload,
    )
    kb._append_event(private, request.task_id, "timed_out", payload, run_id=run_id)
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _applied(_outcome(request, committed=True, event_id=event_id), hook_run_id=run_id)


def _apply_dispatch_failure(private: sqlite3.Connection, request):
    from hermes_cli import kanban_db as kb

    trigger = private.execute(
        "SELECT r.outcome_code,e.run_id FROM task_transition_receipts r "
        "LEFT JOIN task_events e ON e.id=r.event_id "
        "WHERE r.operation_id=? AND r.task_id=? AND r.operation_kind='runtime_timeout'",
        (request.trigger_operation_id, request.task_id),
    ).fetchone()
    if (
        trigger is None
        or trigger["outcome_code"] != "committed"
        or trigger["run_id"] != request.expected_run_id
    ):
        return _applied(_outcome(request, committed=False, refusal="trigger_not_committed"))
    row = private.execute(
        "SELECT consecutive_failures,status,max_retries "
        "FROM tasks WHERE id=?", (request.task_id,),
    ).fetchone()
    if row is None:
        return _applied(_outcome(request, committed=False, refusal="task_not_found"))
    if row["status"] != "recovering":
        return _applied(_outcome(request, committed=False, refusal="compare_and_set"))
    failures = int(row["consecutive_failures"]) + 1
    override = kb._row_get(row, "max_retries")
    effective_limit = int(override) if override is not None else request.failure_limit
    limit_source = "task" if override is not None else "dispatcher"
    if not (request.force_trip or failures >= effective_limit):
        private.execute(
            "UPDATE tasks SET status=?,consecutive_failures=?,last_failure_error=? "
            "WHERE id=? AND status='recovering'",
            (request.retry_status, failures, request.error, request.task_id),
        )
        return _applied(_outcome(request, committed=True))
    updated = private.execute(
        "UPDATE tasks SET status='blocked',consecutive_failures=?,last_failure_error=?,"
        "block_kind=NULL,block_identity=NULL,block_identity_state='unknown',block_recurrences=0 "
        "WHERE id=? AND status='recovering'",
        (failures, request.error, request.task_id),
    )
    if updated.rowcount != 1:
        return _applied(_outcome(request, committed=False, refusal="compare_and_set"))
    kb._append_event(
        private, request.task_id, "block_identity_unknown",
        {"identity_state": "unknown", "unknown_cause": "failure_breaker"},
    )
    payload = {
        "failures": failures,
        "effective_limit": effective_limit,
        "limit_source": limit_source,
        "error": request.error,
        "trigger_outcome": request.outcome,
        "retry_status": request.retry_status,
    }
    if request.force_trip:
        payload["sticky"] = True
    if request.event_payload_extra:
        payload.update(request.event_payload_extra)
    kb._append_event(private, request.task_id, "gave_up", payload)
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _applied(_outcome(request, committed=True, event_id=event_id))


def _apply_stale_running(private: sqlite3.Connection, request):
    from hermes_cli import kanban_db as kb

    updated = private.execute(
        "UPDATE tasks SET status=?,claim_lock=NULL,claim_expires=NULL,worker_pid=NULL,"
        "worker_started_at=NULL,last_heartbeat_at=NULL "
        "WHERE id=? AND status='running' AND current_run_id=? "
        "AND claim_lock IS ? AND worker_pid IS ?",
        (
            request.retry_status, request.task_id, request.expected_run_id,
            request.claim_lock, request.worker_pid,
        ),
    )
    if updated.rowcount != 1:
        return _applied(_outcome(request, committed=False, refusal="compare_and_set"))
    payload = {
        "elapsed_seconds": request.elapsed_seconds,
        "last_heartbeat_at": request.last_heartbeat_at,
        "heartbeat_age_seconds": request.heartbeat_age_seconds,
        "timeout_seconds": request.timeout_seconds,
        "pid": request.worker_pid,
        "retry_status": request.retry_status,
    }
    payload.update(request.termination)
    run_id = kb._end_run(
        private, request.task_id, outcome="stale", status="stale",
        error=request.error, metadata=payload,
    )
    kb._append_event(private, request.task_id, "stale", payload, run_id=run_id)
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _applied(_outcome(request, committed=True, event_id=event_id), hook_run_id=run_id)