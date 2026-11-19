"""Closed, typed task-state operations for Kanban protected writes."""

from __future__ import annotations

import contextlib
import hashlib
import json
import secrets
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Optional

from hermes_cli.kanban_task_state import (
    BlockIdentity,
    canonicalize_block_identity,
    effective_block_identity,
    identity_event_fields,
)

_TUPLE_VERSION = 1
_OPERATION_VERSION = 1
_PROTECTED_TUPLE_COLUMNS = (
    "status",
    "current_run_id",
    "claim_lock",
    "claim_expires",
    "worker_pid",
    "block_kind",
    "block_recurrences",
    "block_identity",
    "block_identity_state",
    "action_generation",
    "park_generation",
)


@dataclass(frozen=True)
class TransitionOutcome:
    committed: bool
    operation_id: str
    event_id: Optional[int] = None
    identity_state: str = "unknown"
    refusal: Optional[str] = None
    tuple_version: Optional[int] = None
    result_tuple_hash: Optional[str] = None


BlockTransitionOutcome = TransitionOutcome


@dataclass(frozen=True)
class _BlockRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    kind: Optional[str]
    identity: BlockIdentity
    raw_subject: object
    reason: Optional[str]
    expected_run_id: Optional[int]
    operation_kind: str = "block"
    authority_kind: str = "block"


@dataclass(frozen=True)
class _ClassifyBlockRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    kind: str
    identity: BlockIdentity
    raw_subject: str
    operation_kind: str = "classify_block"
    authority_kind: str = "classify_block"


@dataclass(frozen=True)
class _UnblockRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    operation_kind: str = "unblock"
    authority_kind: str = "block"


@dataclass(frozen=True)
class _PromoteRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    reason: Optional[str]
    operation_kind: str = "promote"
    authority_kind: str = "block"


@dataclass(frozen=True)
class _ScheduleRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    reason: Optional[str]
    expected_run_id: Optional[int]
    operation_kind: str = "schedule"
    authority_kind: str = "block"


_Request = (
    _BlockRequest
    | _ClassifyBlockRequest
    | _UnblockRequest
    | _PromoteRequest
    | _ScheduleRequest
)


@dataclass(frozen=True)
class _AppliedTransition:
    outcome: TransitionOutcome
    hook_run_id: Optional[int] = None


@dataclass(frozen=True)
class _KernelResult:
    outcome: TransitionOutcome
    applied: bool
    hook_run_id: Optional[int] = None


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _subject_request_token(identity: BlockIdentity, raw_subject: object) -> dict[str, str]:
    if identity.state == "known":
        return {"known": identity.canonical or ""}
    if isinstance(raw_subject, str):
        raw = raw_subject.encode("utf-8", errors="surrogatepass")
        type_name = "str"
    else:
        raw = repr(raw_subject).encode("utf-8", errors="backslashreplace")
        type_name = type(raw_subject).__name__
    return {"invalid_type": type_name, "invalid_digest": hashlib.sha256(raw).hexdigest()}


def _block_request_hash(request: _BlockRequest) -> str:
    return _canonical_hash({
        "task_id": request.task_id,
        "operation_kind": request.operation_kind,
        "actor_scope": request.actor_scope,
        "kind": request.kind,
        "subject": _subject_request_token(request.identity, request.raw_subject),
        "reason": request.reason,
        "expected_run_id": request.expected_run_id,
    })


def _classify_request_hash(request: _ClassifyBlockRequest) -> str:
    return _canonical_hash({
        "task_id": request.task_id,
        "operation_kind": request.operation_kind,
        "actor_scope": request.actor_scope,
        "kind": request.kind,
        "subject": _subject_request_token(request.identity, request.raw_subject),
    })


def _unblock_request_hash(request: _UnblockRequest) -> str:
    return _canonical_hash({
        "task_id": request.task_id,
        "operation_kind": request.operation_kind,
        "actor_scope": request.actor_scope,
    })


def _promote_request_hash(request: _PromoteRequest) -> str:
    return _canonical_hash({
        "task_id": request.task_id,
        "operation_kind": request.operation_kind,
        "actor_scope": request.actor_scope,
        "reason": request.reason,
    })


def _schedule_request_hash(request: _ScheduleRequest) -> str:
    return _canonical_hash({
        "task_id": request.task_id,
        "operation_kind": request.operation_kind,
        "actor_scope": request.actor_scope,
        "reason": request.reason,
        "expected_run_id": request.expected_run_id,
    })


_REQUEST_HASHERS = {
    _BlockRequest: _block_request_hash,
    _ClassifyBlockRequest: _classify_request_hash,
    _UnblockRequest: _unblock_request_hash,
    _PromoteRequest: _promote_request_hash,
    _ScheduleRequest: _schedule_request_hash,
}


def _request_hash(request: _Request) -> str:
    return _REQUEST_HASHERS[type(request)](request)


def _protected_tuple_hash(conn: sqlite3.Connection, task_id: str) -> str:
    columns = ", ".join(_PROTECTED_TUPLE_COLUMNS)
    row = conn.execute(f"SELECT {columns} FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        raise RuntimeError("task disappeared before transition receipt")
    return _canonical_hash({
        "version": _TUPLE_VERSION,
        "values": [row[column] for column in _PROTECTED_TUPLE_COLUMNS],
    })


def _valid_result_hash(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _rollback(private: sqlite3.Connection) -> None:
    with contextlib.suppress(sqlite3.DatabaseError):
        private.execute("ROLLBACK")


def _validate_ingress(
    conn: sqlite3.Connection,
    *,
    actor_scope: str,
    operation_id: str,
) -> None:
    if conn.in_transaction:
        raise RuntimeError("protected task transition refuses an outer transaction")
    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError("operation_id is required")
    if not isinstance(actor_scope, str) or not actor_scope.strip():
        raise ValueError("actor_scope is required")


def _receipt_outcome(
    receipt: sqlite3.Row,
    request: _Request,
    request_hash: str,
) -> TransitionOutcome:
    matches = (
        receipt["task_id"] == request.task_id
        and receipt["operation_kind"] == request.operation_kind
        and receipt["request_hash"] == request_hash
    )
    if not matches:
        return TransitionOutcome(False, request.operation_id, refusal="operation_id_collision")
    if (
        receipt["outcome_code"] != "committed"
        or receipt["operation_version"] != _OPERATION_VERSION
        or receipt["tuple_version"] != _TUPLE_VERSION
        or not _valid_result_hash(receipt["result_tuple_hash"])
        or receipt["result_identity_state"] not in {"none", "known", "unknown"}
    ):
        return TransitionOutcome(False, request.operation_id, refusal="invalid_receipt")
    return TransitionOutcome(
        True,
        request.operation_id,
        receipt["event_id"],
        receipt["result_identity_state"],
        tuple_version=receipt["tuple_version"],
        result_tuple_hash=receipt["result_tuple_hash"],
    )


def _load_receipt(private: sqlite3.Connection, operation_id: str) -> Optional[sqlite3.Row]:
    return private.execute(
        "SELECT task_id, operation_kind, request_hash, outcome_code, event_id, "
        "operation_version, tuple_version, result_tuple_hash, result_identity_state "
        "FROM task_transition_receipts WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()


def _insert_receipt(
    private: sqlite3.Connection,
    request: _Request,
    request_hash: str,
    outcome: TransitionOutcome,
) -> TransitionOutcome:
    result_tuple_hash = _protected_tuple_hash(private, request.task_id)
    private.execute(
        "INSERT INTO task_transition_receipts "
        "(operation_id, task_id, operation_kind, request_hash, outcome_code, event_id, "
        "committed_at, operation_version, tuple_version, result_tuple_hash, "
        "result_identity_state) VALUES (?, ?, ?, ?, 'committed', ?, ?, ?, ?, ?, ?)",
        (
            request.operation_id,
            request.task_id,
            request.operation_kind,
            request_hash,
            outcome.event_id,
            int(time.time()),
            _OPERATION_VERSION,
            _TUPLE_VERSION,
            result_tuple_hash,
            outcome.identity_state,
        ),
    )
    return TransitionOutcome(
        True,
        request.operation_id,
        outcome.event_id,
        outcome.identity_state,
        tuple_version=_TUPLE_VERSION,
        result_tuple_hash=result_tuple_hash,
    )


def _dependency_identity(
    private: sqlite3.Connection,
    task_id: str,
    kind: Optional[str],
    identity: BlockIdentity,
) -> BlockIdentity:
    if identity.state != "known" or kind != "dependency":
        return identity
    parent = private.execute(
        "SELECT status FROM tasks WHERE id = ? AND id IN "
        "(SELECT parent_id FROM task_links WHERE child_id = ?)",
        (identity.subject, task_id),
    ).fetchone()
    if parent is not None and parent[0] not in {"done", "archived"}:
        return identity
    cause = "no_open_parent" if parent else "nonparent_subject"
    return BlockIdentity("unknown", kind=kind, unknown_cause=cause)


def _apply_in_place_classification(
    private: sqlite3.Connection,
    request: _BlockRequest,
    identity: BlockIdentity,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    updated = private.execute(
        "UPDATE tasks SET block_kind = ?, block_recurrences = 1, "
        "block_identity = NULL, block_identity_state = 'unknown' "
        "WHERE id = ? AND status = 'blocked' AND block_kind IS NULL "
        "AND current_run_id IS NULL",
        (request.kind, request.task_id),
    )
    if updated.rowcount != 1:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="compare_and_set"
        ))
    unknown_fields = identity_event_fields(identity)
    kb._append_event(
        private,
        request.task_id,
        "block_identity_unknown",
        {**unknown_fields, "classification_required": True},
    )
    kb._append_event(
        private,
        request.task_id,
        "blocked",
        {
            "kind": request.kind,
            "reason": request.reason,
            "classified_in_place": True,
            **unknown_fields,
        },
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _AppliedTransition(TransitionOutcome(
        True, request.operation_id, event_id, "unknown"
    ))


def _apply_block(
    private: sqlite3.Connection,
    request: _BlockRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    row = private.execute(
        "SELECT status, current_run_id, block_kind, block_recurrences, "
        "block_identity, block_identity_state FROM tasks WHERE id = ?",
        (request.task_id,),
    ).fetchone()
    if row is None:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="task_not_found"
        ))
    identity = _dependency_identity(
        private, request.task_id, request.kind, request.identity
    )
    previous = effective_block_identity(row)
    classify_in_place = (
        row[0] == "blocked"
        and row[2] is None
        and row[1] is None
        and request.kind is not None
        and request.raw_subject is None
        and request.expected_run_id is None
    )
    if classify_in_place:
        return _apply_in_place_classification(private, request, identity)
    source_status = "ready"
    if row[0] == "running":
        source_status = kb._retry_status_for_run(private, request.task_id)
    elif row[0] not in {"ready", "running"}:
        return _AppliedTransition(TransitionOutcome(
            False,
            request.operation_id,
            identity_state=identity.state,
            refusal="invalid_status",
        ))
    same_known = (
        identity.state == "known"
        and previous.state == "known"
        and identity.canonical == previous.canonical
    )
    recurrences = int(row[3] or 0) + 1 if same_known else 1
    new_status = "todo" if request.kind == "dependency" and identity.state == "known" else (
        "triage" if recurrences >= kb.BLOCK_RECURRENCE_LIMIT else "blocked"
    )
    event_kind = "dependency_wait" if new_status == "todo" else (
        "block_loop_detected" if new_status == "triage" else "blocked"
    )
    payload = {
        "reason": request.reason,
        "kind": request.kind,
        "source_status": source_status,
        **identity_event_fields(identity),
    }
    if new_status != "todo":
        payload["recurrences"] = recurrences
    update_sql = (
        "UPDATE tasks SET status = ?, claim_lock = NULL, claim_expires = NULL, "
        "worker_pid = NULL, block_kind = ?, block_recurrences = ?, block_identity = ?, "
        "block_identity_state = ? WHERE id = ? AND status IN ('ready','running')"
    )
    update_params: list[Any] = [
        new_status,
        request.kind,
        0 if new_status == "todo" else recurrences,
        identity.canonical,
        identity.state,
        request.task_id,
    ]
    if request.expected_run_id is not None:
        update_sql += " AND current_run_id = ?"
        update_params.append(int(request.expected_run_id))
    private.execute(update_sql, update_params)
    if private.execute("SELECT changes()").fetchone()[0] != 1:
        return _AppliedTransition(TransitionOutcome(
            False,
            request.operation_id,
            identity_state=identity.state,
            refusal="compare_and_set",
        ))
    hook_run_id = kb._end_or_synthesize_run(
        private,
        request.task_id,
        outcome="blocked",
        status="blocked",
        summary=request.reason,
        synthesize=bool(request.reason),
    )
    if identity.state == "unknown":
        kb._append_event(
            private,
            request.task_id,
            "block_identity_unknown",
            {**identity_event_fields(identity), "classification_required": True},
            run_id=hook_run_id,
        )
    kb._append_event(
        private, request.task_id, event_kind, payload, run_id=hook_run_id
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _AppliedTransition(
        TransitionOutcome(True, request.operation_id, event_id, identity.state),
        hook_run_id,
    )


def _apply_classify_block(
    private: sqlite3.Connection,
    request: _ClassifyBlockRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    row = private.execute(
        "SELECT status, current_run_id, block_identity_state FROM tasks WHERE id = ?",
        (request.task_id,),
    ).fetchone()
    if (
        row is None
        or row[0] not in {"blocked", "todo", "scheduled", "triage"}
        or row[1] is not None
        or row[2] != "unknown"
    ):
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="invalid_status"
        ))
    if request.kind == "dependency" and private.execute(
        "SELECT 1 FROM task_links WHERE parent_id = ? AND child_id = ?",
        (request.identity.subject, request.task_id),
    ).fetchone() is None:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="nonparent_subject"
        ))
    updated = private.execute(
        "UPDATE tasks SET block_kind = ?, block_recurrences = 1, block_identity = ?, "
        "block_identity_state = 'known' WHERE id = ? AND block_identity_state = 'unknown'",
        (request.kind, request.identity.canonical, request.task_id),
    )
    if updated.rowcount != 1:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="compare_and_set"
        ))
    kb._append_event(
        private,
        request.task_id,
        "block_classified",
        {
            "actor": request.actor_scope,
            "kind": request.kind,
            **identity_event_fields(request.identity),
        },
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _AppliedTransition(TransitionOutcome(
        True, request.operation_id, event_id, "known"
    ))


def _apply_unblock(
    private: sqlite3.Connection,
    request: _UnblockRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    row = private.execute(
        "SELECT status, block_identity_state FROM tasks WHERE id = ?",
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
    if row["status"] not in {"blocked", "scheduled"}:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="invalid_status"
        ))
    resume_status = (
        kb._resume_status_from_events(private, request.task_id)
        if row["status"] == "blocked"
        else "ready"
    )
    kb._reclaim_dangling_run(
        private,
        request.task_id,
        statuses=("blocked", "scheduled"),
        now=int(time.time()),
        note="invariant recovery on unblock",
    )
    landing_status = kb._landing_status_after_parents(private, request.task_id)
    new_status = (
        "review"
        if landing_status == "ready" and resume_status == "review"
        else landing_status
    )
    updated = private.execute(
        "UPDATE tasks SET status = ?, current_run_id = NULL, "
        "consecutive_failures = 0, last_failure_error = NULL "
        "WHERE id = ? AND status IN ('blocked', 'scheduled')",
        (new_status, request.task_id),
    )
    if updated.rowcount != 1:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="compare_and_set"
        ))
    kb._append_event(
        private,
        request.task_id,
        "unblocked",
        (
            {"status": new_status, "resume_status": resume_status}
            if new_status != "ready" or resume_status != "ready"
            else None
        ),
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _AppliedTransition(TransitionOutcome(
        True, request.operation_id, event_id, row["block_identity_state"]
    ))


def _promote_refusal(private: sqlite3.Connection, task_id: str) -> Optional[str]:
    row = private.execute(
        "SELECT status, block_identity_state FROM tasks WHERE id = ?", (task_id,)
    ).fetchone()
    if row is None:
        return f"task {task_id} not found"
    if row["block_identity_state"] == "unknown":
        return "block_identity_unknown"
    if row["status"] not in {"todo", "blocked"}:
        return (
            f"task {task_id} is {row['status']!r}; promote only applies to "
            "'todo' or 'blocked'"
        )
    parents = private.execute(
        "SELECT t.id, t.status FROM tasks t "
        "JOIN task_links l ON l.parent_id = t.id WHERE l.child_id = ?",
        (task_id,),
    ).fetchall()
    unsatisfied = [parent["id"] for parent in parents if parent["status"] not in {"done", "archived"}]
    if unsatisfied:
        return (
            f"unsatisfied parent dependencies: {', '.join(unsatisfied)} "
            "(the ready -> running claim re-checks parents, so promotion cannot "
            "bypass them; complete the parents or drop the link with "
            f"`hermes kanban unlink <parent_id> {task_id}`)"
        )
    return None


def _apply_promote(
    private: sqlite3.Connection,
    request: _PromoteRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    refusal = _promote_refusal(private, request.task_id)
    if refusal is not None:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal=refusal
        ))
    identity_state = private.execute(
        "SELECT block_identity_state FROM tasks WHERE id = ?", (request.task_id,)
    ).fetchone()[0]
    updated = private.execute(
        "UPDATE tasks SET status = 'ready' WHERE id = ? AND status IN ('todo', 'blocked')",
        (request.task_id,),
    )
    if updated.rowcount != 1:
        return _AppliedTransition(TransitionOutcome(
            False,
            request.operation_id,
            refusal=f"task {request.task_id} status changed during promotion",
        ))
    kb._append_event(
        private,
        request.task_id,
        "promoted_manual",
        {"actor": request.actor_scope, "reason": request.reason},
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _AppliedTransition(TransitionOutcome(
        True, request.operation_id, event_id, identity_state
    ))


def _apply_schedule(
    private: sqlite3.Connection,
    request: _ScheduleRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    row = private.execute(
        "SELECT status, block_identity_state FROM tasks WHERE id = ?",
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
    params: list[Any] = [request.task_id]
    sql = (
        "UPDATE tasks SET status = 'scheduled', claim_lock = NULL, "
        "claim_expires = NULL, worker_pid = NULL WHERE id = ? "
        "AND status IN ('todo', 'ready', 'running', 'blocked')"
    )
    if request.expected_run_id is not None:
        sql += " AND current_run_id = ?"
        params.append(int(request.expected_run_id))
    updated = private.execute(sql, params)
    if updated.rowcount != 1:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="compare_and_set"
        ))
    run_id = kb._end_or_synthesize_run(
        private,
        request.task_id,
        outcome="scheduled",
        status="scheduled",
        summary=request.reason,
        synthesize=bool(request.reason),
    )
    kb._append_event(
        private,
        request.task_id,
        "scheduled",
        {"reason": request.reason},
        run_id=run_id,
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _AppliedTransition(TransitionOutcome(
        True, request.operation_id, event_id, row["block_identity_state"]
    ), run_id)


_OPERATION_HANDLERS = {
    _BlockRequest: _apply_block,
    _ClassifyBlockRequest: _apply_classify_block,
    _UnblockRequest: _apply_unblock,
    _PromoteRequest: _apply_promote,
    _ScheduleRequest: _apply_schedule,
}


def _run_operation(
    conn: sqlite3.Connection,
    request: _Request,
) -> _KernelResult:
    """Run one closed request with replay, CAS effects, and receipt in one transaction."""
    from hermes_cli import kanban_db_connect as kbc

    _validate_ingress(
        conn, actor_scope=request.actor_scope, operation_id=request.operation_id
    )
    db_path = kbc._main_db_file(conn)
    if not db_path:
        raise ValueError("protected task transition requires a file-backed Kanban database")
    request_hash = _request_hash(request)
    with kbc._protected_connection(db_path, operation=request.authority_kind) as private:
        try:
            kbc._execute_boundary_with_retry(private, "BEGIN IMMEDIATE")
            receipt = _load_receipt(private, request.operation_id)
            if receipt is not None:
                outcome = _receipt_outcome(receipt, request, request_hash)
                kbc._execute_boundary_with_retry(private, "COMMIT")
                return _KernelResult(outcome, False)
            applied = _OPERATION_HANDLERS[type(request)](private, request)
            if not applied.outcome.committed:
                _rollback(private)
                return _KernelResult(applied.outcome, False)
            outcome = _insert_receipt(
                private, request, request_hash, applied.outcome
            )
            kbc._execute_boundary_with_retry(private, "COMMIT")
        except BaseException:
            _rollback(private)
            raise
    return _KernelResult(outcome, True, applied.hook_run_id)


def block_task_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    kind: Optional[str],
    subject: Optional[str] = None,
    reason: Optional[str] = None,
    expected_run_id: Optional[int] = None,
) -> BlockTransitionOutcome:
    """Commit one blocker transition on a fresh private physical connection."""
    from hermes_cli import kanban_db as kb

    if kind is not None and kind not in kb.VALID_BLOCK_KINDS:
        raise ValueError(f"block kind must be one of {sorted(kb.VALID_BLOCK_KINDS)} or None")
    request = _BlockRequest(
        task_id=task_id,
        actor_scope=actor_scope,
        operation_id=operation_id,
        kind=kind,
        identity=canonicalize_block_identity(kind, subject),
        raw_subject=subject,
        reason=reason,
        expected_run_id=expected_run_id,
    )
    result = _run_operation(conn, request)
    if result.applied:
        kb._fire_task_hook(
            "kanban_task_blocked",
            kb.get_task(conn, task_id),
            task_id,
            result.hook_run_id,
            reason=reason,
        )
    return result.outcome


def compatibility_operation_id(task_id: str) -> str:
    return f"legacy-block:{task_id}:{secrets.token_hex(12)}"


def _compatibility_lifecycle_operation_id(kind: str, task_id: str) -> str:
    return f"legacy-{kind}:{task_id}:{secrets.token_hex(12)}"


def unblock_task_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
) -> TransitionOutcome:
    """Release a classified block/schedule through the receipt kernel."""
    request = _UnblockRequest(task_id, actor_scope, operation_id)
    return _run_operation(conn, request).outcome


def unblock_task(conn: sqlite3.Connection, task_id: str) -> bool:
    """Compatibility adapter preserving the literal-bool unblock result."""
    return unblock_task_transition(
        conn,
        task_id,
        actor_scope="legacy",
        operation_id=_compatibility_lifecycle_operation_id("unblock", task_id),
    ).committed


def promote_task_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    reason: Optional[str] = None,
    dry_run: bool = False,
) -> TransitionOutcome:
    """Validate or commit a replay-safe operator promotion."""
    _validate_ingress(conn, actor_scope=actor_scope, operation_id=operation_id)
    if dry_run:
        refusal = _promote_refusal(conn, task_id)
        return TransitionOutcome(
            refusal is None,
            operation_id,
            identity_state="none",
            refusal=refusal,
        )
    request = _PromoteRequest(task_id, actor_scope, operation_id, reason)
    return _run_operation(conn, request).outcome


def promote_task(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor: str,
    reason: Optional[str] = None,
    dry_run: bool = False,
) -> tuple[bool, Optional[str]]:
    """Compatibility adapter preserving ``(ok, reason)`` promotion results."""
    outcome = promote_task_transition(
        conn,
        task_id,
        actor_scope=actor,
        operation_id=_compatibility_lifecycle_operation_id("promote", task_id),
        reason=reason,
        dry_run=dry_run,
    )
    return outcome.committed, outcome.refusal


def schedule_task_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    reason: Optional[str] = None,
    expected_run_id: Optional[int] = None,
) -> TransitionOutcome:
    """Park one eligible task through the replay-safe receipt kernel."""
    request = _ScheduleRequest(
        task_id, actor_scope, operation_id, reason, expected_run_id
    )
    return _run_operation(conn, request).outcome


def schedule_task(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    reason: Optional[str] = None,
    expected_run_id: Optional[int] = None,
) -> bool:
    """Compatibility adapter preserving the literal-bool schedule result."""
    return schedule_task_transition(
        conn,
        task_id,
        actor_scope="legacy",
        operation_id=_compatibility_lifecycle_operation_id("schedule", task_id),
        reason=reason,
        expected_run_id=expected_run_id,
    ).committed


def classify_block_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    operation_id: str,
    actor_scope: str,
    kind: str,
    subject: str,
) -> BlockTransitionOutcome:
    """Attach one actor-attributed identity with replay-safe operation semantics."""
    identity = canonicalize_block_identity(kind, subject)
    if identity.state != "known":
        raise ValueError(identity.unknown_cause or "invalid block identity")
    request = _ClassifyBlockRequest(
        task_id=task_id,
        actor_scope=actor_scope,
        operation_id=operation_id,
        kind=kind,
        identity=identity,
        raw_subject=subject,
    )
    return _run_operation(conn, request).outcome


def classify_block(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    kind: str,
    subject: str,
    actor: str,
) -> bool:
    """Compatibility adapter for actor-attributed blocker classification."""
    outcome = classify_block_transition(
        conn,
        task_id,
        operation_id=f"legacy-classify:{task_id}:{secrets.token_hex(12)}",
        actor_scope=actor,
        kind=kind,
        subject=subject,
    )
    return outcome.committed
