"""Closed, typed task-state operations for Kanban protected writes."""

from __future__ import annotations

import contextlib
import hashlib
import json
import secrets
import sqlite3
import time
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Optional

from hermes_cli.kanban_task_state import (
    BlockIdentity,
    canonicalize_block_identity,
    effective_block_identity,
    identity_event_fields,
)
from hermes_cli.kanban_dispatch_recovery import storage as _recovery

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


@dataclass(frozen=True)
class ClaimTransitionOutcome:
    committed: bool
    operation_id: str
    task: Any = field(default=None, compare=False, repr=False)
    run_id: Optional[int] = None
    event_id: Optional[int] = None
    reason: Optional[str] = None
    tuple_version: Optional[int] = None
    result_tuple_hash: Optional[str] = None


@dataclass(frozen=True)
class GraphTransitionOutcome:
    committed: bool
    operation_id: str
    event_id: Optional[int] = None
    gated: bool = False
    removed: bool = False
    refusal: Optional[str] = None
    tuple_version: Optional[int] = None
    result_tuple_hash: Optional[str] = None


@dataclass(frozen=True)
class CreateTransitionOutcome:
    committed: bool
    operation_id: str
    task_id: str
    event_id: Optional[int] = None
    refusal: Optional[str] = None
    tuple_version: Optional[int] = None
    result_tuple_hash: Optional[str] = None


@dataclass(frozen=True)
class SwarmTransitionOutcome:
    committed: bool
    operation_id: str
    root_id: str
    worker_ids: tuple[str, ...]
    verifier_id: str
    synthesizer_id: str
    event_id: Optional[int] = None
    refusal: Optional[str] = None


@dataclass(frozen=True)
class DecomposeTransitionOutcome:
    committed: bool
    operation_id: str
    task_id: str
    child_ids: tuple[str, ...]
    event_id: Optional[int] = None
    refusal: Optional[str] = None


@dataclass(frozen=True)
class LifecycleTransitionOutcome:
    committed: bool
    operation_id: str
    event_id: Optional[int] = None
    refusal: Optional[str] = None
    detail: Optional[str] = None
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


@dataclass(frozen=True)
class _ArchiveRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    operation_kind: str = "archive"
    authority_kind: str = "archive"


@dataclass(frozen=True)
class _DeleteRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    archived_only: bool
    operation_kind: str
    authority_kind: str = "delete"


@dataclass(frozen=True)
class _ClaimRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    source_status: str
    lock: str
    expires: int
    now: int
    monitor_attempt_id: Optional[str]
    admission_id: Optional[str]
    expect_assignee: Optional[str]
    operation_kind: str
    authority_kind: str = "claim"


@dataclass(frozen=True)
class _LinkRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    parent_id: str
    expected_child_run_id: Optional[int]
    operation_kind: str = "link"
    authority_kind: str = "graph"


@dataclass(frozen=True)
class _UnlinkRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    parent_id: str
    operation_kind: str = "unlink"
    authority_kind: str = "graph"


@dataclass(frozen=True)
class _ReadinessRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    failure_limit: int
    operation_kind: str = "recompute_ready"
    authority_kind: str = "graph"


@dataclass(frozen=True)
class _CreateRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    title: str
    body: Optional[str]
    assignee: Optional[str]
    created_by: Optional[str]
    created_at: int
    workspace_kind: str
    workspace_path: Optional[str]
    branch_name: Optional[str]
    project_id: Optional[str]
    tenant: Optional[str]
    priority: int
    parents: tuple[str, ...]
    triage: bool
    initial_status: str
    idempotency_key: Optional[str]
    max_runtime_seconds: Optional[int]
    skills_json: Optional[str]
    max_retries: Optional[int]
    model_override: Optional[str]
    provider_override: Optional[str]
    reasoning_effort: Optional[str]
    goal_mode: bool
    goal_max_turns: Optional[int]
    session_id: Optional[str]
    creator_task_id: Optional[str]
    completion_contract: Optional[str]
    operation_kind: str = "create"
    authority_kind: str = "create"


@dataclass(frozen=True)
class _SwarmRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    goal: str
    workers: tuple[tuple[str, str, str, tuple[str, ...], int, Optional[int]], ...]
    worker_ids: tuple[str, ...]
    verifier_id: str
    synthesizer_id: str
    verifier_assignee: str
    synthesizer_assignee: str
    root_title: str
    verifier_title: str
    synthesizer_title: str
    tenant: Optional[str]
    created_by: str
    workspace_kind: str
    workspace_paths: tuple[Optional[str], ...]
    branch_names: tuple[Optional[str], ...]
    project_id: Optional[str]
    priority: int
    idempotency_key: Optional[str]
    created_at: int
    operation_kind: str = "create_swarm"
    authority_kind: str = "create"


@dataclass(frozen=True)
class _DecomposeRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    root_assignee: Optional[str]
    children: tuple[dict[str, Any], ...]
    child_ids: tuple[str, ...]
    author: Optional[str]
    created_at: int
    operation_kind: str = "decompose"
    authority_kind: str = "graph"


@dataclass(frozen=True)
class _CompleteRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    result: Optional[str]
    summary: Optional[str]
    metadata: Optional[dict[str, Any]]
    verified_cards: tuple[str, ...]
    expected_run_id: Optional[int]
    force: bool
    acceptance: object = field(compare=False, repr=False)
    operation_kind: str = "complete"
    authority_kind: str = "lifecycle"


@dataclass(frozen=True)
class _RequestReviewRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    summary: Optional[str]
    metadata: Optional[dict[str, Any]]
    reviewer: Optional[str]
    expected_run_id: Optional[int]
    force: bool
    operation_kind: str = "request_review"
    authority_kind: str = "lifecycle"


@dataclass(frozen=True)
class _RequestChangesRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    reason: str
    expected_run_id: Optional[int]
    operation_kind: str = "request_changes"
    authority_kind: str = "lifecycle"


@dataclass(frozen=True)
class _ReopenReviewRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    operation_kind: str = "reopen_review"
    authority_kind: str = "lifecycle"
_RuntimeTimeoutRequest = _recovery.RuntimeTimeoutRequest
_DispatchFailureRequest = _recovery.DispatchFailureRequest


_Request = (
    _BlockRequest
    | _ClassifyBlockRequest
    | _UnblockRequest
    | _PromoteRequest
    | _ScheduleRequest
    | _ArchiveRequest
    | _DeleteRequest
    | _ClaimRequest
    | _LinkRequest
    | _UnlinkRequest
    | _ReadinessRequest
    | _CreateRequest
    | _SwarmRequest
    | _DecomposeRequest
    | _CompleteRequest
    | _RequestReviewRequest
    | _RequestChangesRequest
    | _ReopenReviewRequest
    | _RuntimeTimeoutRequest
    | _DispatchFailureRequest
)


@dataclass(frozen=True)
class _AppliedTransition:
    outcome: TransitionOutcome
    hook_run_id: Optional[int] = None
    worker_pid: Optional[int] = None
    claim_lock: Optional[str] = None
    worker_started_at: object = None
    was_running: bool = False
    attachment_paths: tuple[str, ...] = ()
    staged_copies: tuple[Path, ...] = ()


@dataclass(frozen=True)
class _KernelResult:
    outcome: TransitionOutcome
    applied: bool
    hook_run_id: Optional[int] = None
    worker_pid: Optional[int] = None
    claim_lock: Optional[str] = None
    worker_started_at: object = None
    was_running: bool = False
    attachment_paths: tuple[str, ...] = ()


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


def _archive_request_hash(request: _ArchiveRequest) -> str:
    return _canonical_hash({
        "task_id": request.task_id,
        "operation_kind": request.operation_kind,
        "actor_scope": request.actor_scope,
    })


def _delete_request_hash(request: _DeleteRequest) -> str:
    return _canonical_hash({
        "task_id": request.task_id,
        "operation_kind": request.operation_kind,
        "actor_scope": request.actor_scope,
        "archived_only": request.archived_only,
    })


def _claim_request_hash(request: _ClaimRequest) -> str:
    return _canonical_hash({
        "task_id": request.task_id,
        "operation_kind": request.operation_kind,
        "actor_scope": request.actor_scope,
        "source_status": request.source_status,
        "lock": request.lock,
        "expires": request.expires,
        "now": request.now,
        "monitor_attempt_id": request.monitor_attempt_id,
        "admission_id": request.admission_id,
        "expect_assignee": request.expect_assignee,
    })


def _graph_request_hash(request: _LinkRequest | _UnlinkRequest) -> str:
    value: dict[str, object] = {
        "task_id": request.task_id,
        "operation_kind": request.operation_kind,
        "actor_scope": request.actor_scope,
        "parent_id": request.parent_id,
    }
    if isinstance(request, _LinkRequest):
        value["expected_child_run_id"] = request.expected_child_run_id
    return _canonical_hash(value)


def _readiness_request_hash(request: _ReadinessRequest) -> str:
    return _canonical_hash({
        "task_id": request.task_id,
        "operation_kind": request.operation_kind,
        "actor_scope": request.actor_scope,
        "failure_limit": request.failure_limit,
    })


def _create_request_hash(request: _CreateRequest) -> str:
    return _canonical_hash({
        item.name: getattr(request, item.name)
        for item in fields(request)
        if item.name != "authority_kind"
    })


def _lifecycle_request_hash(
    request: _CompleteRequest | _RequestReviewRequest | _RequestChangesRequest | _ReopenReviewRequest,
) -> str:
    return _canonical_hash({
        item.name: getattr(request, item.name)
        for item in fields(request)
        if item.name not in {"authority_kind", "acceptance"}
    })


_REQUEST_HASHERS = {
    _BlockRequest: _block_request_hash,
    _ClassifyBlockRequest: _classify_request_hash,
    _UnblockRequest: _unblock_request_hash,
    _PromoteRequest: _promote_request_hash,
    _ScheduleRequest: _schedule_request_hash,
    _ArchiveRequest: _archive_request_hash,
    _DeleteRequest: _delete_request_hash,
    _ClaimRequest: _claim_request_hash,
    _LinkRequest: _graph_request_hash,
    _UnlinkRequest: _graph_request_hash,
    _ReadinessRequest: _readiness_request_hash,
    _CreateRequest: _create_request_hash,
    _SwarmRequest: _create_request_hash,
    _DecomposeRequest: _create_request_hash,
    _CompleteRequest: _lifecycle_request_hash,
    _RequestReviewRequest: _lifecycle_request_hash,
    _RequestChangesRequest: _lifecycle_request_hash,
    _ReopenReviewRequest: _lifecycle_request_hash,
    _RuntimeTimeoutRequest: _create_request_hash,
    _DispatchFailureRequest: _create_request_hash,
}


def _request_hash(request: _Request) -> str:
    return _REQUEST_HASHERS[type(request)](request)


def _protected_tuple_hash(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    allow_deleted: bool = False,
) -> str:
    columns = ", ".join(_PROTECTED_TUPLE_COLUMNS)
    row = conn.execute(f"SELECT {columns} FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        if allow_deleted:
            return _canonical_hash({
                "version": _TUPLE_VERSION,
                "task_id": task_id,
                "deleted": True,
            })
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
    result_tuple_hash = _protected_tuple_hash(
        private,
        request.task_id,
        allow_deleted=isinstance(request, _DeleteRequest),
    )
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


def _apply_archive(
    private: sqlite3.Connection,
    request: _ArchiveRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    row = private.execute(
        "SELECT status, claim_lock, worker_pid, worker_started_at, "
        "block_identity_state FROM tasks WHERE id = ?",
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
    if row["status"] == "archived":
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="invalid_status"
        ))
    updated = private.execute(
        "UPDATE tasks SET status = 'archived', claim_lock = NULL, "
        "claim_expires = NULL, worker_pid = NULL, worker_started_at = NULL "
        "WHERE id = ? AND status != 'archived'",
        (request.task_id,),
    )
    if updated.rowcount != 1:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="compare_and_set"
        ))
    run_id = kb._end_run(
        private,
        request.task_id,
        outcome="reclaimed",
        status="reclaimed",
        summary="task archived with run still active",
    )
    kb._append_event(private, request.task_id, "archived", None, run_id=run_id)
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _AppliedTransition(
        TransitionOutcome(
            True,
            request.operation_id,
            event_id,
            row["block_identity_state"],
        ),
        hook_run_id=run_id,
        worker_pid=row["worker_pid"],
        claim_lock=row["claim_lock"],
        worker_started_at=row["worker_started_at"],
        was_running=row["status"] == "running",
    )


def _apply_delete(
    private: sqlite3.Connection,
    request: _DeleteRequest,
) -> _AppliedTransition:
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
    if request.archived_only and row["status"] != "archived":
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="invalid_status"
        ))
    attachment_paths = tuple(
        attachment["stored_path"]
        for attachment in private.execute(
            "SELECT stored_path FROM task_attachments WHERE task_id = ?",
            (request.task_id,),
        ).fetchall()
    )
    private.execute(
        "DELETE FROM task_links WHERE parent_id = ? OR child_id = ?",
        (request.task_id, request.task_id),
    )
    private.execute("DELETE FROM task_comments WHERE task_id = ?", (request.task_id,))
    private.execute("DELETE FROM task_events WHERE task_id = ?", (request.task_id,))
    private.execute("DELETE FROM task_runs WHERE task_id = ?", (request.task_id,))
    private.execute("DELETE FROM kanban_notify_subs WHERE task_id = ?", (request.task_id,))
    private.execute("DELETE FROM task_attachments WHERE task_id = ?", (request.task_id,))
    deleted = private.execute("DELETE FROM tasks WHERE id = ?", (request.task_id,))
    if deleted.rowcount != 1:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="compare_and_set"
        ))
    return _AppliedTransition(
        TransitionOutcome(True, request.operation_id, identity_state="none"),
        attachment_paths=attachment_paths,
    )










class _ClaimRollback(RuntimeError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _claim_and_open_run(
    conn: sqlite3.Connection, task_id: str, source_status: str, lock: str,
    expires: int, now: int, *, event_extra: Optional[dict] = None,
    admission_id: Optional[str] = None, admission_validated: bool = False,
    expect_assignee: Optional[str] = None,
) -> Optional[int]:
    """CAS one queued task to Running and create its run and claimed event."""
    from hermes_cli import kanban_db as kb

    if admission_id is not None and source_status == "ready" and not admission_validated:
        from hermes_cli import kanban_ready_monitor as monitor
        task = conn.execute("SELECT assignee FROM tasks WHERE id=?", (task_id,)).fetchone()
        previous_run = conn.execute(
            "SELECT COALESCE(MAX(id),0) FROM task_runs WHERE task_id=?", (task_id,)
        ).fetchone()[0]
        generation = monitor.ready_generation(conn, task_id)
        if not task or not generation or not monitor.validate_claim_admission(
            conn, admission_id, task_id, task["assignee"] or "", generation[0],
            int(previous_run),
        ):
            return None
    updated = conn.execute(
        "UPDATE tasks SET status='running', claim_lock=?, claim_expires=?, "
        "started_at=COALESCE(started_at,?), dispatch_admission_id=? "
        "WHERE id=? AND status=? AND claim_lock IS NULL "
        "AND (? IS NULL OR assignee=?)",
        (
            lock, expires, now, admission_id, task_id, source_status,
            expect_assignee, expect_assignee,
        ),
    )
    if updated.rowcount != 1:
        return None
    task = conn.execute(
        "SELECT assignee,max_runtime_seconds,current_step_key FROM tasks WHERE id=?",
        (task_id,),
    ).fetchone()
    run = conn.execute(
        "INSERT INTO task_runs (task_id,profile,step_key,status,claim_lock,claim_expires,"
        "max_runtime_seconds,started_at) VALUES (?,?,?,'running',?,?,?,?)",
        (task_id, task["assignee"] if task else None,
         task["current_step_key"] if task else None, lock, expires,
         task["max_runtime_seconds"] if task else None, now),
    )
    if run.lastrowid is None:
        raise RuntimeError("claim run insert did not return a row id")
    run_id = int(run.lastrowid)
    conn.execute("UPDATE tasks SET current_run_id=? WHERE id=?", (run_id, task_id))
    kb._append_event(
        conn, task_id, "claimed",
        {"lock": lock, "expires": expires, "run_id": run_id, **(event_extra or {})},
        run_id=run_id,
    )
    if source_status == "ready":
        from hermes_cli import kanban_ready_monitor as monitor
        monitor.close_for_native_ready_claim(conn, task_id, run_id, now=now)
    return run_id


def _claim_parents_satisfied(conn: sqlite3.Connection, task_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM task_links l JOIN tasks p ON p.id=l.parent_id "
        "WHERE l.child_id=? AND p.status NOT IN ('done','archived') LIMIT 1",
        (task_id,),
    ).fetchone() is None


def _claim_control_refusal(conn: sqlite3.Connection, task_id: str) -> Optional[str]:
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_control import read_control
    if kb._has_unknown_block_identity(conn, task_id):
        return "block_identity_unknown"
    control = read_control(conn)
    if control.state == "paused":
        return "kanban_paused"
    if control.state != "running":
        return "kanban_control_unknown"
    return None


def claim_task_transition(
    conn: sqlite3.Connection, task_id: str, *, actor_scope: str,
    operation_id: str, source_status: str, lock: str, expires: int, now: int,
    monitor_attempt_id: Optional[str] = None, admission_id: Optional[str] = None,
    expect_assignee: Optional[str] = None,
) -> ClaimTransitionOutcome:
    """Claim one Ready or Review task through a closed receipt boundary."""
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_connect as kbc

    if source_status not in {"ready", "review"}:
        raise ValueError("source_status must be 'ready' or 'review'")
    if monitor_attempt_id is not None and admission_id is not None:
        raise ValueError("monitor_attempt_id and admission_id are mutually exclusive")
    _validate_ingress(conn, actor_scope=actor_scope, operation_id=operation_id)
    request = _ClaimRequest(
        task_id, actor_scope, operation_id, source_status, lock, int(expires),
        int(now), monitor_attempt_id, admission_id, expect_assignee,
        f"claim_{source_status}",
    )
    db_path = kbc._main_db_file(conn)
    if not db_path:
        raise ValueError("protected claim requires a file-backed Kanban database")
    request_hash = _request_hash(request)
    applied = False
    with kbc._protected_connection(db_path, operation="claim") as private:
        try:
            kbc._execute_boundary_with_retry(private, "BEGIN IMMEDIATE")
            receipt = _load_receipt(private, operation_id)
            if receipt is not None:
                replay = _receipt_outcome(receipt, request, request_hash)
                if not replay.committed:
                    outcome = ClaimTransitionOutcome(
                        False, operation_id, reason=replay.refusal
                    )
                else:
                    event = private.execute(
                        "SELECT run_id FROM task_events WHERE id=?", (replay.event_id,)
                    ).fetchone()
                    replay_run = int(event[0]) if event and event[0] is not None else None
                    outcome = ClaimTransitionOutcome(
                        True, operation_id, kb.get_task(private, task_id), replay_run,
                        replay.event_id, tuple_version=replay.tuple_version,
                        result_tuple_hash=replay.result_tuple_hash,
                    )
                kbc._execute_boundary_with_retry(private, "COMMIT")
            else:
                refusal = _claim_control_refusal(private, task_id)
                if refusal is not None:
                    kbc._execute_boundary_with_retry(private, "COMMIT")
                    return ClaimTransitionOutcome(False, operation_id, reason=refusal)
                if not _claim_parents_satisfied(private, task_id):
                    if source_status == "ready":
                        private.execute(
                            "UPDATE tasks SET status='todo' WHERE id=? AND status='ready'",
                            (task_id,),
                        )
                        kb._append_event(
                            private, task_id, "claim_rejected",
                            {"reason": "parents_not_done"},
                        )
                    else:
                        demoted = private.execute(
                            "UPDATE tasks SET status='todo' WHERE id=? AND status='review' "
                            "AND claim_lock IS NULL", (task_id,),
                        )
                        if demoted.rowcount == 1:
                            kb._append_event(
                                private, task_id, "dependency_wait",
                                {"reason": "parent_reopened", "source_status": "review"},
                            )
                    kbc._execute_boundary_with_retry(private, "COMMIT")
                    return ClaimTransitionOutcome(
                        False, operation_id, reason="parents_not_done"
                    )
                if source_status == "ready":
                    kb._reclaim_dangling_run(
                        private, task_id, statuses=("ready",), now=now,
                        note="invariant recovery on re-claim",
                    )
                effective_admission = admission_id
                validated = False
                if monitor_attempt_id is not None:
                    from hermes_cli import kanban_ready_monitor as monitor
                    task = private.execute(
                        "SELECT assignee FROM tasks WHERE id=?", (task_id,)
                    ).fetchone()
                    generation = monitor.ready_generation(private, task_id)
                    previous_run = private.execute(
                        "SELECT COALESCE(MAX(id),0) FROM task_runs WHERE task_id=?",
                        (task_id,),
                    ).fetchone()[0]
                    if task is None or generation is None:
                        kbc._execute_boundary_with_retry(private, "COMMIT")
                        return ClaimTransitionOutcome(
                            False, operation_id, reason="claim_lost"
                        )
                    try:
                        effective_admission = monitor.arm_claim_admission(
                            private, attempt_id=monitor_attempt_id, task_id=task_id,
                            ready_generation=generation[0],
                            run_generation=int(previous_run),
                        )
                    except ValueError:
                        kbc._execute_boundary_with_retry(private, "COMMIT")
                        return ClaimTransitionOutcome(
                            False, operation_id, reason="admission_refused"
                        )
                    if not monitor.validate_claim_admission(
                        private, effective_admission, task_id,
                        task["assignee"] or "", generation[0], int(previous_run),
                    ):
                        raise _ClaimRollback("admission_refused")
                    validated = True
                run_id = kb._claim_and_open_run(
                    private, task_id, source_status, lock, expires, now,
                    event_extra={"source_status": "review"}
                    if source_status == "review" else None,
                    admission_id=effective_admission,
                    admission_validated=validated,
                    expect_assignee=expect_assignee,
                )
                if run_id is None:
                    if effective_admission is not None:
                        raise _ClaimRollback("claim_lost")
                    kbc._execute_boundary_with_retry(private, "COMMIT")
                    return ClaimTransitionOutcome(
                        False, operation_id, reason="claim_lost"
                    )
                event_id = private.execute(
                    "SELECT id FROM task_events WHERE task_id=? AND run_id=? "
                    "AND kind='claimed' ORDER BY id DESC LIMIT 1", (task_id, run_id),
                ).fetchone()[0]
                receipt_outcome = _insert_receipt(
                    private, request, request_hash,
                    TransitionOutcome(True, operation_id, event_id, "none"),
                )
                task = kb.get_task(private, task_id)
                kbc._execute_boundary_with_retry(private, "COMMIT")
                applied = True
                outcome = ClaimTransitionOutcome(
                    True, operation_id, task, run_id, event_id,
                    tuple_version=receipt_outcome.tuple_version,
                    result_tuple_hash=receipt_outcome.result_tuple_hash,
                )
        except _ClaimRollback as exc:
            _rollback(private)
            return ClaimTransitionOutcome(False, operation_id, reason=exc.reason)
        except BaseException:
            _rollback(private)
            raise
    if applied and source_status == "ready":
        kb._fire_task_hook("kanban_task_claimed", outcome.task, task_id, outcome.run_id)
    return outcome












from hermes_cli.kanban_state_operations import storage as _state_operations

_state_operations.bind(__import__(__name__, fromlist=["*"]))
_apply_create = _state_operations._apply_create
_apply_swarm_create = _state_operations._apply_swarm_create
_apply_decompose = _state_operations._apply_decompose
_apply_link = _state_operations._apply_link
_apply_unlink = _state_operations._apply_unlink


from hermes_cli.kanban_lifecycle import storage as _lifecycle

_apply_complete = _lifecycle._apply_complete
_apply_request_review = _lifecycle._apply_request_review
_apply_request_changes = _lifecycle._apply_request_changes
_apply_reopen_review = _lifecycle._apply_reopen_review


_apply_runtime_timeout = _recovery._apply_runtime_timeout
_apply_dispatch_failure = _recovery._apply_dispatch_failure


_OPERATION_HANDLERS = {
    _BlockRequest: _apply_block,
    _ClassifyBlockRequest: _apply_classify_block,
    _UnblockRequest: _apply_unblock,
    _PromoteRequest: _apply_promote,
    _ScheduleRequest: _apply_schedule,
    _ArchiveRequest: _apply_archive,
    _DeleteRequest: _apply_delete,
    _LinkRequest: _apply_link,
    _UnlinkRequest: _apply_unlink,
    _CreateRequest: _apply_create,
    _SwarmRequest: _apply_swarm_create,
    _DecomposeRequest: _apply_decompose,
    _CompleteRequest: _apply_complete,
    _RequestReviewRequest: _apply_request_review,
    _RequestChangesRequest: _apply_request_changes,
    _ReopenReviewRequest: _apply_reopen_review,
    _RuntimeTimeoutRequest: _apply_runtime_timeout,
    _DispatchFailureRequest: _apply_dispatch_failure,
}


def _run_operation(
    conn: sqlite3.Connection,
    request: _Request,
) -> _KernelResult:
    """Run one closed request with replay, CAS effects, and receipt in one transaction."""
    from hermes_cli import kanban_db_connect as kbc

    inline_authority = kbc._protected_connection_operation(conn)
    if conn.in_transaction and inline_authority == request.authority_kind:
        if not isinstance(request.actor_scope, str) or not request.actor_scope.strip():
            raise ValueError("actor_scope is required")
        if not isinstance(request.operation_id, str) or not request.operation_id.strip():
            raise ValueError("operation_id is required")
        request_hash = _request_hash(request)
        receipt = _load_receipt(conn, request.operation_id)
        if receipt is not None:
            return _KernelResult(
                _receipt_outcome(receipt, request, request_hash), False
            )
        applied = _OPERATION_HANDLERS[type(request)](conn, request)
        assert applied is not None
        if not applied.outcome.committed:
            return _KernelResult(applied.outcome, False)
        outcome = _insert_receipt(
            conn, request, request_hash, applied.outcome
        )
        return _KernelResult(
            outcome,
            True,
            applied.hook_run_id,
            applied.worker_pid,
            applied.claim_lock,
            applied.worker_started_at,
            applied.was_running,
            applied.attachment_paths,
        )

    _validate_ingress(
        conn, actor_scope=request.actor_scope, operation_id=request.operation_id
    )
    db_path = kbc._main_db_file(conn)
    if not db_path:
        raise ValueError("protected task transition requires a file-backed Kanban database")
    request_hash = _request_hash(request)
    applied: Optional[_AppliedTransition] = None
    with kbc._protected_connection(db_path, operation=request.authority_kind) as private:
        try:
            kbc._execute_boundary_with_retry(private, "BEGIN IMMEDIATE")
            receipt = _load_receipt(private, request.operation_id)
            if receipt is not None:
                outcome = _receipt_outcome(receipt, request, request_hash)
                kbc._execute_boundary_with_retry(private, "COMMIT")
                return _KernelResult(outcome, False)
            applied = _OPERATION_HANDLERS[type(request)](private, request)
            assert applied is not None
            if not applied.outcome.committed:
                _rollback(private)
                return _KernelResult(applied.outcome, False)
            outcome = _insert_receipt(
                private, request, request_hash, applied.outcome
            )
            kbc._execute_boundary_with_retry(private, "COMMIT")
        except BaseException:
            _rollback(private)
            if applied is not None and applied.staged_copies:
                from hermes_cli import kanban_db as kb

                kb._discard_staged_copies(
                    list(applied.staged_copies), applied.staged_copies[0].parent
                )
            raise
    assert applied is not None
    return _KernelResult(
        outcome,
        True,
        applied.hook_run_id,
        applied.worker_pid,
        applied.claim_lock,
        applied.worker_started_at,
        applied.was_running,
        applied.attachment_paths,
    )






































_state_operations.bind(__import__(__name__, fromlist=["*"]))
_graph_transition_outcome = _state_operations._graph_transition_outcome
link_tasks_transition = _state_operations.link_tasks_transition
unlink_tasks_transition = _state_operations.unlink_tasks_transition
create_task_transition = _state_operations.create_task_transition
create_swarm_transition = _state_operations.create_swarm_transition
decompose_task_transition = _state_operations.decompose_task_transition
recompute_ready_transition = _state_operations.recompute_ready_transition
link_tasks = _state_operations.link_tasks
unlink_tasks = _state_operations.unlink_tasks
_default_failure_limit = _state_operations._default_failure_limit
recompute_ready = _state_operations.recompute_ready
block_task_transition = _state_operations.block_task_transition
compatibility_operation_id = _state_operations.compatibility_operation_id


_lifecycle_outcome = _lifecycle._lifecycle_outcome
complete_task_transition = _lifecycle.complete_task_transition
request_review_transition = _lifecycle.request_review_transition
request_changes_transition = _lifecycle.request_changes_transition
reopen_review_task_transition = _lifecycle.reopen_review_task_transition


runtime_timeout_transition = _recovery.runtime_timeout_transition
dispatch_failure_transition = _recovery.dispatch_failure_transition


def _compatibility_lifecycle_operation_id(kind: str, task_id: str) -> str:
    return f"legacy-{kind}:{task_id}:{secrets.token_hex(12)}"


_state_operations.bind(__import__(__name__, fromlist=["*"]))


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


def _record_archive_worker_termination(
    conn: sqlite3.Connection,
    task_id: str,
    termination: dict[str, Any],
    run_id: Optional[int],
) -> None:
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_connect as kbc

    db_path = kbc._main_db_file(conn)
    if not db_path:
        raise ValueError("archive audit requires a file-backed Kanban database")
    with kbc._protected_connection(db_path, operation="archive") as private:
        try:
            kbc._execute_boundary_with_retry(private, "BEGIN IMMEDIATE")
            kb._append_event(
                private,
                task_id,
                "archive_worker_termination",
                termination,
                run_id=run_id,
            )
            kbc._execute_boundary_with_retry(private, "COMMIT")
        except BaseException:
            _rollback(private)
            raise


def archive_task_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    signal_fn=None,
) -> TransitionOutcome:
    """Archive one task atomically, then perform host-local cleanup after close."""
    from hermes_cli import kanban_db as kb

    result = _run_operation(
        conn, _ArchiveRequest(task_id, actor_scope, operation_id)
    )
    if not result.applied:
        return result.outcome
    if result.was_running:
        termination = kb._terminate_reclaimed_worker(
            result.worker_pid,
            result.claim_lock,
            signal_fn=signal_fn,
            started_at=result.worker_started_at,
        )
        _record_archive_worker_termination(
            conn, task_id, termination, result.hook_run_id
        )
    kb.recompute_ready(conn)
    kb._cleanup_workspace(conn, task_id)
    return result.outcome


def archive_task(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    signal_fn=None,
) -> bool:
    """Compatibility adapter preserving the literal-bool archive result."""
    return archive_task_transition(
        conn,
        task_id,
        actor_scope="legacy",
        operation_id=_compatibility_lifecycle_operation_id("archive", task_id),
        signal_fn=signal_fn,
    ).committed


def _delete_attachment_blobs(
    conn: sqlite3.Connection,
    attachment_paths: tuple[str, ...],
) -> None:
    for stored_path in dict.fromkeys(attachment_paths):
        still_referenced = conn.execute(
            "SELECT 1 FROM task_attachments WHERE stored_path = ? LIMIT 1",
            (stored_path,),
        ).fetchone()
        if still_referenced is not None:
            continue
        with contextlib.suppress(OSError):
            path = Path(stored_path)
            if path.is_file():
                path.unlink()


def _delete_task_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    archived_only: bool,
) -> TransitionOutcome:
    from hermes_cli import kanban_db as kb

    operation_kind = "delete_archived" if archived_only else "delete"
    result = _run_operation(
        conn,
        _DeleteRequest(
            task_id,
            actor_scope,
            operation_id,
            archived_only,
            operation_kind,
        ),
    )
    if result.applied:
        kb.recompute_ready(conn)
        _delete_attachment_blobs(conn, result.attachment_paths)
    return result.outcome


def delete_archived_task_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
) -> TransitionOutcome:
    """Delete an archived task and all task-owned rows in one receipt transaction."""
    return _delete_task_transition(
        conn,
        task_id,
        actor_scope=actor_scope,
        operation_id=operation_id,
        archived_only=True,
    )


def delete_task_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
) -> TransitionOutcome:
    """Delete any classified task and all task-owned rows in one receipt transaction."""
    return _delete_task_transition(
        conn,
        task_id,
        actor_scope=actor_scope,
        operation_id=operation_id,
        archived_only=False,
    )


def delete_archived_task(conn: sqlite3.Connection, task_id: str) -> bool:
    """Compatibility adapter preserving archived-only deletion."""
    return delete_archived_task_transition(
        conn,
        task_id,
        actor_scope="legacy",
        operation_id=_compatibility_lifecycle_operation_id("delete-archived", task_id),
    ).committed


def delete_task(conn: sqlite3.Connection, task_id: str) -> bool:
    """Compatibility adapter preserving unrestricted classified deletion."""
    return delete_task_transition(
        conn,
        task_id,
        actor_scope="legacy",
        operation_id=_compatibility_lifecycle_operation_id("delete", task_id),
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
