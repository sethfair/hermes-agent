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
class BlockTransitionOutcome:
    committed: bool
    operation_id: str
    event_id: Optional[int] = None
    identity_state: str = "unknown"
    refusal: Optional[str] = None
    tuple_version: Optional[int] = None
    result_tuple_hash: Optional[str] = None


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


def _request_hash(
    *,
    task_id: str,
    operation_kind: str,
    actor_scope: str,
    kind: Optional[str],
    identity: BlockIdentity,
    raw_subject: object,
    reason: Optional[str] = None,
    expected_run_id: Optional[int] = None,
) -> str:
    return _canonical_hash({
        "task_id": task_id,
        "operation_kind": operation_kind,
        "actor_scope": actor_scope,
        "kind": kind,
        "subject": _subject_request_token(identity, raw_subject),
        "reason": reason,
        "expected_run_id": expected_run_id,
    })


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
    *,
    task_id: str,
    operation_kind: str,
    request_hash: str,
    operation_id: str,
) -> BlockTransitionOutcome:
    matches = (
        receipt["task_id"] == task_id
        and receipt["operation_kind"] == operation_kind
        and receipt["request_hash"] == request_hash
    )
    if not matches:
        return BlockTransitionOutcome(False, operation_id, refusal="operation_id_collision")
    if (
        receipt["outcome_code"] != "committed"
        or receipt["tuple_version"] != _TUPLE_VERSION
        or not _valid_result_hash(receipt["result_tuple_hash"])
        or receipt["result_identity_state"] not in {"known", "unknown"}
    ):
        return BlockTransitionOutcome(False, operation_id, refusal="invalid_receipt")
    return BlockTransitionOutcome(
        True,
        operation_id,
        receipt["event_id"],
        receipt["result_identity_state"],
        tuple_version=receipt["tuple_version"],
        result_tuple_hash=receipt["result_tuple_hash"],
    )


def _load_receipt(private: sqlite3.Connection, operation_id: str) -> Optional[sqlite3.Row]:
    return private.execute(
        "SELECT task_id, operation_kind, request_hash, outcome_code, event_id, "
        "tuple_version, result_tuple_hash, result_identity_state "
        "FROM task_transition_receipts WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()


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
    from hermes_cli import kanban_db_connect as kbc

    _validate_ingress(conn, actor_scope=actor_scope, operation_id=operation_id)
    if kind is not None and kind not in kb.VALID_BLOCK_KINDS:
        raise ValueError(f"block kind must be one of {sorted(kb.VALID_BLOCK_KINDS)} or None")
    identity = canonicalize_block_identity(kind, subject)
    request_hash = _request_hash(
        task_id=task_id,
        operation_kind="block",
        actor_scope=actor_scope,
        kind=kind,
        identity=identity,
        raw_subject=subject,
        reason=reason,
        expected_run_id=expected_run_id,
    )
    db_path = kbc._main_db_file(conn)
    if not db_path:
        raise ValueError("protected task transition requires a file-backed Kanban database")

    hook_run_id: Optional[int] = None
    outcome: BlockTransitionOutcome
    with kbc._protected_connection(db_path, operation="block") as private:
        try:
            private.execute("BEGIN IMMEDIATE")
            receipt = _load_receipt(private, operation_id)
            if receipt is not None:
                outcome = _receipt_outcome(
                    receipt,
                    task_id=task_id,
                    operation_kind="block",
                    request_hash=request_hash,
                    operation_id=operation_id,
                )
                private.execute("COMMIT")
                return outcome

            row = private.execute(
                "SELECT status, current_run_id, block_kind, block_recurrences, "
                "block_identity, block_identity_state FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                private.execute("ROLLBACK")
                return BlockTransitionOutcome(False, operation_id, refusal="task_not_found")
            previous = effective_block_identity(row)
            if identity.state == "known" and kind == "dependency":
                parent = private.execute(
                    "SELECT status FROM tasks WHERE id = ? AND id IN "
                    "(SELECT parent_id FROM task_links WHERE child_id = ?)",
                    (identity.subject, task_id),
                ).fetchone()
                if parent is None or parent[0] in {"done", "archived"}:
                    cause = "no_open_parent" if parent else "nonparent_subject"
                    identity = BlockIdentity("unknown", kind=kind, unknown_cause=cause)
            source_status = "ready"
            if row[0] == "running":
                source_status = kb._retry_status_for_run(private, task_id)
            elif row[0] not in {"ready", "running"}:
                private.execute("ROLLBACK")
                return BlockTransitionOutcome(
                    False,
                    operation_id,
                    identity_state=identity.state,
                    refusal="invalid_status",
                )
            same_known = (
                identity.state == "known"
                and previous.state == "known"
                and identity.canonical == previous.canonical
            )
            recurrences = int(row[3] or 0) + 1 if same_known else 1
            new_status = "todo" if kind == "dependency" and identity.state == "known" else (
                "triage" if recurrences >= kb.BLOCK_RECURRENCE_LIMIT else "blocked"
            )
            event_kind = "dependency_wait" if new_status == "todo" else (
                "block_loop_detected" if new_status == "triage" else "blocked"
            )
            payload = {
                "reason": reason,
                "kind": kind,
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
                kind,
                0 if new_status == "todo" else recurrences,
                identity.canonical,
                identity.state,
                task_id,
            ]
            if expected_run_id is not None:
                update_sql += " AND current_run_id = ?"
                update_params.append(int(expected_run_id))
            private.execute(update_sql, update_params)
            if private.execute("SELECT changes()").fetchone()[0] != 1:
                private.execute("ROLLBACK")
                return BlockTransitionOutcome(
                    False,
                    operation_id,
                    identity_state=identity.state,
                    refusal="compare_and_set",
                )
            hook_run_id = kb._end_or_synthesize_run(
                private,
                task_id,
                outcome="blocked",
                status="blocked",
                summary=reason,
                synthesize=bool(reason),
            )
            if identity.state == "unknown":
                kb._append_event(
                    private,
                    task_id,
                    "block_identity_unknown",
                    {**identity_event_fields(identity), "classification_required": True},
                    run_id=hook_run_id,
                )
            kb._append_event(private, task_id, event_kind, payload, run_id=hook_run_id)
            event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
            result_tuple_hash = _protected_tuple_hash(private, task_id)
            private.execute(
                "INSERT INTO task_transition_receipts "
                "(operation_id, task_id, operation_kind, request_hash, outcome_code, event_id, "
                "committed_at, tuple_version, result_tuple_hash, result_identity_state) "
                "VALUES (?, ?, 'block', ?, 'committed', ?, ?, ?, ?, ?)",
                (
                    operation_id,
                    task_id,
                    request_hash,
                    event_id,
                    int(time.time()),
                    _TUPLE_VERSION,
                    result_tuple_hash,
                    identity.state,
                ),
            )
            private.execute("COMMIT")
            outcome = BlockTransitionOutcome(
                True,
                operation_id,
                event_id,
                identity.state,
                tuple_version=_TUPLE_VERSION,
                result_tuple_hash=result_tuple_hash,
            )
        except BaseException:
            _rollback(private)
            raise

    kb._fire_task_hook(
        "kanban_task_blocked",
        kb.get_task(conn, task_id),
        task_id,
        hook_run_id,
        reason=reason,
    )
    return outcome


def compatibility_operation_id(task_id: str) -> str:
    return f"legacy-block:{task_id}:{secrets.token_hex(12)}"


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
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_connect as kbc

    _validate_ingress(conn, actor_scope=actor_scope, operation_id=operation_id)
    identity = canonicalize_block_identity(kind, subject)
    if identity.state != "known":
        raise ValueError(identity.unknown_cause or "invalid block identity")
    request_hash = _request_hash(
        task_id=task_id,
        operation_kind="classify_block",
        actor_scope=actor_scope,
        kind=kind,
        identity=identity,
        raw_subject=subject,
    )
    db_path = kbc._main_db_file(conn)
    if not db_path:
        raise ValueError("block classification requires a file-backed database")

    with kbc._protected_connection(db_path, operation="classify") as private:
        try:
            private.execute("BEGIN IMMEDIATE")
            receipt = _load_receipt(private, operation_id)
            if receipt is not None:
                outcome = _receipt_outcome(
                    receipt,
                    task_id=task_id,
                    operation_kind="classify_block",
                    request_hash=request_hash,
                    operation_id=operation_id,
                )
                private.execute("COMMIT")
                return outcome
            row = private.execute(
                "SELECT status, current_run_id, block_identity_state FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            if (
                row is None
                or row[0] not in {"blocked", "todo", "scheduled", "triage"}
                or row[1] is not None
                or row[2] != "unknown"
            ):
                private.execute("ROLLBACK")
                return BlockTransitionOutcome(False, operation_id, refusal="invalid_status")
            if kind == "dependency" and private.execute(
                "SELECT 1 FROM task_links WHERE parent_id = ? AND child_id = ?",
                (identity.subject, task_id),
            ).fetchone() is None:
                private.execute("ROLLBACK")
                return BlockTransitionOutcome(False, operation_id, refusal="nonparent_subject")
            updated = private.execute(
                "UPDATE tasks SET block_kind = ?, block_recurrences = 1, block_identity = ?, "
                "block_identity_state = 'known' WHERE id = ? AND block_identity_state = 'unknown'",
                (kind, identity.canonical, task_id),
            )
            if updated.rowcount != 1:
                private.execute("ROLLBACK")
                return BlockTransitionOutcome(False, operation_id, refusal="compare_and_set")
            kb._append_event(
                private,
                task_id,
                "block_classified",
                {"actor": actor_scope, "kind": kind, **identity_event_fields(identity)},
            )
            event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
            result_tuple_hash = _protected_tuple_hash(private, task_id)
            private.execute(
                "INSERT INTO task_transition_receipts "
                "(operation_id, task_id, operation_kind, request_hash, outcome_code, event_id, "
                "committed_at, tuple_version, result_tuple_hash, result_identity_state) "
                "VALUES (?, ?, 'classify_block', ?, 'committed', ?, ?, ?, ?, 'known')",
                (
                    operation_id,
                    task_id,
                    request_hash,
                    event_id,
                    int(time.time()),
                    _TUPLE_VERSION,
                    result_tuple_hash,
                ),
            )
            private.execute("COMMIT")
            return BlockTransitionOutcome(
                True,
                operation_id,
                event_id,
                "known",
                tuple_version=_TUPLE_VERSION,
                result_tuple_hash=result_tuple_hash,
            )
        except BaseException:
            _rollback(private)
            raise


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
