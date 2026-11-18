"""Closed, typed task-state operations for Kanban protected writes."""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

from hermes_cli.kanban_task_state import (
    BlockIdentity,
    canonicalize_block_identity,
    effective_block_identity,
    identity_event_fields,
)


@dataclass(frozen=True)
class BlockTransitionOutcome:
    committed: bool
    operation_id: str
    event_id: Optional[int] = None
    identity_state: str = "unknown"
    refusal: Optional[str] = None


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

    if kind is not None and kind not in kb.VALID_BLOCK_KINDS:
        raise ValueError(f"block kind must be one of {sorted(kb.VALID_BLOCK_KINDS)} or None")
    identity = canonicalize_block_identity(kind, subject)
    request_hash = hashlib.sha256(json.dumps({
        "task_id": task_id, "actor_scope": actor_scope, "kind": kind,
        "subject": identity.subject, "reason": reason, "expected_run_id": expected_run_id,
    }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    db_path = kbc._main_db_file(conn)
    if not db_path:
        raise ValueError("protected task transition requires a file-backed Kanban database")
    with kbc._protected_connection(db_path, operation="block") as private:
        private.execute("BEGIN IMMEDIATE")
        receipt = private.execute(
            "SELECT request_hash, outcome_code, event_id FROM task_transition_receipts "
            "WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if receipt is not None:
            if receipt[0] != request_hash:
                private.execute("ROLLBACK")
                return BlockTransitionOutcome(False, operation_id, refusal="operation_id_collision")
            private.execute("COMMIT")
            return BlockTransitionOutcome(True, operation_id, receipt[2], identity.state)

        row = private.execute(
            "SELECT status, current_run_id, block_kind, block_recurrences, "
            "block_identity, block_identity_state FROM tasks WHERE id = ?", (task_id,)
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
                identity = BlockIdentity("unknown", kind=kind, unknown_cause="no_open_parent" if parent else "nonparent_subject")
        source_status = "ready"
        if row[0] == "running":
            source_status = kb._retry_status_for_run(private, task_id)
        elif row[0] not in {"ready", "running"}:
            private.execute("ROLLBACK")
            return BlockTransitionOutcome(False, operation_id, identity_state=identity.state, refusal="invalid_status")
        same_known = identity.state == "known" and previous.state == "known" and identity.canonical == previous.canonical
        recurrences = int(row[3] or 0) + 1 if same_known else 1
        new_status = "todo" if kind == "dependency" and identity.state == "known" else (
            "triage" if recurrences >= kb.BLOCK_RECURRENCE_LIMIT else "blocked"
        )
        event_kind = "dependency_wait" if new_status == "todo" else (
            "block_loop_detected" if new_status == "triage" else "blocked"
        )
        fields = identity_event_fields(identity)
        payload = {"reason": reason, "kind": kind, "source_status": source_status, **fields}
        if new_status != "todo":
            payload["recurrences"] = recurrences
        update_sql = (
            "UPDATE tasks SET status = ?, claim_lock = NULL, claim_expires = NULL, "
            "worker_pid = NULL, block_kind = ?, block_recurrences = ?, block_identity = ?, "
            "block_identity_state = ? WHERE id = ? AND status IN ('ready','running')"
        )
        update_params = [
            new_status, kind, 0 if new_status == "todo" else recurrences,
            identity.canonical, identity.state, task_id,
        ]
        if expected_run_id is not None:
            update_sql += " AND current_run_id = ?"
            update_params.append(int(expected_run_id))
        private.execute(update_sql, update_params)
        if private.execute("SELECT changes()").fetchone()[0] != 1:
            private.execute("ROLLBACK")
            return BlockTransitionOutcome(False, operation_id, identity_state=identity.state, refusal="compare_and_set")
        run_id = kb._end_or_synthesize_run(
            private, task_id, outcome="blocked", status="blocked", summary=reason, synthesize=bool(reason)
        )
        if identity.state == "unknown":
            kb._append_event(private, task_id, "block_identity_unknown", {
                **identity_event_fields(identity), "classification_required": True,
            }, run_id=run_id)
        kb._append_event(private, task_id, event_kind, payload, run_id=run_id)
        event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
        private.execute(
            "INSERT INTO task_transition_receipts "
            "(operation_id, task_id, operation_kind, request_hash, outcome_code, event_id, committed_at) "
            "VALUES (?, ?, 'block', ?, 'committed', ?, ?)",
            (operation_id, task_id, request_hash, event_id, int(time.time())),
        )
        private.execute("COMMIT")
        kb._fire_task_hook(
            "kanban_task_blocked", kb.get_task(conn, task_id), task_id, run_id,
            reason=reason,
        )
        return BlockTransitionOutcome(True, operation_id, event_id, identity.state)


def compatibility_operation_id(task_id: str) -> str:
    return f"legacy-block:{task_id}:{secrets.token_hex(12)}"


def classify_block(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    kind: str,
    subject: str,
    actor: str,
) -> bool:
    """Attach an actor-attributed identity without releasing the hold."""
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_connect as kbc

    if not str(actor or "").strip():
        raise ValueError("actor is required")
    identity = canonicalize_block_identity(kind, subject)
    if identity.state != "known":
        raise ValueError(identity.unknown_cause or "invalid block identity")
    db_path = kbc._main_db_file(conn)
    if not db_path:
        raise ValueError("block classification requires a file-backed database")
    with kbc._protected_connection(db_path, operation="classify") as private:
        private.execute("BEGIN IMMEDIATE")
        row = private.execute(
            "SELECT status, current_run_id, block_identity_state FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if row is None or row[0] not in {"blocked", "todo", "scheduled", "triage"} \
                or row[1] is not None or row[2] != "unknown":
            private.execute("ROLLBACK")
            return False
        if kind == "dependency" and private.execute(
            "SELECT 1 FROM task_links WHERE parent_id = ? AND child_id = ?", (identity.subject, task_id)
        ).fetchone() is None:
            private.execute("ROLLBACK")
            return False
        updated = private.execute(
            "UPDATE tasks SET block_kind = ?, block_recurrences = 1, block_identity = ?, "
            "block_identity_state = 'known' WHERE id = ? AND block_identity_state = 'unknown'",
            (kind, identity.canonical, task_id),
        )
        if updated.rowcount != 1:
            private.execute("ROLLBACK")
            return False
        kb._append_event(private, task_id, "block_classified", {
            "actor": str(actor), "kind": kind, **identity_event_fields(identity),
        })
        private.execute("COMMIT")
        return True
