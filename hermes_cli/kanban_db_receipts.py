"""Authenticated, idempotent action receipts for the Kanban database."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import re
import secrets
import sqlite3
import time
from typing import Literal, Mapping, cast

from hermes_cli.kanban_principal import (
    ActionReceiptError,
    AuthenticatedCallerContext,
    validate_caller_context,
)
from hermes_cli.kanban_workflow import DEFAULT_WORKFLOW
from hermes_cli.sqlite_util import write_txn

_CAPABILITY = "authenticated_action_receipts_v1"
JSONValue = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]


@dataclass(frozen=True)
class ProjectionRequest:
    schema_version: Literal[1]
    kind: Literal["comment"]
    comment_body: str
    expected_comment_absence: bool = True


@dataclass(frozen=True)
class ProjectionBinding:
    schema_version: Literal[1]
    comment_id: int
    comment_digest: str
    receipt_id: str
    receipt_digest: str
    event_id: int


@dataclass(frozen=True)
class ActionRequest:
    schema_version: Literal[1]
    board: str
    task_id: str
    operation_id: str
    operation: Literal["task.transition"]
    expected_action_generation: int
    expected_guard_fingerprint: str
    park_generation: int
    action_class: str
    from_status: str
    to_status: str
    transition: Mapping[str, JSONValue]
    decision_revision: int | None = None
    projection: ProjectionRequest | None = None


@dataclass(frozen=True)
class ActionReceipt:
    schema_version: Literal[1]
    receipt_id: str
    board: str
    task_id: str
    operation_id: str
    operation: str
    principal_id: str
    credential_id: str
    action_generation: int
    resulting_generation: int
    park_generation: int
    action_class: str
    from_status: str
    to_status: str
    transition_digest: str
    guard_fingerprint: str
    event_id: int
    committed_at: int
    projection: ProjectionBinding | None


@dataclass(frozen=True)
class AdvisoryNoticeRequest:
    schema_version: Literal[1]
    board: str
    task_id: str
    notice_operation_id: str
    notice_kind: Literal["D-prime", "S", "Z"]
    park_generation: int
    expected_action_generation: int
    expected_guard_fingerprint: str
    content: str
    projection: ProjectionRequest | None = None


@dataclass(frozen=True)
class AdvisoryNotice:
    schema_version: Literal[1]
    notice_id: str
    board: str
    task_id: str
    operation_id: str
    notice_kind: str
    principal_id: str
    credential_id: str
    action_generation: int
    park_generation: int
    guard_fingerprint: str
    content: str
    content_digest: str
    event_id: int
    committed_at: int
    projection: ProjectionBinding | None


def _canonical(value: object) -> str:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ActionReceiptError("invalid_request") from exc


def _digest(domain: str, value: object) -> str:
    raw = f"{domain}\0{_canonical(value)}".encode()
    return hashlib.sha256(raw).hexdigest()


def _bounded_text(value: str, *, max_length: int = 512) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > max_length:
        raise ActionReceiptError("invalid_request")
    return value


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ActionReceiptError("invalid_request")
    return value


def _schema_v1(value: object) -> None:
    if type(value) is not int or value != 1:
        raise ActionReceiptError("unsupported_schema")


def _validate_json_value(value: object) -> None:
    if value is None or type(value) in {bool, int, float, str}:
        _canonical(value)
        return
    if isinstance(value, list):
        for item in value:
            _validate_json_value(item)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for item in value.values():
            _validate_json_value(item)
        return
    raise ActionReceiptError("invalid_request")


def configure_action_receipts(
    conn: sqlite3.Connection,
    *,
    board: str,
    mode: Literal["off", "compatible", "live"],
    required_consumer_version: str | None = None,
    cutover_evidence: str | None = None,
) -> None:
    """Record the default-off producer fence; live requires drain evidence."""
    if mode not in {"off", "compatible", "live"}:
        raise ActionReceiptError("invalid_request")
    if mode != "off" and (not required_consumer_version or not cutover_evidence):
        raise ActionReceiptError("incompatible_consumer")
    now = int(time.time())
    with write_txn(conn):
        previous = conn.execute(
            """
            SELECT mode, required_consumer_version, cutover_evidence
              FROM kanban_capabilities
             WHERE board = ? AND capability = ?
            """,
            (board, _CAPABILITY),
        ).fetchone()
        previous_mode = previous["mode"] if previous else "off"
        if previous is not None and mode == "off":
            required_consumer_version = (
                required_consumer_version or previous["required_consumer_version"]
            )
            cutover_evidence = cutover_evidence or previous["cutover_evidence"]
        conn.execute(
            """
            INSERT INTO kanban_capabilities (
                board, capability, mode, required_consumer_version,
                cutover_evidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(board, capability) DO UPDATE SET
                mode = excluded.mode,
                required_consumer_version = excluded.required_consumer_version,
                cutover_evidence = excluded.cutover_evidence,
                updated_at = excluded.updated_at
            """,
            (
                board,
                _CAPABILITY,
                mode,
                required_consumer_version,
                cutover_evidence,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO kanban_capability_events (
                board, capability, previous_mode, mode,
                required_consumer_version, cutover_evidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                board,
                _CAPABILITY,
                previous_mode,
                mode,
                required_consumer_version,
                cutover_evidence,
                now,
            ),
        )


def action_receipts_mode(conn: sqlite3.Connection, *, board: str) -> str:
    row = conn.execute(
        "SELECT mode FROM kanban_capabilities WHERE board = ? AND capability = ?",
        (board, _CAPABILITY),
    ).fetchone()
    return row["mode"] if row else "off"


def guard_fingerprint(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    action_class: str | None = None,
    from_status: str | None = None,
    to_status: str | None = None,
    decision_revision: int | None = None,
) -> str:
    """Hash persisted state and optional exact transition intent."""
    row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        raise ActionReceiptError("unknown_task")
    task = {
        key: row[key]
        for key in (
            "id",
            "body",
            "assignee",
            "status",
            "claim_lock",
            "claim_expires",
            "current_run_id",
            "block_kind",
            "action_generation",
            "park_generation",
        )
    }
    links = [
        [link["parent_id"], link["child_id"]]
        for link in conn.execute(
            "SELECT parent_id, child_id FROM task_links "
            "WHERE parent_id = ? OR child_id = ? ORDER BY parent_id, child_id",
            (task_id, task_id),
        )
    ]
    comment = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS id FROM task_comments WHERE task_id = ?",
        (task_id,),
    ).fetchone()["id"]
    event = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS id FROM task_events WHERE task_id = ?",
        (task_id,),
    ).fetchone()["id"]
    intent = {
        "action_class": action_class,
        "from_status": from_status,
        "to_status": to_status,
        "decision_revision": decision_revision,
    }
    return _digest(
        "hermes.kanban.guard.v1",
        {"task": task, "links": links, "comment": comment, "event": event, "intent": intent},
    )


def _projection_payload(request: ProjectionRequest | None) -> dict | None:
    if request is None:
        return None
    _schema_v1(request.schema_version)
    if request.kind != "comment":
        raise ActionReceiptError("unsupported_schema")
    if request.expected_comment_absence is not True:
        raise ActionReceiptError("invalid_request")
    _bounded_text(request.comment_body, max_length=100_000)
    return asdict(request)


def _action_payload(request: ActionRequest, context: AuthenticatedCallerContext) -> dict:
    _schema_v1(request.schema_version)
    if request.operation != "task.transition":
        raise ActionReceiptError("unsupported_schema")
    for value in (
        request.board,
        request.task_id,
        request.operation_id,
        request.action_class,
        request.from_status,
        request.to_status,
    ):
        _bounded_text(value)
    _integer(request.expected_action_generation)
    _integer(request.park_generation)
    if not re.fullmatch(r"[0-9a-f]{64}", request.expected_guard_fingerprint):
        raise ActionReceiptError("invalid_request")
    if request.decision_revision is not None:
        _integer(request.decision_revision)
    if not re.fullmatch(r"[a-z][a-z0-9_.-]{0,127}", request.action_class):
        raise ActionReceiptError("invalid_request")
    try:
        allowed = DEFAULT_WORKFLOW.can_move(request.from_status, request.to_status)
    except ValueError as exc:
        raise ActionReceiptError("invalid_request") from exc
    if not allowed:
        raise ActionReceiptError("status_conflict")
    # Statuses with run, completion, review, scheduling, archive, or hold
    # semantics must continue through their existing structured verbs. The v1
    # receipt transition is intentionally limited to the side-effect-free lane.
    safe_sources = {"todo", "ready", "blocked"}
    safe_destinations = {"triage", "todo", "ready"}
    if request.from_status not in safe_sources or request.to_status not in safe_destinations:
        raise ActionReceiptError("status_conflict")
    transition = dict(request.transition)
    _validate_json_value(transition)
    if transition != {"status": request.to_status}:
        raise ActionReceiptError("invalid_request")
    return {
        "schema_version": 1,
        "kind": "action",
        "board": request.board,
        "task_id": request.task_id,
        "operation_id": request.operation_id,
        "operation": request.operation,
        "principal_id": context.principal_id,
        "credential_id": context.credential_id,
        "expected_action_generation": request.expected_action_generation,
        "expected_guard_fingerprint": request.expected_guard_fingerprint,
        "park_generation": request.park_generation,
        "action_class": request.action_class,
        "from_status": request.from_status,
        "to_status": request.to_status,
        "transition": dict(request.transition),
        "decision_revision": request.decision_revision,
        "projection": _projection_payload(request.projection),
    }


def _ensure_live(conn: sqlite3.Connection, board: str) -> None:
    if action_receipts_mode(conn, board=board) != "live":
        raise ActionReceiptError("capability_disabled")


def _operation_row(conn: sqlite3.Connection, board: str, operation_id: str):
    return conn.execute(
        "SELECT * FROM operation_keys WHERE board = ? AND operation_id = ?",
        (board, operation_id),
    ).fetchone()


def _projection_binding(
    conn: sqlite3.Connection,
    *,
    task_id: str,
    request: ProjectionRequest | None,
    record_id: str,
    event_id: int,
    record_digest: str,
) -> ProjectionBinding | None:
    if request is None:
        return None
    if request.expected_comment_absence:
        existing = conn.execute(
            "SELECT 1 FROM task_comments WHERE task_id = ? AND body = ? LIMIT 1",
            (task_id, request.comment_body),
        ).fetchone()
        if existing:
            raise ActionReceiptError("projection_mismatch")
    now = int(time.time())
    cur = conn.execute(
        "INSERT INTO task_comments (task_id, author, body, created_at) VALUES (?, ?, ?, ?)",
        (task_id, "action-receipt", request.comment_body, now),
    )
    if cur.lastrowid is None:
        raise ActionReceiptError("storage_unavailable")
    return ProjectionBinding(
        schema_version=1,
        comment_id=int(cur.lastrowid),
        comment_digest=_digest("hermes.kanban.comment.v1", request.comment_body),
        receipt_id=record_id,
        receipt_digest=record_digest,
        event_id=event_id,
    )


def commit_action(
    conn: sqlite3.Connection,
    request: ActionRequest,
    *,
    caller_context: AuthenticatedCallerContext | None,
) -> ActionReceipt:
    """Commit one guarded transition, event, receipt, and optional projection."""
    try:
        with write_txn(conn):
            context = validate_caller_context(
                conn,
                caller_context,
                board=request.board,
                operation_scope="task.transition",
            )
            payload = _action_payload(request, context)
            digest = _digest("hermes.kanban.action.v1", payload)
            transition_digest = _digest(
                "hermes.kanban.transition.v1", dict(request.transition)
            )
            existing = _operation_row(conn, request.board, request.operation_id)
            if existing is not None:
                if (
                    existing["kind"] == "action"
                    and existing["principal_id"] == context.principal_id
                    and existing["credential_id"] == context.credential_id
                    and existing["digest"] == digest
                ):
                    receipt = lookup_action_receipt(
                        conn,
                        board=request.board,
                        operation_id=request.operation_id,
                        caller_context=context,
                    )
                    if receipt is None:
                        raise ActionReceiptError("receipt_missing")
                    return receipt
                raise ActionReceiptError("operation_conflict")
            _ensure_live(conn, request.board)

            task = conn.execute(
                """
                SELECT status, action_generation, park_generation, block_kind,
                       claim_lock, current_run_id
                  FROM tasks WHERE id = ?
                """,
                (request.task_id,),
            ).fetchone()
            if task is None:
                raise ActionReceiptError("unknown_task")
            if task["status"] != request.from_status:
                raise ActionReceiptError("status_conflict")
            if task["status"] == "blocked" or task["block_kind"] is not None:
                raise ActionReceiptError("hold_active")
            if task["claim_lock"] is not None or task["current_run_id"] is not None:
                raise ActionReceiptError("status_conflict")
            if request.to_status == "ready":
                blocking_parent = conn.execute(
                    """
                    SELECT 1 FROM task_links l
                    JOIN tasks p ON p.id = l.parent_id
                    WHERE l.child_id = ? AND p.status NOT IN ('done', 'archived')
                    LIMIT 1
                    """,
                    (request.task_id,),
                ).fetchone()
                if blocking_parent is not None:
                    raise ActionReceiptError("parent_gate")
            if int(task["action_generation"]) != request.expected_action_generation:
                raise ActionReceiptError("generation_conflict")
            if int(task["park_generation"]) != request.park_generation:
                raise ActionReceiptError("park_conflict")
            if guard_fingerprint(
                conn,
                request.task_id,
                action_class=request.action_class,
                from_status=request.from_status,
                to_status=request.to_status,
                decision_revision=request.decision_revision,
            ) != request.expected_guard_fingerprint:
                raise ActionReceiptError("generation_conflict")

            changed = conn.execute(
                "UPDATE tasks SET status = ? WHERE id = ? AND status = ? AND action_generation = ?",
                (
                    request.to_status,
                    request.task_id,
                    request.from_status,
                    request.expected_action_generation,
                ),
            )
            if changed.rowcount != 1:
                raise ActionReceiptError("generation_conflict")
            now = int(time.time())
            receipt_id = "ar_" + secrets.token_hex(16)
            event_payload = {
                "operation_id": request.operation_id,
                "receipt_id": receipt_id,
                "request_digest": digest,
                "to_status": request.to_status,
                "transition_digest": transition_digest,
            }
            event = conn.execute(
                "INSERT INTO task_events (task_id, kind, payload, created_at) VALUES (?, ?, ?, ?)",
                (
                    request.task_id,
                    "authenticated_action",
                    _canonical(event_payload),
                    now,
                ),
            )
            if event.lastrowid is None:
                raise ActionReceiptError("storage_unavailable")
            event_id = int(event.lastrowid)
            projection = _projection_binding(
                conn,
                task_id=request.task_id,
                request=request.projection,
                record_id=receipt_id,
                event_id=event_id,
                record_digest=digest,
            )
            resulting = int(
                conn.execute(
                    "SELECT action_generation FROM tasks WHERE id = ?", (request.task_id,)
                ).fetchone()["action_generation"]
            )
            conn.execute(
                """
                INSERT INTO action_receipts (
                    receipt_id, schema_version, board, task_id, operation_id,
                    operation, principal_id, credential_id, action_generation,
                    resulting_generation, park_generation, action_class,
                    from_status, to_status, decision_revision, transition_json,
                    transition_digest, request_digest, guard_fingerprint, outcome,
                    event_id, committed_at, projection_json, projection_comment_id
                ) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt_id,
                    request.board,
                    request.task_id,
                    request.operation_id,
                    request.operation,
                    context.principal_id,
                    context.credential_id,
                    request.expected_action_generation,
                    resulting,
                    request.park_generation,
                    request.action_class,
                    request.from_status,
                    request.to_status,
                    request.decision_revision,
                    _canonical(dict(request.transition)),
                    transition_digest,
                    digest,
                    request.expected_guard_fingerprint,
                    "committed",
                    event_id,
                    now,
                    _canonical(asdict(projection)) if projection else None,
                    projection.comment_id if projection else None,
                ),
            )
            conn.execute(
                """
                INSERT INTO operation_keys (
                    board, operation_id, kind, record_id,
                    principal_id, credential_id, digest
                ) VALUES (?, ?, 'action', ?, ?, ?, ?)
                """,
                (
                    request.board,
                    request.operation_id,
                    receipt_id,
                    context.principal_id,
                    context.credential_id,
                    digest,
                ),
            )
            return ActionReceipt(
                schema_version=1,
                receipt_id=receipt_id,
                board=request.board,
                task_id=request.task_id,
                operation_id=request.operation_id,
                operation=request.operation,
                principal_id=context.principal_id,
                credential_id=context.credential_id,
                action_generation=request.expected_action_generation,
                resulting_generation=resulting,
                park_generation=request.park_generation,
                action_class=request.action_class,
                from_status=request.from_status,
                to_status=request.to_status,
                transition_digest=transition_digest,
                guard_fingerprint=request.expected_guard_fingerprint,
                event_id=event_id,
                committed_at=now,
                projection=projection,
            )
    except sqlite3.IntegrityError as exc:
        if _operation_row(conn, request.board, request.operation_id) is not None:
            raise ActionReceiptError("operation_conflict") from exc
        raise ActionReceiptError("storage_unavailable") from exc


def _projection_from_json(raw: str | None) -> ProjectionBinding | None:
    try:
        return ProjectionBinding(**json.loads(raw)) if raw else None
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ActionReceiptError("projection_mismatch") from exc


def _verify_record_integrity(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    kind: Literal["action", "notice"],
) -> None:
    record_column = "receipt_id" if kind == "action" else "notice_id"
    digest_column = "request_digest"
    key = conn.execute(
        """
        SELECT record_id, digest FROM operation_keys
         WHERE board = ? AND operation_id = ? AND kind = ?
        """,
        (row["board"], row["operation_id"], kind),
    ).fetchone()
    if (
        key is None
        or key["record_id"] != row[record_column]
        or key["digest"] != row[digest_column]
    ):
        raise ActionReceiptError("receipt_missing")
    transition: object = None
    if kind == "action":
        try:
            transition = json.loads(row["transition_json"])
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ActionReceiptError("receipt_missing") from exc
        if (
            row["outcome"] != "committed"
            or int(row["resulting_generation"]) <= int(row["action_generation"])
            or _digest("hermes.kanban.transition.v1", transition)
            != row["transition_digest"]
        ):
            raise ActionReceiptError("receipt_missing")
    elif _digest("hermes.kanban.notice-content.v1", row["content"]) != row["content_digest"]:
        raise ActionReceiptError("receipt_missing")
    if conn.execute("SELECT 1 FROM tasks WHERE id = ?", (row["task_id"],)).fetchone() is None:
        raise ActionReceiptError("receipt_missing")
    expected_event_kind = "authenticated_action" if kind == "action" else "authenticated_advisory"
    event = conn.execute(
        "SELECT task_id, kind, payload FROM task_events WHERE id = ?", (row["event_id"],)
    ).fetchone()
    if event is None or event["task_id"] != row["task_id"] or event["kind"] != expected_event_kind:
        raise ActionReceiptError("receipt_missing")
    projection = _projection_from_json(row["projection_json"])
    projection_payload = None
    if projection is None:
        if row["projection_comment_id"] is not None:
            raise ActionReceiptError("projection_mismatch")
    else:
        if (
            projection.receipt_id != row[record_column]
            or projection.event_id != row["event_id"]
            or projection.comment_id != row["projection_comment_id"]
            or projection.receipt_digest != row[digest_column]
        ):
            raise ActionReceiptError("projection_mismatch")
        comment = conn.execute(
            "SELECT task_id, body FROM task_comments WHERE id = ?", (projection.comment_id,)
        ).fetchone()
        if (
            comment is None
            or comment["task_id"] != row["task_id"]
            or _digest("hermes.kanban.comment.v1", comment["body"])
            != projection.comment_digest
        ):
            raise ActionReceiptError("projection_mismatch")
        projection_payload = {
            "schema_version": 1,
            "kind": "comment",
            "comment_body": comment["body"],
            "expected_comment_absence": True,
        }
    if kind == "action":
        expected_payload = {
            "schema_version": 1,
            "kind": "action",
            "board": row["board"],
            "task_id": row["task_id"],
            "operation_id": row["operation_id"],
            "operation": row["operation"],
            "principal_id": row["principal_id"],
            "credential_id": row["credential_id"],
            "expected_action_generation": row["action_generation"],
            "expected_guard_fingerprint": row["guard_fingerprint"],
            "park_generation": row["park_generation"],
            "action_class": row["action_class"],
            "from_status": row["from_status"],
            "to_status": row["to_status"],
            "transition": transition,
            "decision_revision": row["decision_revision"],
            "projection": projection_payload,
        }
        expected_event_payload = {
            "operation_id": row["operation_id"],
            "receipt_id": row["receipt_id"],
            "request_digest": row["request_digest"],
            "to_status": row["to_status"],
            "transition_digest": row["transition_digest"],
        }
        digest_domain = "hermes.kanban.action.v1"
    else:
        expected_payload = {
            "schema_version": 1,
            "board": row["board"],
            "task_id": row["task_id"],
            "notice_operation_id": row["operation_id"],
            "notice_kind": row["notice_kind"],
            "park_generation": row["park_generation"],
            "expected_action_generation": row["action_generation"],
            "expected_guard_fingerprint": row["guard_fingerprint"],
            "content": row["content"],
            "projection": projection_payload,
            "principal_id": row["principal_id"],
            "credential_id": row["credential_id"],
            "kind": "notice",
        }
        expected_event_payload = {
            "content_digest": row["content_digest"],
            "notice_id": row["notice_id"],
            "notice_kind": row["notice_kind"],
            "operation_id": row["operation_id"],
            "request_digest": row["request_digest"],
        }
        digest_domain = "hermes.kanban.notice.v1"
    if _digest(digest_domain, expected_payload) != row["request_digest"]:
        raise ActionReceiptError("receipt_missing")
    try:
        actual_event_payload = json.loads(event["payload"])
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ActionReceiptError("receipt_missing") from exc
    if actual_event_payload != expected_event_payload:
        raise ActionReceiptError("receipt_missing")


def _receipt_from_row(row: sqlite3.Row) -> ActionReceipt:
    return ActionReceipt(
        schema_version=cast(Literal[1], int(row["schema_version"])),
        receipt_id=row["receipt_id"],
        board=row["board"],
        task_id=row["task_id"],
        operation_id=row["operation_id"],
        operation=row["operation"],
        principal_id=row["principal_id"],
        credential_id=row["credential_id"],
        action_generation=int(row["action_generation"]),
        resulting_generation=int(row["resulting_generation"]),
        park_generation=int(row["park_generation"]),
        action_class=row["action_class"],
        from_status=row["from_status"],
        to_status=row["to_status"],
        transition_digest=row["transition_digest"],
        guard_fingerprint=row["guard_fingerprint"],
        event_id=int(row["event_id"]),
        committed_at=int(row["committed_at"]),
        projection=_projection_from_json(row["projection_json"]),
    )


def lookup_action_receipt(
    conn: sqlite3.Connection,
    *,
    board: str,
    operation_id: str | None = None,
    receipt_id: str | None = None,
    caller_context: AuthenticatedCallerContext,
) -> ActionReceipt | None:
    validate_caller_context(
        conn, caller_context, board=board, operation_scope="task.transition"
    )
    if (operation_id is None) == (receipt_id is None):
        raise ActionReceiptError("invalid_request")
    if operation_id is not None:
        row = conn.execute(
            """
            SELECT r.* FROM operation_keys k
            JOIN action_receipts r ON r.receipt_id = k.record_id
            WHERE k.board = ? AND k.operation_id = ? AND k.kind = 'action'
            """,
            (board, operation_id),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM action_receipts WHERE board = ? AND receipt_id = ?",
            (board, receipt_id),
        ).fetchone()
    if row is None:
        return None
    if (
        row["principal_id"] != caller_context.principal_id
        or row["credential_id"] != caller_context.credential_id
    ):
        raise ActionReceiptError("unauthorized_principal")
    _verify_record_integrity(conn, row, kind="action")
    return _receipt_from_row(row)


def append_advisory_notice(
    conn: sqlite3.Connection,
    request: AdvisoryNoticeRequest,
    *,
    caller_context: AuthenticatedCallerContext,
) -> AdvisoryNotice:
    try:
        return _append_advisory_notice(
            conn, request, caller_context=caller_context
        )
    except sqlite3.IntegrityError as exc:
        if _operation_row(conn, request.board, request.notice_operation_id) is not None:
            raise ActionReceiptError("operation_conflict") from exc
        raise ActionReceiptError("storage_unavailable") from exc


def _append_advisory_notice(
    conn: sqlite3.Connection,
    request: AdvisoryNoticeRequest,
    *,
    caller_context: AuthenticatedCallerContext,
) -> AdvisoryNotice:
    """Append one guarded no-transition advisory with shared idempotency."""
    _schema_v1(request.schema_version)
    if request.notice_kind not in {"D-prime", "S", "Z"}:
        raise ActionReceiptError("unsupported_schema")
    for value in (request.board, request.task_id, request.notice_operation_id):
        _bounded_text(value)
    _bounded_text(request.content, max_length=100_000)
    _integer(request.expected_action_generation)
    _integer(request.park_generation)
    if not re.fullmatch(r"[0-9a-f]{64}", request.expected_guard_fingerprint):
        raise ActionReceiptError("invalid_request")
    with write_txn(conn):
        context = validate_caller_context(
            conn,
            caller_context,
            board=request.board,
            operation_scope="advisory.notice",
        )
        payload = {
            **asdict(request),
            "principal_id": context.principal_id,
            "credential_id": context.credential_id,
            "kind": "notice",
        }
        digest = _digest("hermes.kanban.notice.v1", payload)
        content_digest = _digest("hermes.kanban.notice-content.v1", request.content)
        existing = _operation_row(conn, request.board, request.notice_operation_id)
        if existing is not None:
            if (
                existing["kind"] == "notice"
                and existing["principal_id"] == context.principal_id
                and existing["credential_id"] == context.credential_id
                and existing["digest"] == digest
            ):
                notice = lookup_operation(
                    conn,
                    board=request.board,
                    operation_id=request.notice_operation_id,
                    caller_context=context,
                )
                if isinstance(notice, AdvisoryNotice):
                    return notice
                raise ActionReceiptError("receipt_missing")
            raise ActionReceiptError("operation_conflict")
        _ensure_live(conn, request.board)
        task = conn.execute(
            "SELECT action_generation, park_generation FROM tasks WHERE id = ?",
            (request.task_id,),
        ).fetchone()
        if task is None:
            raise ActionReceiptError("unknown_task")
        if int(task["action_generation"]) != request.expected_action_generation:
            raise ActionReceiptError("generation_conflict")
        if int(task["park_generation"]) != request.park_generation:
            raise ActionReceiptError("park_conflict")
        if guard_fingerprint(conn, request.task_id) != request.expected_guard_fingerprint:
            raise ActionReceiptError("generation_conflict")
        now = int(time.time())
        notice_id = "an_" + secrets.token_hex(16)
        event_payload = {
            "content_digest": content_digest,
            "notice_id": notice_id,
            "notice_kind": request.notice_kind,
            "operation_id": request.notice_operation_id,
            "request_digest": digest,
        }
        event = conn.execute(
            "INSERT INTO task_events (task_id, kind, payload, created_at) VALUES (?, ?, ?, ?)",
            (
                request.task_id,
                "authenticated_advisory",
                _canonical(event_payload),
                now,
            ),
        )
        if event.lastrowid is None:
            raise ActionReceiptError("storage_unavailable")
        event_id = int(event.lastrowid)
        projection = _projection_binding(
            conn,
            task_id=request.task_id,
            request=request.projection,
            record_id=notice_id,
            event_id=event_id,
            record_digest=digest,
        )
        conn.execute(
            """
            INSERT INTO advisory_notices (
                notice_id, schema_version, board, task_id, operation_id,
                notice_kind, principal_id, credential_id, action_generation,
                park_generation, guard_fingerprint, content, content_digest,
                request_digest, event_id, committed_at, projection_json,
                projection_comment_id
            ) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                notice_id,
                request.board,
                request.task_id,
                request.notice_operation_id,
                request.notice_kind,
                context.principal_id,
                context.credential_id,
                request.expected_action_generation,
                request.park_generation,
                request.expected_guard_fingerprint,
                request.content,
                content_digest,
                digest,
                event_id,
                now,
                _canonical(asdict(projection)) if projection else None,
                projection.comment_id if projection else None,
            ),
        )
        conn.execute(
            """
            INSERT INTO operation_keys (
                board, operation_id, kind, record_id,
                principal_id, credential_id, digest
            ) VALUES (?, ?, 'notice', ?, ?, ?, ?)
            """,
            (
                request.board,
                request.notice_operation_id,
                notice_id,
                context.principal_id,
                context.credential_id,
                digest,
            ),
        )
        return AdvisoryNotice(
            schema_version=1,
            notice_id=notice_id,
            board=request.board,
            task_id=request.task_id,
            operation_id=request.notice_operation_id,
            notice_kind=request.notice_kind,
            principal_id=context.principal_id,
            credential_id=context.credential_id,
            action_generation=request.expected_action_generation,
            park_generation=request.park_generation,
            guard_fingerprint=request.expected_guard_fingerprint,
            content=request.content,
            content_digest=content_digest,
            event_id=event_id,
            committed_at=now,
            projection=projection,
        )


def _notice_from_row(row: sqlite3.Row) -> AdvisoryNotice:
    return AdvisoryNotice(
        schema_version=cast(Literal[1], int(row["schema_version"])),
        notice_id=row["notice_id"],
        board=row["board"],
        task_id=row["task_id"],
        operation_id=row["operation_id"],
        notice_kind=row["notice_kind"],
        principal_id=row["principal_id"],
        credential_id=row["credential_id"],
        action_generation=int(row["action_generation"]),
        park_generation=int(row["park_generation"]),
        guard_fingerprint=row["guard_fingerprint"],
        content=row["content"],
        content_digest=row["content_digest"],
        event_id=int(row["event_id"]),
        committed_at=int(row["committed_at"]),
        projection=_projection_from_json(row["projection_json"]),
    )


def lookup_operation(
    conn: sqlite3.Connection,
    *,
    board: str,
    operation_id: str,
    caller_context: AuthenticatedCallerContext,
) -> ActionReceipt | AdvisoryNotice | None:
    scope = caller_context.operation_scope
    expected_kind = {
        "task.transition": "action",
        "advisory.notice": "notice",
    }.get(scope)
    if expected_kind is None:
        raise ActionReceiptError("unauthorized_principal")
    validate_caller_context(conn, caller_context, board=board, operation_scope=scope)
    key = _operation_row(conn, board, operation_id)
    if key is None:
        return None
    if key["kind"] != expected_kind:
        raise ActionReceiptError("unauthorized_principal")
    if (
        key["principal_id"] != caller_context.principal_id
        or key["credential_id"] != caller_context.credential_id
    ):
        raise ActionReceiptError("unauthorized_principal")
    if key["kind"] == "action":
        row = conn.execute(
            "SELECT * FROM action_receipts WHERE receipt_id = ?", (key["record_id"],)
        ).fetchone()
        if row is None:
            raise ActionReceiptError("receipt_missing")
        _verify_record_integrity(conn, row, kind="action")
        return _receipt_from_row(row)
    row = conn.execute(
        "SELECT * FROM advisory_notices WHERE notice_id = ?", (key["record_id"],)
    ).fetchone()
    if row is None:
        raise ActionReceiptError("receipt_missing")
    _verify_record_integrity(conn, row, kind="notice")
    return _notice_from_row(row)
