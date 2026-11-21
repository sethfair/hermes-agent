"""Receipt-backed edits of task presentation fields."""
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
        _compatibility_lifecycle_operation_id,
        _run_operation,
    )


@dataclass(frozen=True)
class EditFieldsRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    title: Optional[str]
    body: Optional[str]
    operation_kind: str = "edit_fields"
    authority_kind: str = "edit_fields"


def bind(owner) -> None:
    """Bind initialized owner primitives without duplicating storage authority."""
    globals().update({
        name: value for name, value in vars(owner).items()
        if not name.startswith("__")
    })


def apply(
    private: sqlite3.Connection,
    request: EditFieldsRequest,
) -> _AppliedTransition:
    fields: list[str] = []
    if request.title is not None:
        fields.append("title")
    if request.body is not None:
        fields.append("body")
    updated = private.execute(
        "UPDATE tasks SET "
        "title=CASE WHEN ? THEN ? ELSE title END, "
        "body=CASE WHEN ? THEN ? ELSE body END "
        "WHERE id=?",
        (
            request.title is not None,
            request.title,
            request.body is not None,
            request.body,
            request.task_id,
        ),
    )
    if updated.rowcount != 1:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="task_not_found"
        ))
    from hermes_cli import kanban_db as kb

    kb._append_event(
        private,
        request.task_id,
        "edited",
        {"fields": fields},
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _AppliedTransition(TransitionOutcome(
        True, request.operation_id, event_id, "none"
    ))


def transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    title: Optional[str] = None,
    body: Optional[str] = None,
) -> TransitionOutcome:
    """Edit title/body atomically through the protected receipt kernel."""
    if title is None and body is None:
        raise ValueError("at least one editable field is required")
    if title is not None:
        title = title.strip()
        if not title:
            raise ValueError("title cannot be empty")
    result: _KernelResult = _run_operation(
        conn,
        EditFieldsRequest(task_id, actor_scope, operation_id, title, body),
    )
    return result.outcome


def compatibility(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    title: Optional[str] = None,
    body: Optional[str] = None,
    actor_scope: str = "dashboard",
) -> bool:
    return transition(
        conn,
        task_id,
        actor_scope=actor_scope,
        operation_id=_compatibility_lifecycle_operation_id("edit-fields", task_id),
        title=title,
        body=body,
    ).committed
