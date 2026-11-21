"""Authenticated action composition for the task-state storage boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
import sqlite3
from typing import Any

_STATE = None


def bind(state) -> None:
    global _STATE
    _STATE = state


def _state():
    if _STATE is None:
        raise RuntimeError("authenticated action storage is not bound")
    return _STATE


@dataclass(frozen=True)
class AuthenticatedActionRequest:
    task_id: str
    actor_scope: str
    operation_id: str
    action_request: Any = field(compare=False, repr=False)
    caller_context: Any = field(compare=False, repr=False)
    operation_kind: str = "authenticated_action"
    authority_kind: str = "authenticated_action"


def request_hash(request: AuthenticatedActionRequest) -> str:
    from hermes_cli import kanban_db_receipts as receipts
    from hermes_cli.kanban_principal import AuthenticatedCallerContext

    if not isinstance(request.caller_context, AuthenticatedCallerContext):
        return _state()._canonical_hash({
            "task_id": request.task_id,
            "operation_kind": request.operation_kind,
            "operation_id": request.operation_id,
            "caller": None,
        })
    payload = receipts._action_payload(request.action_request, request.caller_context)
    return receipts._digest("hermes.kanban.action.v1", payload)


def apply(
    private: sqlite3.Connection,
    request: AuthenticatedActionRequest,
):
    from hermes_cli import kanban_db_receipts as receipts

    receipt = receipts._commit_action_in_transaction(
        private,
        request.action_request,
        caller_context=request.caller_context,
    )
    state = _state()
    return state._AppliedTransition(
        state.TransitionOutcome(True, request.operation_id, receipt.event_id)
    )


def transition(
    conn: sqlite3.Connection,
    action_request: Any,
    *,
    caller_context: Any,
):
    principal = getattr(caller_context, "principal_id", "unauthenticated")
    request = AuthenticatedActionRequest(
        task_id=action_request.task_id,
        actor_scope=f"authenticated:{principal}",
        operation_id=action_request.operation_id,
        action_request=action_request,
        caller_context=caller_context,
    )
    return _state()._run_operation(conn, request).outcome
