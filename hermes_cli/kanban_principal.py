"""Board-local caller admission for authenticated Kanban producers."""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3
import time

from hermes_cli.sqlite_util import write_txn


class ActionReceiptError(RuntimeError):
    """Stable typed failure raised by the action-receipt producer."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class AuthenticatedCallerContext:
    principal_id: str
    credential_id: str
    registry_revision: int
    board: str
    operation_scope: str


def require_caller_context(
    conn: sqlite3.Connection,
    caller_context: AuthenticatedCallerContext | None,
) -> AuthenticatedCallerContext:
    """Reject a protected mutation that has no producer-admitted context."""
    del conn
    if caller_context is None:
        raise ActionReceiptError("unauthenticated_session")
    return caller_context


def protected_claim_task(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    caller_context: AuthenticatedCallerContext | None,
    board: str = "default",
):
    """Authenticated producer seam over the legacy-compatible claim callable."""
    context = validate_caller_context(
        conn,
        caller_context,
        board=board,
        operation_scope="task.transition",
    )
    del context
    from hermes_cli import kanban_db

    return kanban_db.claim_task(conn, task_id, ttl_seconds=None)


def provision_service_principal(
    conn: sqlite3.Connection,
    *,
    board: str,
    operation_scope: str,
    principal_id: str,
    credential_id: str,
    configure: bool = False,
) -> None:
    """Provision a board-local producer principal."""
    values = (board, operation_scope, principal_id, credential_id)
    if any(not value or not value.strip() for value in values):
        raise ActionReceiptError("invalid_request")
    now = int(time.time())
    with write_txn(conn):
        conn.execute(
            """
            INSERT INTO service_principals (
                board, principal_id, credential_id, operation_scope,
                enabled, registry_revision, created_at, disabled_at
            ) VALUES (?, ?, ?, ?, 1, 1, ?, NULL)
            ON CONFLICT(board, principal_id, operation_scope) DO UPDATE SET
                credential_id = excluded.credential_id,
                enabled = 1,
                registry_revision = service_principals.registry_revision + 1
            """,
            (board, principal_id, credential_id, operation_scope, now),
        )
        revision = conn.execute(
            """
            SELECT registry_revision FROM service_principals
             WHERE board = ? AND principal_id = ? AND operation_scope = ?
            """,
            (board, principal_id, operation_scope),
        ).fetchone()["registry_revision"]
        conn.execute(
            """
            INSERT INTO principal_registry_events (
                board, operation_scope, principal_id, credential_id,
                registry_revision, action, created_at
            ) VALUES (?, ?, ?, ?, ?, 'provisioned', ?)
            """,
            (board, operation_scope, principal_id, credential_id, revision, now),
        )
        if configure:
            conn.execute(
                """
                INSERT INTO configured_callers (
                    board, operation_scope, principal_id, credential_id
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(board, operation_scope) DO UPDATE SET
                    principal_id = excluded.principal_id,
                    credential_id = excluded.credential_id
                """,
                (board, operation_scope, principal_id, credential_id),
            )


def disable_service_principal(
    conn: sqlite3.Connection,
    *,
    board: str,
    principal_id: str,
) -> None:
    """Disable a provisioned producer principal."""
    now = int(time.time())
    with write_txn(conn):
        granted = conn.execute(
            """
            SELECT operation_scope, credential_id, registry_revision
              FROM service_principals
             WHERE board = ? AND principal_id = ? AND enabled = 1
            """,
            (board, principal_id),
        ).fetchall()
        updated = conn.execute(
            """
            UPDATE service_principals
               SET enabled = 0,
                   registry_revision = registry_revision + 1,
                   disabled_at = ?
             WHERE board = ? AND principal_id = ? AND enabled = 1
            """,
            (now, board, principal_id),
        )
        if updated.rowcount == 0:
            raise ActionReceiptError("revoked_principal")
        conn.executemany(
            """
            INSERT INTO principal_registry_events (
                board, operation_scope, principal_id, credential_id,
                registry_revision, action, created_at
            ) VALUES (?, ?, ?, ?, ?, 'disabled', ?)
            """,
            [
                (
                    board,
                    row["operation_scope"],
                    principal_id,
                    row["credential_id"],
                    int(row["registry_revision"]) + 1,
                    now,
                )
                for row in granted
            ],
        )


def admit_configured_caller(
    conn: sqlite3.Connection,
    *,
    board: str,
    operation_scope: str,
) -> AuthenticatedCallerContext:
    """Resolve the configured principal without caller-supplied identity."""
    row = conn.execute(
        """
        SELECT p.principal_id, p.credential_id, p.registry_revision, p.enabled
          FROM configured_callers c
          JOIN service_principals p
            ON p.board = c.board
           AND p.operation_scope = c.operation_scope
           AND p.principal_id = c.principal_id
         WHERE c.board = ? AND c.operation_scope = ?
           AND p.credential_id = c.credential_id
        """,
        (board, operation_scope),
    ).fetchone()
    if row is None:
        raise ActionReceiptError("unauthenticated_session")
    if not row["enabled"]:
        raise ActionReceiptError("revoked_principal")
    return AuthenticatedCallerContext(
        principal_id=row["principal_id"],
        credential_id=row["credential_id"],
        registry_revision=int(row["registry_revision"]),
        board=board,
        operation_scope=operation_scope,
    )


def validate_caller_context(
    conn: sqlite3.Connection,
    caller_context: AuthenticatedCallerContext | None,
    *,
    board: str,
    operation_scope: str,
) -> AuthenticatedCallerContext:
    """Validate the context against the board's configured producer."""
    context = require_caller_context(conn, caller_context)
    configured = conn.execute(
        """
        SELECT principal_id, credential_id
          FROM configured_callers
         WHERE board = ? AND operation_scope = ?
        """,
        (board, operation_scope),
    ).fetchone()
    if configured is None:
        raise ActionReceiptError("unauthenticated_session")
    if (
        context.board != board
        or context.operation_scope != operation_scope
        or context.principal_id != configured["principal_id"]
        or context.credential_id != configured["credential_id"]
    ):
        raise ActionReceiptError("unauthorized_principal")
    row = conn.execute(
        """
        SELECT enabled, registry_revision
          FROM service_principals
         WHERE board = ? AND operation_scope = ?
           AND principal_id = ? AND credential_id = ?
        """,
        (board, operation_scope, context.principal_id, context.credential_id),
    ).fetchone()
    if row is None or not row["enabled"]:
        raise ActionReceiptError("revoked_principal")
    if int(row["registry_revision"]) != context.registry_revision:
        raise ActionReceiptError("revoked_principal")
    return context
