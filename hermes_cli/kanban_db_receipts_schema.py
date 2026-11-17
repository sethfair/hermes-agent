"""Additive schema for authenticated Kanban action receipts."""

import sqlite3
import time
import uuid

ACTION_RECEIPT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS kanban_board_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    board TEXT NOT NULL UNIQUE,
    board_scope_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    identity_version INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS service_principals (
    board TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    operation_scope TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    registry_revision INTEGER NOT NULL DEFAULT 1 CHECK (registry_revision > 0),
    created_at INTEGER NOT NULL,
    disabled_at INTEGER,
    PRIMARY KEY (board, principal_id, operation_scope)
);
CREATE TABLE IF NOT EXISTS configured_callers (
    board TEXT NOT NULL,
    operation_scope TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    PRIMARY KEY (board, operation_scope)
);
CREATE TABLE IF NOT EXISTS principal_registry_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    board TEXT NOT NULL,
    operation_scope TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    registry_revision INTEGER NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('provisioned', 'disabled')),
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS kanban_capabilities (
    board TEXT NOT NULL,
    capability TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'off' CHECK (mode IN ('off', 'compatible', 'live')),
    required_consumer_version TEXT,
    cutover_evidence TEXT,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (board, capability)
);
CREATE TABLE IF NOT EXISTS kanban_capability_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    board TEXT NOT NULL,
    capability TEXT NOT NULL,
    previous_mode TEXT NOT NULL,
    mode TEXT NOT NULL,
    required_consumer_version TEXT,
    cutover_evidence TEXT,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS operation_keys (
    board TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('action', 'notice')),
    record_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    digest TEXT NOT NULL,
    PRIMARY KEY (board, operation_id)
);
CREATE TABLE IF NOT EXISTS action_receipts (
    receipt_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    board TEXT NOT NULL,
    task_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    action_generation INTEGER NOT NULL CHECK (action_generation >= 0),
    resulting_generation INTEGER NOT NULL CHECK (resulting_generation > action_generation),
    park_generation INTEGER NOT NULL CHECK (park_generation >= 0),
    action_class TEXT NOT NULL,
    from_status TEXT NOT NULL,
    to_status TEXT NOT NULL,
    decision_revision INTEGER,
    transition_json TEXT NOT NULL,
    transition_digest TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    guard_fingerprint TEXT NOT NULL,
    outcome TEXT NOT NULL,
    event_id INTEGER NOT NULL,
    committed_at INTEGER NOT NULL,
    projection_json TEXT,
    projection_comment_id INTEGER,
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE RESTRICT,
    FOREIGN KEY (event_id) REFERENCES task_events(id) ON DELETE RESTRICT,
    FOREIGN KEY (projection_comment_id) REFERENCES task_comments(id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS advisory_notices (
    notice_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    board TEXT NOT NULL,
    task_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    notice_kind TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    action_generation INTEGER NOT NULL CHECK (action_generation >= 0),
    park_generation INTEGER NOT NULL CHECK (park_generation >= 0),
    guard_fingerprint TEXT NOT NULL,
    content TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    event_id INTEGER NOT NULL,
    committed_at INTEGER NOT NULL,
    projection_json TEXT,
    projection_comment_id INTEGER,
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE RESTRICT,
    FOREIGN KEY (event_id) REFERENCES task_events(id) ON DELETE RESTRICT,
    FOREIGN KEY (projection_comment_id) REFERENCES task_comments(id) ON DELETE RESTRICT
);
"""


def ensure_board_identity_schema(
    conn: sqlite3.Connection, *, identity_board: str = "default"
) -> None:
    """Atomically upgrade and seal the receipt identity row."""
    rows = conn.execute("PRAGMA table_info(kanban_board_identity)").fetchall()
    if not rows:
        return

    savepoint = "board_identity_schema_upgrade"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        columns = {
            row["name"] if isinstance(row, sqlite3.Row) else row[1] for row in rows
        }
        for name, definition in (
            ("board", "TEXT"),
            ("board_scope_id", "TEXT"),
            ("created_at", "INTEGER"),
            ("identity_version", "INTEGER"),
        ):
            if name not in columns:
                conn.execute(
                    f"ALTER TABLE kanban_board_identity ADD COLUMN {name} {definition}"
                )

        row = conn.execute(
            "SELECT board, board_scope_id, created_at, identity_version "
            "FROM kanban_board_identity WHERE singleton = 1"
        ).fetchone()
        if row is not None:
            values = tuple(row)
            needs_backfill = any(value is None for value in values)
            if needs_backfill:
                conn.execute(
                    "DROP TRIGGER IF EXISTS kanban_board_identity_immutable_update"
                )
                now = int(time.time())
                conn.execute(
                    "UPDATE kanban_board_identity "
                    "SET board=COALESCE(board, ?), "
                    "board_scope_id=COALESCE(board_scope_id, ?), "
                    "created_at=COALESCE(created_at, ?), "
                    "identity_version=COALESCE(identity_version, 1) "
                    "WHERE singleton=1",
                    (identity_board, str(uuid.uuid4()), now),
                )

        conn.execute(
            "CREATE TRIGGER IF NOT EXISTS kanban_board_identity_immutable_update "
            "BEFORE UPDATE ON kanban_board_identity "
            "BEGIN SELECT RAISE(ABORT, 'immutable_board_identity'); END"
        )
        conn.execute(
            "CREATE TRIGGER IF NOT EXISTS kanban_board_identity_immutable_delete "
            "BEFORE DELETE ON kanban_board_identity "
            "BEGIN SELECT RAISE(ABORT, 'immutable_board_identity'); END"
        )
    except BaseException:
        conn.execute(f"ROLLBACK TO {savepoint}")
        conn.execute(f"RELEASE {savepoint}")
        raise
    conn.execute(f"RELEASE {savepoint}")
