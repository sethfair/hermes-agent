"""Additive schema for authenticated Kanban action receipts."""

import sqlite3
import time
import uuid


DRIFT_TABLES = (
    "task_events",
    "task_comments",
    "task_runs",
    "kanban_notify_subs",
)

READY_CLAIM_TRIGGER_SQL = """CREATE TRIGGER kanban_ready_claim_admission
BEFORE UPDATE OF status ON tasks
WHEN OLD.status = 'ready' AND NEW.status = 'running'
 AND EXISTS (SELECT 1 FROM kanban_monitor_scope_activation s
             WHERE s.profile = COALESCE(OLD.assignee, '') AND s.disabled_at IS NULL
               AND s.state IN ('pending','active','faulted'))
BEGIN
 INSERT INTO kanban_writer_faults(board_scope_id, profile, activation_epoch, diagnostic_code, created_at)
 SELECT s.board_scope_id, s.profile, s.activation_epoch, 'unadmitted_ready_claim_refused', strftime('%s','now')
 FROM kanban_monitor_scope_activation s
 WHERE s.profile = COALESCE(OLD.assignee, '') AND s.disabled_at IS NULL
   AND s.state IN ('pending','active','faulted')
   AND NOT EXISTS (SELECT 1 FROM kanban_claim_admissions a
                   WHERE a.token = NEW.dispatch_admission_id AND a.state = 'armed'
                     AND a.task_id = OLD.id AND a.profile = OLD.assignee
                     AND a.activation_epoch = s.activation_epoch
                     AND a.attempt_id IN (SELECT id FROM kanban_dispatch_attempts WHERE state = 'open'));
 SELECT RAISE(IGNORE)
 WHERE NOT EXISTS (SELECT 1 FROM kanban_claim_admissions a
                   JOIN kanban_dispatch_attempts d ON d.id = a.attempt_id
                   JOIN kanban_monitor_scope_activation s
                     ON s.board_scope_id = a.board_scope_id AND s.profile = a.profile
                    AND s.activation_epoch = a.activation_epoch AND s.disabled_at IS NULL
                   WHERE a.token = NEW.dispatch_admission_id AND a.state = 'armed'
                     AND d.state = 'open' AND a.task_id = OLD.id AND a.profile = OLD.assignee
                     AND d.board_scope_id = a.board_scope_id AND d.profile = a.profile
                     AND d.activation_epoch = a.activation_epoch
                     AND d.writer_ref_digest = a.writer_ref_digest
                     AND d.controller_generation = a.controller_generation
                     AND a.ready_generation = COALESCE((SELECT MAX(id) FROM task_events
                         WHERE task_id=OLD.id AND kind IN ('created','promoted','promoted_manual',
                         'unblocked','changes_requested','review_reopened','status','reclaimed',
                         'timed_out','stale','protocol_violation','rate_limited','crashed',
                         'spawn_failed','reconciled','imported')), 0)
                     AND a.run_generation = COALESCE((SELECT MAX(id) FROM task_runs WHERE task_id=OLD.id), 0));
 UPDATE kanban_claim_admissions SET state='consumed', consumed_at=strftime('%s','now')
 WHERE token = NEW.dispatch_admission_id AND state='armed';
END"""

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
