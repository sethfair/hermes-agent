"""Additive schema for authenticated Kanban action receipts."""

ACTION_RECEIPT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS service_principals (
    board TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    operation_scope TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    registry_revision INTEGER NOT NULL DEFAULT 1,
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
    schema_version INTEGER NOT NULL,
    board TEXT NOT NULL,
    task_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    action_generation INTEGER NOT NULL,
    resulting_generation INTEGER NOT NULL,
    park_generation INTEGER NOT NULL,
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
    schema_version INTEGER NOT NULL,
    board TEXT NOT NULL,
    task_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    notice_kind TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    action_generation INTEGER NOT NULL,
    park_generation INTEGER NOT NULL,
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
