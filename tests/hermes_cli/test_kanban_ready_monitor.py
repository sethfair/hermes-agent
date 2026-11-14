"""Behavioral contracts for aged-Ready monitoring."""

import sqlite3

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc

from hermes_cli import kanban_ready_monitor as monitor


def test_ready_monitor_generation_exposes_frozen_classifier_api():
    assert monitor.READY_ENTRY_CLASSIFIERS
    assert callable(monitor.ready_generation)
    assert callable(monitor.canonical_guard_fingerprint)
    assert callable(monitor.classify_ready_state)


def _db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(kb.SCHEMA_SQL)
    kbc._migrate_add_optional_columns(conn)
    return conn


def test_schema_migration_is_additive_and_identity_is_stable():
    conn = _db()
    tables = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    assert {
        "kanban_board_identity", "kanban_monitor_scope_activation",
        "kanban_dispatch_attempts", "kanban_claim_admissions",
        "kanban_writer_faults", "kanban_controller_observations",
        "kanban_ready_tick_observations",
    } <= tables
    admission_col = {r["name"] for r in conn.execute("PRAGMA table_info(tasks)")}
    assert "dispatch_admission_id" in admission_col
    first = conn.execute(
        "SELECT board_scope_id FROM kanban_board_identity WHERE singleton=1"
    ).fetchone()[0]
    monitor.ensure_schema(conn)
    assert conn.execute(
        "SELECT board_scope_id FROM kanban_board_identity WHERE singleton=1"
    ).fetchone()[0] == first


def test_unadmitted_ready_claim_is_ignored_and_fault_is_durable():
    conn = _db()
    task_id = "task-ready"
    conn.execute(
        "INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)",
        (task_id, "Ready", "ready", 100, "p1"),
    )
    conn.execute(
        "INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
        (task_id, "created", '{"status":"ready"}', 100),
    )
    monitor.activate_scope(conn, profile="p1", state="pending")
    conn.commit()
    with kb.write_txn(conn):
        run_id = kb._claim_and_open_run(conn, task_id, "ready", "old", 200, 150)
    assert run_id is None
    assert conn.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()[0] == "ready"
    assert conn.execute("SELECT COUNT(*) FROM task_runs").fetchone()[0] == 0
    assert conn.execute(
        "SELECT diagnostic_code FROM kanban_writer_faults"
    ).fetchone()[0] == "unadmitted_ready_claim_refused"


def test_matching_admission_is_single_use_and_replay_is_refused():
    conn = _db()
    task_id = "task-admitted"
    conn.execute(
        "INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)",
        (task_id, "Ready", "ready", 100, "p1"),
    )
    scope = monitor.activate_scope(conn, profile="p1", state="active", legacy_exclusion_proof_state="proven", roster_generation="r1", roster_digest="rd")
    ctx = monitor.MonitorWriterContext(
        board_scope_id=scope["board_scope_id"], profile="p1", writer_ref="w1",
        controller_generation="g1", activation_epoch=scope["activation_epoch"],
        roster_generation="r1", roster_digest="rd", installed_revision="rev",
    )
    attempt = monitor.begin_dispatch_attempt(conn, controller=ctx, surface="standalone", now=150)
    token = monitor.arm_claim_admission(
        conn, attempt_id=attempt["id"], task_id=task_id, ready_generation=0, run_generation=0,
    )
    conn.commit()
    with kb.write_txn(conn):
        run_id = kb._claim_and_open_run(
            conn, task_id, "ready", "new", 200, 150, admission_id=token
        )
    assert run_id is not None
    assert conn.execute(
        "SELECT state FROM kanban_claim_admissions WHERE token=?", (token,)
    ).fetchone()[0] == "consumed"


def test_tick_ledger_replay_does_not_increment_or_duplicate_episode():
    conn = _db()
    task_id = "task-replay"
    conn.execute(
        "INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)",
        (task_id, "Ready", "ready", 100, "p1"),
    )
    conn.execute(
        "INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
        (task_id, "created", '{"status":"ready"}', 100),
    )
    scope = monitor.activate_scope(conn, profile="p1", state="active", legacy_exclusion_proof_state="proven", roster_generation="r1", roster_digest="rd")
    ctx = monitor.MonitorWriterContext(
        board_scope_id=scope["board_scope_id"], profile="p1", writer_ref="w1",
        controller_generation="g1", activation_epoch=scope["activation_epoch"],
        roster_generation="r1", roster_digest="rd", installed_revision="rev", tick_id="tick-1",
    )
    attempt = monitor.begin_dispatch_attempt(conn, controller=ctx, surface="standalone", now=200)
    decision = {"task_id": task_id, "decision": "eligible_missed", "min_age_seconds": 0, "min_eligible_misses": 1}
    assert monitor.record_dispatch_tick(conn, controller=ctx, attempt_id=attempt["id"], decisions=[decision], now=200)
    assert conn.execute("SELECT eligible_miss_count FROM kanban_ready_state WHERE task_id=?", (task_id,)).fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM kanban_ready_episodes").fetchone()[0] == 1
    assert monitor.record_dispatch_tick(conn, controller=ctx, attempt_id=attempt["id"], decisions=[decision], now=201) is False
    assert conn.execute("SELECT eligible_miss_count FROM kanban_ready_state WHERE task_id=?", (task_id,)).fetchone()[0] == 1
