"""Dispatcher boundary contracts for protected monitor scopes."""

import sqlite3
import uuid
from contextlib import contextmanager

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_dispatch as dispatch
from hermes_cli import kanban_ready_monitor as monitor


def test_protected_board_refuses_one_shot_dispatch_before_housekeeping():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(kb.SCHEMA_SQL)
    monitor.ensure_schema(conn)
    conn.execute("INSERT INTO tasks(id,title,status,created_at,assignee) VALUES('t','T','ready',1,'p1')")
    monitor.activate_scope(conn, profile="p1", state="pending")
    conn.commit()
    result = dispatch.dispatch_once(conn, spawn_fn=lambda *args: 1)
    assert result.monitor_scope_refused is True
    assert conn.execute("SELECT status FROM tasks WHERE id='t'").fetchone()[0] == "ready"
    assert conn.execute("SELECT COUNT(*) FROM task_events").fetchone()[0] == 0
    assert conn.execute("SELECT diagnostic_code FROM kanban_writer_faults ORDER BY id DESC LIMIT 1").fetchone()[0] == "missing_writer_context"


def test_protected_dispatch_claims_and_spawns_with_admission(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(kb.SCHEMA_SQL)
    monitor.ensure_schema(conn)
    task_id = "real-protected-claim"
    conn.execute("INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)",
                 (task_id, "T", "ready", 1, "p1"))
    conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
                 (task_id, "created", '{"status":"ready"}', 1))
    board = conn.execute("SELECT board_scope_id FROM kanban_board_identity").fetchone()[0]
    writer = str(uuid.uuid4())
    generation = str(uuid.uuid4())
    config = {"dispatch_profiles": ["p1"], "aged_ready": {
        "observation_enabled": True, "controller_id": writer,
        "expected_writer_scopes": {board: {"roster_generation": generation, "members": [{
            "writer_ref": writer, "profiles": ["p1"], "protocol_version": 1,
            "compatible_revisions": [monitor.COMPATIBILITY_IDENTITY]}]}}}}
    digest = monitor.parse_aged_ready_config(config)["expected_writer_scopes"][board]["roster_digest"]
    scope = monitor.activate_scope(conn, profile="p1", state="active",
                                   legacy_exclusion_proof_state="proven",
                                   roster_generation=generation, roster_digest=digest)
    ctx = monitor.MonitorWriterContext(
        board_scope_id=board, profile="p1", writer_ref=writer,
        controller_generation="g1", activation_epoch=scope["activation_epoch"],
        roster_generation=scope["roster_generation"], roster_digest=digest, installed_revision="rev",
        dispatch_profiles=("p1",),
    )
    monkeypatch.setattr("hermes_cli.config.load_config", lambda: {"kanban": config})
    monkeypatch.setattr(dispatch._kbw, "resolve_workspace", lambda task, board=None: "/tmp/kanban-test")
    monkeypatch.setattr(dispatch, "_profile_exists_fn", lambda: lambda profile: True)
    result = dispatch.dispatch_once(conn, spawn_fn=lambda task, workspace: 123,
                                    monitor_context=ctx, max_spawn=1)
    assert result.spawned and result.monitor_attempt_id
    assert conn.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()[0] == "running"
    assert conn.execute("SELECT COUNT(*) FROM kanban_claim_admissions WHERE state='consumed'").fetchone()[0] == 1


def test_unprotected_db_path_failure_keeps_dispatch_fallback(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(kb.SCHEMA_SQL)
    monitor.ensure_schema(conn)
    monkeypatch.setattr(dispatch._kb, "kanban_db_path", lambda board=None: (_ for _ in ()).throw(OSError("no path")))
    result = dispatch.dispatch_once(conn)
    assert not result.monitor_scope_refused


def test_all_partitions_are_prevalidated_while_one_board_lock_is_held(monkeypatch, tmp_path):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(kb.SCHEMA_SQL)
    monitor.ensure_schema(conn)
    monitor.activate_scope(conn, profile="p1")
    monitor.activate_scope(conn, profile="p2")
    conn.commit()
    held = False
    validations = 0

    @contextmanager
    def lock(_path):
        nonlocal held
        held = True
        yield True
        held = False

    def validate(_conn, contexts, _config):
        nonlocal validations
        validations += 1
        assert held
        assert len(contexts) == 2
        return "writer_scope_incomplete"

    monkeypatch.setattr(dispatch._kb, "kanban_db_path", lambda board=None: tmp_path / "board.db")
    monkeypatch.setattr(dispatch._kbc, "_dispatch_tick_lock", lock)
    monkeypatch.setattr(monitor, "validate_runtime_contexts", validate)
    monkeypatch.setattr("hermes_cli.config.load_config", lambda: {"kanban": {}})
    result = dispatch.dispatch_once(conn, monitor_context=(object(), object()))
    assert result.monitor_scope_refused and validations == 1 and not held
    assert conn.execute("SELECT COUNT(*) FROM kanban_writer_faults WHERE diagnostic_code='writer_scope_incomplete'").fetchone()[0] == 2


def test_one_locked_tick_shares_budget_across_valid_profile_partitions(monkeypatch, tmp_path):
    conn = sqlite3.connect(":memory:"); conn.row_factory = sqlite3.Row
    conn.executescript(kb.SCHEMA_SQL); monitor.ensure_schema(conn)
    board = conn.execute("SELECT board_scope_id FROM kanban_board_identity").fetchone()[0]
    writer = str(uuid.uuid4()); generation = str(uuid.uuid4())
    config = {"dispatch_profiles": ["p1", "p2"], "aged_ready": {"observation_enabled": True,
        "controller_id": writer, "expected_writer_scopes": {board: {"roster_generation": generation,
        "members": [{"writer_ref": writer, "profiles": ["p1", "p2"], "protocol_version": 1,
        "compatible_revisions": [monitor.COMPATIBILITY_IDENTITY]}]}}}}
    digest = monitor.parse_aged_ready_config(config)["expected_writer_scopes"][board]["roster_digest"]
    contexts = []
    for profile in ("p1", "p2"):
        conn.execute("INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)",
                     (profile, profile, "ready", 1, profile))
        conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
                     (profile, "created", '{"status":"ready"}', 1))
        scope = monitor.activate_scope(conn, profile=profile, state="active",
            legacy_exclusion_proof_state="proven", roster_generation=generation, roster_digest=digest)
        contexts.append(monitor.MonitorWriterContext(board, profile, writer, "g1",
            scope["activation_epoch"], generation, digest, "rev", dispatch_profiles=("p1", "p2")))
    conn.commit(); hooks = []
    monkeypatch.setattr("hermes_cli.config.load_config", lambda: {"kanban": config})
    monkeypatch.setattr(dispatch._kb, "kanban_db_path", lambda board=None: tmp_path / "board.db")
    monkeypatch.setattr(dispatch._kb, "_fire_dispatch_tick_hook", lambda *a, **k: hooks.append(1))
    monkeypatch.setattr(dispatch._kbw, "resolve_workspace", lambda task, board=None: str(tmp_path))
    monkeypatch.setattr(dispatch, "_profile_exists_fn", lambda: lambda profile: True)
    result = dispatch.dispatch_once(conn, monitor_context=contexts, spawn_fn=lambda task, workspace: 123,
                                    max_spawn=1)
    assert len(result.spawned) == 1 and hooks == [1]
    assert conn.execute("SELECT COUNT(*) FROM kanban_dispatch_attempts WHERE state='complete'").fetchone()[0] == 2
