"""Dispatcher boundary contracts for protected monitor scopes."""

import sqlite3

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
