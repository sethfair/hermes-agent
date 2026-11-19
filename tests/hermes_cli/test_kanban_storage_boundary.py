"""Behavior contracts for the private Kanban task-state storage boundary."""

from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli.kanban_db_task_state import block_task_transition


@pytest.fixture
def home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()


def _ready_task(conn, title="storage boundary"):
    task_id = kb.create_task(conn, title=title, assignee="worker")
    with kb.write_txn(conn):
        conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (task_id,))
    return task_id


def _block(conn, task_id, operation_id="storage:block"):
    return block_task_transition(
        conn,
        task_id,
        actor_scope="test",
        operation_id=operation_id,
        kind="capability",
        subject="missing-access",
        reason="waiting",
    )


def test_trigger_cannot_widen_private_authorizer(home):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn)
        conn.execute("CREATE TABLE forbidden_effect (value TEXT NOT NULL)")
        conn.execute(
            "CREATE TRIGGER forbidden_task_effect AFTER UPDATE ON tasks "
            "BEGIN INSERT INTO forbidden_effect(value) VALUES ('escaped'); END"
        )

        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            _block(conn, task_id)

        assert kb.get_task(conn, task_id).status == "ready"
        assert conn.execute("SELECT * FROM forbidden_effect").fetchall() == []
        assert conn.execute(
            "SELECT * FROM task_transition_receipts WHERE operation_id='storage:block'"
        ).fetchall() == []


def test_outer_transaction_is_refused_before_private_open(home, monkeypatch):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn)
        conn.execute("BEGIN")
        opened = False
        original = kbc._protected_connection

        @contextlib.contextmanager
        def tracked(*args, **kwargs):
            nonlocal opened
            opened = True
            with original(*args, **kwargs) as private:
                yield private

        monkeypatch.setattr(kbc, "_protected_connection", tracked)
        with pytest.raises(RuntimeError, match="outer transaction"):
            _block(conn, task_id)
        assert opened is False
        conn.execute("ROLLBACK")
        assert kb.get_task(conn, task_id).status == "ready"


def test_private_connection_closes_before_hook_delivery(home, monkeypatch):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn)
        closed = False
        original = kbc._protected_connection

        @contextlib.contextmanager
        def tracked(*args, **kwargs):
            nonlocal closed
            with original(*args, **kwargs) as private:
                yield private
            with pytest.raises(sqlite3.ProgrammingError):
                private.execute("SELECT 1")
            closed = True

        def hook(*_args, **_kwargs):
            assert closed is True

        monkeypatch.setattr(kbc, "_protected_connection", tracked)
        monkeypatch.setattr(kb, "_fire_task_hook", hook)

        assert _block(conn, task_id).committed is True


def test_receipt_failure_rolls_back_task_run_and_events(home):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn)
        before_task = tuple(conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone())
        before_runs = conn.execute("SELECT * FROM task_runs WHERE task_id=?", (task_id,)).fetchall()
        before_events = conn.execute("SELECT * FROM task_events WHERE task_id=?", (task_id,)).fetchall()
        conn.execute(
            "CREATE TRIGGER refuse_transition_receipt BEFORE INSERT ON task_transition_receipts "
            "BEGIN SELECT RAISE(ABORT, 'forced receipt failure'); END"
        )

        with pytest.raises(sqlite3.IntegrityError, match="forced receipt failure"):
            _block(conn, task_id)

        assert tuple(conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()) == before_task
        assert conn.execute("SELECT * FROM task_runs WHERE task_id=?", (task_id,)).fetchall() == before_runs
        assert conn.execute("SELECT * FROM task_events WHERE task_id=?", (task_id,)).fetchall() == before_events
        assert conn.execute(
            "SELECT * FROM task_transition_receipts WHERE operation_id='storage:block'"
        ).fetchall() == []
