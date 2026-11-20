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


@pytest.mark.parametrize("cached_statements", [None, 0, 1, 128])
def test_ordinary_facade_denies_protected_writes_across_statement_caches(
    home, cached_statements
):
    path = kb.kanban_db_path()
    with kbc.connect_closing() as legacy:
        task_id = _ready_task(legacy, title=f"cache {cached_statements}")

    kwargs = {} if cached_statements is None else {"cached_statements": cached_statements}
    with kbc._ordinary_connection(path, **kwargs) as conn:
        attempts = (
            lambda: conn.execute("UPDATE tasks SET status='done' WHERE id=?", (task_id,)),
            lambda: conn.cursor().execute(
                "UPDATE tasks SET status='done' WHERE id=?", (task_id,)
            ),
            lambda: conn.executemany(
                "UPDATE tasks SET status='done' WHERE id=?", [(task_id,)]
            ),
            lambda: conn.cursor().executemany(
                "UPDATE tasks SET status='done' WHERE id=?", [(task_id,)]
            ),
        )
        for attempt in attempts:
            with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                attempt()
        assert conn.execute(kbc.OrdinaryRead.TASK_STATUS, (task_id,)).fetchone()[0] == "ready"


@pytest.mark.parametrize(
    "statement",
    [
        "BEGIN",
        "bEgIn ImMeDiAtE",
        "/* boundary */ BEGIN EXCLUSIVE",
        "COMMIT",
        "-- boundary\nROLLBACK",
        "SAVEPOINT nested",
        "RELEASE nested",
        "ROLLBACK TO nested",
        "ATTACH DATABASE ':memory:' AS escaped",
        "DETACH DATABASE escaped",
        "CREATE TABLE escaped(value TEXT)",
        "PRAGMA writable_schema=ON",
    ],
)
def test_ordinary_facade_denies_transaction_schema_and_attachment_control(home, statement):
    with kbc._ordinary_connection(kb.kanban_db_path()) as conn:
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            conn.execute(statement)


def test_ordinary_facade_and_results_hide_physical_control_surfaces(home):
    with kbc._ordinary_connection(kb.kanban_db_path()) as conn:
        result = conn.execute(kbc.OrdinaryRead.ONE)
        cursor = conn.cursor()

        assert result.fetchone()["value"] == 1
        assert not hasattr(conn, "connection")
        assert not hasattr(result, "connection")
        assert not hasattr(cursor, "connection")
        for hidden in (
            "commit",
            "rollback",
            "backup",
            "executescript",
            "set_authorizer",
            "enable_load_extension",
            "load_extension",
        ):
            assert not hasattr(conn, hidden)
            assert not hasattr(result, hidden)
            assert not hasattr(cursor, hidden)


def test_ordinary_facade_returns_safe_iterable_results(home):
    with kbc.KanbanConnection(kb.kanban_db_path()) as conn:
        result = conn.execute(kbc.OrdinaryRead.ORDERED_VALUES)
        assert [row["value"] for row in result] == [1, 2]
        assert result.description[0][0] == "value"


def test_connect_remains_legacy_until_global_activation(home):
    conn = kbc.connect()
    try:
        assert isinstance(conn, sqlite3.Connection)
        conn.execute("BEGIN")
        conn.execute("ROLLBACK")
    finally:
        conn.close()


def test_each_private_factory_open_gets_a_fresh_physical_connection(home, monkeypatch):
    opened = []
    original = kbc._sqlite_connect

    def tracked(*args, **kwargs):
        physical = original(*args, **kwargs)
        opened.append(physical)
        return physical

    monkeypatch.setattr(kbc, "_sqlite_connect", tracked)
    path = kb.kanban_db_path()
    with kbc._protected_connection(path, operation="block"):
        pass
    with kbc._protected_connection(path, operation="block"):
        pass

    assert len(opened) == 2
    assert opened[0] is not opened[1]
    for physical in opened:
        with pytest.raises(sqlite3.ProgrammingError):
            physical.execute("SELECT 1")
