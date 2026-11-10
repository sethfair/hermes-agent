"""Behavior contracts for explicit operator release from Kanban triage."""

from __future__ import annotations

from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_lifecycle as kbl


@pytest.fixture
def conn(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    db_path = kb.kanban_db_path(board="default")
    kb._INITIALIZED_PATHS.discard(str(db_path.resolve()))
    kb.init_db()
    connection = kbc.connect()
    try:
        yield connection
    finally:
        connection.close()


def _triage_task(conn, *, parents=()):
    task_id = kb.create_task(conn, title="specified recovery", parents=list(parents))
    conn.execute("UPDATE tasks SET status = 'triage' WHERE id = ?", (task_id,))
    return task_id


def test_release_triage_preserves_spec_and_records_audit_event(conn):
    task_id = _triage_task(conn)
    before = kb.get_task(conn, task_id)

    ok, error = kbl.release_triage_task(
        conn, task_id, actor="coordinator", reason="clearing evidence verified",
    )

    assert ok and error is None
    after = kb.get_task(conn, task_id)
    assert after.status == "ready"
    assert (after.title, after.body) == (before.title, before.body)
    event = [e for e in kb.list_events(conn, task_id) if e.kind == "triage_released"][-1]
    assert event.payload == {
        "actor": "coordinator",
        "reason": "clearing evidence verified",
        "from_status": "triage",
    }


def test_release_triage_refuses_unfinished_parent(conn):
    parent_id = kb.create_task(conn, title="unfinished parent")
    task_id = _triage_task(conn, parents=[parent_id])

    ok, error = kbl.release_triage_task(conn, task_id, actor="coordinator")

    assert not ok
    assert error and parent_id in error
    assert kb.get_task(conn, task_id).status == "triage"


def test_release_triage_refuses_non_triage_source_status(conn):
    task_id = kb.create_task(conn, title="already runnable")

    ok, error = kbl.release_triage_task(conn, task_id, actor="coordinator")

    assert not ok
    assert error and "only applies to 'triage'" in error
    task = kb.get_task(conn, task_id)
    assert task is not None
    assert task.status == "ready"


def test_release_triage_dry_run_validates_without_mutation(conn):
    task_id = _triage_task(conn)

    ok, error = kbl.release_triage_task(
        conn, task_id, actor="coordinator", reason="preview", dry_run=True,
    )

    assert ok and error is None
    assert kb.get_task(conn, task_id).status == "triage"
    assert not [e for e in kb.list_events(conn, task_id) if e.kind == "triage_released"]