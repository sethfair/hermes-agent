"""Receipt-backed task creation storage boundary."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_task_state as task_state


@pytest.fixture
def board(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    with kbc.connect_closing() as conn:
        yield conn


def _create(board, task_id="t_typed_create", operation_id="create:one", **overrides):
    values = {
        "actor_scope": "test",
        "operation_id": operation_id,
        "title": "typed create",
        "body": None,
        "assignee": "worker",
        "created_by": "test",
        "created_at": 1_900_000_000,
        "workspace_kind": "scratch",
        "workspace_path": None,
        "branch_name": None,
        "project_id": None,
        "tenant": None,
        "priority": 0,
        "parents": (),
        "triage": False,
        "initial_status": "running",
        "idempotency_key": None,
        "max_runtime_seconds": None,
        "skills_json": None,
        "max_retries": None,
        "model_override": None,
        "provider_override": None,
        "reasoning_effort": None,
        "goal_mode": False,
        "goal_max_turns": None,
        "session_id": None,
        "creator_task_id": None,
        "completion_contract": "local-only",
    }
    values.update(overrides)
    return task_state.create_task_transition(board, task_id, **values)


def test_typed_create_commits_task_event_and_receipt_once(board):
    first = _create(board)
    replay = _create(board)

    assert first == replay
    assert first.committed is True and first.task_id == "t_typed_create"
    task = kb.get_task(board, first.task_id)
    assert task is not None and task.status == "ready"
    assert board.execute(
        "SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='created'",
        (first.task_id,),
    ).fetchone()[0] == 1
    assert board.execute(
        "SELECT operation_kind FROM task_transition_receipts WHERE operation_id=?",
        ("create:one",),
    ).fetchone()[0] == "create"


def test_create_receipt_failure_rolls_back_task_links_and_events(board):
    parent = kb.create_task(board, title="parent")
    board.execute(
        "CREATE TRIGGER refuse_create_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='create' "
        "BEGIN SELECT RAISE(ABORT, 'forced create receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced create receipt failure"):
        _create(board, task_id="t_create_rollback", parents=(parent,))

    assert kb.get_task(board, "t_create_rollback") is None
    assert board.execute(
        "SELECT * FROM task_links WHERE child_id='t_create_rollback'"
    ).fetchall() == []
    assert board.execute(
        "SELECT * FROM task_events WHERE task_id='t_create_rollback'"
    ).fetchall() == []


def test_create_operation_id_collision_and_outer_transaction_refuse_without_mutation(board):
    _create(board, task_id="t_create_first", operation_id="create:collision")

    collision = _create(
        board, task_id="t_create_second", operation_id="create:collision"
    )
    assert collision.committed is False
    assert collision.refusal == "operation_id_collision"
    assert kb.get_task(board, "t_create_second") is None

    board.execute("BEGIN")
    try:
        with pytest.raises(RuntimeError, match="refuses an outer transaction"):
            _create(board, task_id="t_create_outer", operation_id="create:outer")
    finally:
        board.rollback()
    assert kb.get_task(board, "t_create_outer") is None


def test_compatibility_create_uses_closed_receipt_operation(board):
    parent = kb.create_task(board, title="open parent", tenant="tenant-a")

    child = kb.create_task(board, title="compat child", parents=[parent])

    task = kb.get_task(board, child)
    assert task is not None
    assert task.status == "todo" and task.tenant == "tenant-a"
    assert kb.parent_ids(board, child) == [parent]
    assert board.execute(
        "SELECT COUNT(*) FROM task_transition_receipts "
        "WHERE task_id=? AND operation_kind='create'", (child,),
    ).fetchone()[0] == 1
