"""Receipt-backed graph and automatic readiness storage operations."""

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


def test_typed_link_commits_edge_events_demotion_and_receipt_once(board):
    parent = kb.create_task(board, title="open parent")
    child = kb.create_task(board, title="ready child")

    first = task_state.link_tasks_transition(
        board,
        parent,
        child,
        actor_scope="test",
        operation_id="graph:link",
    )
    replay = task_state.link_tasks_transition(
        board,
        parent,
        child,
        actor_scope="test",
        operation_id="graph:link",
    )

    assert first == replay
    assert first.committed is True and first.gated is True
    assert kb.parent_ids(board, child) == [parent]
    task = kb.get_task(board, child)
    assert task is not None and task.status == "todo"
    assert board.execute(
        "SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='linked'", (child,)
    ).fetchone()[0] == 1
    assert board.execute(
        "SELECT operation_kind FROM task_transition_receipts WHERE operation_id=?",
        ("graph:link",),
    ).fetchone()[0] == "link"


def test_link_receipt_failure_rolls_back_edge_status_and_events(board):
    parent = kb.create_task(board, title="parent")
    child = kb.create_task(board, title="child")
    before = tuple(board.execute("SELECT * FROM tasks WHERE id=?", (child,)).fetchone())
    before_events = board.execute("SELECT * FROM task_events WHERE task_id=?", (child,)).fetchall()
    board.execute(
        "CREATE TRIGGER refuse_link_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='link' "
        "BEGIN SELECT RAISE(ABORT, 'forced link receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced link receipt failure"):
        task_state.link_tasks_transition(
            board,
            parent,
            child,
            actor_scope="test",
            operation_id="graph:rollback",
        )

    assert kb.parent_ids(board, child) == []
    assert tuple(board.execute("SELECT * FROM tasks WHERE id=?", (child,)).fetchone()) == before
    assert board.execute("SELECT * FROM task_events WHERE task_id=?", (child,)).fetchall() == before_events


def test_graph_operation_id_collision_and_outer_transaction_refuse_without_mutation(board):
    parent = kb.create_task(board, title="parent")
    first_child = kb.create_task(board, title="first child")
    second_child = kb.create_task(board, title="second child")
    task_state.link_tasks_transition(
        board, parent, first_child, actor_scope="test", operation_id="graph:collision"
    )

    collision = task_state.link_tasks_transition(
        board, parent, second_child, actor_scope="test", operation_id="graph:collision"
    )
    assert collision.committed is False
    assert collision.refusal == "operation_id_collision"
    assert kb.parent_ids(board, second_child) == []

    board.execute("BEGIN")
    try:
        with pytest.raises(RuntimeError, match="refuses an outer transaction"):
            task_state.link_tasks_transition(
                board, parent, second_child,
                actor_scope="test", operation_id="graph:outer",
            )
    finally:
        board.rollback()
    assert kb.parent_ids(board, second_child) == []


def test_unlink_then_recompute_promotes_with_separate_receipts(board):
    parent = kb.create_task(board, title="parent")
    child = kb.create_task(board, title="child", parents=[parent])
    task = kb.get_task(board, child)
    assert task is not None and task.status == "todo"

    outcome = task_state.unlink_tasks_transition(
        board,
        parent,
        child,
        actor_scope="test",
        operation_id="graph:unlink",
    )
    promoted = task_state.recompute_ready_transition(
        board,
        actor_scope="test",
        operation_id="readiness:one",
    )

    assert outcome.committed is True and outcome.removed is True
    assert promoted == 1
    assert kb.get_task(board, child).status == "ready"
    kinds = {
        row[0] for row in board.execute(
            "SELECT operation_kind FROM task_transition_receipts WHERE task_id=?", (child,)
        ).fetchall()
    }
    assert {"unlink", "recompute_ready"} <= kinds


def test_readiness_receipt_failure_rolls_back_promotion_and_event(board):
    parent = kb.create_task(board, title="parent")
    child = kb.create_task(board, title="child", parents=[parent])
    board.execute("UPDATE tasks SET status='done' WHERE id=?", (parent,))
    before_events = board.execute(
        "SELECT * FROM task_events WHERE task_id=?", (child,)
    ).fetchall()
    board.execute(
        "CREATE TRIGGER refuse_readiness_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='recompute_ready' "
        "BEGIN SELECT RAISE(ABORT, 'forced readiness receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced readiness receipt failure"):
        task_state.recompute_ready_transition(
            board, actor_scope="test", operation_id="readiness:rollback"
        )

    task = kb.get_task(board, child)
    assert task is not None and task.status == "todo"
    assert board.execute(
        "SELECT * FROM task_events WHERE task_id=?", (child,)
    ).fetchall() == before_events


def test_compatibility_link_unlink_and_recompute_use_closed_operations(board):
    parent = kb.create_task(board, title="compat parent")
    child = kb.create_task(board, title="compat child")

    assert kb.link_tasks(board, parent, child) is True
    assert kb.unlink_tasks(board, parent, child) is True
    assert kb.get_task(board, child).status == "ready"

    kinds = {
        row[0] for row in board.execute(
            "SELECT operation_kind FROM task_transition_receipts WHERE task_id=?", (child,)
        ).fetchall()
    }
    assert {"link", "unlink", "recompute_ready"} <= kinds
