"""Receipt-backed triage decomposition storage boundary."""

from pathlib import Path
import sqlite3

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_graph as graph
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


def _triage(board):
    return kb.create_task(board, title="root", triage=True, assignee="specifier")


def _children():
    return [
        {"title": "first", "assignee": "worker-a"},
        {"title": "second", "assignee": "worker-b", "parents": [0]},
    ]


def test_compatibility_decompose_commits_graph_and_receipt(board):
    root = _triage(board)

    children = graph.decompose_triage_task(
        board, root, root_assignee="orchestrator", children=_children(),
        author="planner", auto_promote=False,
    )

    assert children is not None and len(children) == 2
    assert kb.parent_ids(board, children[1]) == [children[0]]
    assert set(kb.parent_ids(board, root)) == set(children)
    task = kb.get_task(board, root)
    assert task is not None and task.status == "todo"
    assert board.execute(
        "SELECT COUNT(*) FROM task_transition_receipts "
        "WHERE task_id=? AND operation_kind='decompose'", (root,),
    ).fetchone()[0] == 1


def test_typed_decompose_replays_and_collision_refuses(board):
    root = _triage(board)
    child_ids = ("t_decompose_a", "t_decompose_b")
    values = dict(
        actor_scope="test", operation_id="decompose:one",
        root_assignee="orchestrator", children=tuple(_children()),
        child_ids=child_ids, author="planner",
    )

    first = task_state.decompose_task_transition(board, root, **values)
    replay = task_state.decompose_task_transition(board, root, **values)
    assert first == replay
    assert first.committed is True and first.child_ids == child_ids

    other = _triage(board)
    collision = task_state.decompose_task_transition(
        board, other, **values | {"child_ids": ("t_other_a", "t_other_b")}
    )
    assert collision.committed is False
    assert collision.refusal == "operation_id_collision"
    assert kb.parent_ids(board, other) == []


def test_decompose_receipt_failure_rolls_back_entire_graph(board):
    root = _triage(board)
    before = tuple(board.execute("SELECT * FROM tasks WHERE id=?", (root,)).fetchone())
    before_events = board.execute(
        "SELECT * FROM task_events WHERE task_id=?", (root,)
    ).fetchall()
    board.execute(
        "CREATE TRIGGER refuse_decompose_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='decompose' "
        "BEGIN SELECT RAISE(ABORT, 'forced decompose receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced decompose receipt failure"):
        task_state.decompose_task_transition(
            board, root, actor_scope="test", operation_id="decompose:rollback",
            root_assignee="orchestrator", children=tuple(_children()),
            child_ids=("t_rollback_a", "t_rollback_b"), author="planner",
        )

    assert tuple(board.execute("SELECT * FROM tasks WHERE id=?", (root,)).fetchone()) == before
    assert kb.get_task(board, "t_rollback_a") is None
    assert kb.get_task(board, "t_rollback_b") is None
    assert board.execute(
        "SELECT * FROM task_events WHERE task_id=?", (root,)
    ).fetchall() == before_events


def test_decompose_refuses_outer_transaction(board):
    root = _triage(board)
    board.execute("BEGIN")
    try:
        with pytest.raises(RuntimeError, match="refuses an outer transaction"):
            task_state.decompose_task_transition(
                board, root, actor_scope="test", operation_id="decompose:outer",
                root_assignee=None, children=tuple(_children()),
                child_ids=("t_outer_a", "t_outer_b"), author=None,
            )
    finally:
        board.rollback()
    assert kb.parent_ids(board, root) == []
