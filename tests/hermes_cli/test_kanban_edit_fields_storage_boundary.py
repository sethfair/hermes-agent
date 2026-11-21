"""Receipt-backed task title/body edit storage operation."""

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


def _edit(
    board, task_id, *, operation_id="edit:one", title="after", body: str | None = "new"
):
    return task_state.edit_task_fields_transition(
        board,
        task_id,
        actor_scope="test",
        operation_id=operation_id,
        title=title,
        body=body,
    )


def test_edit_fields_replays_without_duplicate_event(board):
    task_id = kb.create_task(board, title="before", body="old")

    first = _edit(board, task_id)
    replay = _edit(board, task_id)

    assert first == replay
    assert first.committed is True
    task = kb.get_task(board, task_id)
    assert task is not None
    assert (task.title, task.body) == ("after", "new")
    assert board.execute(
        "SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='edited'",
        (task_id,),
    ).fetchone()[0] == 1


def test_edit_fields_operation_id_collision_has_no_effect(board):
    first_task = kb.create_task(board, title="first")
    second_task = kb.create_task(board, title="second")
    assert _edit(board, first_task, operation_id="edit:collision").committed is True

    collision = _edit(
        board,
        second_task,
        operation_id="edit:collision",
        title="must not land",
        body=None,
    )

    assert collision.committed is False
    assert collision.refusal == "operation_id_collision"
    second = kb.get_task(board, second_task)
    assert second is not None and second.title == "second"


def test_edit_fields_receipt_failure_rolls_back_task_and_event(board):
    task_id = kb.create_task(board, title="before", body="old")
    before_events = board.execute(
        "SELECT * FROM task_events WHERE task_id=?", (task_id,)
    ).fetchall()
    board.execute(
        "CREATE TRIGGER refuse_edit_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='edit_fields' "
        "BEGIN SELECT RAISE(ABORT, 'forced edit receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced edit receipt failure"):
        _edit(board, task_id, operation_id="edit:rollback")

    task = kb.get_task(board, task_id)
    assert task is not None
    assert (task.title, task.body) == ("before", "old")
    assert board.execute(
        "SELECT * FROM task_events WHERE task_id=?", (task_id,)
    ).fetchall() == before_events
