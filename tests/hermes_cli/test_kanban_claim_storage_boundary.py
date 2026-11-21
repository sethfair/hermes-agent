"""Receipt-backed Ready and Review claims at the private storage boundary."""

from __future__ import annotations

import contextlib
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


def _ready(board, *, status="ready"):
    task_id = kb.create_task(board, title=f"typed {status} claim", assignee="worker")
    with kb.write_txn(board):
        board.execute("UPDATE tasks SET status=? WHERE id=?", (status, task_id))
    return task_id


def _claim(board, task_id, operation_id="claim:one", **overrides):
    values = {
        "actor_scope": "dispatcher",
        "operation_id": operation_id,
        "source_status": "ready",
        "lock": "host:worker",
        "expires": 2_000_000_000,
        "now": 1_900_000_000,
    }
    values.update(overrides)
    return task_state.claim_task_transition(board, task_id, **values)


def test_typed_ready_claim_commits_task_run_event_and_receipt_once(board, monkeypatch):
    task_id = _ready(board)
    hooks = []
    monkeypatch.setattr(kb, "_fire_task_hook", lambda *args: hooks.append(args))

    first = _claim(board, task_id)
    replay = _claim(board, task_id)

    assert first == replay
    assert first.committed is True
    assert first.run_id is not None
    assert first.event_id is not None
    assert first.task is not None and first.task.status == "running"
    assert len(first.result_tuple_hash) == 64
    assert board.execute(
        "SELECT COUNT(*) FROM task_runs WHERE task_id=?", (task_id,)
    ).fetchone()[0] == 1
    assert board.execute(
        "SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='claimed'", (task_id,)
    ).fetchone()[0] == 1
    receipt = board.execute(
        "SELECT operation_kind, event_id FROM task_transition_receipts WHERE operation_id=?",
        ("claim:one",),
    ).fetchone()
    assert tuple(receipt) == ("claim_ready", first.event_id)
    assert len(hooks) == 1


def test_claim_operation_id_collision_has_no_effect(board):
    first_task = _ready(board)
    second_task = _ready(board)
    assert _claim(board, first_task).committed is True

    collision = _claim(board, second_task)

    assert (collision.committed, collision.reason) == (False, "operation_id_collision")
    assert kb.get_task(board, second_task).status == "ready"
    assert board.execute(
        "SELECT COUNT(*) FROM task_runs WHERE task_id=?", (second_task,)
    ).fetchone()[0] == 0


def test_original_claim_receipt_wins_after_later_lifecycle_transition(board):
    task_id = _ready(board)
    first = _claim(board, task_id)
    assert kb.block_task(board, task_id, kind="capability", reason="later") is True
    event_count = len(kb.list_events(board, task_id))

    replay = _claim(board, task_id)

    assert replay == first
    assert len(kb.list_events(board, task_id)) == event_count
    current = kb.get_task(board, task_id)
    assert current is not None and current.status == "blocked"


def test_claim_receipt_failure_rolls_back_every_protected_effect(board):
    task_id = _ready(board)
    before_task = tuple(board.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone())
    before_events = board.execute("SELECT * FROM task_events WHERE task_id=?", (task_id,)).fetchall()
    board.execute(
        "CREATE TRIGGER refuse_claim_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='claim_ready' "
        "BEGIN SELECT RAISE(ABORT, 'forced claim receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced claim receipt failure"):
        _claim(board, task_id)

    assert tuple(board.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()) == before_task
    assert board.execute("SELECT * FROM task_runs WHERE task_id=?", (task_id,)).fetchall() == []
    assert board.execute("SELECT * FROM task_events WHERE task_id=?", (task_id,)).fetchall() == before_events
    assert board.execute(
        "SELECT * FROM task_transition_receipts WHERE operation_id='claim:one'"
    ).fetchall() == []


def test_claim_private_authority_cannot_write_unrelated_table(board):
    task_id = _ready(board)
    board.execute("CREATE TABLE forbidden_claim_effect (value TEXT NOT NULL)")
    board.execute(
        "CREATE TRIGGER escape_claim AFTER UPDATE ON tasks "
        "BEGIN INSERT INTO forbidden_claim_effect(value) VALUES ('escaped'); END"
    )

    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        _claim(board, task_id)

    assert kb.get_task(board, task_id).status == "ready"
    assert board.execute("SELECT * FROM forbidden_claim_effect").fetchall() == []


def test_typed_review_claim_uses_same_closed_boundary(board):
    task_id = _ready(board, status="review")

    result = _claim(
        board,
        task_id,
        operation_id="claim:review",
        source_status="review",
    )

    assert result.committed is True
    assert result.task is not None and result.task.status == "running"
    assert board.execute(
        "SELECT operation_kind FROM task_transition_receipts WHERE operation_id=?",
        ("claim:review",),
    ).fetchone()[0] == "claim_review"


def test_compatibility_claim_routes_through_receipt_boundary(board):
    task_id = _ready(board)

    claimed = kb.claim_task(board, task_id, claimer="compatibility")

    assert claimed is not None and claimed.status == "running"
    assert board.execute(
        "SELECT COUNT(*) FROM task_transition_receipts "
        "WHERE task_id=? AND operation_kind='claim_ready'", (task_id,),
    ).fetchone()[0] == 1


def test_claim_refuses_outer_transaction_before_private_open(board, monkeypatch):
    task_id = _ready(board)
    opened = False
    original = kbc._protected_connection

    @contextlib.contextmanager
    def tracked(*args, **kwargs):
        nonlocal opened
        opened = True
        with original(*args, **kwargs) as private:
            yield private

    monkeypatch.setattr(kbc, "_protected_connection", tracked)
    board.execute("BEGIN")
    with pytest.raises(RuntimeError, match="outer transaction"):
        _claim(board, task_id)
    board.execute("ROLLBACK")

    assert opened is False
    task = kb.get_task(board, task_id)
    assert task is not None and task.status == "ready"
