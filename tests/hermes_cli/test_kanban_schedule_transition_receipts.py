"""Receipt-backed scheduling lifecycle transitions."""

from __future__ import annotations

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


def test_compatibility_schedule_commits_transition_receipt(board):
    task_id = kb.create_task(board, title="scheduled", assignee="worker")

    assert kb.schedule_task(board, task_id, reason="wait") is True

    task = kb.get_task(board, task_id)
    assert task.status == "scheduled"
    receipt = board.execute(
        "SELECT operation_kind, outcome_code, event_id, result_tuple_hash "
        "FROM task_transition_receipts WHERE task_id = ? AND operation_kind = 'schedule'",
        (task_id,),
    ).fetchone()
    assert receipt is not None
    assert tuple(receipt[:2]) == ("schedule", "committed")
    assert receipt["event_id"] is not None
    assert len(receipt["result_tuple_hash"]) == 64


def test_schedule_transition_replays_without_duplicate_event_or_generation(board):
    task_id = kb.create_task(board, title="replay", assignee="worker")

    first = task_state.schedule_task_transition(
        board,
        task_id,
        actor_scope="operator",
        operation_id="schedule:one",
        reason="wait",
    )
    generations_after_first = tuple(board.execute(
        "SELECT action_generation, park_generation FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone())
    event_count = len(kb.list_events(board, task_id))

    replay = task_state.schedule_task_transition(
        board,
        task_id,
        actor_scope="operator",
        operation_id="schedule:one",
        reason="wait",
    )

    assert replay == first
    assert first.committed is True
    assert len(kb.list_events(board, task_id)) == event_count
    generations_after_replay = tuple(board.execute(
        "SELECT action_generation, park_generation FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone())
    assert generations_after_replay == generations_after_first


def test_schedule_transition_preserves_unknown_and_status_gates(board):
    unknown = kb.create_task(board, title="unknown", assignee="worker")
    assert kb.block_task(board, unknown, kind="needs_input", reason="unclassified")
    before = len(kb.list_events(board, unknown))

    refused_unknown = task_state.schedule_task_transition(
        board,
        unknown,
        actor_scope="operator",
        operation_id="schedule:unknown",
        reason="wait",
    )

    assert (refused_unknown.committed, refused_unknown.refusal) == (
        False,
        "block_identity_unknown",
    )
    assert kb.get_task(board, unknown).status == "blocked"
    assert len(kb.list_events(board, unknown)) == before
    assert board.execute(
        "SELECT 1 FROM task_transition_receipts WHERE operation_id = ?",
        ("schedule:unknown",),
    ).fetchone() is None

    review = kb.create_task(board, title="review", assignee="worker")
    board.execute("UPDATE tasks SET status = 'review' WHERE id = ?", (review,))
    refused_review = task_state.schedule_task_transition(
        board,
        review,
        actor_scope="operator",
        operation_id="schedule:review",
    )
    assert (refused_review.committed, refused_review.refusal) == (
        False,
        "compare_and_set",
    )
    assert kb.get_task(board, review).status == "review"


def test_schedule_transition_honors_expected_run_and_operation_collision(board):
    task_id = kb.create_task(board, title="running", assignee="worker")
    claimed = kb.claim_task(board, task_id, claimer="worker")
    assert claimed is not None
    assert claimed.current_run_id is not None
    run_id = claimed.current_run_id

    stale = task_state.schedule_task_transition(
        board,
        task_id,
        actor_scope="operator",
        operation_id="schedule:stale",
        expected_run_id=run_id + 1,
    )
    assert (stale.committed, stale.refusal) == (False, "compare_and_set")
    assert kb.get_task(board, task_id).status == "running"

    first = task_state.schedule_task_transition(
        board,
        task_id,
        actor_scope="operator",
        operation_id="schedule:run",
        reason="wait",
        expected_run_id=run_id,
    )
    collision = task_state.schedule_task_transition(
        board,
        task_id,
        actor_scope="different-operator",
        operation_id="schedule:run",
        reason="wait",
        expected_run_id=run_id,
    )

    assert first.committed is True
    assert (collision.committed, collision.refusal) == (
        False,
        "operation_id_collision",
    )
    assert kb.get_task(board, task_id).status == "scheduled"
