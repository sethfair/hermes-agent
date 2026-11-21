"""Receipt-backed unblock and manual-promotion lifecycle transitions."""

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


def _known_block(conn, task_id: str) -> None:
    assert task_state.block_task_transition(
        conn,
        task_id,
        actor_scope="test",
        operation_id=f"block:{task_id}",
        kind="needs_input",
        subject="owner/decision",
        reason="waiting",
    ).committed


def test_unblock_transition_replays_one_committed_event_and_receipt(board):
    task_id = kb.create_task(board, title="blocked", assignee="worker")
    _known_block(board, task_id)

    first = task_state.unblock_task_transition(
        board,
        task_id,
        actor_scope="operator",
        operation_id="unblock:one",
    )
    replay = task_state.unblock_task_transition(
        board,
        task_id,
        actor_scope="operator",
        operation_id="unblock:one",
    )

    assert first == replay
    assert first.committed is True
    assert kb.get_task(board, task_id).status == "ready"
    assert len([event for event in kb.list_events(board, task_id) if event.kind == "unblocked"]) == 1
    receipt = board.execute(
        "SELECT operation_kind, outcome_code FROM task_transition_receipts WHERE operation_id = ?",
        ("unblock:one",),
    ).fetchone()
    assert tuple(receipt) == ("unblock", "committed")


def test_unblock_unknown_refuses_before_event_or_receipt(board):
    task_id = kb.create_task(board, title="unknown", assignee="worker")
    assert kb.block_task(board, task_id, kind="needs_input", reason="unclassified")
    before = len(kb.list_events(board, task_id))

    outcome = task_state.unblock_task_transition(
        board,
        task_id,
        actor_scope="operator",
        operation_id="unblock:unknown",
    )

    assert (outcome.committed, outcome.refusal) == (False, "block_identity_unknown")
    assert kb.get_task(board, task_id).status == "blocked"
    assert len(kb.list_events(board, task_id)) == before
    assert board.execute(
        "SELECT 1 FROM task_transition_receipts WHERE operation_id = ?",
        ("unblock:unknown",),
    ).fetchone() is None


def test_compatibility_unblock_generates_receipt(board):
    task_id = kb.create_task(board, title="blocked", assignee="worker")
    _known_block(board, task_id)

    assert kb.unblock_task(board, task_id) is True
    receipt = board.execute(
        "SELECT operation_kind FROM task_transition_receipts "
        "WHERE task_id = ? AND operation_kind = 'unblock'",
        (task_id,),
    ).fetchone()
    assert receipt[0] == "unblock"


def test_promote_transition_replays_and_preserves_parent_gate(board):
    parent = kb.create_task(board, title="parent", assignee="worker")
    child = kb.create_task(board, title="child", assignee="worker", parents=[parent])

    refused = task_state.promote_task_transition(
        board,
        child,
        actor_scope="operator",
        operation_id="promote:gated",
        reason="recover",
    )
    assert refused.committed is False
    assert parent in (refused.refusal or "")
    assert kb.get_task(board, child).status == "todo"

    board.execute("UPDATE tasks SET status = 'done' WHERE id = ?", (parent,))
    first = task_state.promote_task_transition(
        board,
        child,
        actor_scope="operator",
        operation_id="promote:one",
        reason="recover",
    )
    replay = task_state.promote_task_transition(
        board,
        child,
        actor_scope="operator",
        operation_id="promote:one",
        reason="recover",
    )
    assert first == replay
    assert first.committed is True
    assert kb.get_task(board, child).status == "ready"
    assert len([event for event in kb.list_events(board, child) if event.kind == "promoted_manual"]) == 1


def test_promote_dry_run_validates_without_receipt_or_effect(board):
    task_id = kb.create_task(board, title="todo", assignee="worker", triage=True)
    board.execute("UPDATE tasks SET status = 'todo' WHERE id = ?", (task_id,))

    assert kb.promote_task(board, task_id, actor="operator", dry_run=True) == (True, None)
    assert kb.get_task(board, task_id).status == "todo"
    assert board.execute(
        "SELECT 1 FROM task_transition_receipts WHERE task_id = ? AND operation_kind = 'promote'",
        (task_id,),
    ).fetchone() is None
