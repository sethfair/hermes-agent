"""Storage-boundary contracts for completion and same-card review lifecycle."""

from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_task_state as task_state


@pytest.fixture
def board(tmp_path: Path):
    db_path = tmp_path / "kanban.db"
    conn = kbc.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def _receipt(conn, operation_id: str):
    return conn.execute(
        "SELECT operation_kind, event_id FROM task_transition_receipts WHERE operation_id=?",
        (operation_id,),
    ).fetchone()


def test_complete_transition_replays_without_duplicate_run_or_event(board) -> None:
    task_id = kb.create_task(board, title="complete", assignee="builder")
    claimed = kb.claim_task(board, task_id)
    assert claimed is not None

    first = task_state.complete_task_transition(
        board,
        task_id,
        actor_scope="builder",
        operation_id="complete:1",
        summary="verified",
        metadata={"commit": "abc"},
        expected_run_id=claimed.current_run_id,
    )
    replay = task_state.complete_task_transition(
        board,
        task_id,
        actor_scope="builder",
        operation_id="complete:1",
        summary="verified",
        metadata={"commit": "abc"},
        expected_run_id=claimed.current_run_id,
    )

    assert first.committed and replay.committed
    assert replay.event_id == first.event_id
    assert kb.get_task(board, task_id).status == "done"
    assert len([e for e in kb.list_events(board, task_id) if e.kind == "completed"]) == 1
    assert len([r for r in kb.list_runs(board, task_id) if r.outcome == "completed"]) == 1
    assert _receipt(board, "complete:1")["operation_kind"] == "complete"

    collision = task_state.complete_task_transition(
        board,
        task_id,
        actor_scope="builder",
        operation_id="complete:1",
        summary="different",
        expected_run_id=claimed.current_run_id,
    )
    assert not collision.committed
    assert collision.refusal == "operation_id_collision"


def test_review_cycle_transitions_are_receipt_backed_and_replay_safe(board) -> None:
    task_id = kb.create_task(board, title="review", assignee="builder")
    implementation = kb.claim_task(board, task_id)
    assert implementation is not None
    requested = task_state.request_review_transition(
        board,
        task_id,
        actor_scope="builder",
        operation_id="review:request:1",
        summary="ready",
        metadata={"commit": "abc"},
        reviewer="reviewer",
        expected_run_id=implementation.current_run_id,
    )
    replay = task_state.request_review_transition(
        board,
        task_id,
        actor_scope="builder",
        operation_id="review:request:1",
        summary="ready",
        metadata={"commit": "abc"},
        reviewer="reviewer",
        expected_run_id=implementation.current_run_id,
    )
    assert requested.committed and replay.committed
    assert kb.get_task(board, task_id).status == "review"
    assert len([e for e in kb.list_events(board, task_id) if e.kind == "review_requested"]) == 1

    review = kb.claim_review_task(board, task_id)
    assert review is not None
    changed = task_state.request_changes_transition(
        board,
        task_id,
        actor_scope="reviewer",
        operation_id="review:changes:1",
        reason="fix it",
        expected_run_id=review.current_run_id,
    )
    changed_replay = task_state.request_changes_transition(
        board,
        task_id,
        actor_scope="reviewer",
        operation_id="review:changes:1",
        reason="fix it",
        expected_run_id=review.current_run_id,
    )
    assert changed.committed and changed_replay.committed
    assert kb.get_task(board, task_id).status == "ready"
    assert changed.detail == "builder"
    assert changed_replay.detail == "builder"

    implementation_2 = kb.claim_task(board, task_id)
    assert implementation_2 is not None
    assert task_state.request_review_transition(
        board,
        task_id,
        actor_scope="builder",
        operation_id="review:request:2",
        summary="fixed",
        expected_run_id=implementation_2.current_run_id,
    ).committed
    reopened = task_state.reopen_review_task_transition(
        board,
        task_id,
        actor_scope="operator",
        operation_id="review:reopen:1",
    )
    reopened_replay = task_state.reopen_review_task_transition(
        board,
        task_id,
        actor_scope="operator",
        operation_id="review:reopen:1",
    )
    assert reopened.committed and reopened_replay.committed
    assert kb.get_task(board, task_id).status == "ready"
    assert len([e for e in kb.list_events(board, task_id) if e.kind == "review_reopened"]) == 1


def test_completion_receipt_failure_rolls_back_task_run_event_and_attachment(board, tmp_path: Path) -> None:
    task_id = kb.create_task(board, title="rollback", assignee="builder")
    claimed = kb.claim_task(board, task_id)
    assert claimed is not None
    artifact = tmp_path / "proof.txt"
    artifact.write_text("proof", encoding="utf-8")
    before_events = list(kb.list_events(board, task_id))
    board.execute(
        "CREATE TRIGGER refuse_completion_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='complete' BEGIN SELECT RAISE(ABORT, 'forced completion receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced completion receipt failure"):
        task_state.complete_task_transition(
            board,
            task_id,
            actor_scope="builder",
            operation_id="complete:rollback",
            summary="verified",
            metadata={"artifacts": [str(artifact)]},
            expected_run_id=claimed.current_run_id,
        )

    task = kb.get_task(board, task_id)
    assert task.status == "running"
    assert task.current_run_id == claimed.current_run_id
    assert list(kb.list_events(board, task_id)) == before_events
    assert board.execute("SELECT * FROM task_attachments WHERE task_id=?", (task_id,)).fetchall() == []
    attachments_dir = Path(kbc._main_db_file(board)).parent / "attachments" / task_id
    assert not attachments_dir.exists() or list(attachments_dir.iterdir()) == []


def test_completion_private_connection_closes_before_cleanup_and_hook(board, monkeypatch) -> None:
    task_id = kb.create_task(board, title="close order", assignee="builder")
    claimed = kb.claim_task(board, task_id)
    assert claimed is not None
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

    def cleanup(*_args, **_kwargs):
        assert closed

    def hook(*_args, **_kwargs):
        assert closed

    monkeypatch.setattr(kbc, "_protected_connection", tracked)
    monkeypatch.setattr(kb, "_cleanup_workspace", cleanup)
    monkeypatch.setattr(kb, "_fire_task_hook", hook)
    assert task_state.complete_task_transition(
        board,
        task_id,
        actor_scope="builder",
        operation_id="complete:close-order",
        summary="verified",
        expected_run_id=claimed.current_run_id,
    ).committed
