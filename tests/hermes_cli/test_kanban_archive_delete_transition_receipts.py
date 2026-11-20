"""Receipt-backed archive and delete lifecycle transitions."""

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


def test_compatibility_archive_commits_transition_event_and_receipt(board):
    task_id = kb.create_task(board, title="archive", assignee="worker")

    assert kb.archive_task(board, task_id) is True

    assert kb.get_task(board, task_id).status == "archived"
    receipt = board.execute(
        "SELECT operation_kind, outcome_code, event_id, result_tuple_hash "
        "FROM task_transition_receipts WHERE task_id = ? AND operation_kind = 'archive'",
        (task_id,),
    ).fetchone()
    assert receipt is not None
    assert tuple(receipt[:2]) == ("archive", "committed")
    assert receipt["event_id"] is not None
    assert len(receipt["result_tuple_hash"]) == 64
    event = board.execute(
        "SELECT kind FROM task_events WHERE id = ?", (receipt["event_id"],)
    ).fetchone()
    assert event["kind"] == "archived"


def test_archive_transition_replays_and_fences_unknown_before_effects(board):
    replay_task = kb.create_task(board, title="replay", assignee="worker")
    first = task_state.archive_task_transition(
        board,
        replay_task,
        actor_scope="operator",
        operation_id="archive:one",
    )
    event_count = len(kb.list_events(board, replay_task))
    replay = task_state.archive_task_transition(
        board,
        replay_task,
        actor_scope="operator",
        operation_id="archive:one",
    )
    collision = task_state.archive_task_transition(
        board,
        replay_task,
        actor_scope="different-operator",
        operation_id="archive:one",
    )

    assert replay == first
    assert first.committed is True
    assert len(kb.list_events(board, replay_task)) == event_count
    assert (collision.committed, collision.refusal) == (
        False,
        "operation_id_collision",
    )

    unknown = kb.create_task(board, title="unknown", assignee="worker")
    assert kb.block_task(board, unknown, kind="needs_input", reason="classify")
    before = len(kb.list_events(board, unknown))
    refused = task_state.archive_task_transition(
        board,
        unknown,
        actor_scope="operator",
        operation_id="archive:unknown",
    )

    assert (refused.committed, refused.refusal) == (
        False,
        "block_identity_unknown",
    )
    assert kb.get_task(board, unknown).status == "blocked"
    assert len(kb.list_events(board, unknown)) == before
    assert board.execute(
        "SELECT 1 FROM task_transition_receipts WHERE operation_id = ?",
        ("archive:unknown",),
    ).fetchone() is None


def _seed_delete_relations(board, task_id: str, tmp_path: Path) -> Path:
    child = kb.create_task(board, title="child")
    kb.link_tasks(board, task_id, child)
    kb.add_comment(board, task_id, "operator", "remove me")
    board.execute(
        "INSERT INTO kanban_notify_subs "
        "(task_id, platform, chat_id, thread_id, created_at, last_event_id) "
        "VALUES (?, 'test', 'chat', '', 0, 0)",
        (task_id,),
    )
    blob = tmp_path / "attachment.txt"
    blob.write_text("payload", encoding="utf-8")
    kb.add_attachment(
        board,
        task_id,
        filename=blob.name,
        stored_path=str(blob),
        size=blob.stat().st_size,
    )
    return blob


def test_delete_archived_transition_cleans_relations_and_replays(board, tmp_path):
    task_id = kb.create_task(board, title="delete", assignee="worker")
    blob = _seed_delete_relations(board, task_id, tmp_path)
    assert kb.archive_task(board, task_id)

    first = task_state.delete_archived_task_transition(
        board,
        task_id,
        actor_scope="operator",
        operation_id="delete-archived:one",
    )
    replay = task_state.delete_archived_task_transition(
        board,
        task_id,
        actor_scope="operator",
        operation_id="delete-archived:one",
    )
    collision = task_state.delete_archived_task_transition(
        board,
        task_id,
        actor_scope="different-operator",
        operation_id="delete-archived:one",
    )

    assert replay == first
    assert first.committed is True
    assert (collision.committed, collision.refusal) == (
        False,
        "operation_id_collision",
    )
    assert kb.get_task(board, task_id) is None
    for table in (
        "task_links",
        "task_comments",
        "task_events",
        "task_runs",
        "kanban_notify_subs",
        "task_attachments",
    ):
        where = "parent_id = ? OR child_id = ?" if table == "task_links" else "task_id = ?"
        params = (task_id, task_id) if table == "task_links" else (task_id,)
        assert board.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {where}", params
        ).fetchone()[0] == 0
    assert not blob.exists()
    receipt = board.execute(
        "SELECT operation_kind, event_id, result_tuple_hash "
        "FROM task_transition_receipts WHERE operation_id = ?",
        ("delete-archived:one",),
    ).fetchone()
    assert tuple(receipt[:2]) == ("delete_archived", None)
    assert len(receipt["result_tuple_hash"]) == 64


def test_delete_transitions_preserve_status_and_unknown_restrictions(board):
    live = kb.create_task(board, title="live")
    refused = task_state.delete_archived_task_transition(
        board,
        live,
        actor_scope="operator",
        operation_id="delete-archived:live",
    )
    assert (refused.committed, refused.refusal) == (False, "invalid_status")
    assert kb.get_task(board, live) is not None

    assert kb.delete_task(board, live) is True
    assert kb.get_task(board, live) is None
    assert board.execute(
        "SELECT operation_kind FROM task_transition_receipts "
        "WHERE task_id = ? AND operation_kind = 'delete'",
        (live,),
    ).fetchone() is not None

    unknown = kb.create_task(board, title="unknown-delete")
    assert kb.block_task(board, unknown, kind="capability", reason="classify")
    before = len(kb.list_events(board, unknown))
    assert kb.delete_task(board, unknown) is False
    assert kb.get_task(board, unknown).status == "blocked"
    assert len(kb.list_events(board, unknown)) == before


def test_delete_receipt_failure_rolls_back_task_and_relations(board, tmp_path):
    task_id = kb.create_task(board, title="rollback")
    blob = _seed_delete_relations(board, task_id, tmp_path)
    assert kb.archive_task(board, task_id)
    board.execute(
        "CREATE TRIGGER fail_delete_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind = 'delete_archived' "
        "BEGIN SELECT RAISE(ABORT, 'forced delete receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced delete receipt failure"):
        task_state.delete_archived_task_transition(
            board,
            task_id,
            actor_scope="operator",
            operation_id="delete-archived:rollback",
        )

    assert kb.get_task(board, task_id).status == "archived"
    assert kb.list_comments(board, task_id)
    assert kb.list_events(board, task_id)
    assert kb.list_attachments(board, task_id)
    assert blob.exists()
