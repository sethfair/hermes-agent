"""Receipt and transaction contracts for typed Kanban task-state operations."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_task_state as task_state
from hermes_cli.kanban_db_task_state import block_task_transition


@pytest.fixture
def home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()


def _ready_task(conn, title="receipt kernel"):
    task_id = kb.create_task(conn, title=title, assignee="worker")
    with kb.write_txn(conn):
        conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (task_id,))
    return task_id


def _block(conn, task_id, operation_id="receipt:block"):
    return block_task_transition(
        conn,
        task_id,
        actor_scope="test",
        operation_id=operation_id,
        kind="capability",
        subject="missing-access",
        reason="waiting",
    )


def _rows(conn, task_id):
    return {
        "task": tuple(conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()),
        "runs": [tuple(row) for row in conn.execute(
            "SELECT * FROM task_runs WHERE task_id=? ORDER BY id", (task_id,)
        )],
        "events": [tuple(row) for row in conn.execute(
            "SELECT * FROM task_events WHERE task_id=? ORDER BY id", (task_id,)
        )],
        "receipts": [tuple(row) for row in conn.execute(
            "SELECT * FROM task_transition_receipts WHERE task_id=? ORDER BY operation_id",
            (task_id,),
        )],
    }


def test_commit_boundary_failure_rolls_back_all_protected_effects(home, monkeypatch):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn)
        before = _rows(conn, task_id)
        original = kbc._execute_boundary_with_retry

        def fail_commit(private, sql):
            if sql == "COMMIT":
                raise sqlite3.OperationalError("forced commit failure")
            return original(private, sql)

        monkeypatch.setattr(kbc, "_execute_boundary_with_retry", fail_commit)

        with pytest.raises(sqlite3.OperationalError, match="forced commit failure"):
            _block(conn, task_id)

        assert _rows(conn, task_id) == before


def test_original_receipt_wins_after_a_later_competing_transition(home):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn)
        first = _block(conn, task_id, "receipt:first")
        assert first.committed is True
        assert kb.unblock_task(conn, task_id) is True
        second = block_task_transition(
            conn,
            task_id,
            actor_scope="test",
            operation_id="receipt:second",
            kind="capability",
            subject="different-access",
            reason="later transition",
        )
        assert second.committed is True
        latest = kb.get_task(conn, task_id)
        event_count = len(kb.list_events(conn, task_id))

        replay = _block(conn, task_id, "receipt:first")

        assert replay == first
        assert kb.get_task(conn, task_id) == latest
        assert len(kb.list_events(conn, task_id)) == event_count


def test_same_operation_id_with_changed_actor_scope_is_a_collision(home):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn)
        first = _block(conn, task_id, "receipt:actor")
        event_count = len(kb.list_events(conn, task_id))

        collision = block_task_transition(
            conn,
            task_id,
            actor_scope="different-actor",
            operation_id="receipt:actor",
            kind="capability",
            subject="missing-access",
            reason="waiting",
        )

        assert first.committed is True
        assert (collision.committed, collision.refusal) == (
            False,
            "operation_id_collision",
        )
        assert len(kb.list_events(conn, task_id)) == event_count


def test_version_one_receipt_hash_remains_replay_compatible(home):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn, "version one receipt")
        first = _block(conn, task_id, "receipt:v1")
        version_one_identity = {
            "task_id": task_id,
            "operation_kind": "block",
            "actor_scope": "test",
            "kind": "capability",
            "subject": {
                "known": '{"kind":"capability","subject":"missing-access","v":1}'
            },
            "reason": "waiting",
            "expected_run_id": None,
        }
        request_hash = hashlib.sha256(json.dumps(
            version_one_identity,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")).hexdigest()
        conn.execute(
            "UPDATE task_transition_receipts SET request_hash=? WHERE operation_id='receipt:v1'",
            (request_hash,),
        )

        replay = _block(conn, task_id, "receipt:v1")

        assert replay == first


@pytest.mark.parametrize(
    ("stage", "trigger_sql"),
    [
        (
            "task",
            "CREATE TRIGGER fail_task BEFORE UPDATE ON tasks "
            "BEGIN SELECT RAISE(ABORT, 'forced task failure'); END",
        ),
        (
            "event",
            "CREATE TRIGGER fail_event BEFORE INSERT ON task_events "
            "BEGIN SELECT RAISE(ABORT, 'forced event failure'); END",
        ),
        (
            "receipt",
            "CREATE TRIGGER fail_receipt BEFORE INSERT ON task_transition_receipts "
            "BEGIN SELECT RAISE(ABORT, 'forced receipt failure'); END",
        ),
    ],
)
def test_task_event_and_receipt_failures_roll_back_every_effect(
    home, monkeypatch, stage, trigger_sql
):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn, title=f"failure at {stage}")
        before = _rows(conn, task_id)
        hooks = []
        monkeypatch.setattr(kb, "_fire_task_hook", lambda *args, **kwargs: hooks.append(args))
        conn.execute(trigger_sql)

        with pytest.raises(sqlite3.IntegrityError, match=f"forced {stage} failure"):
            _block(conn, task_id, f"receipt:fail:{stage}")

        assert _rows(conn, task_id) == before
        assert hooks == []


def test_run_failure_rolls_back_task_event_and_receipt(home, monkeypatch):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn, "run failure")
        assert kb.claim_task(conn, task_id, claimer="worker") is not None
        before = _rows(conn, task_id)
        hooks = []
        monkeypatch.setattr(kb, "_fire_task_hook", lambda *args, **kwargs: hooks.append(args))
        conn.execute(
            "CREATE TRIGGER fail_run BEFORE UPDATE ON task_runs "
            "BEGIN SELECT RAISE(ABORT, 'forced run failure'); END"
        )

        with pytest.raises(sqlite3.IntegrityError, match="forced run failure"):
            _block(conn, task_id, "receipt:fail:run")

        assert _rows(conn, task_id) == before
        assert hooks == []


def test_busy_begin_failure_has_no_effect_and_suppresses_hook(home, monkeypatch):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn, "busy begin")
        before = _rows(conn, task_id)
        hooks = []
        original = kbc._execute_boundary_with_retry

        def fail_busy(private, sql):
            if sql == "BEGIN IMMEDIATE":
                raise sqlite3.OperationalError("database is locked")
            return original(private, sql)

        monkeypatch.setattr(kbc, "_execute_boundary_with_retry", fail_busy)
        monkeypatch.setattr(kb, "_fire_task_hook", lambda *args, **kwargs: hooks.append(args))

        with pytest.raises(sqlite3.OperationalError, match="locked"):
            _block(conn, task_id, "receipt:busy")

        assert _rows(conn, task_id) == before
        assert hooks == []


def test_process_loss_before_commit_rolls_back_and_suppresses_hook(home, monkeypatch):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn, "process loss")
        before = _rows(conn, task_id)
        hooks = []

        def interrupt_before_receipt(*_args, **_kwargs):
            raise KeyboardInterrupt("simulated process loss")

        monkeypatch.setattr(task_state, "_insert_receipt", interrupt_before_receipt)
        monkeypatch.setattr(kb, "_fire_task_hook", lambda *args, **kwargs: hooks.append(args))

        with pytest.raises(KeyboardInterrupt, match="simulated process loss"):
            _block(conn, task_id, "receipt:process-loss")

        assert _rows(conn, task_id) == before
        assert hooks == []


def test_lost_acknowledgement_replays_without_duplicate_event_or_hook(home, monkeypatch):
    with kbc.connect_closing() as conn:
        task_id = _ready_task(conn, "lost acknowledgement")
        delivered = []

        def lose_ack(*_args, **_kwargs):
            delivered.append("attempted")
            raise RuntimeError("lost acknowledgement")

        monkeypatch.setattr(kb, "_fire_task_hook", lose_ack)
        with pytest.raises(RuntimeError, match="lost acknowledgement"):
            _block(conn, task_id, "receipt:lost-ack")

        committed = conn.execute(
            "SELECT event_id, result_tuple_hash FROM task_transition_receipts "
            "WHERE operation_id='receipt:lost-ack'"
        ).fetchone()
        assert committed is not None
        event_count = len(kb.list_events(conn, task_id))
        monkeypatch.setattr(
            kb, "_fire_task_hook", lambda *_args, **_kwargs: delivered.append("duplicate")
        )

        replay = _block(conn, task_id, "receipt:lost-ack")

        assert replay.committed is True
        assert replay.event_id == committed[0]
        assert replay.result_tuple_hash == committed[1]
        assert len(kb.list_events(conn, task_id)) == event_count
        assert delivered == ["attempted"]
