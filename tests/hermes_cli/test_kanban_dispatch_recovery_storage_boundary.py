"""Receipt-backed storage contracts for dispatcher recovery."""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_dispatch as kbd
from hermes_cli import kanban_db_task_state as task_state


@pytest.fixture
def board(tmp_path: Path):
    conn = kbc.connect(tmp_path / "kanban.db")
    try:
        yield conn
    finally:
        conn.close()


def _overrun(board) -> tuple[str, int]:
    task_id = kb.create_task(
        board,
        title="bounded worker",
        assignee="worker",
        max_runtime_seconds=1,
    )
    claimed = kb.claim_task(board, task_id)
    assert claimed is not None and claimed.current_run_id is not None
    kbd._set_worker_pid(board, task_id, os.getpid())
    old = int(time.time()) - 30
    with kb.write_txn(board):
        board.execute(
            "UPDATE task_runs SET started_at=? WHERE id=?",
            (old, claimed.current_run_id),
        )
    return task_id, claimed.current_run_id


def _receipts(board, task_id: str):
    return board.execute(
        "SELECT operation_kind, operation_id FROM task_transition_receipts "
        "WHERE task_id=? AND operation_kind IN ('dispatch_failure','runtime_timeout') "
        "ORDER BY operation_kind",
        (task_id,),
    ).fetchall()


def _recover(board, task_id: str, run_id: int, *, operation_id: str = "timeout:1"):
    task = kb.get_task(board, task_id)
    assert task is not None and task.worker_pid is not None and task.claim_lock is not None
    return task_state.runtime_timeout_transition(
        board,
        task_id,
        actor_scope="dispatcher",
        operation_id=operation_id,
        expected_run_id=run_id,
        worker_pid=task.worker_pid,
        claim_lock=task.claim_lock,
        elapsed_seconds=30,
        limit_seconds=1,
        sigkill=False,
        retry_status="ready",
        error="elapsed 30s > limit 1s",
    )


def test_runtime_timeout_and_failure_accounting_have_separate_receipts(
    board, monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_id, run_id = _overrun(board)
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)

    assert kbd.enforce_max_runtime(board, signal_fn=lambda *_args: None) == [task_id]

    task = kb.get_task(board, task_id)
    run = kb.latest_run(board, task_id)
    assert task is not None and task.status == "ready"
    assert task.consecutive_failures == 1
    assert run is not None and run.id == run_id and run.outcome == "timed_out"
    assert run.error is not None
    assert [row["operation_kind"] for row in _receipts(board, task_id)] == [
        "dispatch_failure",
        "runtime_timeout",
    ]
    assert len([event for event in kb.list_events(board, task_id) if event.kind == "timed_out"]) == 1

    failure_operation_id = f"dispatch-failure:timed-out:{task_id}:{run_id}"
    failure_kwargs = {
        "actor_scope": "dispatcher",
        "operation_id": failure_operation_id,
        "trigger_operation_id": f"runtime-timeout:{task_id}:{run_id}",
        "expected_run_id": run_id,
        "error": run.error,
        "outcome": "timed_out",
        "retry_status": "ready",
        "failure_limit": 2,
        "event_payload_extra": {
            "pid": os.getpid(), "sigkill": False, "retry_status": "ready",
        },
    }
    assert task_state.dispatch_failure_transition(board, task_id, **failure_kwargs).committed
    replayed_task = kb.get_task(board, task_id)
    assert replayed_task is not None and replayed_task.consecutive_failures == 1
    collision = task_state.dispatch_failure_transition(
        board, task_id, **{**failure_kwargs, "error": "different"},
    )
    assert not collision.committed and collision.refusal == "operation_id_collision"


def test_runtime_timeout_replays_and_rejects_operation_id_collision(board) -> None:
    task_id, run_id = _overrun(board)
    first = _recover(board, task_id, run_id)
    task_after = kb.get_task(board, task_id)
    assert task_after is not None

    replay = task_state.runtime_timeout_transition(
        board,
        task_id,
        actor_scope="dispatcher",
        operation_id="timeout:1",
        expected_run_id=run_id,
        worker_pid=os.getpid(),
        claim_lock="different-after-commit-is-hashed",
        elapsed_seconds=30,
        limit_seconds=1,
        sigkill=False,
        retry_status="ready",
        error="elapsed 30s > limit 1s",
    )
    assert first.committed
    assert not replay.committed and replay.refusal == "operation_id_collision"

    original_claim_lock = kb.list_runs(board, task_id)[-1].claim_lock
    assert original_claim_lock is not None
    exact_replay = task_state.runtime_timeout_transition(
        board,
        task_id,
        actor_scope="dispatcher",
        operation_id="timeout:1",
        expected_run_id=run_id,
        worker_pid=os.getpid(),
        claim_lock=original_claim_lock,
        elapsed_seconds=30,
        limit_seconds=1,
        sigkill=False,
        retry_status="ready",
        error="elapsed 30s > limit 1s",
    )
    assert exact_replay.committed and exact_replay.event_id == first.event_id
    assert kb.get_task(board, task_id) == task_after
    assert len([event for event in kb.list_events(board, task_id) if event.kind == "timed_out"]) == 1


def test_runtime_timeout_receipt_failure_rolls_back_task_run_and_event(board) -> None:
    task_id, run_id = _overrun(board)
    before_events = list(kb.list_events(board, task_id))
    board.execute(
        "CREATE TRIGGER refuse_timeout_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='runtime_timeout' "
        "BEGIN SELECT RAISE(ABORT, 'forced timeout receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced timeout receipt failure"):
        _recover(board, task_id, run_id)

    task = kb.get_task(board, task_id)
    run = kb.latest_run(board, task_id)
    assert task is not None and task.status == "running" and task.current_run_id == run_id
    assert run is not None and run.ended_at is None and run.outcome is None
    assert list(kb.list_events(board, task_id)) == before_events


def test_failure_receipt_failure_keeps_timeout_but_rolls_back_accounting(board) -> None:
    task_id, run_id = _overrun(board)
    assert _recover(board, task_id, run_id).committed
    board.execute(
        "CREATE TRIGGER refuse_failure_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='dispatch_failure' "
        "BEGIN SELECT RAISE(ABORT, 'forced failure receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced failure receipt failure"):
        task_state.dispatch_failure_transition(
            board,
            task_id,
            actor_scope="dispatcher",
            operation_id="failure:1",
            trigger_operation_id="timeout:1",
            expected_run_id=run_id,
            error="elapsed 30s > limit 1s",
            outcome="timed_out",
            retry_status="ready",
            failure_limit=3,
        )

    task = kb.get_task(board, task_id)
    assert task is not None and task.status == "recovering" and task.consecutive_failures == 0
    assert len([event for event in kb.list_events(board, task_id) if event.kind == "timed_out"]) == 1

    board.execute("DROP TRIGGER refuse_failure_receipt")
    assert kbd._reconcile_pending_runtime_timeouts(board, failure_limit=3) == [task_id]
    task = kb.get_task(board, task_id)
    assert task is not None and task.status == "ready" and task.consecutive_failures == 1


def test_stale_running_recovery_is_receipt_backed(board, monkeypatch) -> None:
    task_id, run_id = _overrun(board)
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)

    assert kbd.detect_stale_running(
        board, stale_timeout_seconds=1, signal_fn=lambda *_args: None,
    ) == [task_id]

    task = kb.get_task(board, task_id)
    run = kb.latest_run(board, task_id)
    receipt = board.execute(
        "SELECT operation_kind FROM task_transition_receipts "
        "WHERE operation_id=?",
        (f"stale-running:{task_id}:{run_id}",),
    ).fetchone()
    assert task is not None and task.status == "ready"
    assert run is not None and run.outcome == "stale"
    assert receipt is not None and receipt["operation_kind"] == "stale_running"


def test_stale_receipt_failure_rolls_back_task_run_and_event(board, monkeypatch) -> None:
    task_id, run_id = _overrun(board)
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    before_events = list(kb.list_events(board, task_id))
    board.execute(
        "CREATE TRIGGER refuse_stale_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='stale_running' "
        "BEGIN SELECT RAISE(ABORT, 'forced stale receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced stale receipt failure"):
        kbd.detect_stale_running(
            board, stale_timeout_seconds=1, signal_fn=lambda *_args: None,
        )

    task = kb.get_task(board, task_id)
    run = kb.latest_run(board, task_id)
    assert task is not None and task.status == "running" and task.current_run_id == run_id
    assert run is not None and run.ended_at is None
    assert list(kb.list_events(board, task_id)) == before_events


def _orphan(board) -> tuple[str, int]:
    task_id, run_id = _overrun(board)
    with kb.write_txn(board):
        board.execute(
            "UPDATE tasks SET claim_lock=NULL,worker_pid=NULL,worker_started_at=NULL "
            "WHERE id=?",
            (task_id,),
        )
    return task_id, run_id


def test_orphan_reconciliation_is_receipt_backed(board) -> None:
    task_id, run_id = _orphan(board)

    assert kbd.reconcile_orphaned_running(board) == [task_id]

    task = kb.get_task(board, task_id)
    run = kb.latest_run(board, task_id)
    receipt = board.execute(
        "SELECT operation_kind FROM task_transition_receipts WHERE operation_id=?",
        (f"orphan-running:{task_id}:{run_id}",),
    ).fetchone()
    comments = board.execute(
        "SELECT body FROM task_comments WHERE task_id=? ORDER BY id", (task_id,),
    ).fetchall()
    assert task is not None and task.status == "ready"
    assert run is not None and run.outcome == "reclaimed"
    assert receipt is not None and receipt["operation_kind"] == "orphan_running"
    assert comments[-1]["body"].startswith("reconciliation:")


def test_orphan_receipt_failure_rolls_back_task_run_event_and_comment(board) -> None:
    task_id, run_id = _orphan(board)
    before_events = list(kb.list_events(board, task_id))
    before_comments = board.execute(
        "SELECT * FROM task_comments WHERE task_id=?", (task_id,),
    ).fetchall()
    board.execute(
        "CREATE TRIGGER refuse_orphan_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='orphan_running' "
        "BEGIN SELECT RAISE(ABORT, 'forced orphan receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced orphan receipt failure"):
        kbd.reconcile_orphaned_running(board)

    task = kb.get_task(board, task_id)
    run = kb.latest_run(board, task_id)
    after_comments = board.execute(
        "SELECT * FROM task_comments WHERE task_id=?", (task_id,),
    ).fetchall()
    assert task is not None and task.status == "running" and task.current_run_id == run_id
    assert run is not None and run.ended_at is None
    assert list(kb.list_events(board, task_id)) == before_events
    assert after_comments == before_comments


def _dead_worker(board) -> tuple[str, int]:
    task_id, run_id = _overrun(board)
    old = int(time.time()) - 300
    with kb.write_txn(board):
        board.execute("UPDATE tasks SET started_at=? WHERE id=?", (old, task_id))
    return task_id, run_id


def test_dead_worker_recovery_and_failure_accounting_have_receipts(board, monkeypatch) -> None:
    task_id, run_id = _dead_worker(board)
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)

    assert kbd.detect_crashed_workers(board) == [task_id]

    task = kb.get_task(board, task_id)
    run = kb.latest_run(board, task_id)
    receipts = board.execute(
        "SELECT operation_kind FROM task_transition_receipts "
        "WHERE operation_id IN (?,?) ORDER BY operation_kind",
        (
            f"dead-worker:{task_id}:{run_id}",
            f"dispatch-failure:crashed:{task_id}:{run_id}",
        ),
    ).fetchall()
    assert task is not None and task.status == "ready" and task.consecutive_failures == 1
    assert run is not None and run.outcome == "crashed"
    assert [row["operation_kind"] for row in receipts] == ["dead_worker", "dispatch_failure"]


def test_dead_worker_receipt_failure_rolls_back_task_run_and_event(board, monkeypatch) -> None:
    task_id, run_id = _dead_worker(board)
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    before_events = list(kb.list_events(board, task_id))
    board.execute(
        "CREATE TRIGGER refuse_dead_receipt BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='dead_worker' "
        "BEGIN SELECT RAISE(ABORT, 'forced dead receipt failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced dead receipt failure"):
        kbd.detect_crashed_workers(board)

    task = kb.get_task(board, task_id)
    run = kb.latest_run(board, task_id)
    assert task is not None and task.status == "running" and task.current_run_id == run_id
    assert run is not None and run.ended_at is None
    assert list(kb.list_events(board, task_id)) == before_events


def test_pending_dead_worker_failure_accounting_is_reconciled(board, monkeypatch) -> None:
    task_id, _run_id = _dead_worker(board)
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    board.execute(
        "CREATE TRIGGER refuse_dead_accounting BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='dispatch_failure' "
        "BEGIN SELECT RAISE(ABORT, 'forced dead accounting failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced dead accounting failure"):
        kbd.detect_crashed_workers(board)
    interrupted = kb.get_task(board, task_id)
    assert interrupted is not None and interrupted.status == "recovering"

    board.execute("DROP TRIGGER refuse_dead_accounting")
    assert kbd.detect_crashed_workers(board) == []
    recovered = kb.get_task(board, task_id)
    assert recovered is not None and recovered.status == "ready"
    assert recovered.consecutive_failures == 1


def test_pending_host_loss_accounting_is_reconciled_without_spending_failure_budget(
    board, monkeypatch,
) -> None:
    task_id, _run_id = _dead_worker(board)
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)

    def mark_infrastructure(_conn, _task_id, _pid, dead) -> None:
        dead.infrastructure = True
        dead.event_payload["infrastructure"] = True

    monkeypatch.setattr(
        "hermes_cli.kanban_owner_recovery.apply_attempt_note",
        mark_infrastructure,
    )
    board.execute("UPDATE tasks SET consecutive_failures=1 WHERE id=?", (task_id,))
    board.execute(
        "CREATE TRIGGER refuse_host_loss_accounting BEFORE INSERT ON task_transition_receipts "
        "WHEN NEW.operation_kind='dispatch_failure' "
        "BEGIN SELECT RAISE(ABORT, 'forced host-loss accounting failure'); END"
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced host-loss accounting failure"):
        kbd.detect_crashed_workers(board)
    interrupted = kb.get_task(board, task_id)
    assert interrupted is not None and interrupted.status == "recovering"

    board.execute("DROP TRIGGER refuse_host_loss_accounting")
    assert kbd.detect_crashed_workers(board) == []
    recovered = kb.get_task(board, task_id)
    run = kb.latest_run(board, task_id)
    assert recovered is not None and recovered.status == "ready"
    assert recovered.consecutive_failures == 1
    assert run is not None and run.metadata is not None
    assert run.metadata["infrastructure"] is True
