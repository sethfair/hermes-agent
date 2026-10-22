"""A dead Kanban worker is booked the same way whichever process notices it.

``_recent_worker_exits`` is filled by ``os.waitpid`` and so only knows children of
the process running the sweep; a per-tick ``hermes kanban dispatch`` process finds it
empty. The worker's own exit trailer in its log is the durable witness the sweep reads
instead, and a tripped protocol-violation budget must hold the card until an operator
unblocks it.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_dispatch as kbd
from hermes_cli.quiet_single_query import KANBAN_WORKER_EXIT_TRAILER, exit_single_query


def _selected_goal_code(monkeypatch, error, *, real_judge=False):
    """Select a disposition through the real goal loop before native booking."""
    from unittest.mock import patch
    from hermes_cli import goals
    from hermes_cli.cli_single_query import _goal_infrastructure_exit_code
    from hermes_cli.kanban_goal_result import GoalLoopProgress, GoalTurnResult
    first = GoalTurnResult("test receipt", {"completed": True, "final_response": "test receipt"}, 0)
    if real_judge:
        actual = goals.judge_goal
        seen = []
        def observe(*args, **kwargs):
            result = actual(*args, **kwargs)
            seen.append(result)
            return result
        monkeypatch.setattr(goals, "judge_goal", observe)
        with patch("agent.auxiliary_client.call_llm", side_effect=error):
            outcome = goals.run_kanban_goal_loop(
                task_id="t_testowned", goal_text="test-owned task", first_turn=first,
                progress=GoalLoopProgress(first), run_turn=lambda p: pytest.fail("no model"),
                task_status_fn=lambda: "running", block_fn=lambda p: pytest.fail("no block"),
                infrastructure_exit_code=_goal_infrastructure_exit_code)
        assert seen and seen[0][-1] is True
    else:
        monkeypatch.setattr(goals, "judge_goal", lambda *a: (_ for _ in ()).throw(error))
        outcome = goals.run_kanban_goal_loop(
            task_id="t_testowned", goal_text="test-owned task", first_turn=first,
            progress=GoalLoopProgress(first), run_turn=lambda p: pytest.fail("no model"),
            task_status_fn=lambda: "running", block_fn=lambda p: pytest.fail("no block"),
            infrastructure_exit_code=_goal_infrastructure_exit_code)
    return outcome.exit_code


@pytest.fixture
def kanban_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    kbd._recent_worker_exits.clear()
    kb.init_db()
    return home


def _dead_worker_with_log(conn, tid: str, pid: int, rc: int) -> None:
    """Claim ``tid`` for a worker that already exited ``rc`` and wrote its log — never reaped here."""
    host = kb._claimer_id().split(":", 1)[0]
    kb.claim_task(conn, tid, claimer=f"{host}:w{pid}")
    conn.execute(
        "UPDATE tasks SET worker_pid=?, worker_started_at=NULL, started_at=? WHERE id=?",
        (pid, int(time.time()) - 120, tid),
    )
    conn.commit()
    log = kb.worker_log_path(tid)
    log.parent.mkdir(parents=True, exist_ok=True)
    with open(log, "a", encoding="utf-8") as f:
        f.write(f"the model said something\n\nResume this session with:\n  hermes --resume x\n\n{KANBAN_WORKER_EXIT_TRAILER}{rc}\n")


@pytest.mark.parametrize(
    "rc, event, failure_counted",
    [(0, "protocol_violation", False), (kb.KANBAN_RATE_LIMIT_EXIT_CODE, "rate_limited", False)],
)
def test_fresh_process_sweep_books_the_logged_exit_code(kanban_home, rc, event, failure_counted):
    """Empty reap registry + exit trailer in the log: a clean exit is the protocol violation
    (marker, streak, no unified-budget hit) and a 75 is a rate-limit requeue — not a bare
    ``pid N not alive`` crash that counts a failure."""
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="t", assignee="a")
        _dead_worker_with_log(conn, tid, 70001, rc)

        kbd.detect_crashed_workers(conn)

        ev = conn.execute(
            "SELECT kind FROM task_events WHERE task_id=? ORDER BY id DESC LIMIT 1", (tid,)).fetchone()
        run = conn.execute(
            "SELECT outcome, error, metadata FROM task_runs WHERE task_id=? ORDER BY id DESC LIMIT 1",
            (tid,)).fetchone()
        task = kb.get_task(conn, tid)
        assert ev["kind"] == event
        assert "not alive" not in (run["error"] or "")
        assert task.status == "ready"
        assert task.consecutive_failures == (1 if failure_counted else 0)
        # The decoded rc lands in the run row so quota (75) vs crash stays tellable after
        # the fact even though the worker_output tail is trimmed (#113611).
        assert kb._json_dict(run["metadata"]).get("exit_code") == rc
        if rc == 0:
            assert kb._json_dict(run["metadata"]).get("protocol_violation") is True
            assert kbd._protocol_violation_streak(conn, tid) == 1
            assert KANBAN_WORKER_EXIT_TRAILER not in (run["error"] or "")
        else:
            assert run["outcome"] == "rate_limited"


def test_violation_budget_trip_holds_until_operator_unblock(kanban_home):
    """The third consecutive clean exit trips the violation budget and ``recompute_ready``
    must not promote the card back the same tick (``consecutive_failures`` is still below
    ``failure_limit``); ``unblock_task`` lifts the hold."""
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="loop", assignee="a")
        for i in range(kbd._PROTOCOL_VIOLATION_FAILURE_LIMIT):
            _dead_worker_with_log(conn, tid, 71000 + i, 0)
            kbd.detect_crashed_workers(conn)
            kb.recompute_ready(conn, failure_limit=10)
        task = kb.get_task(conn, tid)
        assert task.status == "blocked"
        assert task.consecutive_failures < 10

        kb.unblock_task(conn, tid)
        assert kb.get_task(conn, tid).status == "ready"
        kb.recompute_ready(conn, failure_limit=10)
        assert kb.get_task(conn, tid).status == "ready"


def test_plain_budget_trip_still_auto_recovers(kanban_home):
    """A unified-budget trip carries no ``sticky`` marker, so the two recovery paths on main
    survive: raising the dispatcher ``failure_limit`` past the counter promotes the card, and
    ``assign_task`` to a fresh profile (counter reset by design) promotes it too."""
    with kbc.connect() as conn:
        tids = [kb.create_task(conn, title=t, assignee="a") for t in ("raise-limit", "reassign")]
        for tid in tids:
            for i in range(2):
                kbd._record_task_failure(
                    conn, tid, error=f"boom{i}", outcome="crashed", failure_limit=2,
                    release_claim=False, end_run=False,
                )
            assert kb.get_task(conn, tid).status == "blocked"
        assert kb.recompute_ready(conn, failure_limit=2) == 0

        assert kb.recompute_ready(conn, failure_limit=5) == 2
        assert kb.get_task(conn, tids[0]).status == "ready"

        for i in range(2):
            kbd._record_task_failure(
                conn, tids[1], error=f"again{i}", outcome="crashed", failure_limit=2,
                release_claim=False, end_run=False,
            )
        assert kb.get_task(conn, tids[1]).status == "blocked"
        kb.assign_task(conn, tids[1], "other-profile")
        assert kb.recompute_ready(conn, failure_limit=2) == 1
        assert kb.get_task(conn, tids[1]).status == "ready"


def test_goal_loop_local_exits_preserve_dispatcher_booking_distinction(kanban_home, monkeypatch):
    transient = _selected_goal_code(monkeypatch, TimeoutError("judge unavailable"))
    deterministic = _selected_goal_code(monkeypatch, ValueError("schema mismatch"))
    assert transient == 75 and deterministic == 1
    seen_crashes = []
    original_account = kbd._account_crashes
    def account(conn, crash_details, *args, **kwargs):
        seen_crashes.extend(crash_details)
        return original_account(conn, crash_details, *args, **kwargs)
    monkeypatch.setattr(kbd, "_account_crashes", account)
    with kbc.connect() as conn:
        tids = [kb.create_task(conn, title=f"transient-{i}", assignee="a") for i in range(3)]
        for i, tid in enumerate(tids):
            _dead_worker_with_log(conn, tid, 80001 + i, transient)
        broken = kb.create_task(conn, title="deterministic", assignee="a")
        _dead_worker_with_log(conn, broken, 80009, deterministic)
        kbd.detect_crashed_workers(conn)
        assert len(seen_crashes) == 1 and seen_crashes[0][0] == broken
        for tid in tids:
            task = kb.get_task(conn, tid)
            run = kb.latest_run(conn, tid)
            assert task.status == "ready" and task.consecutive_failures == 0
            assert run is not None and run.outcome == "rate_limited"
        task = kb.get_task(conn, broken)
        run = kb.latest_run(conn, broken)
        assert task.consecutive_failures == 1
        assert run is not None and run.outcome == "crashed"


def test_three_deterministic_goal_exits_trip_systemic_sticky_containment(kanban_home, monkeypatch):
    code = _selected_goal_code(monkeypatch, ValueError("deterministic schema failure"))
    assert code == 1
    with kbc.connect() as conn:
        tids = [kb.create_task(conn, title=f"schema-{i}", assignee="a") for i in range(3)]
        for i, tid in enumerate(tids):
            _dead_worker_with_log(conn, tid, 82001 + i, code)
        kbd.detect_crashed_workers(conn)
        for tid in tids:
            task = kb.get_task(conn, tid)
            run = kb.latest_run(conn, tid)
            assert task is not None and task.status == "blocked" and task.consecutive_failures >= 1
            assert run is not None and run.outcome == "crashed"
        assert kb.recompute_ready(conn, failure_limit=10) == 0
        assert all(kb.get_task(conn, tid).status == "blocked" for tid in tids)


@pytest.mark.parametrize("error", [
    ValueError("bad auxiliary config"), TimeoutError("swallowed transient"),
])
def test_real_judge_catch_all_selected_one_books_native_crash(kanban_home, monkeypatch, error):
    code = _selected_goal_code(monkeypatch, error, real_judge=True)
    assert code == 1 and kbd._exit_code_kind(code)[0] == "nonzero_exit"
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="real judge flag", assignee="a")
        _dead_worker_with_log(conn, tid, 83001, code)
        kbd.detect_crashed_workers(conn)
        task, run = kb.get_task(conn, tid), kb.latest_run(conn, tid)
    assert task is not None and task.consecutive_failures == 1 and task.status == "ready"
    assert run is not None and run.outcome == "crashed"


def test_exit_single_query_writes_trailer_only_for_kanban_workers(monkeypatch, capsys):
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    with pytest.raises(SystemExit) as exc:
        exit_single_query(1)
    assert exc.value.code == 1
    assert KANBAN_WORKER_EXIT_TRAILER not in capsys.readouterr().err

    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_1")
    with pytest.raises(SystemExit) as exc:
        exit_single_query(kb.KANBAN_RATE_LIMIT_EXIT_CODE)
    assert exc.value.code == kb.KANBAN_RATE_LIMIT_EXIT_CODE
    assert f"{KANBAN_WORKER_EXIT_TRAILER}{kb.KANBAN_RATE_LIMIT_EXIT_CODE}" in capsys.readouterr().err
