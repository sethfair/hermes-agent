from pathlib import Path
import json
import sqlite3

import pytest
from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_graph as kbg


@pytest.fixture
def home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()


def test_subject_interface(home):
    with kbc.connect_closing() as conn:
        tid = kb.create_task(conn, title="identity interface", assignee="worker")
        with kb.write_txn(conn):
            conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (tid,))
        assert kb.block_task(
            conn,
            tid,
            kind="needs_input",
            subject="adrian/decision-a",
            reason="same",
        )


def test_distinct_identity(home):
    with kbc.connect_closing() as conn:
        tid = kb.create_task(conn, title="identity behavior", assignee="worker")
        with kb.write_txn(conn):
            conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (tid,))
        assert kb.claim_task(conn, tid, claimer="worker") is not None
        assert kb.block_task(
            conn,
            tid,
            kind="needs_input",
            subject="adrian/decision-a",
            reason="same",
        )
        assert kb.unblock_task(conn, tid)
        with kb.write_txn(conn):
            conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (tid,))
        assert kb.claim_task(conn, tid, claimer="worker") is not None
        assert kb.block_task(
            conn,
            tid,
            kind="needs_input",
            subject="adrian/decision-b",
            reason="same",
        )
        row = kb.get_task(conn, tid)
        assert (row.status, row.block_recurrences) == ("blocked", 1)


def _ready_task(conn, title="identity behavior"):
    tid = kb.create_task(conn, title=title, assignee="worker")
    with kb.write_txn(conn):
        conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (tid,))
    return tid


def test_same_identity_changed_reason_hits_existing_limit(home):
    with kbc.connect_closing() as conn:
        tid = _ready_task(conn)
        assert kb.block_task(
            conn, tid, kind="needs_input", subject="Adrian/Decision-A", reason="first prose"
        )
        first = kb.get_task(conn, tid)
        assert first.block_identity == (
            '{"kind":"needs_input","subject":"adrian/decision-a","v":1}'
        )
        assert kb.unblock_task(conn, tid)
        assert kb.block_task(
            conn, tid, kind="needs_input", subject="adrian/decision-a", reason="different prose"
        )
        row = kb.get_task(conn, tid)
        assert (row.status, row.block_recurrences) == ("triage", kb.BLOCK_RECURRENCE_LIMIT)
        event = kb.list_events(conn, tid)[-1]
        assert event.kind == "block_loop_detected"
        assert event.payload["identity"] == row.block_identity
        assert event.payload["identity_state"] == "known"


def test_invalid_explicit_subject_is_unknown_and_redacted(home):
    secret = "Owner/decision\nBearer private-value"
    with kbc.connect_closing() as conn:
        tid = _ready_task(conn)
        assert kb.block_task(
            conn, tid, kind="needs_input", subject=secret, reason="safe explanation"
        )
        row = kb.get_task(conn, tid)
        assert (row.status, row.block_identity_state, row.block_identity) == (
            "blocked", "unknown", None
        )
        events = kb.list_events(conn, tid)
        assert [event.kind for event in events[-2:]] == ["block_identity_unknown", "blocked"]
        encoded = json.dumps([event.payload for event in events])
        assert "private-value" not in encoded
        assert events[-1].payload["unknown_cause"] == "invalid_subject"


def test_unknown_requires_classification_before_unblock(home):
    with kbc.connect_closing() as conn:
        tid = _ready_task(conn)
        assert kb.block_task(conn, tid, kind="needs_input", reason="owner is not yet classified")
        before = kb.get_task(conn, tid)
        assert before.block_identity_state == "unknown"
        assert kb.unblock_task(conn, tid) is False
        assert kb.classify_block(
            conn,
            tid,
            kind="needs_input",
            subject="adrian/decision-a",
            actor="operator",
        )
        classified = kb.get_task(conn, tid)
        assert classified.status == "blocked"
        assert classified.block_identity_state == "known"
        assert classified.block_recurrences == 1
        assert kb.unblock_task(conn, tid)


def _unknown_triage(conn):
    tid = kb.create_task(conn, title="unknown triage", assignee="worker", triage=True)
    with kb.write_txn(conn):
        conn.execute(
            "UPDATE tasks SET block_identity_state='unknown', block_kind='needs_input', "
            "block_recurrences=1 WHERE id=?",
            (tid,),
        )
    return tid


def _state_snapshot(conn, tid):
    return {
        "task": tuple(conn.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()),
        "runs": [tuple(row) for row in conn.execute(
            "SELECT * FROM task_runs WHERE task_id=? ORDER BY id", (tid,)
        )],
        "comments": [tuple(row) for row in conn.execute(
            "SELECT * FROM task_comments WHERE task_id=? ORDER BY id", (tid,)
        )],
        "links": [tuple(row) for row in conn.execute(
            "SELECT * FROM task_links WHERE parent_id=? OR child_id=? ORDER BY parent_id, child_id",
            (tid, tid),
        )],
        "events": [tuple(row) for row in conn.execute(
            "SELECT * FROM task_events WHERE task_id=? ORDER BY id", (tid,)
        )],
    }


def test_unknown_triage_refuses_guarded_and_unguarded_release_without_effects(home):
    with kbc.connect_closing() as conn:
        tid = _unknown_triage(conn)
        before = _state_snapshot(conn, tid)
        guard_called = False

        def guard(_conn, _task_id):
            nonlocal guard_called
            guard_called = True
            return kb.TriageReleaseGuardResult(True)

        receipt = kb.guarded_specify_triage_task(
            conn, tid, guard=guard, title="must not apply", author="operator"
        )
        assert (receipt.released, receipt.outcome, receipt.status) == (
            False, "block_identity_unknown", "triage"
        )
        assert guard_called is False
        assert _state_snapshot(conn, tid) == before
        assert kb.specify_triage_task(conn, tid, title="must not apply", author="operator") is False
        assert _state_snapshot(conn, tid) == before


def test_unknown_triage_refuses_decomposition_without_fanout(home):
    with kbc.connect_closing() as conn:
        tid = _unknown_triage(conn)
        before = _state_snapshot(conn, tid)
        assert kbg.decompose_triage_task(
            conn,
            tid,
            root_assignee="orchestrator",
            children=[{"title": "must not exist", "assignee": "worker"}],
            author="operator",
        ) is None
        assert _state_snapshot(conn, tid) == before
        assert conn.execute(
            "SELECT 1 FROM tasks WHERE title='must not exist'"
        ).fetchone() is None


def test_dependency_identity_requires_named_open_direct_parent(home):
    with kbc.connect_closing() as conn:
        parent = kb.create_task(conn, title="parent", assignee="worker")
        child = kb.create_task(conn, title="child", assignee="worker", parents=[parent])
        with kb.write_txn(conn):
            conn.execute("UPDATE tasks SET status='ready' WHERE id IN (?, ?)", (parent, child))
        assert kb.block_task(
            conn, child, kind="dependency", subject="t_deadbeef", reason="wrong parent"
        )
        row = kb.get_task(conn, child)
        assert (row.status, row.block_identity_state, row.block_kind) == (
            "blocked", "unknown", "dependency"
        )
        assert kb.recompute_ready(conn) == 0


def test_dependency_identity_waits_for_all_parents(home):
    with kbc.connect_closing() as conn:
        first = kb.create_task(conn, title="first parent", assignee="worker")
        second = kb.create_task(conn, title="second parent", assignee="worker")
        child = kb.create_task(
            conn, title="child", assignee="worker", parents=[first, second]
        )
        with kb.write_txn(conn):
            conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (child,))
        assert kb.block_task(
            conn, child, kind="dependency", subject=first, reason="waiting"
        )
        parked = kb.get_task(conn, child)
        assert (parked.status, parked.block_identity_state) == ("todo", "known")
        assert kb.complete_task(conn, first, summary="first done")
        assert kb.get_task(conn, child).status == "todo"
        assert kb.complete_task(conn, second, summary="second done")
        assert kb.get_task(conn, child).status == "ready"


def test_dependency_with_terminal_parent_becomes_unknown_sticky(home):
    with kbc.connect_closing() as conn:
        parent = kb.create_task(conn, title="done parent", assignee="worker")
        child = kb.create_task(conn, title="child", assignee="worker", parents=[parent])
        assert kb.complete_task(conn, parent, summary="done")
        with kb.write_txn(conn):
            conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (child,))
        assert kb.block_task(
            conn, child, kind="dependency", subject=parent, reason="stale wait"
        )
        row = kb.get_task(conn, child)
        assert (row.status, row.block_identity_state, row.block_kind) == (
            "blocked", "unknown", "dependency"
        )
        assert kb.recompute_ready(conn) == 0


def test_unknown_todo_cannot_recompute_promote_or_claim(home):
    with kbc.connect_closing() as conn:
        tid = kb.create_task(conn, title="legacy dependency hold", assignee="worker")
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE tasks SET status='todo', block_kind='dependency', "
                "block_recurrences=1, block_identity_state='unknown' WHERE id=?",
                (tid,),
            )
        assert kb.recompute_ready(conn) == 0
        assert kb.get_task(conn, tid).status == "todo"
        assert kb.promote_task(conn, tid, actor="operator", dry_run=True)[0] is False
        assert kb.promote_task(conn, tid, actor="operator")[0] is False
        assert kb.claim_task(conn, tid, claimer="worker") is None


def test_completion_clears_block_identity(home):
    with kbc.connect_closing() as conn:
        tid = _ready_task(conn)
        assert kb.block_task(
            conn, tid, kind="capability", subject="missing-access", reason="waiting"
        )
        assert kb.complete_task(conn, tid, summary="operator closed the work")
        row = kb.get_task(conn, tid)
        assert (row.block_kind, row.block_recurrences, row.block_identity, row.block_identity_state) == (
            None, 0, None, "none"
        )


def test_legacy_migration_uses_adjacent_graph_evidence_and_is_idempotent(tmp_path):
    db_path = tmp_path / "legacy-kanban.db"
    with sqlite3.connect(db_path) as legacy:
        legacy.executescript(
            """
            CREATE TABLE tasks (
                id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL,
                created_at INTEGER NOT NULL, workspace_kind TEXT NOT NULL,
                block_kind TEXT, block_recurrences INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL,
                kind TEXT NOT NULL, payload TEXT, created_at INTEGER NOT NULL
            );
            CREATE TABLE task_links (
                parent_id TEXT NOT NULL, child_id TEXT NOT NULL,
                PRIMARY KEY (parent_id, child_id)
            );
            """
        )
        tasks = [
            ("t_00000001", "parent", "ready", None, 0),
            ("t_00000002", "ordinary", "todo", None, 0),
            ("t_00000003", "blocked", "blocked", "needs_input", 1),
            ("t_00000004", "graph child", "todo", None, 0),
            ("t_00000005", "malformed", "todo", None, 0),
        ]
        legacy.executemany(
            "INSERT INTO tasks (id,title,status,created_at,workspace_kind,block_kind,block_recurrences) "
            "VALUES (?,?,?,1,'scratch',?,?)",
            tasks,
        )
        events = [
            ("t_00000001", "created", "{}"),
            ("t_00000002", "created", "{}"),
            ("t_00000003", "created", "{}"),
            ("t_00000003", "blocked", '{"kind":"needs_input"}'),
            ("t_00000004", "created", "{}"),
            (
                "t_00000004",
                "dependency_wait",
                '{"reason":"parent_not_done","demoted":true,"parent":"t_00000001"}',
            ),
            (
                "t_00000004",
                "linked",
                '{"parent":"t_00000001","child":"t_00000004"}',
            ),
            ("t_00000005", "created", "{}"),
            ("t_00000005", "edited", "{"),
        ]
        legacy.executemany(
            "INSERT INTO task_events (task_id,kind,payload,created_at) VALUES (?,?,?,1)", events
        )
        legacy.execute(
            "INSERT INTO task_links (parent_id,child_id) VALUES ('t_00000001','t_00000004')"
        )

    kb.init_db(db_path)
    kb.init_db(db_path)
    with kbc.connect_closing(db_path) as conn:
        states = dict(conn.execute(
            "SELECT id, block_identity_state FROM tasks ORDER BY id"
        ).fetchall())
        assert states == {
            "t_00000001": "none",
            "t_00000002": "none",
            "t_00000003": "unknown",
            "t_00000004": "none",
            "t_00000005": "unknown",
        }
        migration_counts = dict(conn.execute(
            "SELECT task_id, COUNT(*) FROM task_events "
            "WHERE kind IN ('block_identity_migrated','block_identity_unknown') GROUP BY task_id"
        ).fetchall())
        assert migration_counts == {task_id: 1 for task_id in states}
