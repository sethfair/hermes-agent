"""Tests for kanban board export / import (``hermes_cli.kanban_transfer``).

The contract these pin down is "a board survives the trip to another
machine, and nothing that only made sense on the exporting machine comes
with it":

* Content round-trips — tasks, comments, links, events, attachment blobs.
* Runtime state does not — claims, worker PIDs, heartbeats, session ids,
  and gateway chat subscriptions are gone on the far side.
* Paths are re-anchored — attachment rows point into the importing
  board's tree, and tasks whose workspace cannot be rebuilt here are
  parked instead of being fed to the dispatcher.
* An import never mutates a board that already exists.
* A hostile archive cannot write outside the import destination.
"""

from __future__ import annotations

import json
import sys
import tarfile
import time
from pathlib import Path

import pytest

# Ensure the worktree (not the stale global clone) is first on sys.path.
_WORKTREE = Path(__file__).resolve().parents[2]
if str(_WORKTREE) not in sys.path:
    sys.path.insert(0, str(_WORKTREE))

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_transfer as kt
from hermes_cli.archive_safe import normalize_archive_parts, safe_extract_targz


@pytest.fixture
def kanban_root(tmp_path, monkeypatch):
    """Point kanban at an empty root, and hand back a switcher.

    Export and import have to run against two different machines' state.
    Calling the returned function re-points every kanban path helper at a
    fresh root, which is as close to "the other machine" as a unit test
    gets.
    """
    def _use(name: str) -> Path:
        root = tmp_path / name
        root.mkdir(exist_ok=True)
        monkeypatch.setenv("HERMES_HOME", str(root))
        monkeypatch.setenv("HERMES_KANBAN_HOME", str(root))
        for var in ("HERMES_KANBAN_DB", "HERMES_KANBAN_WORKSPACES_ROOT",
                    "HERMES_KANBAN_ATTACHMENTS_ROOT", "HERMES_KANBAN_BOARD"):
            monkeypatch.delenv(var, raising=False)
        kb._INITIALIZED_PATHS.clear()
        return root

    _use("source")
    return _use


def _seed_board(slug: str = "alpha") -> dict[str, str]:
    """Create a board with one task of each interesting shape."""
    kb.create_board(slug, name="Alpha Board")
    ids = {}
    with kbc.connect_closing(board=slug) as conn:
        ids["scratch"] = kb.create_task(
            conn, title="scratch task", body="body", assignee="coder"
        )
        ids["worktree"] = kb.create_task(
            conn, title="worktree task", assignee="coder",
            workspace_kind="worktree", workspace_path="/exporter/repo",
        )
        kb.add_comment(conn, ids["scratch"], "brooklyn", "a comment")
        kb.link_tasks(conn, ids["scratch"], ids["worktree"])
        kb.store_attachment_bytes(
            conn, ids["scratch"], "notes.txt", b"hello attachment", board=slug
        )
    return ids


def _claim(task_id: str, slug: str = "alpha") -> None:
    """Put a task into the state a live worker would leave behind."""
    with kbc.connect_closing(board=slug) as conn:
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE tasks SET status='running', claim_lock='lock-1', "
                "claim_expires=?, worker_pid=4242, last_heartbeat_at=?, "
                "session_id='sess-xyz', consecutive_failures=2 WHERE id=?",
                (int(time.time()) + 600, int(time.time()), task_id),
            )


def _subscribe(task_id: str, slug: str = "alpha") -> None:
    with kbc.connect_closing(board=slug) as conn:
        with kb.write_txn(conn):
            conn.execute(
                "INSERT INTO kanban_notify_subs "
                "(task_id, platform, chat_id, thread_id, created_at) "
                "VALUES (?, 'telegram', '12345', '', ?)",
                (task_id, int(time.time())),
            )


def _tasks_by_title(slug: str) -> dict[str, dict]:
    with kbc.connect_closing(board=slug) as conn:
        return {
            row["title"]: dict(row)
            for row in conn.execute("SELECT * FROM tasks").fetchall()
        }


def _seed_authenticated_state(
    slug: str, task_id: str, *, only: str | None = None
) -> None:
    with kbc.connect_closing(board=slug) as conn:
        event_id = conn.execute(
            "SELECT id FROM task_events WHERE task_id = ? ORDER BY id LIMIT 1",
            (task_id,),
        ).fetchone()[0]
        inserts = {
            "service_principals": (
            "INSERT INTO service_principals "
            "(board, principal_id, credential_id, operation_scope, enabled, registry_revision, created_at) "
            "VALUES (?, 'office', 'office-local', 'task.transition', 1, 1, 1)",
                (slug,),
            ),
            "configured_callers": (
            "INSERT INTO configured_callers "
            "(board, operation_scope, principal_id, credential_id) "
            "VALUES (?, 'task.transition', 'office', 'office-local')",
                (slug,),
            ),
            "principal_registry_events": (
                "INSERT INTO principal_registry_events "
                "(board, operation_scope, principal_id, credential_id, registry_revision, action, created_at) "
                "VALUES (?, 'task.transition', 'office', 'office-local', 1, 'provisioned', 1)",
                (slug,),
            ),
            "kanban_capabilities": (
            "INSERT INTO kanban_capabilities "
            "(board, capability, mode, required_consumer_version, cutover_evidence, updated_at) "
            "VALUES (?, 'action_receipts', 'live', '1.0.0', '{}', 1)",
                (slug,),
            ),
            "kanban_capability_events": (
                "INSERT INTO kanban_capability_events "
                "(board, capability, previous_mode, mode, required_consumer_version, cutover_evidence, created_at) "
                "VALUES (?, 'action_receipts', 'off', 'live', '1.0.0', '{}', 1)",
                (slug,),
            ),
            "action_receipts": (
            "INSERT INTO action_receipts "
            "(receipt_id, schema_version, board, task_id, operation_id, operation, "
            "principal_id, credential_id, action_generation, resulting_generation, "
            "park_generation, action_class, from_status, to_status, transition_json, "
            "transition_digest, request_digest, guard_fingerprint, outcome, event_id, committed_at) "
            "VALUES ('receipt-1', 1, ?, ?, 'operation-1', 'task.transition', "
            "'office', 'office-local', 0, 1, 0, 'office.approval', 'ready', 'todo', "
            "'{}', 'transition-digest', 'request-digest', 'guard-digest', 'committed', ?, 1)",
                (slug, task_id, event_id),
            ),
            "advisory_notices": (
                "INSERT INTO advisory_notices "
                "(notice_id, schema_version, board, task_id, operation_id, notice_kind, "
                "principal_id, credential_id, action_generation, park_generation, "
                "guard_fingerprint, content, content_digest, request_digest, event_id, committed_at) "
                "VALUES ('notice-1', 1, ?, ?, 'notice-operation-1', 'review', "
                "'office', 'office-local', 0, 0, 'guard-digest', 'Review', "
                "'content-digest', 'notice-request-digest', ?, 1)",
                (slug, task_id, event_id),
            ),
            "operation_keys": (
            "INSERT INTO operation_keys "
            "(board, operation_id, kind, record_id, principal_id, credential_id, digest) "
            "VALUES (?, 'operation-1', 'action', 'receipt-1', 'office', 'office-local', 'request-digest')",
                (slug,),
            ),
        }
        selected = inserts.items() if only is None else ((only, inserts[only]),)
        for _table, (sql, params) in selected:
            conn.execute(sql, params)
        conn.commit()


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------

def test_round_trip_preserves_content(kanban_root, tmp_path):
    _seed_board()
    archive = kt.export_board("alpha", str(tmp_path / "alpha"))["archive"]

    kanban_root("target")
    result = kt.import_board(archive)

    assert result["counts"]["tasks"] == 2
    assert result["counts"]["task_comments"] == 1
    assert result["counts"]["task_links"] == 1

    tasks = _tasks_by_title(result["board"])
    assert set(tasks) == {"scratch task", "worktree task"}
    assert tasks["scratch task"]["body"] == "body"
    assert tasks["scratch task"]["assignee"] == "coder"


def test_attachment_blob_travels_and_is_readable(kanban_root, tmp_path):
    _seed_board()
    archive = kt.export_board("alpha", str(tmp_path / "alpha"))["archive"]

    target_root = kanban_root("target")
    result = kt.import_board(archive)

    with kbc.connect_closing(board=result["board"]) as conn:
        row = conn.execute(
            "SELECT filename, stored_path FROM task_attachments"
        ).fetchone()

    assert row["filename"] == "notes.txt"
    stored = Path(row["stored_path"])
    # Re-anchored under the importing machine's board, not the exporter's.
    assert target_root in stored.parents
    assert stored.read_bytes() == b"hello attachment"


def test_export_without_attachments_drops_the_rows(kanban_root, tmp_path):
    _seed_board()
    archive = kt.export_board(
        "alpha", str(tmp_path / "alpha"), include_attachments=False
    )["archive"]

    kanban_root("target")
    result = kt.import_board(archive)

    # A row whose blob never travelled would be a broken download link in
    # every UI that lists it, so it is dropped and reported.
    assert result["counts"]["task_attachments"] == 0
    assert any("attachment" in w for w in result["warnings"])


def test_workspaces_are_never_exported(kanban_root, tmp_path):
    _seed_board()
    workspace = kb.workspaces_root("alpha") / "junk"
    workspace.mkdir(parents=True)
    (workspace / "huge.bin").write_bytes(b"x" * 1024)

    archive = kt.export_board("alpha", str(tmp_path / "alpha"))["archive"]

    with tarfile.open(archive, "r:gz") as tf:
        names = tf.getnames()
    assert not any("workspaces" in name for name in names)


# ---------------------------------------------------------------------------
# Machine-local state does not travel
# ---------------------------------------------------------------------------

def test_claimed_task_arrives_unclaimed_and_queued(kanban_root, tmp_path):
    ids = _seed_board()
    _claim(ids["scratch"])
    archive = kt.export_board("alpha", str(tmp_path / "alpha"))["archive"]

    kanban_root("target")
    result = kt.import_board(archive)

    task = _tasks_by_title(result["board"])["scratch task"]
    # A claim held by a PID on the exporting machine must not survive, or
    # the importing dispatcher inherits a lock nothing will ever release.
    assert task["status"] == "ready"
    assert task["claim_lock"] is None
    assert task["claim_expires"] is None
    assert task["worker_pid"] is None
    assert task["last_heartbeat_at"] is None
    assert task["current_run_id"] is None
    assert task["session_id"] is None
    assert task["consecutive_failures"] == 0


def test_gateway_subscriptions_never_travel(kanban_root, tmp_path):
    ids = _seed_board()
    _subscribe(ids["scratch"])
    archive = kt.export_board("alpha", str(tmp_path / "alpha"))["archive"]

    # Not merely dropped on import — the chat id must not be in the file
    # at all, because the archive is the thing that gets shared.
    kanban_root("target")
    result = kt.import_board(archive)
    with kbc.connect_closing(board=result["board"]) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM kanban_notify_subs"
        ).fetchone()[0] == 0

    assert b"12345" not in Path(archive).read_bytes()


def test_unresolvable_workspaces_are_parked_not_dispatched(kanban_root, tmp_path):
    _seed_board()
    archive = kt.export_board("alpha", str(tmp_path / "alpha"))["archive"]

    kanban_root("target")
    result = kt.import_board(archive)
    tasks = _tasks_by_title(result["board"])

    # The worktree lived on the exporting machine's disk. Letting the
    # dispatcher claim this would fail workspace resolution twice and trip
    # the failure breaker, so it waits for a human instead.
    assert tasks["worktree task"]["status"] == "triage"
    assert tasks["worktree task"]["workspace_path"] is None
    assert result["tasks_parked"] == 1

    # A scratch task needs no path — it regenerates one under this board.
    assert tasks["scratch task"]["status"] == "ready"
    assert tasks["scratch task"]["workspace_path"] is None


def test_board_metadata_loses_exporter_local_paths(kanban_root, tmp_path):
    kb.create_board("alpha", name="Alpha Board",
                    default_workdir="/exporter/repo", project_id="proj-1")
    archive = kt.export_board("alpha", str(tmp_path / "alpha"))["archive"]

    kanban_root("target")
    result = kt.import_board(archive)
    meta = kb.read_board_metadata(result["board"])

    assert meta["name"] == "Alpha Board"
    assert meta["default_workdir"] is None
    assert meta["project_id"] is None


# ---------------------------------------------------------------------------
# Import never overwrites
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["rename", "collision"])
def test_authenticated_archive_rejects_board_rebinding_without_partial_import(
    kanban_root, tmp_path, mode
):
    ids = _seed_board()
    _seed_authenticated_state("alpha", ids["scratch"])
    archive = kt.export_board("alpha", str(tmp_path / "alpha-auth"))["archive"]

    kanban_root("target")
    if mode == "collision":
        kb.create_board("alpha", name="Existing Alpha")
        target = "alpha-2"
        import_kwargs = {}
    else:
        target = "beta"
        import_kwargs = {"slug": target}

    with pytest.raises(ValueError, match="authenticated.*cannot be imported as"):
        kt.import_board(archive, **import_kwargs)

    assert not kb.board_dir(target).exists()
    if mode == "collision":
        assert kb.read_board_metadata("alpha")["name"] == "Existing Alpha"


@pytest.mark.parametrize("table", kt._AUTHENTICATED_BOARD_TABLES)
def test_each_authenticated_table_independently_prevents_rebinding(
    kanban_root, tmp_path, table
):
    ids = _seed_board()
    _seed_authenticated_state("alpha", ids["scratch"], only=table)
    archive = kt.export_board("alpha", str(tmp_path / f"alpha-{table}"))["archive"]

    kanban_root("target")
    with pytest.raises(ValueError, match="authenticated.*cannot be imported as"):
        kt.import_board(archive, slug="beta")

    assert not kb.board_dir("beta").exists()

def test_slug_collision_creates_a_new_board(kanban_root, tmp_path):
    _seed_board()
    archive = kt.export_board("alpha", str(tmp_path / "alpha"))["archive"]

    kanban_root("target")
    first = kt.import_board(archive)
    second = kt.import_board(archive)

    assert first["board"] == "alpha"
    assert second["board"] != first["board"]
    assert second["renamed"] is True
    assert second["requested_board"] == "alpha"
    # The board that was already there is untouched.
    assert len(_tasks_by_title(first["board"])) == 2


def test_import_never_targets_the_default_board(kanban_root, tmp_path):
    with kbc.connect_closing(board="default") as conn:
        kb.create_task(conn, title="exported default task")
    archive = kt.export_board("default", str(tmp_path / "default"))["archive"]

    kanban_root("target")
    with kbc.connect_closing(board="default") as conn:
        kb.create_task(conn, title="local default task")

    result = kt.import_board(archive)

    assert result["board"] != "default"
    assert set(_tasks_by_title("default")) == {"local default task"}
    assert set(_tasks_by_title(result["board"])) == {"exported default task"}


def test_explicit_slug_is_honoured(kanban_root, tmp_path):
    _seed_board()
    archive = kt.export_board("alpha", str(tmp_path / "alpha"))["archive"]

    kanban_root("target")
    result = kt.import_board(archive, "renamed-board", activate=True)

    assert result["board"] == "renamed-board"
    assert result["renamed"] is False
    assert kb.get_current_board() == "renamed-board"


# ---------------------------------------------------------------------------
# Hostile / malformed input
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("member", [
    "../escape.txt",
    "board/../../escape.txt",
    "/etc/passwd",
    "C:\\Windows\\system32",
    "board\\..\\..\\escape.txt",
])
def test_traversal_members_are_rejected(member):
    with pytest.raises(ValueError):
        normalize_archive_parts(member)


def test_extract_refuses_a_symlink_member(tmp_path):
    archive = tmp_path / "evil.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        link = tarfile.TarInfo("payload/link")
        link.type = tarfile.SYMTYPE
        link.linkname = "/etc/passwd"
        tf.addfile(link)

    with pytest.raises(ValueError, match="Unsupported archive member"):
        safe_extract_targz(archive, tmp_path / "out")


def test_import_rejects_a_non_kanban_archive(kanban_root, tmp_path):
    payload = tmp_path / "notaboard"
    payload.mkdir()
    (payload / "readme.txt").write_text("hi")
    archive = tmp_path / "other.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(payload, arcname="notaboard")

    with pytest.raises(ValueError, match="manifest"):
        kt.import_board(str(archive))


def test_import_rejects_a_future_format_version(kanban_root, tmp_path):
    _seed_board()
    archive = Path(kt.export_board("alpha", str(tmp_path / "alpha"))["archive"])

    staged = tmp_path / "restage"
    safe_extract_targz(archive, staged)
    manifest_path = staged / "alpha" / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["format_version"] = kt.ARCHIVE_FORMAT_VERSION + 1
    manifest_path.write_text(json.dumps(manifest))

    bumped = tmp_path / "bumped.tar.gz"
    with tarfile.open(bumped, "w:gz") as tf:
        tf.add(staged / "alpha", arcname="alpha")

    kanban_root("target")
    with pytest.raises(ValueError, match="newer than this Hermes"):
        kt.import_board(str(bumped))
