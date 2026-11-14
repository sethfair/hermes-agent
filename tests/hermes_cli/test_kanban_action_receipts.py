"""Behavioral contract for authenticated Kanban action receipts."""
# pyright: reportPossiblyUnboundVariable=false

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import threading
import time

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_receipts as receipts
try:  # Base-RED compatibility: the selected first test must collect before these modules exist.
    from hermes_cli.kanban_db_receipts import (
        ActionRequest,
        AdvisoryNoticeRequest,
        ProjectionRequest,
        action_receipts_mode,
        append_advisory_notice,
        commit_action,
        configure_action_receipts,
        guard_fingerprint,
        lookup_action_receipt,
        lookup_operation,
    )
    from hermes_cli.kanban_principal import (
        ActionReceiptError,
        AuthenticatedCallerContext,
        admit_configured_caller,
        disable_service_principal,
        provision_service_principal,
        validate_caller_context,
    )
except ModuleNotFoundError:
    pass


@pytest.fixture
def conn(tmp_path):
    connection = kbc.connect(tmp_path / "kanban.db")
    try:
        yield connection
    finally:
        connection.close()


def protected_claim_adapter(conn, task_id: str, caller_context=None):
    """Use the production seam when present; pinned base falls through to legacy claim."""
    try:
        from hermes_cli.kanban_principal import protected_claim_task
    except ModuleNotFoundError:
        return kb.claim_task(conn, task_id, ttl_seconds=None)
    return protected_claim_task(
        conn,
        task_id,
        caller_context=caller_context,
        board="default",
    )


def test_unadmitted_protected_claim_is_rejected_without_task_mutation(tmp_path):
    db_path = tmp_path / "kanban.db"
    with closing(kbc.connect(db_path)) as connection:
        task_id = kb.create_task(connection, title="protected", assignee="worker")
        before = kb.get_task(connection, task_id)
        before_events = kb.list_events(connection, task_id)

        with pytest.raises(Exception, match="unauthenticated_session"):
            protected_claim_adapter(connection, task_id, caller_context=None)

        after = kb.get_task(connection, task_id)
        assert after.status == before.status
        assert after.claim_lock == before.claim_lock
        assert kb.list_events(connection, task_id) == before_events


def test_configured_principal_is_admitted_and_revocation_is_effective(conn):
    provision_service_principal(
        conn,
        board="default",
        operation_scope="task.transition",
        principal_id="office",
        credential_id="office-local",
        configure=True,
    )
    context = admit_configured_caller(
        conn, board="default", operation_scope="task.transition"
    )
    assert context.registry_revision == 1
    assert validate_caller_context(
        conn, context, board="default", operation_scope="task.transition"
    ) == context

    disable_service_principal(conn, board="default", principal_id="office")
    with pytest.raises(ActionReceiptError, match="revoked_principal"):
        validate_caller_context(
            conn, context, board="default", operation_scope="task.transition"
        )
    provision_service_principal(
        conn,
        board="default",
        operation_scope="task.transition",
        principal_id="office",
        credential_id="office-local-v2",
        configure=True,
    )
    with pytest.raises(ActionReceiptError, match="unauthorized_principal|revoked_principal"):
        validate_caller_context(
            conn, context, board="default", operation_scope="task.transition"
        )
    history = conn.execute(
        """
        SELECT action, registry_revision FROM principal_registry_events
         WHERE board = 'default' AND principal_id = 'office' ORDER BY id
        """
    ).fetchall()
    assert [(row["action"], row["registry_revision"]) for row in history] == [
        ("provisioned", 1),
        ("disabled", 2),
        ("provisioned", 3),
    ]
    assert conn.execute(
        "SELECT disabled_at FROM service_principals WHERE principal_id = 'office'"
    ).fetchone()[0] is not None


def test_valid_but_wrong_configured_context_is_rejected(conn):
    provision_service_principal(
        conn,
        board="default",
        operation_scope="task.transition",
        principal_id="office",
        credential_id="office-local",
        configure=True,
    )
    wrong = AuthenticatedCallerContext(
        principal_id="other",
        credential_id="other-local",
        registry_revision=1,
        board="default",
        operation_scope="task.transition",
    )
    with pytest.raises(ActionReceiptError, match="unauthorized_principal"):
        validate_caller_context(
            conn, wrong, board="default", operation_scope="task.transition"
        )


def _live_transition_setup(conn):
    task_id = kb.create_task(conn, title="receipt task", assignee="worker")
    provision_service_principal(
        conn,
        board="default",
        operation_scope="task.transition",
        principal_id="office",
        credential_id="office-local",
        configure=True,
    )
    context = admit_configured_caller(
        conn, board="default", operation_scope="task.transition"
    )
    row = conn.execute(
        "SELECT status, action_generation, park_generation FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone()
    request = ActionRequest(
        schema_version=1,
        board="default",
        task_id=task_id,
        operation_id="op-transition-1",
        operation="task.transition",
        expected_action_generation=int(row["action_generation"]),
        expected_guard_fingerprint=guard_fingerprint(
            conn,
            task_id,
            action_class="office.approval",
            from_status=row["status"],
            to_status="todo",
            decision_revision=1,
        ),
        park_generation=int(row["park_generation"]),
        action_class="office.approval",
        from_status=row["status"],
        to_status="todo",
        transition={"status": "todo"},
        decision_revision=1,
        projection=ProjectionRequest(
            schema_version=1,
            kind="comment",
            comment_body="Authenticated approval recorded.",
        ),
    )
    return task_id, context, request


def _enable_live(conn):
    configure_action_receipts(
        conn,
        board="default",
        mode="live",
        required_consumer_version="1.0.0",
        cutover_evidence=json.dumps(
            {
                "schema_version": 1,
                "board": "default",
                "consumer_build": "office-test",
                "consumer_version": "1.0.0",
                "supported_receipt_schema_versions": [1],
                "legacy_readers_drained": True,
                "verifier": "repository-test",
                "verified_at": int(time.time()),
            },
            sort_keys=True,
        ),
    )


def _retarget_request(conn, request, *, from_status, to_status, operation_id):
    row = conn.execute(
        "SELECT action_generation, park_generation FROM tasks WHERE id = ?",
        (request.task_id,),
    ).fetchone()
    return replace(
        request,
        operation_id=operation_id,
        from_status=from_status,
        to_status=to_status,
        transition={"status": to_status},
        expected_action_generation=int(row["action_generation"]),
        park_generation=int(row["park_generation"]),
        expected_guard_fingerprint=guard_fingerprint(
            conn,
            request.task_id,
            action_class=request.action_class,
            from_status=from_status,
            to_status=to_status,
            decision_revision=request.decision_revision,
        ),
    )


def test_commit_api_rejects_missing_and_forged_context_before_mutation(conn):
    task_id, _context, request = _live_transition_setup(conn)
    before = dict(conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone())
    with pytest.raises(ActionReceiptError, match="unauthenticated_session"):
        commit_action(conn, request, caller_context=None)
    forged = AuthenticatedCallerContext(
        principal_id="forged",
        credential_id="forged-local",
        registry_revision=1,
        board="default",
        operation_scope="task.transition",
    )
    with pytest.raises(ActionReceiptError, match="unauthorized_principal"):
        commit_action(conn, request, caller_context=forged)
    assert dict(conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()) == before
    assert conn.execute("SELECT COUNT(*) FROM action_receipts").fetchone()[0] == 0


def test_capability_defaults_off_and_live_requires_cutover_evidence(conn):
    task_id, context, request = _live_transition_setup(conn)
    before = dict(conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone())

    assert action_receipts_mode(conn, board="default") == "off"
    with pytest.raises(ActionReceiptError, match="capability_disabled"):
        commit_action(conn, request, caller_context=context)
    assert dict(conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()) == before
    assert conn.execute("SELECT COUNT(*) FROM action_receipts").fetchone()[0] == 0

    with pytest.raises(ActionReceiptError, match="incompatible_consumer"):
        configure_action_receipts(conn, board="default", mode="live")
    _enable_live(conn)
    assert action_receipts_mode(conn, board="default") == "live"


def test_action_commit_is_atomic_exactly_replayable_and_conflict_safe(conn):
    task_id, context, request = _live_transition_setup(conn)
    _enable_live(conn)

    receipt = commit_action(conn, request, caller_context=context)
    retry = commit_action(conn, request, caller_context=context)

    assert retry == receipt
    task = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert task["status"] == "todo"
    assert receipt.resulting_generation == task["action_generation"]
    assert conn.execute("SELECT COUNT(*) FROM action_receipts").fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM task_events WHERE task_id = ? AND kind = 'authenticated_action'",
        (task_id,),
    ).fetchone()[0] == 1
    assert request.projection is not None
    assert conn.execute(
        "SELECT COUNT(*) FROM task_comments WHERE task_id = ? AND body = ?",
        (task_id, request.projection.comment_body),
    ).fetchone()[0] == 1
    assert lookup_action_receipt(
        conn,
        board="default",
        operation_id=request.operation_id,
        caller_context=context,
    ) == receipt

    conflicting = replace(request, to_status="triage", transition={"status": "triage"})
    with pytest.raises(ActionReceiptError, match="operation_conflict"):
        commit_action(conn, conflicting, caller_context=context)
    assert conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)).fetchone()[0] == "todo"

    configure_action_receipts(conn, board="default", mode="off")
    capability = conn.execute(
        """
        SELECT mode, required_consumer_version, cutover_evidence
          FROM kanban_capabilities WHERE board = 'default'
        """
    ).fetchone()
    assert capability["mode"] == "off"
    assert capability["required_consumer_version"] == "1.0.0"
    retained_evidence = json.loads(capability["cutover_evidence"])
    assert retained_evidence["consumer_build"] == "office-test"
    assert retained_evidence["legacy_readers_drained"] is True
    assert [
        tuple(row)
        for row in conn.execute(
            "SELECT previous_mode, mode FROM kanban_capability_events ORDER BY id"
        )
    ] == [("off", "live"), ("live", "off")]
    assert commit_action(conn, request, caller_context=context) == receipt
    assert lookup_action_receipt(
        conn,
        board="default",
        receipt_id=receipt.receipt_id,
        caller_context=context,
    ) == receipt
    new_operation = replace(request, operation_id="op-transition-2")
    with pytest.raises(ActionReceiptError, match="capability_disabled"):
        commit_action(conn, new_operation, caller_context=context)
    assert conn.execute("SELECT COUNT(*) FROM action_receipts").fetchone()[0] == 1


def test_no_comment_receipt_is_authoritative_without_projection(conn):
    _task_id, context, request = _live_transition_setup(conn)
    request = replace(request, projection=None, operation_id="op-no-comment")
    _enable_live(conn)
    receipt = commit_action(conn, request, caller_context=context)
    assert receipt.projection is None
    assert lookup_action_receipt(
        conn,
        board="default",
        receipt_id=receipt.receipt_id,
        caller_context=context,
    ) == receipt


def test_guard_and_generation_reject_stale_action_without_partial_writes(conn):
    task_id, context, request = _live_transition_setup(conn)
    _enable_live(conn)
    before_events = conn.execute(
        "SELECT COUNT(*) FROM task_events WHERE task_id = ?", (task_id,)
    ).fetchone()[0]
    conn.execute("UPDATE tasks SET body = 'legacy writer changed this' WHERE id = ?", (task_id,))
    conn.commit()

    with pytest.raises(ActionReceiptError, match="generation_conflict"):
        commit_action(conn, request, caller_context=context)
    assert conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)).fetchone()[0] == request.from_status
    assert conn.execute("SELECT COUNT(*) FROM action_receipts").fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM task_events WHERE task_id = ?", (task_id,)
    ).fetchone()[0] == before_events


def test_direct_writer_fences_cover_comments_links_runs_and_park(conn):
    task_id = kb.create_task(conn, title="fenced", assignee="worker")

    def generations():
        row = conn.execute(
            "SELECT action_generation, park_generation FROM tasks WHERE id = ?",
            (task_id,),
        ).fetchone()
        return int(row["action_generation"]), int(row["park_generation"])

    action, park = generations()
    other_id = kb.create_task(conn, title="other", assignee="worker")
    comment = conn.execute(
        "INSERT INTO task_comments (task_id, author, body, created_at) VALUES (?, 'legacy', 'x', 1)",
        (task_id,),
    )
    conn.execute(
        "INSERT INTO task_comments (task_id, author, body, created_at) VALUES (?, 'legacy', 'later', 2)",
        (task_id,),
    )
    assert generations()[0] > action
    action, _ = generations()
    other_before = conn.execute(
        "SELECT action_generation FROM tasks WHERE id = ?", (other_id,)
    ).fetchone()[0]
    conn.execute(
        "UPDATE task_comments SET task_id = ? WHERE id = ?", (other_id, comment.lastrowid)
    )
    assert generations()[0] > action
    assert conn.execute(
        "SELECT action_generation FROM tasks WHERE id = ?", (other_id,)
    ).fetchone()[0] > other_before
    action, _ = generations()

    parent_id = kb.create_task(conn, title="parent", assignee="worker")
    conn.execute(
        "INSERT INTO task_links (parent_id, child_id) VALUES (?, ?)",
        (parent_id, task_id),
    )
    assert generations()[0] > action
    action, _ = generations()
    replacement_parent = kb.create_task(conn, title="replacement parent", assignee="worker")
    parent_before = conn.execute(
        "SELECT action_generation FROM tasks WHERE id = ?", (parent_id,)
    ).fetchone()[0]
    replacement_before = conn.execute(
        "SELECT action_generation FROM tasks WHERE id = ?", (replacement_parent,)
    ).fetchone()[0]
    conn.execute(
        "UPDATE task_links SET parent_id = ? WHERE parent_id = ? AND child_id = ?",
        (replacement_parent, parent_id, task_id),
    )
    assert generations()[0] > action
    assert conn.execute(
        "SELECT action_generation FROM tasks WHERE id = ?", (parent_id,)
    ).fetchone()[0] > parent_before
    assert conn.execute(
        "SELECT action_generation FROM tasks WHERE id = ?", (replacement_parent,)
    ).fetchone()[0] > replacement_before
    action, _ = generations()

    conn.execute(
        "INSERT INTO task_runs (task_id, status, started_at) VALUES (?, 'running', 1)",
        (task_id,),
    )
    assert generations()[0] > action
    action, park = generations()

    conn.execute("UPDATE tasks SET status = 'scheduled' WHERE id = ?", (task_id,))
    next_action, next_park = generations()
    assert next_action > action
    assert next_park > park


def test_native_parent_hold_and_status_gates_fail_before_mutation(conn):
    task_id, context, request = _live_transition_setup(conn)
    _enable_live(conn)

    invalid = replace(
        request,
        operation_id="op-invalid-status",
        to_status="not-a-status",
        transition={"status": "not-a-status"},
    )
    with pytest.raises(ActionReceiptError, match="invalid_request"):
        commit_action(conn, invalid, caller_context=context)

    parent_id = kb.create_task(conn, title="open parent", assignee="worker")
    kb.link_tasks(conn, parent_id, task_id)
    gated = _retarget_request(
        conn,
        request,
        from_status="todo",
        to_status="ready",
        operation_id="op-parent-gated",
    )
    with pytest.raises(ActionReceiptError, match="parent_gate"):
        commit_action(conn, gated, caller_context=context)

    kb.unlink_tasks(conn, parent_id, task_id)
    assert kb.block_task(conn, task_id, reason="operator hold", kind="needs_input")
    held = _retarget_request(
        conn,
        request,
        from_status="blocked",
        to_status="ready",
        operation_id="op-held",
    )
    with pytest.raises(ActionReceiptError, match="hold_active"):
        commit_action(conn, held, caller_context=context)
    assert conn.execute("SELECT COUNT(*) FROM action_receipts").fetchone()[0] == 0


def test_storage_failure_rolls_back_action_event_receipt_and_projection(conn):
    task_id, context, request = _live_transition_setup(conn)
    _enable_live(conn)
    before = dict(conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone())
    conn.execute(
        """
        CREATE TEMP TRIGGER reject_receipt BEFORE INSERT ON action_receipts
        BEGIN SELECT RAISE(ABORT, 'injected receipt failure'); END
        """
    )

    with pytest.raises(ActionReceiptError, match="storage_unavailable"):
        commit_action(conn, request, caller_context=context)
    assert dict(conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()) == before
    assert conn.execute("SELECT COUNT(*) FROM action_receipts").fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM task_events WHERE task_id = ? AND kind = 'authenticated_action'",
        (task_id,),
    ).fetchone()[0] == 0
    assert request.projection is not None
    assert conn.execute(
        "SELECT COUNT(*) FROM task_comments WHERE task_id = ? AND body = ?",
        (task_id, request.projection.comment_body),
    ).fetchone()[0] == 0


def test_receipt_rows_and_bound_evidence_are_immutable(conn, monkeypatch):
    task_id, context, request = _live_transition_setup(conn)
    _enable_live(conn)
    receipt = commit_action(conn, request, caller_context=context)
    assert request.projection is not None
    assert receipt.projection is not None

    with pytest.raises(sqlite3.IntegrityError, match="immutable_action_receipt"):
        conn.execute(
            "UPDATE action_receipts SET action_class = 'tampered' WHERE receipt_id = ?",
            (receipt.receipt_id,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="immutable_action_receipt"):
        conn.execute("DELETE FROM action_receipts WHERE receipt_id = ?", (receipt.receipt_id,))
    with pytest.raises(sqlite3.IntegrityError, match="receipt_bound_comment"):
        conn.execute(
            "UPDATE task_comments SET body = 'forged' WHERE id = ?",
            (receipt.projection.comment_id,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="receipt_bound_event"):
        conn.execute("DELETE FROM task_events WHERE id = ?", (receipt.event_id,))

    assert kb.archive_task(conn, task_id)
    with pytest.raises(sqlite3.IntegrityError, match="receipt_bound_comment|receipt_bound_task"):
        kb.delete_archived_task(conn, task_id)
    monkeypatch.setattr(kb.time, "time", lambda: receipt.committed_at + 100)
    kb.gc_events(conn, older_than_seconds=0)
    assert conn.execute(
        "SELECT 1 FROM task_events WHERE id = ?", (receipt.event_id,)
    ).fetchone() is not None
    assert lookup_action_receipt(
        conn,
        board="default",
        receipt_id=receipt.receipt_id,
        caller_context=context,
    ) == receipt


def test_lookup_recomputes_request_digest_and_event_linkage(conn):
    _task_id, context, request = _live_transition_setup(conn)
    request = replace(request, projection=None, operation_id="op-corrupt-digest")
    _enable_live(conn)
    receipt = commit_action(conn, request, caller_context=context)
    conn.execute("DROP TRIGGER operation_keys_immutable_update")
    conn.execute("DROP TRIGGER action_receipts_immutable_update")
    forged = "0" * 64
    conn.execute(
        "UPDATE operation_keys SET digest = ? WHERE board = ? AND operation_id = ?",
        (forged, request.board, request.operation_id),
    )
    conn.execute(
        "UPDATE action_receipts SET request_digest = ? WHERE receipt_id = ?",
        (forged, receipt.receipt_id),
    )
    with pytest.raises(ActionReceiptError, match="receipt_missing"):
        lookup_action_receipt(
            conn,
            board="default",
            receipt_id=receipt.receipt_id,
            caller_context=context,
        )


def test_notice_is_no_transition_and_shares_operation_conflict_domain(conn):
    task_id = kb.create_task(conn, title="notice task", assignee="worker")
    provision_service_principal(
        conn,
        board="default",
        operation_scope="advisory.notice",
        principal_id="office",
        credential_id="office-local",
        configure=True,
    )
    context = admit_configured_caller(
        conn, board="default", operation_scope="advisory.notice"
    )
    _enable_live(conn)
    row = conn.execute(
        "SELECT status, action_generation, park_generation FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone()
    request = AdvisoryNoticeRequest(
        schema_version=1,
        board="default",
        task_id=task_id,
        notice_operation_id="op-notice-1",
        notice_kind="D-prime",
        park_generation=int(row["park_generation"]),
        expected_action_generation=int(row["action_generation"]),
        expected_guard_fingerprint=guard_fingerprint(conn, task_id),
        content="Decision is approaching its due window.",
    )

    notice = append_advisory_notice(conn, request, caller_context=context)
    assert append_advisory_notice(conn, request, caller_context=context) == notice
    assert conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)).fetchone()[0] == row["status"]
    assert lookup_operation(
        conn,
        board="default",
        operation_id=request.notice_operation_id,
        caller_context=context,
    ) == notice
    with pytest.raises(ActionReceiptError, match="operation_conflict"):
        append_advisory_notice(
            conn,
            replace(request, content="Conflicting notice content."),
            caller_context=context,
        )


def test_concurrent_exact_retries_converge_on_one_receipt(tmp_path):
    db_path = tmp_path / "concurrent.db"
    with closing(kbc.connect(db_path)) as setup:
        _task_id, context, request = _live_transition_setup(setup)
        _enable_live(setup)
    assert request.projection is not None

    barrier = threading.Barrier(2)

    def attempt():
        with closing(kbc.connect(db_path)) as connection:
            barrier.wait(timeout=10)
            return commit_action(connection, request, caller_context=context)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = [future.result() for future in (pool.submit(attempt), pool.submit(attempt))]

    assert first == second
    with closing(kbc.connect(db_path)) as verify:
        assert verify.execute("SELECT COUNT(*) FROM action_receipts").fetchone()[0] == 1
        assert verify.execute(
            "SELECT COUNT(*) FROM task_events WHERE kind = 'authenticated_action'"
        ).fetchone()[0] == 1
        assert verify.execute(
            "SELECT COUNT(*) FROM task_comments WHERE body = ?",
            (request.projection.comment_body,),
        ).fetchone()[0] == 1


def test_action_and_notice_race_share_one_operation_key(tmp_path):
    db_path = tmp_path / "cross-kind.db"
    with closing(kbc.connect(db_path)) as setup:
        task_id, action_context, action_request = _live_transition_setup(setup)
        action_request = replace(action_request, operation_id="op-shared-kind")
        provision_service_principal(
            setup,
            board="default",
            operation_scope="advisory.notice",
            principal_id="office",
            credential_id="office-local",
            configure=True,
        )
        notice_context = admit_configured_caller(
            setup, board="default", operation_scope="advisory.notice"
        )
        row = setup.execute(
            "SELECT action_generation, park_generation FROM tasks WHERE id = ?",
            (task_id,),
        ).fetchone()
        notice_request = AdvisoryNoticeRequest(
            schema_version=1,
            board="default",
            task_id=task_id,
            notice_operation_id="op-shared-kind",
            notice_kind="S",
            park_generation=int(row["park_generation"]),
            expected_action_generation=int(row["action_generation"]),
            expected_guard_fingerprint=guard_fingerprint(setup, task_id),
            content="Shared-key race advisory.",
        )
        _enable_live(setup)

    barrier = threading.Barrier(2)

    def action_attempt():
        with closing(kbc.connect(db_path)) as connection:
            barrier.wait(timeout=10)
            try:
                commit_action(connection, action_request, caller_context=action_context)
                return "committed"
            except ActionReceiptError as exc:
                return exc.code

    def notice_attempt():
        with closing(kbc.connect(db_path)) as connection:
            barrier.wait(timeout=10)
            try:
                append_advisory_notice(connection, notice_request, caller_context=notice_context)
                return "committed"
            except ActionReceiptError as exc:
                return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = (pool.submit(action_attempt), pool.submit(notice_attempt))
        outcomes = [future.result() for future in futures]
    assert sorted(outcomes) == ["committed", "operation_conflict"]
    with closing(kbc.connect(db_path)) as verify:
        assert verify.execute("SELECT COUNT(*) FROM operation_keys").fetchone()[0] == 1
        assert verify.execute(
            "SELECT (SELECT COUNT(*) FROM action_receipts) + "
            "(SELECT COUNT(*) FROM advisory_notices)"
        ).fetchone()[0] == 1


def test_legacy_schema_migrates_default_off_and_tracks_old_writer_changes(tmp_path):
    db_path = tmp_path / "legacy.db"
    raw = sqlite3.connect(db_path)
    raw.executescript(kb.SCHEMA_SQL)
    for table in (
        "operation_keys",
        "action_receipts",
        "advisory_notices",
        "kanban_capabilities",
        "configured_callers",
        "service_principals",
    ):
        raw.execute(f"DROP TABLE {table}")
    raw.execute(
        "INSERT INTO tasks (id, title, status, created_at) VALUES ('legacy-task', 'Legacy', 'ready', 1)"
    )
    raw.commit()
    raw.close()

    with closing(kbc.connect(db_path)) as migrated:
        row = migrated.execute(
            "SELECT action_generation, park_generation FROM tasks WHERE id = 'legacy-task'"
        ).fetchone()
        assert tuple(row) == (0, 0)
        assert action_receipts_mode(migrated, board="default") == "off"
        migrated.execute("UPDATE tasks SET body = 'old reader write' WHERE id = 'legacy-task'")
        migrated.commit()
        assert migrated.execute(
            "SELECT action_generation FROM tasks WHERE id = 'legacy-task'"
        ).fetchone()[0] == 1
        for table in (
            "operation_keys",
            "action_receipts",
            "advisory_notices",
            "kanban_capabilities",
            "configured_callers",
            "service_principals",
        ):
            assert migrated.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
            ).fetchone() is not None


def test_board_a_rejects_valid_board_b_context_without_mutation(conn, tmp_path):
    task_id = kb.create_task(conn, title="physical board", assignee="worker")
    other_path = tmp_path / "other.db"
    raw_other = sqlite3.connect(other_path)
    raw_other.executescript(kb.SCHEMA_SQL)
    raw_other.execute(
        "INSERT INTO kanban_board_identity (singleton, board) VALUES (1, 'other')"
    )
    raw_other.commit()
    raw_other.close()
    with closing(kbc.connect(other_path, board="other")) as other:
        provision_service_principal(
            other,
            board="other",
            operation_scope="task.transition",
            principal_id="office",
            credential_id="office-local",
            configure=True,
        )
        context = admit_configured_caller(
            other, board="other", operation_scope="task.transition"
        )
    row = conn.execute(
        "SELECT status, action_generation, park_generation FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone()
    request = ActionRequest(
        schema_version=1,
        board="other",
        task_id=task_id,
        operation_id="op-cross-board",
        operation="task.transition",
        expected_action_generation=int(row["action_generation"]),
        expected_guard_fingerprint=guard_fingerprint(
            conn,
            task_id,
            action_class="office.approval",
            from_status=row["status"],
            to_status="todo",
            decision_revision=1,
        ),
        park_generation=int(row["park_generation"]),
        action_class="office.approval",
        from_status=row["status"],
        to_status="todo",
        transition={"status": "todo"},
        decision_revision=1,
    )
    with pytest.raises(ActionReceiptError, match="board_mismatch"):
        commit_action(conn, request, caller_context=context)
    assert conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)).fetchone()[0] == "ready"
    assert conn.execute("SELECT COUNT(*) FROM operation_keys").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM action_receipts").fetchone()[0] == 0


@pytest.mark.parametrize(
    "evidence",
    [
        "free-form evidence",
        json.dumps(
            {
                "schema_version": 1,
                "board": "default",
                "consumer_build": "office-test",
                "consumer_version": "0.9.0",
                "supported_receipt_schema_versions": [1],
                "legacy_readers_drained": True,
                "verifier": "repository-test",
                "verified_at": int(time.time()),
            }
        ),
        json.dumps(
            {
                "schema_version": 1,
                "board": "default",
                "consumer_build": "office-test",
                "consumer_version": "1.0.0",
                "supported_receipt_schema_versions": [1],
                "legacy_readers_drained": False,
                "verifier": "repository-test",
                "verified_at": int(time.time()),
            }
        ),
        json.dumps(
            {
                "schema_version": 1,
                "board": "default",
                "consumer_build": "office-test",
                "consumer_version": "1.0.0",
                "supported_receipt_schema_versions": [1],
                "legacy_readers_drained": True,
                "verifier": "repository-test",
                "verified_at": int(time.time()) - 86_401,
            }
        ),
    ],
)
def test_live_rejects_malformed_incompatible_or_undrained_evidence(conn, evidence):
    with pytest.raises(ActionReceiptError, match="incompatible_consumer"):
        configure_action_receipts(
            conn,
            board="default",
            mode="live",
            required_consumer_version="1.0.0",
            cutover_evidence=evidence,
        )
    assert action_receipts_mode(conn, board="default") == "off"


def test_lost_commit_ack_is_transaction_unknown_and_same_key_recovers_action(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "lost-ack-action.db"
    with closing(kbc.connect(db_path)) as connection:
        _task_id, context, request = _live_transition_setup(connection)
        _enable_live(connection)
        original = receipts.write_txn

        @contextmanager
        def commit_then_lose_ack(conn):
            with original(conn):
                yield conn
            raise RuntimeError("injected lost commit acknowledgement")

        monkeypatch.setattr(receipts, "write_txn", commit_then_lose_ack)
        with pytest.raises(ActionReceiptError, match="transaction_unknown"):
            commit_action(connection, request, caller_context=context)

    monkeypatch.setattr(receipts, "write_txn", original)
    with closing(kbc.connect(db_path)) as retry:
        recovered = commit_action(retry, request, caller_context=context)
        assert recovered.operation_id == request.operation_id
        assert retry.execute("SELECT COUNT(*) FROM action_receipts").fetchone()[0] == 1
        assert retry.execute("SELECT COUNT(*) FROM operation_keys").fetchone()[0] == 1


def test_lost_commit_ack_is_transaction_unknown_and_same_key_recovers_notice(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "lost-ack-notice.db"
    with closing(kbc.connect(db_path)) as connection:
        task_id = kb.create_task(connection, title="notice ack", assignee="worker")
        provision_service_principal(
            connection,
            board="default",
            operation_scope="advisory.notice",
            principal_id="office",
            credential_id="office-local",
            configure=True,
        )
        context = admit_configured_caller(
            connection, board="default", operation_scope="advisory.notice"
        )
        _enable_live(connection)
        row = connection.execute(
            "SELECT action_generation, park_generation FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        request = AdvisoryNoticeRequest(
            schema_version=1,
            board="default",
            task_id=task_id,
            notice_operation_id="op-lost-ack-notice",
            notice_kind="Z",
            park_generation=int(row["park_generation"]),
            expected_action_generation=int(row["action_generation"]),
            expected_guard_fingerprint=guard_fingerprint(connection, task_id),
            content="Lost acknowledgement notice.",
        )
        original = receipts.write_txn

        @contextmanager
        def commit_then_lose_ack(conn):
            with original(conn):
                yield conn
            raise RuntimeError("injected lost commit acknowledgement")

        monkeypatch.setattr(receipts, "write_txn", commit_then_lose_ack)
        with pytest.raises(ActionReceiptError, match="transaction_unknown"):
            append_advisory_notice(connection, request, caller_context=context)

    monkeypatch.setattr(receipts, "write_txn", original)
    with closing(kbc.connect(db_path)) as retry:
        recovered = append_advisory_notice(retry, request, caller_context=context)
        assert recovered.operation_id == request.notice_operation_id
        assert retry.execute("SELECT COUNT(*) FROM advisory_notices").fetchone()[0] == 1
        assert retry.execute("SELECT COUNT(*) FROM operation_keys").fetchone()[0] == 1


@pytest.mark.parametrize("related_write", ["comment", "link", "hold"])
def test_related_write_makes_precomputed_action_stale(conn, related_write):
    task_id, context, request = _live_transition_setup(conn)
    _enable_live(conn)
    if related_write == "comment":
        kb.add_comment(conn, task_id, "legacy", "intervening comment")
    elif related_write == "link":
        parent_id = kb.create_task(conn, title="parent", assignee="worker")
        kb.link_tasks(conn, parent_id, task_id)
    else:
        kb.block_task(conn, task_id, reason="intervening hold", kind="needs_input")

    with pytest.raises(
        ActionReceiptError, match="generation_conflict|status_conflict|hold_active"
    ):
        commit_action(conn, request, caller_context=context)
    assert conn.execute("SELECT COUNT(*) FROM action_receipts").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM operation_keys").fetchone()[0] == 0


def test_producer_off_retains_markers_visible_to_legacy_sql_reader(conn):
    _task_id, context, request = _live_transition_setup(conn)
    _enable_live(conn)
    receipt = commit_action(conn, request, caller_context=context)
    configure_action_receipts(conn, board="default", mode="off")
    assert receipt.projection is not None
    assert request.projection is not None

    legacy_comment = conn.execute(
        "SELECT author, body FROM task_comments WHERE id = ?",
        (receipt.projection.comment_id,),
    ).fetchone()
    legacy_event = conn.execute(
        "SELECT kind, payload FROM task_events WHERE id = ?", (receipt.event_id,)
    ).fetchone()
    assert tuple(legacy_comment) == (
        "action-receipt",
        request.projection.comment_body,
    )
    assert legacy_event["kind"] == "authenticated_action"
    assert json.loads(legacy_event["payload"])["receipt_id"] == receipt.receipt_id
    with pytest.raises(ActionReceiptError, match="capability_disabled"):
        commit_action(
            conn,
            replace(request, operation_id="op-after-off"),
            caller_context=context,
        )


def test_exact_base_schema_migrates_idempotently_across_reconnects(tmp_path):
    db_path = tmp_path / "exact-base.db"
    fixture = Path(__file__).parents[1] / "fixtures" / "kanban_pre_action_receipts.sql"
    raw = sqlite3.connect(db_path)
    raw.executescript(fixture.read_text(encoding="utf-8"))
    raw.execute(
        "INSERT INTO tasks (id, title, status, created_at) VALUES ('base-task', 'Base', 'ready', 1)"
    )
    raw.commit()
    raw.close()

    inventories = []
    for _ in range(3):
        kbc.init_db(db_path)
        with closing(kbc.connect(db_path)) as migrated:
            row = migrated.execute(
                "SELECT action_generation, park_generation FROM tasks WHERE id = 'base-task'"
            ).fetchone()
            assert tuple(row) == (0, 0)
            assert action_receipts_mode(migrated, board="default") == "off"
            inventories.append(
                [
                    tuple(item)
                    for item in migrated.execute(
                        "SELECT type, name, sql FROM sqlite_master "
                        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
                    )
                ]
            )
    assert inventories[0] == inventories[1] == inventories[2]


def test_action_and_notice_malformed_schema_versions_fail_closed(conn):
    _task_id, action_context, action_request = _live_transition_setup(conn)
    action_request = replace(action_request, projection=None, operation_id="op-bad-action-schema")
    _enable_live(conn)
    receipt = commit_action(conn, action_request, caller_context=action_context)
    conn.execute("DROP TRIGGER action_receipts_immutable_update")
    conn.execute("PRAGMA ignore_check_constraints=ON")
    conn.execute(
        "UPDATE action_receipts SET schema_version = 2 WHERE receipt_id = ?",
        (receipt.receipt_id,),
    )
    with pytest.raises(ActionReceiptError, match="receipt_missing"):
        lookup_action_receipt(
            conn,
            board="default",
            receipt_id=receipt.receipt_id,
            caller_context=action_context,
        )
    conn.execute("PRAGMA ignore_check_constraints=OFF")

    task_id = kb.create_task(conn, title="bad notice", assignee="worker")
    provision_service_principal(
        conn,
        board="default",
        operation_scope="advisory.notice",
        principal_id="office",
        credential_id="office-local",
        configure=True,
    )
    notice_context = admit_configured_caller(
        conn, board="default", operation_scope="advisory.notice"
    )
    row = conn.execute(
        "SELECT action_generation, park_generation FROM tasks WHERE id = ?", (task_id,)
    ).fetchone()
    notice_request = AdvisoryNoticeRequest(
        schema_version=1,
        board="default",
        task_id=task_id,
        notice_operation_id="op-bad-notice-schema",
        notice_kind="S",
        park_generation=int(row["park_generation"]),
        expected_action_generation=int(row["action_generation"]),
        expected_guard_fingerprint=guard_fingerprint(conn, task_id),
        content="Malformed authority probe.",
    )
    notice = append_advisory_notice(conn, notice_request, caller_context=notice_context)
    conn.execute("DROP TRIGGER advisory_notices_immutable_update")
    conn.execute("PRAGMA ignore_check_constraints=ON")
    conn.execute(
        "UPDATE advisory_notices SET schema_version = 2 WHERE notice_id = ?",
        (notice.notice_id,),
    )
    with pytest.raises(ActionReceiptError, match="receipt_missing"):
        lookup_operation(
            conn,
            board="default",
            operation_id=notice.operation_id,
            caller_context=notice_context,
        )
    conn.execute("PRAGMA ignore_check_constraints=OFF")
