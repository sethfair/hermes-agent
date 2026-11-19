"""Creation, graph, readiness, and block transition operations."""

# Runtime names are injected by bind(); TYPE_CHECKING imports keep static analysis exact.
# ruff: noqa: TC004

from __future__ import annotations

import json
import secrets
import sqlite3
import time
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from hermes_cli.kanban_db_task_state import (
        BlockTransitionOutcome,
        CreateTransitionOutcome,
        DecomposeTransitionOutcome,
        GraphTransitionOutcome,
        SwarmTransitionOutcome,
        TransitionOutcome,
        _AppliedTransition,
        _BlockRequest,
        _CreateRequest,
        _DecomposeRequest,
        _KernelResult,
        _LinkRequest,
        _ReadinessRequest,
        _SwarmRequest,
        _UnlinkRequest,
        _compatibility_lifecycle_operation_id,
        _insert_receipt,
        _load_receipt,
        _receipt_outcome,
        _request_hash,
        _rollback,
        _run_operation,
        _validate_ingress,
        canonicalize_block_identity,
    )


def bind(owner) -> None:
    """Bind initialized owner primitives without duplicating storage authority."""
    globals().update({
        name: value for name, value in vars(owner).items()
        if not name.startswith("__")
    })


def _apply_link(
    private: sqlite3.Connection, request: _LinkRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    if request.parent_id == request.task_id:
        raise ValueError("a task cannot depend on itself")
    missing = kb._missing_task_ids(private, [request.parent_id, request.task_id])
    if missing:
        raise ValueError(f"unknown task(s): {', '.join(missing)}")
    child = private.execute(
        "SELECT status,current_run_id FROM tasks WHERE id=?", (request.task_id,),
    ).fetchone()
    if child["status"] == "running" and (
        request.expected_child_run_id is None
        or child["current_run_id"] != request.expected_child_run_id
    ):
        raise ValueError(
            f"cannot link {request.parent_id} -> {request.task_id}: child is already running"
        )
    if kb._would_cycle(private, request.parent_id, request.task_id):
        raise ValueError(
            f"linking {request.parent_id} -> {request.task_id} would create a cycle"
        )
    private.execute(
        "INSERT OR IGNORE INTO task_links (parent_id,child_id) VALUES (?,?)",
        (request.parent_id, request.task_id),
    )
    gated = False
    if kb._task_status(private, request.parent_id) not in {"done", "archived"}:
        updated = private.execute(
            "UPDATE tasks SET status='todo' WHERE id=? AND status='ready'",
            (request.task_id,),
        )
        gated = updated.rowcount == 1
        if gated:
            kb._append_event(
                private,
                request.task_id,
                "dependency_wait",
                {"reason": "parent_not_done", "demoted": True, "parent": request.parent_id},
            )
    kb._append_event(
        private,
        request.task_id,
        "linked",
        {"parent": request.parent_id, "child": request.task_id, "gated": gated},
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    kb._inherit_notify_subs(private, request.task_id, (request.parent_id,))
    return _AppliedTransition(TransitionOutcome(
        True, request.operation_id, event_id, "none"
    ))


def _apply_unlink(
    private: sqlite3.Connection, request: _UnlinkRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    removed = private.execute(
        "DELETE FROM task_links WHERE parent_id=? AND child_id=?",
        (request.parent_id, request.task_id),
    )
    if removed.rowcount != 1:
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="link_not_found"
        ))
    kb._append_event(
        private,
        request.task_id,
        "unlinked",
        {"parent": request.parent_id, "child": request.task_id, "removed": True},
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _AppliedTransition(TransitionOutcome(
        True, request.operation_id, event_id, "none"
    ))


def _apply_create(
    private: sqlite3.Connection, request: _CreateRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    parent_rows: dict[str, sqlite3.Row] = {}
    if request.parents:
        placeholders = ",".join("?" for _ in request.parents)
        parent_rows = {
            row["id"]: row
            for row in private.execute(
                f"SELECT id,status,tenant FROM tasks WHERE id IN ({placeholders})",
                request.parents,
            ).fetchall()
        }
        missing = [parent for parent in request.parents if parent not in parent_rows]
        if missing:
            raise ValueError(f"unknown parent task(s): {', '.join(missing)}")
    tenant = request.tenant
    if tenant is None:
        tenant = next(
            (parent_rows[parent]["tenant"] for parent in request.parents
             if parent_rows[parent]["tenant"]),
            None,
        )
    if request.initial_status == "blocked":
        status = "blocked"
    elif request.triage:
        status = "triage"
    elif any(row["status"] != "done" for row in parent_rows.values()):
        status = "todo"
    else:
        status = "ready"
    private.execute(
        "INSERT INTO tasks (id,title,body,assignee,status,priority,created_by,created_at,"
        "workspace_kind,workspace_path,branch_name,project_id,tenant,idempotency_key,"
        "max_runtime_seconds,skills,max_retries,model_override,provider_override,"
        "reasoning_effort,goal_mode,goal_max_turns,session_id,completion_contract) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            request.task_id, request.title, request.body, request.assignee, status,
            request.priority, request.created_by, request.created_at,
            request.workspace_kind, request.workspace_path, request.branch_name,
            request.project_id, tenant, request.idempotency_key,
            request.max_runtime_seconds, request.skills_json, request.max_retries,
            request.model_override, request.provider_override, request.reasoning_effort,
            1 if request.goal_mode else 0, request.goal_max_turns, request.session_id,
            request.completion_contract,
        ),
    )
    for parent in request.parents:
        private.execute(
            "INSERT OR IGNORE INTO task_links (parent_id,child_id) VALUES (?,?)",
            (parent, request.task_id),
        )
    kb._append_event(
        private,
        request.task_id,
        "created",
        {
            "assignee": request.assignee,
            "status": status,
            "parents": list(request.parents),
            "creator_task_id": request.creator_task_id,
            "tenant": tenant,
            "workspace_kind": request.workspace_kind,
            "workspace_path": request.workspace_path,
            "branch_name": request.branch_name,
            "project_id": request.project_id,
            "skills": json.loads(request.skills_json) if request.skills_json else None,
            "goal_mode": bool(request.goal_mode) or None,
            "model_override": request.model_override,
            "provider_override": request.provider_override,
        },
    )
    if status == "blocked":
        kb._append_event(
            private,
            request.task_id,
            "blocked",
            {"reason": "initial_status", "status": "blocked",
             "actor": request.created_by or "user"},
        )
    if status == "todo":
        gating = [
            parent for parent in request.parents
            if parent_rows[parent]["status"] not in {"done", "archived"}
        ]
        if gating:
            kb._append_event(
                private,
                request.task_id,
                "dependency_wait",
                {"reason": "parent_not_done", "parent": gating[0]},
            )
    if request.creator_task_id:
        private.execute(
            "UPDATE tasks SET session_id=COALESCE(session_id,"
            "(SELECT session_id FROM tasks WHERE id=?)) WHERE id=?",
            (request.creator_task_id, request.task_id),
        )
        kb._inherit_notify_subs(
            private,
            request.task_id,
            (request.creator_task_id,),
            created_at=request.created_at,
        )
    kb._inherit_notify_subs(
        private,
        request.task_id,
        request.parents,
        created_at=request.created_at,
    )
    event_id = private.execute(
        "SELECT id FROM task_events WHERE task_id=? ORDER BY id DESC LIMIT 1",
        (request.task_id,),
    ).fetchone()[0]
    return _AppliedTransition(TransitionOutcome(
        True, request.operation_id, event_id, "none"
    ))


def _apply_swarm_create(
    private: sqlite3.Connection, request: _SwarmRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb
    # A swarm is one closed typed composite: every child create carries its own
    # receipt, while the topology comment and root activation share this outer
    # transaction and the final create_swarm receipt.
    all_ids = (
        request.task_id, *request.worker_ids, request.verifier_id,
        request.synthesizer_id,
    )
    branch_by_id = dict(zip(all_ids, request.branch_names))
    path_by_id = dict(zip(all_ids, request.workspace_paths))

    def create_one(
        task_id: str, *, title: str, body: str, assignee: str,
        parents: tuple[str, ...] = (), priority: int = 0,
        skills: Optional[tuple[str, ...]] = None,
        max_runtime_seconds: Optional[int] = None,
        initial_status: str = "running",
        idempotency_key: Optional[str] = None,
    ) -> None:
        command = _CreateRequest(
            task_id, request.actor_scope, f"{request.operation_id}:create:{task_id}",
            title, body, assignee, request.created_by, request.created_at,
            request.workspace_kind, path_by_id.get(task_id), branch_by_id.get(task_id),
            request.project_id, request.tenant, priority, parents, False,
            initial_status, idempotency_key, max_runtime_seconds,
            json.dumps(list(skills)) if skills else None, None, None, None, None,
            False, None, None, None, "local-only",
        )
        outcome = _run_operation(private, command).outcome
        if not outcome.committed:
            raise RuntimeError(outcome.refusal or f"could not create swarm task {task_id}")

    create_one(
        request.task_id,
        title=request.root_title,
        body=(
            "Kanban Swarm v1 planning/root card. This card is completed "
            "immediately so parallel workers can start while it remains the "
            f"shared blackboard and audit anchor.\n\nGoal:\n{request.goal}"
        ),
        assignee=request.created_by,
        priority=request.priority,
        initial_status="blocked",
        idempotency_key=request.idempotency_key,
    )
    context = (
        f"\n\n## Swarm protocol\n- Swarm root / shared blackboard: `{request.task_id}`.\n"
        "- Read sibling/parent handoffs from Kanban context before working.\n"
        "- Put machine-readable facts in completion metadata.\n"
        "- Put cross-worker notes on the root task using structured comments.\n"
        f"- Goal: {request.goal}\n"
    )
    for worker_id, worker in zip(request.worker_ids, request.workers):
        profile, title, body, skills, worker_priority, max_runtime = worker
        create_one(
            worker_id,
            title=title,
            body=body + context,
            assignee=profile,
            parents=(request.task_id,),
            priority=worker_priority or request.priority,
            skills=skills,
            max_runtime_seconds=max_runtime,
        )
    create_one(
        request.verifier_id,
        title=request.verifier_title,
        body=(
            "Review every worker handoff and blackboard update. Gate the swarm: "
            "complete only with metadata {\"gate\": \"pass\"} when evidence is "
            "sufficient; otherwise block with exact missing work." + context
        ),
        assignee=request.verifier_assignee,
        parents=request.worker_ids,
        priority=request.priority,
        skills=("requesting-code-review",),
    )
    create_one(
        request.synthesizer_id,
        title=request.synthesizer_title,
        body=(
            "Synthesize the verified worker outputs into the final deliverable. "
            "Do not start until the verifier has passed the gate." + context
        ),
        assignee=request.synthesizer_assignee,
        parents=(request.verifier_id,),
        priority=request.priority,
        skills=("humanizer",),
    )
    topology = {
        "root_id": request.task_id,
        "worker_ids": list(request.worker_ids),
        "verifier_id": request.verifier_id,
        "synthesizer_id": request.synthesizer_id,
        "goal": request.goal,
    }
    private.execute(
        "INSERT INTO task_comments (task_id,author,body,created_at) VALUES (?,?,?,?)",
        (
            request.task_id,
            request.created_by,
            "[swarm:blackboard] " + json.dumps(
                {"key": "topology", "value": topology},
                ensure_ascii=False,
                sort_keys=True,
            ),
            request.created_at,
        ),
    )
    summary = "Swarm topology planned; root remains the shared blackboard."
    updated = private.execute(
        "UPDATE tasks SET status='done',completed_at=?,claim_lock=NULL,"
        "claim_expires=NULL,worker_pid=NULL WHERE id=? AND status='blocked'",
        (request.created_at, request.task_id),
    )
    if updated.rowcount != 1:
        raise RuntimeError("could not activate the completed swarm topology")
    run_id = kb._synthesize_ended_run(
        private,
        request.task_id,
        outcome="completed",
        summary=summary,
        metadata={
            "kind": "kanban_swarm_v1",
            "goal": request.goal,
            "worker_count": len(request.worker_ids),
        },
    )
    kb._append_event(
        private,
        request.task_id,
        "completed",
        {"result_len": 0, "summary": summary},
        run_id=run_id,
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _AppliedTransition(TransitionOutcome(
        True, request.operation_id, event_id, "none"
    ))


def _apply_decompose(
    private: sqlite3.Connection, request: _DecomposeRequest,
) -> _AppliedTransition:
    from hermes_cli import kanban_db as kb

    if len(request.children) != len(request.child_ids):
        raise ValueError("child_ids must match children")
    if len(set(request.child_ids)) != len(request.child_ids):
        raise ValueError("child_ids must be unique")
    if kb._has_unknown_block_identity(private, request.task_id):
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="unknown_block_identity"
        ))
    root = private.execute(
        "SELECT id,status,tenant,workspace_kind,workspace_path,project_id "
        "FROM tasks WHERE id=?", (request.task_id,),
    ).fetchone()
    if root is None or root["status"] != "triage":
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="not_triage"
        ))
    if private.execute(
        "SELECT 1 FROM task_events WHERE task_id=? AND kind='decomposed' LIMIT 1",
        (request.task_id,),
    ).fetchone():
        return _AppliedTransition(TransitionOutcome(
            False, request.operation_id, refusal="already_decomposed"
        ))

    for child_id, child in zip(request.child_ids, request.children):
        root_kind = root["workspace_kind"] or "scratch"
        child_kind = child.get("workspace_kind") or root_kind
        if child.get("workspace_path"):
            child_path = child.get("workspace_path")
        elif child_kind == "worktree":
            child_path = None
        elif child_kind == root_kind:
            child_path = root["workspace_path"]
        else:
            child_path = None
        body = child.get("body")
        private.execute(
            "INSERT INTO tasks (id,title,body,assignee,status,workspace_kind,"
            "workspace_path,tenant,created_at,created_by,session_id) "
            "VALUES (?,?,?,?, 'todo',?,?,?,?,?,"
            "(SELECT session_id FROM tasks WHERE id=?))",
            (
                child_id, child["title"].strip(),
                body if isinstance(body, str) else None,
                kb._canonical_assignee(child.get("assignee")),
                child_kind, child_path, root["tenant"],
                request.created_at, request.author or "decomposer", request.task_id,
            ),
        )
        kb._append_event(
            private,
            child_id,
            "created",
            {"by": request.author or "decomposer",
             "from_decompose_of": request.task_id},
        )
        kb._inherit_notify_subs(
            private, child_id, (request.task_id,), created_at=request.created_at
        )

    for index, child in enumerate(request.children):
        for parent_index in child.get("parents") or []:
            parent_id = request.child_ids[parent_index]
            child_id = request.child_ids[index]
            private.execute(
                "INSERT OR IGNORE INTO task_links (parent_id,child_id) VALUES (?,?)",
                (parent_id, child_id),
            )
            kb._append_event(
                private, child_id, "linked",
                {"parent": parent_id, "child": child_id},
            )
    for child_id in request.child_ids:
        private.execute(
            "INSERT OR IGNORE INTO task_links (parent_id,child_id) VALUES (?,?)",
            (child_id, request.task_id),
        )
    if request.root_assignee is None:
        private.execute(
            "UPDATE tasks SET status='todo' WHERE id=? AND status='triage'",
            (request.task_id,),
        )
    else:
        private.execute(
            "UPDATE tasks SET status='todo',assignee=? "
            "WHERE id=? AND status='triage'",
            (request.root_assignee, request.task_id),
        )
    if request.author and request.author.strip():
        private.execute(
            "INSERT INTO task_comments (task_id,author,body,created_at) VALUES (?,?,?,?)",
            (
                request.task_id,
                request.author.strip(),
                "Decomposed into " + ", ".join(request.child_ids)
                + ". Root will wake when all children complete.",
                request.created_at,
            ),
        )
    kb._append_event(
        private,
        request.task_id,
        "decomposed",
        {"child_ids": list(request.child_ids),
         "root_assignee": request.root_assignee},
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _AppliedTransition(TransitionOutcome(
        True, request.operation_id, event_id, "none"
    ))


def _graph_transition_outcome(
    conn: sqlite3.Connection,
    result: _KernelResult,
    operation_id: str,
    operation_kind: str,
) -> GraphTransitionOutcome:
    from hermes_cli import kanban_db as kb

    payload: dict[str, Any] = {}
    if result.outcome.event_id is not None:
        row = conn.execute(
            "SELECT payload FROM task_events WHERE id=?", (result.outcome.event_id,)
        ).fetchone()
        payload = kb._json_dict(row["payload"] if row else None)
    return GraphTransitionOutcome(
        result.outcome.committed,
        operation_id,
        result.outcome.event_id,
        gated=bool(payload.get("gated")) if operation_kind == "link" else False,
        removed=bool(payload.get("removed")) if operation_kind == "unlink" else False,
        refusal=result.outcome.refusal,
        tuple_version=result.outcome.tuple_version,
        result_tuple_hash=result.outcome.result_tuple_hash,
    )


def link_tasks_transition(
    conn: sqlite3.Connection,
    parent_id: str,
    child_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    expected_child_run_id: Optional[int] = None,
) -> GraphTransitionOutcome:
    request = _LinkRequest(
        child_id, actor_scope, operation_id, parent_id, expected_child_run_id
    )
    return _graph_transition_outcome(
        conn, _run_operation(conn, request), operation_id, "link"
    )


def create_task_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    title: str,
    body: Optional[str],
    assignee: Optional[str],
    created_by: Optional[str],
    created_at: int,
    workspace_kind: str,
    workspace_path: Optional[str],
    branch_name: Optional[str],
    project_id: Optional[str],
    tenant: Optional[str],
    priority: int,
    parents: tuple[str, ...],
    triage: bool,
    initial_status: str,
    idempotency_key: Optional[str],
    max_runtime_seconds: Optional[int],
    skills_json: Optional[str],
    max_retries: Optional[int],
    model_override: Optional[str],
    provider_override: Optional[str],
    reasoning_effort: Optional[str],
    goal_mode: bool,
    goal_max_turns: Optional[int],
    session_id: Optional[str],
    creator_task_id: Optional[str],
    completion_contract: Optional[str],
) -> CreateTransitionOutcome:
    request = _CreateRequest(
        task_id, actor_scope, operation_id, title, body, assignee, created_by,
        created_at, workspace_kind, workspace_path, branch_name, project_id,
        tenant, priority, parents, triage, initial_status, idempotency_key,
        max_runtime_seconds, skills_json, max_retries, model_override,
        provider_override, reasoning_effort, goal_mode, goal_max_turns,
        session_id, creator_task_id, completion_contract,
    )
    result = _run_operation(conn, request)
    return CreateTransitionOutcome(
        result.outcome.committed,
        operation_id,
        task_id,
        result.outcome.event_id,
        result.outcome.refusal,
        result.outcome.tuple_version,
        result.outcome.result_tuple_hash,
    )


def create_swarm_transition(
    conn: sqlite3.Connection,
    *,
    actor_scope: str,
    operation_id: str,
    root_id: str,
    worker_ids: tuple[str, ...],
    verifier_id: str,
    synthesizer_id: str,
    goal: str,
    workers: tuple[tuple[str, str, str, tuple[str, ...], int, Optional[int]], ...],
    verifier_assignee: str,
    synthesizer_assignee: str,
    root_title: str,
    verifier_title: str,
    synthesizer_title: str,
    tenant: Optional[str],
    created_by: str,
    workspace_kind: str,
    workspace_paths: tuple[Optional[str], ...],
    branch_names: tuple[Optional[str], ...],
    project_id: Optional[str],
    priority: int,
    idempotency_key: Optional[str],
    created_at: int,
) -> SwarmTransitionOutcome:
    request = _SwarmRequest(
        root_id, actor_scope, operation_id, goal, workers, worker_ids,
        verifier_id, synthesizer_id, verifier_assignee, synthesizer_assignee,
        root_title, verifier_title, synthesizer_title, tenant, created_by,
        workspace_kind, workspace_paths, branch_names, project_id, priority,
        idempotency_key, created_at,
    )
    result = _run_operation(conn, request)
    return SwarmTransitionOutcome(
        result.outcome.committed,
        operation_id,
        root_id,
        worker_ids,
        verifier_id,
        synthesizer_id,
        result.outcome.event_id,
        result.outcome.refusal,
    )


def decompose_task_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    root_assignee: Optional[str],
    children: tuple[dict[str, Any], ...],
    child_ids: tuple[str, ...],
    author: Optional[str],
) -> DecomposeTransitionOutcome:
    request = _DecomposeRequest(
        task_id, actor_scope, operation_id, root_assignee, children,
        child_ids, author, int(time.time()),
    )
    result = _run_operation(conn, request)
    return DecomposeTransitionOutcome(
        result.outcome.committed,
        operation_id,
        task_id,
        child_ids,
        result.outcome.event_id,
        result.outcome.refusal,
    )


def unlink_tasks_transition(
    conn: sqlite3.Connection,
    parent_id: str,
    child_id: str,
    *,
    actor_scope: str,
    operation_id: str,
) -> GraphTransitionOutcome:
    request = _UnlinkRequest(child_id, actor_scope, operation_id, parent_id)
    return _graph_transition_outcome(
        conn, _run_operation(conn, request), operation_id, "unlink"
    )


def recompute_ready_transition(
    conn: sqlite3.Connection,
    *,
    actor_scope: str,
    operation_id: str,
    failure_limit: Optional[int] = None,
) -> int:
    """Atomically promote all eligible queued tasks with one receipt per task."""
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_connect as kbc

    if failure_limit is None:
        failure_limit = int(kb.DEFAULT_FAILURE_LIMIT)
    _validate_ingress(conn, actor_scope=actor_scope, operation_id=operation_id)
    db_path = kbc._main_db_file(conn)
    if not db_path:
        # In-memory boards are process-local compatibility/test databases. Bring
        # their raw SCHEMA_SQL up to the same optional-column contract as a
        # file-backed board before receipt-backed readiness transitions.
        kbc._migrate_add_optional_columns(conn)
    promoted = 0
    connection = (
        kbc._protected_connection(db_path, operation="graph")
        if db_path else nullcontext(conn)
    )
    with connection as private:
        nested = not db_path and private.in_transaction
        try:
            if nested:
                private.execute("SAVEPOINT readiness_recompute")
            else:
                kbc._execute_boundary_with_retry(private, "BEGIN IMMEDIATE")
            rows = private.execute(
                "SELECT id,status,consecutive_failures,max_retries "
                "FROM tasks WHERE status IN ('todo','blocked') ORDER BY id"
            ).fetchall()
            for row in rows:
                task_id = row["id"]
                request = _ReadinessRequest(
                    task_id,
                    actor_scope,
                    f"{operation_id}:{task_id}",
                    int(failure_limit),
                )
                request_hash = _request_hash(request)
                receipt = _load_receipt(private, request.operation_id)
                if receipt is not None:
                    replay = _receipt_outcome(receipt, request, request_hash)
                    if replay.refusal == "operation_id_collision":
                        raise ValueError(
                            f"readiness operation collision for task {task_id}"
                        )
                    continue
                if kb._has_unknown_block_identity(private, task_id):
                    continue
                if row["status"] == "blocked" and kb._has_sticky_block(private, task_id):
                    continue
                parents = private.execute(
                    "SELECT t.status FROM tasks t JOIN task_links l ON l.parent_id=t.id "
                    "WHERE l.child_id=?", (task_id,),
                ).fetchall()
                if not all(parent["status"] in {"done", "archived"} for parent in parents):
                    continue
                if row["status"] == "blocked":
                    task_limit = row["max_retries"]
                    effective_limit = int(task_limit) if task_limit is not None else int(failure_limit)
                    if int(row["consecutive_failures"] or 0) >= effective_limit:
                        continue
                resume_status = kb._resume_status_from_events(private, task_id)
                updated = private.execute(
                    "UPDATE tasks SET status=? WHERE id=? AND status=?",
                    (resume_status, task_id, row["status"]),
                )
                if updated.rowcount != 1:
                    continue
                kb._append_event(
                    private,
                    task_id,
                    "promoted",
                    {"status": resume_status} if resume_status != "ready" else None,
                )
                event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
                _insert_receipt(
                    private,
                    request,
                    request_hash,
                    TransitionOutcome(True, request.operation_id, event_id, "none"),
                )
                promoted += 1
            if nested:
                private.execute("RELEASE SAVEPOINT readiness_recompute")
            else:
                kbc._execute_boundary_with_retry(private, "COMMIT")
        except BaseException:
            if nested:
                private.execute("ROLLBACK TO SAVEPOINT readiness_recompute")
                private.execute("RELEASE SAVEPOINT readiness_recompute")
            else:
                _rollback(private)
            raise
    return promoted


def link_tasks(
    conn: sqlite3.Connection,
    parent_id: str,
    child_id: str,
    *,
    expected_child_run_id: Optional[int] = None,
) -> bool:
    return link_tasks_transition(
        conn,
        parent_id,
        child_id,
        actor_scope="legacy",
        operation_id=_compatibility_lifecycle_operation_id("link", child_id),
        expected_child_run_id=expected_child_run_id,
    ).gated


def unlink_tasks(conn: sqlite3.Connection, parent_id: str, child_id: str) -> bool:
    outcome = unlink_tasks_transition(
        conn,
        parent_id,
        child_id,
        actor_scope="legacy",
        operation_id=_compatibility_lifecycle_operation_id("unlink", child_id),
    )
    if outcome.removed:
        recompute_ready_transition(
            conn,
            actor_scope="legacy",
            operation_id=_compatibility_lifecycle_operation_id("recompute-ready", child_id),
            failure_limit=_default_failure_limit(),
        )
    return outcome.removed


def _default_failure_limit() -> int:
    from hermes_cli import kanban_db as kb
    return int(kb.DEFAULT_FAILURE_LIMIT)


def recompute_ready(
    conn: sqlite3.Connection, failure_limit: Optional[int] = None,
) -> int:
    return recompute_ready_transition(
        conn,
        actor_scope="legacy",
        operation_id=_compatibility_lifecycle_operation_id("recompute-ready", "board"),
        failure_limit=_default_failure_limit() if failure_limit is None else int(failure_limit),
    )


def block_task_transition(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    actor_scope: str,
    operation_id: str,
    kind: Optional[str],
    subject: Optional[str] = None,
    reason: Optional[str] = None,
    expected_run_id: Optional[int] = None,
) -> BlockTransitionOutcome:
    """Commit one blocker transition on a fresh private physical connection."""
    from hermes_cli import kanban_db as kb

    if kind is not None and kind not in kb.VALID_BLOCK_KINDS:
        raise ValueError(f"block kind must be one of {sorted(kb.VALID_BLOCK_KINDS)} or None")
    request = _BlockRequest(
        task_id=task_id,
        actor_scope=actor_scope,
        operation_id=operation_id,
        kind=kind,
        identity=canonicalize_block_identity(kind, subject),
        raw_subject=subject,
        reason=reason,
        expected_run_id=expected_run_id,
    )
    result = _run_operation(conn, request)
    if result.applied:
        kb._fire_task_hook(
            "kanban_task_blocked",
            kb.get_task(conn, task_id),
            task_id,
            result.hook_run_id,
            reason=reason,
        )
    return result.outcome


def compatibility_operation_id(task_id: str) -> str:
    return f"legacy-block:{task_id}:{secrets.token_hex(12)}"
