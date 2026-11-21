"""Kanban Swarm v1: thin swarm topology helpers on top of Kanban.

Deliberately no second scheduler — a small task graph written into the
existing Kanban kernel:

    planning root (completed immediately)
        ├─ parallel specialist workers (ready)
        └─ verifier (todo until all workers done)
             └─ synthesizer (todo until verifier done)

The shared blackboard is structured JSON comments on the root task, so all
state lives in existing task_comments/task_events rows and the dashboard,
notifier, slash command and dispatcher keep working without a new service.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
import secrets
import sqlite3
import time
from typing import Any, Iterable, Optional

from hermes_cli import kanban_db as kb

BLACKBOARD_PREFIX = "[swarm:blackboard] "


@dataclass(frozen=True)
class SwarmWorkerSpec:
    """A single parallel worker card in a swarm."""

    profile: str
    title: str
    body: str
    skills: list[str] = field(default_factory=list)
    priority: int = 0
    max_runtime_seconds: Optional[int] = None


@dataclass(frozen=True)
class SwarmCreated:
    """IDs produced by :func:`create_swarm`."""

    root_id: str
    worker_ids: list[str]
    verifier_id: str
    synthesizer_id: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _require_text(value: str, field_name: str) -> str:
    text = (value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    return text


def create_swarm(
    conn: sqlite3.Connection,
    *,
    goal: str,
    workers: Iterable[SwarmWorkerSpec],
    verifier_assignee: str,
    synthesizer_assignee: str,
    root_title: Optional[str] = None,
    verifier_title: str = "Verify swarm outputs",
    synthesizer_title: str = "Synthesize swarm outputs",
    tenant: Optional[str] = None,
    created_by: str = "swarm-orchestrator",
    workspace_kind: Optional[str] = None,
    workspace_path: Optional[str] = None,
    priority: int = 0,
    idempotency_key: Optional[str] = None,
) -> SwarmCreated:
    """Atomically create a durable, immediately dispatchable Kanban swarm."""
    goal = _require_text(goal, "goal")
    verifier_assignee = _require_text(verifier_assignee, "verifier_assignee")
    synthesizer_assignee = _require_text(synthesizer_assignee, "synthesizer_assignee")
    worker_specs = list(workers)
    if not worker_specs:
        raise ValueError("at least one worker is required")
    for index, spec in enumerate(worker_specs, start=1):
        _require_text(spec.profile, f"workers[{index}].profile")
        _require_text(spec.title, f"workers[{index}].title")

    if idempotency_key:
        row = conn.execute(
            "SELECT id FROM tasks WHERE idempotency_key=? AND status!='archived' "
            "ORDER BY created_at DESC LIMIT 1", (idempotency_key,),
        ).fetchone()
        if row:
            existing = latest_blackboard(conn, row["id"]).get("topology")
            if isinstance(existing, dict):
                existing_workers = [str(item) for item in existing.get("worker_ids", []) if item]
                if existing_workers and existing.get("verifier_id") and existing.get("synthesizer_id"):
                    return SwarmCreated(
                        row["id"], existing_workers, str(existing["verifier_id"]),
                        str(existing["synthesizer_id"]),
                    )

    project_id = None
    if workspace_kind != "scratch":
        project_id = (kb._board_meta_for(None).get("project_id") or "").strip() or None
    resolved_kind = workspace_kind or "scratch"
    project_id, project_obj, project_repo, resolved_kind = kb._resolve_project_link(
        conn, project_id, None, resolved_kind, workspace_path
    )
    if workspace_path is None and project_repo is None and resolved_kind in {"dir", "worktree"}:
        workspace_path = kb._board_meta_for(None).get("default_workdir") or None

    root_id = kb._new_task_id()
    worker_ids = tuple(kb._new_task_id() for _ in worker_specs)
    verifier_id = kb._new_task_id()
    synthesizer_id = kb._new_task_id()
    all_ids = (root_id, *worker_ids, verifier_id, synthesizer_id)
    paths: list[Optional[str]] = []
    branches: list[Optional[str]] = []
    for task_id in all_ids:
        path = workspace_path
        branch = None
        if project_obj is not None and resolved_kind == "worktree":
            if project_repo and not path:
                path = str(Path(project_repo) / ".worktrees" / task_id)
            branch = kb._project_branch_name(project_obj, task_id, root_title or goal)
        paths.append(path)
        branches.append(branch)

    from hermes_cli.kanban_db_task_state import create_swarm_transition

    operation_id = f"create-swarm:{root_id}:{secrets.token_hex(8)}"
    outcome = create_swarm_transition(
        conn,
        actor_scope="legacy",
        operation_id=operation_id,
        root_id=root_id,
        worker_ids=worker_ids,
        verifier_id=verifier_id,
        synthesizer_id=synthesizer_id,
        goal=goal,
        workers=tuple(
            (
                spec.profile, spec.title, spec.body or "", tuple(spec.skills),
                spec.priority, spec.max_runtime_seconds,
            )
            for spec in worker_specs
        ),
        verifier_assignee=verifier_assignee,
        synthesizer_assignee=synthesizer_assignee,
        root_title=root_title or f"Swarm: {goal.splitlines()[0][:80]}",
        verifier_title=verifier_title,
        synthesizer_title=synthesizer_title,
        tenant=tenant,
        created_by=created_by,
        workspace_kind=resolved_kind,
        workspace_paths=tuple(paths),
        branch_names=tuple(branches),
        project_id=project_id,
        priority=priority,
        idempotency_key=idempotency_key,
        created_at=int(time.time()),
    )
    if not outcome.committed:
        raise RuntimeError(outcome.refusal or "could not create swarm")
    created = SwarmCreated(
        outcome.root_id, list(outcome.worker_ids), outcome.verifier_id,
        outcome.synthesizer_id,
    )
    kb.recompute_ready(conn)
    root = kb.get_task(conn, created.root_id)
    run = kb.latest_run(conn, created.root_id)
    kb._fire_kanban_lifecycle_hook(
        "kanban_task_completed",
        created.root_id,
        board=kb.get_current_board(),
        assignee=root.assignee if root else None,
        run_id=run.id if run else None,
        summary="Swarm topology planned; root remains the shared blackboard.",
    )
    return created


def post_blackboard_update(conn: sqlite3.Connection, root_id: str, *, author: str, key: str, value: Any) -> int:
    """Append one structured update to the swarm root blackboard."""
    _require_text(root_id, "root_id")
    author = _require_text(author, "author")
    key = _require_text(key, "key")
    payload = json.dumps({"key": key, "value": value}, ensure_ascii=False, sort_keys=True)
    return kb.add_comment(conn, root_id, author=author, body=BLACKBOARD_PREFIX + payload)


def latest_blackboard(conn: sqlite3.Connection, root_id: str) -> dict[str, Any]:
    """Merge structured blackboard comments on a root card. Later comments
    replace earlier values for the same key; ``_authors`` records the author
    of the winning value for traceability."""
    merged: dict[str, Any] = {}
    authors: dict[str, str] = {}
    for comment in kb.list_comments(conn, root_id):
        body = comment.body or ""
        if not body.startswith(BLACKBOARD_PREFIX):
            continue
        try:
            payload = json.loads(body[len(BLACKBOARD_PREFIX):])
        except json.JSONDecodeError:
            continue
        key = payload.get("key")
        if not isinstance(key, str) or not key:
            continue
        merged[key] = payload.get("value")
        authors[key] = comment.author
    if authors:
        merged["_authors"] = authors
    return merged


def parse_worker_arg(raw: str) -> SwarmWorkerSpec:
    """Parse CLI ``--worker profile:title[:skill,skill]`` values."""
    parts = [p.strip() for p in raw.split(":", 2)]
    if len(parts) < 2:
        raise ValueError("worker must be profile:title or profile:title:skill,skill")
    skills = [s.strip() for s in parts[2].split(",") if s.strip()] if len(parts) == 3 and parts[2] else []
    return SwarmWorkerSpec(profile=parts[0], title=parts[1], body=parts[1], skills=skills)
