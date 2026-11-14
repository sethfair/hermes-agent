"""Per-lane claim and spawn behavior for the Kanban dispatcher facade."""

from __future__ import annotations

from typing import Any, Optional

from hermes_cli import kanban_db as _kb
from hermes_cli import kanban_db_dispatch as _dispatch
from hermes_cli import kanban_db_workspace as _kbw


def _any_spawnable_review(
    conn: Any, review_rows: list[Any], *, per_profile_cap: Optional[int] = None,
    per_profile_running: Optional[dict[str, int]] = None,
) -> bool:
    """Mirror review dispatch gates before reserving ready-lane capacity."""
    if not review_rows:
        return False
    profile_exists = _dispatch._profile_exists_fn()
    running = per_profile_running or {}
    for row in review_rows:
        assignee = row["assignee"]
        if (not assignee or (profile_exists is not None and not profile_exists(assignee))
                or (per_profile_cap is not None and running.get(assignee, 0) >= per_profile_cap)):
            continue
        if _dispatch.check_respawn_guard(conn, row["id"], lane="review") is None:
            return True
    return False


def _decision(result: Any, task_id: str, kind: str, reason: Optional[str] = None) -> None:
    result.monitor_decisions.append({"task_id": task_id, "decision": kind, "guard_reason": reason})


def _guard_decision(reason: str) -> str:
    return {
        "active_pr": "auth_or_quota_guard",
        "blocker_auth": "auth_or_quota_guard",
        "recent_success": "recent_success",
        "rate_limited": "rate_limit_cooldown",
    }.get(reason, "guard_unknown")


def _claim_lane_task(
    conn: Any, task_id: str, lane: str, ttl_seconds: Optional[int],
    monitor_attempt_id: Optional[str], monitor_context: Any,
) -> Any:
    claim = _kb.claim_review_task if lane == "review" else _kb.claim_task
    admission_id = None
    if lane == "ready" and monitor_attempt_id and monitor_context is not None:
        from hermes_cli import kanban_ready_monitor as monitor

        generation = monitor.ready_generation(conn, task_id)
        run_generation = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM task_runs WHERE task_id=?", (task_id,)
        ).fetchone()[0]
        admission_id = monitor.arm_claim_admission(
            conn, attempt_id=monitor_attempt_id, task_id=task_id,
            ready_generation=generation[0] if generation else 0, run_generation=int(run_generation),
        )
        conn.commit()
    if lane == "ready":
        return claim(conn, task_id, ttl_seconds=ttl_seconds, admission_id=admission_id)
    return claim(conn, task_id, ttl_seconds=ttl_seconds)


def _count_spawn(
    name: str, per_profile_cap: Optional[int], per_profile_running: dict[str, int],
) -> None:
    if per_profile_cap is not None and name:
        per_profile_running[name] = per_profile_running.get(name, 0) + 1


def _record_nonspawnable(conn: Any, task_id: str, assignee: str) -> None:
    with _kb.write_txn(conn):
        last = conn.execute(
            "SELECT kind, payload FROM task_events WHERE task_id = ? "
            "ORDER BY created_at DESC, id DESC LIMIT 1", (task_id,),
        ).fetchone()
        if (last is None or last["kind"] != "skipped_nonspawnable"
                or last["payload"] != _kb._json_or_null({"assignee": assignee})):
            _kb._append_event(conn, task_id, "skipped_nonspawnable", {"assignee": assignee})


def _dispatch_lane_task(
    conn: Any,
    row: Any,
    assignee: str,
    result: Any,
    *,
    lane: str,
    dry_run: bool,
    ttl_seconds: Optional[int],
    board: Optional[str],
    failure_limit: int,
    spawn_fn: Any,
    per_profile_cap: Optional[int],
    per_profile_running: dict[str, int],
    monitor_attempt_id: Optional[str] = None,
    monitor_context: Any = None,
) -> bool:
    """Guard, claim, resolve the workspace and spawn one lane row."""
    task_id = row["id"]
    profile_exists = _dispatch._profile_exists_fn()
    if profile_exists is not None and not profile_exists(assignee):
        result.skipped_nonspawnable.append(task_id)
        _decision(result, task_id, "nonspawnable_profile")
        if not dry_run:
            _record_nonspawnable(conn, task_id, assignee)
        return False
    if per_profile_cap is not None and per_profile_running.get(assignee, 0) >= per_profile_cap:
        result.skipped_per_profile_capped.append(
            (task_id, assignee, per_profile_running.get(assignee, 0)),
        )
        _decision(result, task_id, "profile_cap")
        return False
    guard_reason = _dispatch.check_respawn_guard(conn, task_id, lane=lane)
    if guard_reason is not None:
        result.respawn_guarded.append((task_id, guard_reason))
        _decision(result, task_id, _guard_decision(guard_reason), guard_reason)
        if not dry_run:
            with _kb.write_txn(conn):
                _kb._append_event(conn, task_id, "respawn_guarded", {"reason": guard_reason})
        return False
    if dry_run:
        _decision(result, task_id, "dry_run")
        result.spawned.append((task_id, assignee, ""))
        _count_spawn(assignee, per_profile_cap, per_profile_running)
        return True
    claimed = _claim_lane_task(conn, task_id, lane, ttl_seconds, monitor_attempt_id, monitor_context)
    if claimed is None:
        if lane == "ready":
            _decision(result, task_id, "eligible_missed")
        return False
    workspace, branch_name, workspace_error = _dispatch._resolve_lane_workspace(claimed, board)
    if workspace_error is not None:
        _decision(result, task_id, "workspace_failed", str(workspace_error))
        if _dispatch._record_task_failure(
            conn, claimed.id, f"workspace: {workspace_error}", outcome="spawn_failed",
            failure_limit=failure_limit, release_claim=True, end_run=True,
        ):
            result.auto_blocked.append(claimed.id)
        return False
    _kbw.set_workspace_path(conn, claimed.id, workspace)
    if claimed.workspace_kind == "worktree":
        _kbw.set_branch_name(
            conn, claimed.id, branch_name or (claimed.branch_name or "").strip()
            or f"wt/{claimed.id}",
        )
    _kbw._maybe_emit_scratch_tip(conn, claimed.id, claimed.workspace_kind)
    if lane == "review":
        claimed.skills = list(dict.fromkeys([*(claimed.skills or []), "sdlc-review"]))
    spawned, spawn_error, infrastructure = _dispatch._spawn_lane_worker(
        conn, claimed, workspace, board, spawn_fn,
    )
    if spawned:
        result.spawned.append((claimed.id, claimed.assignee or "", workspace))
        _decision(result, task_id, "claimed")
        _count_spawn(claimed.assignee, per_profile_cap, per_profile_running)
        return True
    if _dispatch._record_task_failure(
        conn, claimed.id, str(spawn_error), outcome="spawn_failed", failure_limit=failure_limit,
        release_claim=True, end_run=True, infrastructure=infrastructure,
    ):
        result.auto_blocked.append(claimed.id)
    _decision(result, task_id, "spawn_failed", str(spawn_error))
    return False
