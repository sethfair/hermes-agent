"""One Kanban dispatcher tick and its spawn-budget orchestration.

The dispatcher facade keeps the supported names. This module late-binds every
patchable helper through that facade so existing tests and integrations retain
their monkeypatch seams after the extraction.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Mapping, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from hermes_cli.kanban_db_dispatch import DispatchResult


def _run_reclaim_phase(
    conn: sqlite3.Connection,
    result: DispatchResult,
    *,
    stale_timeout_seconds: int,
    failure_limit: int,
    reconcile_orphans: bool,
    board: Optional[str] = None,
) -> None:
    """Reclaim stale/orphaned/crashed/timed-out running tasks, then promote."""
    _dispatch.reap_worker_zombies()
    result.reaped_terminal_workers = _dispatch.reap_terminal_workers(conn)
    result.reclaimed = _kb.release_stale_claims(conn, failure_limit=failure_limit)
    if reconcile_orphans:
        result.reconciled_orphans = _dispatch.reconcile_orphaned_running(conn)
    result.stale = _dispatch.detect_stale_running(
        conn, stale_timeout_seconds=stale_timeout_seconds,
    )
    result.crashed = _dispatch.detect_crashed_workers(conn, board=board)
    result.auto_blocked.extend(
        getattr(_dispatch.detect_crashed_workers, "_last_auto_blocked", [])
    )
    result.rate_limited.extend(
        getattr(_dispatch.detect_crashed_workers, "_last_rate_limited", [])
    )
    result.timed_out = _dispatch.enforce_max_runtime(conn)
    result.promoted = _kb.recompute_ready(conn, failure_limit=failure_limit)


def _tick_spawn_budget(
    conn: sqlite3.Connection,
    result: DispatchResult,
    *,
    max_spawn: Optional[int],
    max_in_progress: Optional[int],
    board: Optional[str],
) -> tuple[bool, Optional[int]]:
    """Return whether this tick may spawn and its shared lane budget."""
    running_count = 0
    spawn_budget: Optional[int] = None
    if max_spawn is not None or max_in_progress is not None:
        running_count = _dispatch.count_running_tasks(conn)

    if max_spawn is not None:
        if running_count >= max_spawn:
            return False, None
        spawn_budget = max_spawn - running_count

    if max_in_progress is not None:
        total_running = running_count + _dispatch.count_running_tasks_other_boards(board)
        if total_running >= max_in_progress:
            return False, None
        remaining = max_in_progress - total_running
        if spawn_budget is None or spawn_budget > remaining:
            spawn_budget = remaining

    pressure = _dispatch._memory_pressure_level()
    if pressure == "critical":
        result.memory_pressure = pressure
        _kb._log.warning(
            "kanban dispatch: system memory pressure is critical; "
            "spawning no new workers this tick (deferred, not dropped)"
        )
        return False, None
    if pressure == "elevated":
        result.memory_pressure = pressure
        if spawn_budget is None or spawn_budget > 1:
            _kb._log.warning(
                "kanban dispatch: system memory pressure is elevated; "
                "limiting to at most 1 new worker this tick"
            )
            spawn_budget = 1
    return True, spawn_budget


def _lane_rows(conn: sqlite3.Connection, status: str) -> list[sqlite3.Row]:
    """Unclaimed rows of one lane in dispatch order."""
    return conn.execute(
        "SELECT id, assignee FROM tasks "
        f"WHERE status = '{status}' AND claim_lock IS NULL "
        "ORDER BY priority DESC, created_at ASC"
    ).fetchall()


def _resolve_default_assignee(default_assignee: Optional[str]) -> Optional[str]:
    """Return a configured default only when it names an eligible profile."""
    name = (default_assignee or "").strip() or None
    if name:
        profile_exists = _dispatch._profile_exists_fn()
        if profile_exists is not None and not profile_exists(name):
            return None
    return name


def _monitor_profiles(
    partitions: Optional[Mapping[str, Any]], profile: Optional[str],
) -> set[str]:
    return set(partitions or ()) or ({profile} if profile else set())


def _ensure_partition_decisions(
    conn, result, reason: str, profiles: set[str],
) -> None:
    from hermes_cli import kanban_ready_monitor as monitor
    for profile in profiles or {None}:
        monitor.ensure_ready_decisions(conn, result, reason, profile=profile)


def _monitor_partition(partitions, profile, attempt_id, context):
    return (partitions or {}).get(profile) or (attempt_id, context)


def _dispatch_once_locked(
    conn: sqlite3.Connection,
    *,
    spawn_fn=None,
    ttl_seconds: Optional[int] = None,
    dry_run: bool = False,
    max_spawn: Optional[int] = None,
    max_in_progress: Optional[int] = None,
    failure_limit: int = 2,
    stale_timeout_seconds: int = 0,
    board: Optional[str] = None,
    default_assignee: Optional[str] = None,
    max_in_progress_per_profile: Optional[int] = None,
    reconcile_orphans: bool = True,
    monitor_attempt_id: Optional[str] = None,
    monitor_context=None,
    monitor_profile: Optional[str] = None,
    monitor_partitions: Optional[dict[str, tuple[str, Any]]] = None,
    skip_reclaim: bool = False,
) -> DispatchResult:
    result = _dispatch.DispatchResult()
    from hermes_cli import kanban_ready_monitor as monitor
    if not skip_reclaim:
        _dispatch._run_reclaim_phase(
            conn, result, stale_timeout_seconds=stale_timeout_seconds,
            failure_limit=failure_limit, reconcile_orphans=reconcile_orphans, board=board,
        )
    may_spawn, spawn_budget = _dispatch._tick_spawn_budget(
        conn, result, max_spawn=max_spawn,
        max_in_progress=max_in_progress, board=board,
    )
    if conn.in_transaction:
        conn.commit()
    if not may_spawn:
        for episode in conn.execute(
            "SELECT task_id FROM kanban_ready_episodes WHERE state='open'"
        ):
            monitor.reconcile_truthful_hold(conn, episode["task_id"])
        _dispatch._ensure_partition_decisions(
            conn, result, "memory_pressure" if result.memory_pressure else "board_cap",
            _dispatch._monitor_profiles(monitor_partitions, monitor_profile),
        )
        return result

    ready_rows = _dispatch._lane_rows(conn, "ready")
    allowed_profiles = _dispatch._monitor_profiles(monitor_partitions, monitor_profile)
    if allowed_profiles:
        ready_rows = [row for row in ready_rows if row["assignee"] in allowed_profiles]
    review_rows = (
        _dispatch._lane_rows(conn, "review")
        if _dispatch.review_dispatch_enabled() else []
    )
    if allowed_profiles:
        review_rows = [row for row in review_rows if row["assignee"] in allowed_profiles]
    per_profile_cap = max_in_progress_per_profile if (
        isinstance(max_in_progress_per_profile, int)
        and max_in_progress_per_profile > 0
    ) else None
    per_profile_running: dict[str, int] = {}
    if per_profile_cap is not None:
        for prow in conn.execute(
            "SELECT assignee, COUNT(*) AS n FROM tasks "
            "WHERE status = 'running' AND assignee IS NOT NULL "
            "GROUP BY assignee"
        ):
            per_profile_running[prow["assignee"]] = int(prow["n"])
    ready_budget = spawn_budget
    if spawn_budget is not None and spawn_budget > 0 and _dispatch._any_spawnable_review(
        conn, review_rows,
        per_profile_cap=per_profile_cap, per_profile_running=per_profile_running,
    ):
        ready_budget = max(spawn_budget - 1, 0)
    lane_kwargs: dict[str, Any] = dict(
        dry_run=dry_run, ttl_seconds=ttl_seconds, board=board,
        failure_limit=failure_limit, spawn_fn=spawn_fn,
        per_profile_cap=per_profile_cap, per_profile_running=per_profile_running,
    )
    default_assignee = _dispatch._resolve_default_assignee(default_assignee)
    spawned = 0
    for row in ready_rows:
        if ready_budget is not None and spawned >= ready_budget:
            result.monitor_decisions.append(
                {"task_id": row["id"], "decision": "shared_budget_exhausted"}
            )
            continue
        row_assignee = row["assignee"]
        if not row_assignee:
            if not default_assignee or not _dispatch._apply_default_assignee(
                conn, row["id"], default_assignee, dry_run=dry_run,
            ):
                result.skipped_unassigned.append(row["id"])
                result.monitor_decisions.append(
                    {"task_id": row["id"], "decision": "unassigned"}
                )
                continue
            row_assignee = default_assignee
            result.auto_assigned_default.append(row["id"])
        attempt_id, context = _dispatch._monitor_partition(
            monitor_partitions, row_assignee, monitor_attempt_id, monitor_context,
        )
        if _dispatch._dispatch_lane_task(
            conn, row, row_assignee, result, lane="ready",
            monitor_attempt_id=attempt_id, monitor_context=context, **lane_kwargs,
        ):
            spawned += 1

    for row in review_rows:
        if spawn_budget is not None and spawned >= spawn_budget:
            continue
        if not row["assignee"]:
            result.skipped_unassigned.append(row["id"])
            continue
        attempt_id, context = _dispatch._monitor_partition(
            monitor_partitions, row["assignee"], monitor_attempt_id, monitor_context,
        )
        if _dispatch._dispatch_lane_task(
            conn, row, row["assignee"], result, lane="review",
            monitor_attempt_id=attempt_id, monitor_context=context, **lane_kwargs,
        ):
            spawned += 1
    for episode in conn.execute(
        "SELECT task_id FROM kanban_ready_episodes WHERE state='open'"
    ):
        monitor.reconcile_truthful_hold(conn, episode["task_id"])
    _dispatch._ensure_partition_decisions(
        conn, result, "shared_budget_exhausted",
        _dispatch._monitor_profiles(monitor_partitions, monitor_profile),
    )
    return result


from hermes_cli import kanban_db as _kb
from hermes_cli import kanban_db_dispatch as _dispatch
