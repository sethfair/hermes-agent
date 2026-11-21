"""Crash failure-budget accounting, separate from worker liveness and requeue."""
import sqlite3


def account_crashes(conn: sqlite3.Connection, crash_details: list) -> list[str]:
    """Count each crash against the breaker; returns the task ids it tripped.

    Protocol violations get a BOUNDED violation-only budget independent of
    ``consecutive_failures`` (per-task ``max_retries`` takes precedence);
    systemic same-error crashes (>= 3 identical fingerprints this tick) and
    terminal provider errors (credential revoked, model gone — a retry cannot
    heal them) trip immediately.
    """
    from hermes_cli import kanban_db as _kb
    from hermes_cli.kanban_db_dispatch import (
        DEFAULT_FAILURE_LIMIT,
        _error_fingerprint,
        _protocol_violation_streak,
        _PROTOCOL_VIOLATION_FAILURE_LIMIT,
    )
    from hermes_cli.kanban_dispatch_recovery.dead_worker import (
        record_recovered_worker_failure,
    )
    auto_blocked: list[str] = []
    fp_counts: dict[str, int] = {}
    for _, _, _, _, dead in crash_details:
        if dead.infrastructure:
            continue  # a stalled host kills every worker at once: not a systemic card fault
        fp = _error_fingerprint(dead.error_text)
        fp_counts[fp] = fp_counts.get(fp, 0) + 1
    for tid, pid, claimer, run_id, dead in crash_details:
        error_text = dead.error_text
        if dead.infrastructure:
            # Host-side loss: finalize the reclaim without consuming the task's failure budget.
            record_recovered_worker_failure(
                conn, tid, pid, claimer, run_id, dead,
                default_failure_limit=DEFAULT_FAILURE_LIMIT,
                count_failure=False,
                event_payload_extra={"infrastructure": True},
            )
            continue
        if dead.protocol_violation:
            streak = _protocol_violation_streak(conn, tid)
            trow = conn.execute("SELECT max_retries FROM tasks WHERE id = ?", (tid,)).fetchone()
            if trow is None:
                continue  # task deleted mid-loop
            task_override = _kb._row_get(trow, "max_retries")
            violation_limit = (
                int(task_override) if task_override is not None else _PROTOCOL_VIOLATION_FAILURE_LIMIT
            )
            if streak < violation_limit:
                record_recovered_worker_failure(
                    conn, tid, pid, claimer, run_id, dead,
                    default_failure_limit=DEFAULT_FAILURE_LIMIT,
                    failure_limit=violation_limit,
                    count_failure=False,
                    event_payload_extra={
                        "protocol_violations": streak,
                        "protocol_violation_limit": violation_limit,
                    },
                )
                continue
            # ``force_trip``: the decision (incl. per-task ``max_retries``) was
            # already made against the violation streak above.
            tripped = record_recovered_worker_failure(
                conn, tid, pid, claimer, run_id, dead,
                default_failure_limit=DEFAULT_FAILURE_LIMIT,
                failure_limit=violation_limit,
                force_trip=True,
                event_payload_extra={
                    "protocol_violations": streak,
                    "protocol_violation_limit": violation_limit,
                },
            )
        elif dead.terminal_provider:
            # A retry cannot heal a revoked credential or a missing model, so
            # the whole ``failure_limit`` budget would be spent on identical
            # failures. ``force_trip`` blocks now, sticky: ``recompute_ready``
            # must not auto-resume it before the operator fixes the provider.
            tripped = record_recovered_worker_failure(
                conn, tid, pid, claimer, run_id, dead,
                default_failure_limit=DEFAULT_FAILURE_LIMIT,
                force_trip=True,
                event_payload_extra={"terminal_provider": True},
            )
        else:
            is_systemic = fp_counts.get(_error_fingerprint(error_text), 0) >= 3
            extra = {}
            if is_systemic:
                # Trips at 1, below any ``failure_limit``: hold it for an operator.
                extra["sticky"] = True
            tripped = record_recovered_worker_failure(
                conn, tid, pid, claimer, run_id, dead,
                default_failure_limit=DEFAULT_FAILURE_LIMIT,
                failure_limit=1 if is_systemic else None,
                event_payload_extra=extra,
            )
        if tripped:
            auto_blocked.append(tid)
    return auto_blocked
