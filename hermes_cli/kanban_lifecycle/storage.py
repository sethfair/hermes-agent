"""Receipt-backed completion and same-card review lifecycle storage."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, Optional


def _state():
    from hermes_cli import kanban_db_task_state

    return kanban_db_task_state


def _applied(outcome, **kwargs):
    return _state()._AppliedTransition(outcome, **kwargs)


def _outcome(request, *, committed: bool, event_id=None, refusal=None):
    return _state().TransitionOutcome(
        committed, request.operation_id, event_id, "none", refusal
    )


def _apply_complete(private: sqlite3.Connection, request):
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_pr_acceptance_store import record_acceptance

    if kb._has_unknown_block_identity(private, request.task_id):
        return _applied(_outcome(request, committed=False, refusal="block_identity_unknown"))
    if not kb._parents_satisfied(private, request.task_id):
        return _applied(_outcome(request, committed=False, refusal="parent_dependencies_unsatisfied"))
    if request.acceptance is False:
        return _applied(_outcome(request, committed=False, refusal="acceptance_refused"))
    if request.acceptance is not None and not record_acceptance(
        private, request.task_id, request.acceptance
    ):
        return _applied(
            _outcome(request, committed=False, refusal="acceptance_refused"),
            persist_refusal=True,
        )
    trow = private.execute(
        "SELECT status,claim_lock,worker_pid,worker_started_at FROM tasks WHERE id=?",
        (request.task_id,),
    ).fetchone()
    prior_status = trow["status"] if trow else None
    if request.expected_run_id is None and not request.force and trow and kb._claim_is_live(trow):
        raise kb.LiveClaimError(request.task_id)
    now = int(time.time())
    sql = """UPDATE tasks SET status='done',result=?,completed_at=?,claim_lock=NULL,
             claim_expires=NULL,worker_pid=NULL,block_kind=NULL,block_recurrences=0,
             block_identity=NULL,block_identity_state='none'
             WHERE id=? AND status IN ('running','ready','blocked','review')"""
    params: tuple[Any, ...] = (request.result, now, request.task_id)
    if request.expected_run_id is not None:
        sql += " AND current_run_id=?"
        params = (*params, int(request.expected_run_id))
    if private.execute(sql, params).rowcount != 1:
        return _applied(_outcome(request, committed=False, refusal="task_not_completable"))
    staged: list[Path] = []
    try:
        if isinstance(request.metadata, dict):
            staged = kb._stage_completion_artifacts(
                private, request.task_id, request.metadata, now
            )
        handoff = request.summary if request.summary is not None else request.result
        run_id = kb._end_run(
            private, request.task_id, outcome="completed", status="done",
            summary=handoff, metadata=request.metadata,
        )
        if run_id is None and (
            request.summary or request.metadata or request.result or prior_status == "review"
        ):
            synth_summary, synth_metadata = handoff, request.metadata
            if prior_status == "review" and not synth_summary and not synth_metadata:
                synth_summary = kb._REVIEW_APPROVED_NOTE
                synth_metadata = {"source_status": "review", "approval": "manual"}
            run_id = kb._synthesize_ended_run(
                private, request.task_id, outcome="completed",
                summary=synth_summary, metadata=synth_metadata,
            )
        event_summary = handoff
        if prior_status == "review" and not event_summary:
            event_summary = kb._REVIEW_APPROVED_NOTE
        kb._append_event(
            private, request.task_id, "completed",
            kb._completed_event_payload(
                request.result, event_summary, list(request.verified_cards), request.metadata
            ),
            run_id=run_id,
        )
        event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
        return _applied(
            _outcome(request, committed=True, event_id=event_id),
            hook_run_id=run_id,
            staged_copies=tuple(staged),
        )
    except BaseException:
        if staged:
            kb._discard_staged_copies(staged, staged[0].parent)
        raise


def _apply_request_review(private: sqlite3.Connection, request):
    from hermes_cli import kanban_db as kb

    if kb._has_unknown_block_identity(private, request.task_id):
        return _applied(_outcome(request, committed=False, refusal="block_identity_unknown"))
    if not kb._parents_satisfied(private, request.task_id):
        return _applied(_outcome(request, committed=False, refusal="parent dependencies are not satisfied"))
    trow = private.execute(
        "SELECT assignee,status,claim_lock,current_run_id,worker_pid,worker_started_at "
        "FROM tasks WHERE id=?", (request.task_id,),
    ).fetchone()
    if trow is None:
        return _applied(_outcome(request, committed=False, refusal="task not found"))
    if request.expected_run_id is None and not request.force and kb._claim_is_live(trow):
        return _applied(_outcome(
            request, committed=False,
            refusal="task is running under a live claim; pass expected_run_id (worker ownership) or force=True (explicit operator override) instead of clearing the live run's claim",
        ))
    reviewer = request.reviewer
    if reviewer is None:
        reviewer = kb._prior_reviewer(private, request.task_id)
        if reviewer is False:
            return _applied(_outcome(
                request, committed=False,
                refusal="re-review has no durable reviewer provenance (the latest changes_requested event is missing or malformed); pass reviewer= explicitly",
            ))
    reviewer = kb._canonical_assignee(reviewer)
    implementer = None
    if trow["current_run_id"] is not None:
        row = private.execute(
            "SELECT profile FROM task_runs WHERE id=?", (trow["current_run_id"],)
        ).fetchone()
        implementer = row["profile"] if row else None
    if implementer is None and trow["assignee"] != reviewer:
        implementer = trow["assignee"]
    assignee_sql = ",assignee=?" if reviewer is not None else ""
    run_guard = "" if request.expected_run_id is None else " AND current_run_id=?"
    params = (*(() if reviewer is None else (reviewer,)), request.task_id,
              *(() if request.expected_run_id is None else (int(request.expected_run_id),)))
    updated = private.execute(
        "UPDATE tasks SET status='review',claim_lock=NULL,claim_expires=NULL,worker_pid=NULL"
        + assignee_sql + " WHERE id=? AND status IN ('running','ready')" + run_guard,
        params,
    )
    if updated.rowcount != 1:
        return _applied(_outcome(
            request, committed=False,
            refusal="task is not in running/ready (or expected_run_id did not match the current run)",
        ))
    staged: list[Path] = []
    try:
        if isinstance(request.metadata, dict):
            staged = kb._stage_completion_artifacts(
                private, request.task_id, request.metadata, int(time.time()),
                uploaded_by="kanban_request_review",
            )
        run_id = kb._end_or_synthesize_run(
            private, request.task_id, outcome="review_requested", status="review",
            summary=request.summary, metadata=request.metadata,
            synthesize=bool(request.summary or request.metadata), profile=implementer,
        )
        payload = {
            "summary": kb._first_line(request.summary, 400) or None,
            "implementer": implementer,
            "reviewer": reviewer,
        }
        artifacts = kb._cleaned_artifact_paths(request.metadata)
        if artifacts:
            payload["artifacts"] = artifacts
        kb._append_event(private, request.task_id, "review_requested", payload, run_id=run_id)
        event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
        return _applied(
            _outcome(request, committed=True, event_id=event_id),
            hook_run_id=run_id,
            staged_copies=tuple(staged),
        )
    except BaseException:
        if staged:
            kb._discard_staged_copies(staged, staged[0].parent)
        raise


def _apply_request_changes(private: sqlite3.Connection, request):
    from hermes_cli import kanban_db as kb

    if not request.reason:
        return _applied(_outcome(request, committed=False, refusal="reason is required"))
    if kb._has_unknown_block_identity(private, request.task_id):
        return _applied(_outcome(request, committed=False, refusal="block_identity_unknown"))
    task = private.execute(
        "SELECT status,assignee,current_run_id FROM tasks WHERE id=?", (request.task_id,)
    ).fetchone()
    if task is None:
        return _applied(_outcome(request, committed=False, refusal="task not found"))
    run_id = task["current_run_id"]
    if task["status"] != "running" or run_id is None:
        return _applied(_outcome(request, committed=False, refusal="task is not in an active review run"))
    if request.expected_run_id is not None and int(run_id) != int(request.expected_run_id):
        return _applied(_outcome(request, committed=False, refusal="run_id mismatch"))
    claimed = kb._latest_event(private, request.task_id, "claimed", run_id)
    if kb._json_dict(kb._row_get(claimed, "payload")).get("source_status") != "review":
        return _applied(_outcome(request, committed=False, refusal="active run was not claimed from review"))
    requested = kb._latest_event(private, request.task_id, "review_requested")
    if requested is None:
        return _applied(_outcome(request, committed=False, refusal="no prior review_requested event"))
    implementer = kb._nonblank_str(kb._json_dict(requested["payload"]).get("implementer"))
    if implementer is None:
        return _applied(_outcome(request, committed=False, refusal="review handoff has no valid implementer provenance"))
    reviewer = kb._canonical_assignee(kb._nonblank_str(task["assignee"]))
    status = kb._landing_status_after_parents(private, request.task_id)
    updated = private.execute(
        "UPDATE tasks SET status=?,assignee=COALESCE(?,assignee),claim_lock=NULL,"
        "claim_expires=NULL,worker_pid=NULL,worker_started_at=NULL "
        "WHERE id=? AND status='running' AND current_run_id=?",
        (status, implementer, request.task_id, int(run_id)),
    )
    if updated.rowcount != 1:
        return _applied(_outcome(request, committed=False, refusal="task changed during review handoff"))
    ended = kb._end_run(
        private, request.task_id, outcome="changes_requested", status=status,
        summary=request.reason,
    )
    kb._append_event(
        private, request.task_id, "changes_requested",
        {"reason": request.reason, "implementer": implementer,
         "reviewer": reviewer, "status": status, "transition_detail": implementer},
        run_id=ended,
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _applied(_outcome(request, committed=True, event_id=event_id), hook_run_id=ended)


def _apply_reopen_review(private: sqlite3.Connection, request):
    from hermes_cli import kanban_db as kb

    if kb._has_unknown_block_identity(private, request.task_id):
        return _applied(_outcome(request, committed=False, refusal="block_identity_unknown"))
    kb._reclaim_dangling_run(
        private, request.task_id, statuses=("review",), now=int(time.time()),
        note="invariant recovery on review reopen",
    )
    status = kb._landing_status_after_parents(private, request.task_id)
    event = kb._latest_event(private, request.task_id, "review_requested")
    implementer = kb._nonblank_str(kb._json_dict(kb._row_get(event, "payload")).get("implementer"))
    reset = status == "todo"
    identity_sql = (
        ",block_kind=NULL,block_recurrences=0,block_identity=NULL,block_identity_state='unknown'"
        if reset else ""
    )
    params = (status, *((implementer,) if implementer else ()), request.task_id)
    updated = private.execute(
        "UPDATE tasks SET status=?,current_run_id=NULL,claim_lock=NULL,claim_expires=NULL,"
        "worker_pid=NULL,worker_started_at=NULL"
        + (",assignee=?" if implementer else "") + identity_sql
        + " WHERE id=? AND status='review'", params,
    )
    if updated.rowcount != 1:
        return _applied(_outcome(request, committed=False, refusal="task_not_in_review"))
    if reset:
        kb._append_event(
            private, request.task_id, "block_identity_unknown",
            {"identity_state": "unknown", "identity_version": 1,
             "unknown_cause": "review_reopened_parent_wait", "classification_required": True},
        )
    payload: dict[str, Any] = {"status": status}
    if implementer:
        payload["implementer"] = implementer
    kb._append_event(
        private, request.task_id, "review_reopened",
        payload if payload != {"status": "ready"} else None,
    )
    event_id = private.execute("SELECT last_insert_rowid()").fetchone()[0]
    return _applied(_outcome(request, committed=True, event_id=event_id))


def _lifecycle_outcome(conn, result, operation_id: str):
    state = _state()
    payload = {}
    if result.outcome.event_id is not None:
        from hermes_cli import kanban_db as kb

        row = conn.execute(
            "SELECT payload FROM task_events WHERE id=?", (result.outcome.event_id,)
        ).fetchone()
        payload = kb._json_dict(row["payload"] if row else None)
    return state.LifecycleTransitionOutcome(
        result.outcome.committed, operation_id, result.outcome.event_id,
        result.outcome.refusal, payload.get("transition_detail"),
        result.outcome.tuple_version, result.outcome.result_tuple_hash,
    )


def complete_task_transition(
    conn, task_id: str, *, actor_scope: str, operation_id: str,
    result: Optional[str] = None, summary: Optional[str] = None,
    metadata: Optional[dict] = None, created_cards: Optional[Iterable[str]] = None,
    expected_run_id: Optional[int] = None, fire_lifecycle_hook: bool = True,
    force: bool = False,
):
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_pr_acceptance_store import prepare_acceptance

    if not kb._parents_satisfied(conn, task_id):
        state = _state()
        return state.LifecycleTransitionOutcome(
            False, operation_id, refusal="parent_dependencies_unsatisfied"
        )
    verified = kb._gate_created_cards(conn, task_id, created_cards, summary or result)
    kb._gate_empty_completion(conn, task_id, result=result, summary=summary)
    metadata = kb._merge_completion_prose_artifacts(
        conn, task_id, metadata, summary=summary, result=result
    )
    acceptance = prepare_acceptance(conn, task_id, expected_run_id, metadata)
    state = _state()
    request = state._CompleteRequest(
        task_id, actor_scope, operation_id, result, summary, metadata,
        tuple(verified), expected_run_id, force, acceptance,
    )
    kernel = state._run_operation(conn, request)
    outcome = _lifecycle_outcome(conn, kernel, operation_id)
    if kernel.applied:
        handoff = summary if summary is not None else result
        kb._flag_phantom_prose_refs(conn, task_id, kernel.hook_run_id, summary, result, verified)
        kb._clear_failure_counter(conn, task_id)
        state.recompute_ready(conn)
        kb._cleanup_workspace(conn, task_id)
        if fire_lifecycle_hook:
            kb._fire_task_hook(
                "kanban_task_completed", kb.get_task(conn, task_id), task_id,
                kernel.hook_run_id, summary=handoff,
            )
    return outcome


def request_review_transition(
    conn, task_id: str, *, actor_scope: str, operation_id: str,
    summary: Optional[str] = None, metadata: Optional[dict] = None,
    reviewer: Optional[str] = None, expected_run_id: Optional[int] = None,
    force: bool = False,
):
    from hermes_cli import kanban_db as kb

    summary = kb.redact_review_value(summary)
    metadata = kb.redact_review_value(metadata)
    metadata = kb._merge_completion_prose_artifacts(
        conn, task_id, metadata, summary=summary, result=None
    )
    state = _state()
    request = state._RequestReviewRequest(
        task_id, actor_scope, operation_id, summary, metadata, reviewer,
        expected_run_id, force,
    )
    return _lifecycle_outcome(conn, state._run_operation(conn, request), operation_id)


def request_changes_transition(
    conn, task_id: str, *, actor_scope: str, operation_id: str,
    reason: str, expected_run_id: Optional[int] = None,
):
    from hermes_cli import kanban_db as kb

    reason = str(kb.redact_review_value(reason or "")).strip()
    state = _state()
    request = state._RequestChangesRequest(
        task_id, actor_scope, operation_id, reason, expected_run_id
    )
    return _lifecycle_outcome(conn, state._run_operation(conn, request), operation_id)


def reopen_review_task_transition(
    conn, task_id: str, *, actor_scope: str, operation_id: str,
):
    state = _state()
    request = state._ReopenReviewRequest(task_id, actor_scope, operation_id)
    return _lifecycle_outcome(conn, state._run_operation(conn, request), operation_id)
