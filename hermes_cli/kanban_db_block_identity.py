"""Canonical blocker identity validation and fail-closed state evaluation."""

from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Optional

IDENTITY_VERSION = 1
IDENTITY_STATES = frozenset({"none", "known", "unknown"})
_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_TASK_ID_RE = re.compile(r"^t_[0-9a-f]{8}$")


@dataclass(frozen=True)
class EffectiveBlockIdentity:
    state: str
    canonical: Optional[str] = None
    kind: Optional[str] = None
    subject: Optional[str] = None
    unknown_cause: Optional[str] = None


def _canonical_json(kind: str, subject: str) -> str:
    return json.dumps(
        {"v": IDENTITY_VERSION, "kind": kind, "subject": subject},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def canonicalize_requested_identity(
    kind: Optional[str], subject: Optional[str]
) -> EffectiveBlockIdentity:
    """Validate an explicit blocker key without retaining invalid raw input."""
    if kind is None:
        return EffectiveBlockIdentity("unknown", unknown_cause="missing_kind")
    if subject is None:
        return EffectiveBlockIdentity("unknown", kind=kind, unknown_cause="missing_subject")
    if not isinstance(subject, str):
        return EffectiveBlockIdentity("unknown", kind=kind, unknown_cause="invalid_subject")

    if kind == "dependency":
        normalized = subject
        if not _TASK_ID_RE.fullmatch(normalized):
            return EffectiveBlockIdentity("unknown", kind=kind, unknown_cause="invalid_subject")
    elif kind == "needs_input":
        normalized = subject.strip().lower()
        parts = normalized.split("/")
        if len(parts) != 2 or not all(_SLUG_RE.fullmatch(part) for part in parts):
            return EffectiveBlockIdentity("unknown", kind=kind, unknown_cause="invalid_subject")
    elif kind in {"capability", "transient"}:
        normalized = subject.strip().lower()
        if not _SLUG_RE.fullmatch(normalized):
            return EffectiveBlockIdentity("unknown", kind=kind, unknown_cause="invalid_subject")
    else:
        return EffectiveBlockIdentity("unknown", unknown_cause="invalid_kind")

    canonical = _canonical_json(kind, normalized)
    return EffectiveBlockIdentity("known", canonical, kind, normalized)


def effective_identity(row: Any) -> EffectiveBlockIdentity:
    """Interpret persisted state; every malformed or inconsistent form is UNKNOWN."""
    state = _row_value(row, "block_identity_state")
    raw = _row_value(row, "block_identity")
    kind = _row_value(row, "block_kind")
    recurrences = _row_value(row, "block_recurrences")

    if state == "none":
        if raw is None and kind is None and int(recurrences or 0) == 0:
            return EffectiveBlockIdentity("none")
        return EffectiveBlockIdentity("unknown", unknown_cause="inconsistent_state")
    if state == "unknown":
        return EffectiveBlockIdentity("unknown", unknown_cause="stored_unknown")
    if state != "known" or not isinstance(raw, str):
        return EffectiveBlockIdentity("unknown", unknown_cause="invalid_state")

    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return EffectiveBlockIdentity("unknown", unknown_cause="malformed_identity")
    if not isinstance(parsed, dict) or set(parsed) != {"v", "kind", "subject"}:
        return EffectiveBlockIdentity("unknown", unknown_cause="malformed_identity")
    if parsed.get("v") != IDENTITY_VERSION:
        return EffectiveBlockIdentity("unknown", unknown_cause="unsupported_version")
    candidate = canonicalize_requested_identity(parsed.get("kind"), parsed.get("subject"))
    if (
        candidate.state != "known"
        or candidate.canonical != raw
        or candidate.kind != kind
        or int(recurrences or 0) < (0 if kind == "dependency" else 1)
    ):
        return EffectiveBlockIdentity("unknown", unknown_cause="malformed_identity")
    return candidate


def event_identity_fields(identity: EffectiveBlockIdentity) -> dict[str, object]:
    if identity.state == "known":
        return {
            "identity_version": IDENTITY_VERSION,
            "identity_state": "known",
            "identity": identity.canonical,
        }
    return {
        "identity_version": IDENTITY_VERSION,
        "identity_state": "unknown",
        "unknown_cause": identity.unknown_cause or "unknown",
    }


def _row_value(row: Any, key: str) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError, TypeError):
        return None


def migrate_legacy_block_identities(conn: sqlite3.Connection) -> None:
    """Classify pre-identity rows once from complete, corroborated event evidence."""
    pending = conn.execute(
        "SELECT id, status, block_kind, block_recurrences FROM tasks "
        "WHERE block_identity_state IS NULL ORDER BY id"
    ).fetchall()
    if not pending:
        return

    event_rows = conn.execute(
        "SELECT id, task_id, kind, payload FROM task_events ORDER BY id"
    ).fetchall()
    event_by_id = {int(row["id"]): row for row in event_rows}
    events_by_task: dict[str, list[Any]] = {}
    for event in event_rows:
        events_by_task.setdefault(event["task_id"], []).append(event)
    ids = [int(row["id"]) for row in event_rows]
    complete_history = bool(ids and ids[0] == 1 and len(ids) == ids[-1])

    for row in pending:
        task_id = row["id"]
        unknown_cause: Optional[str] = None
        blocker_evidence = bool(
            row["status"] == "blocked"
            or row["block_kind"] is not None
            or int(row["block_recurrences"] or 0) > 0
        )
        created_seen = False
        for event in events_by_task.get(task_id, []):
            try:
                payload = json.loads(event["payload"]) if event["payload"] is not None else None
            except (TypeError, ValueError):
                unknown_cause = "malformed_event_history"
                break
            if payload is not None and not isinstance(payload, dict):
                unknown_cause = "malformed_event_history"
                break
            created_seen = created_seen or event["kind"] == "created"
            if event["kind"] in {"blocked", "block_loop_detected"}:
                blocker_evidence = True
            if event["kind"] == "dependency_wait" and not _graph_only_wait(
                event, payload or {}, event_by_id, conn
            ):
                blocker_evidence = True

        if unknown_cause is None and (blocker_evidence or not complete_history or not created_seen):
            unknown_cause = "legacy_block_history" if blocker_evidence else "incomplete_event_history"
        state = "unknown" if unknown_cause else "none"
        conn.execute(
            "UPDATE tasks SET block_identity = NULL, block_identity_state = ? WHERE id = ?",
            (state, task_id),
        )
        payload = {"identity_version": IDENTITY_VERSION, "identity_state": state, "migration": True}
        event_kind = "block_identity_migrated"
        if unknown_cause:
            payload.update({"unknown_cause": unknown_cause, "classification_required": True})
            event_kind = "block_identity_unknown"
        conn.execute(
            "INSERT INTO task_events (task_id, kind, payload, created_at) VALUES (?, ?, ?, ?)",
            (task_id, event_kind, json.dumps(payload, separators=(",", ":")), int(time.time())),
        )


def _graph_only_wait(
    event: Any,
    payload: dict[str, Any],
    event_by_id: dict[int, Any],
    conn: sqlite3.Connection,
) -> bool:
    if set(payload) != {"reason", "demoted", "parent"}:
        return False
    parent = payload.get("parent")
    if (
        payload.get("reason") != "parent_not_done"
        or payload.get("demoted") is not True
        or not isinstance(parent, str)
        or not parent
    ):
        return False
    linked = event_by_id.get(int(event["id"]) + 1)
    if linked is None or linked["task_id"] != event["task_id"] or linked["kind"] != "linked":
        return False
    try:
        linked_payload = json.loads(linked["payload"])
    except (TypeError, ValueError):
        return False
    if not isinstance(linked_payload, dict):
        return False
    if linked_payload.get("child") != event["task_id"] or linked_payload.get("parent") != parent:
        return False
    return conn.execute("SELECT 1 FROM tasks WHERE id = ?", (parent,)).fetchone() is not None
