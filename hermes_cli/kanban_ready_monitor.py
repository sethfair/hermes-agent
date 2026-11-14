"""Durable, read-only aged-Ready observations.

This module deliberately contains no delivery or card mutation API.  A monitor
observation is a controller fact; it is not evidence that a worker was sent.
"""
from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Mapping

PROTOCOL_VERSION = 1
COMPATIBILITY_IDENTITY = "aged-ready-v1"


class ReadyDecision(str, Enum):
    ELIGIBLE_MISSED = "eligible_missed"
    CLAIMED = "claimed"
    UNKNOWN = "unknown"


SUPPRESSION_REASONS = (
    "active_claim", "parents_open", "review_hold", "release_or_pr_hold",
    "scheduled_or_blocked", "unknown_block_identity", "unassigned",
    "nonspawnable_profile", "profile_cap", "board_cap", "host_cap",
    "review_reservation", "shared_budget_exhausted", "memory_pressure",
    "dispatch_profile_denied", "dispatcher_lock_lost", "rate_limit_cooldown",
    "infrastructure_cooldown", "recent_success", "auth_or_quota_guard",
    "guard_unknown", "dry_run",
)


@dataclass(frozen=True)
class ReadyEntryClassifier:
    event_kind: str
    predicate: Callable[[Any], bool]


def _payload(value: Any) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return None
    return value if isinstance(value, Mapping) else None


def _absent_or_ready(value: Any) -> bool:
    data = _payload(value)
    return data is None or data.get("status") == "ready"


def _retry_ready(value: Any) -> bool:
    data = _payload(value)
    return bool(data and data.get("retry_status") == "ready")


def _manual_import_ready(value: Any) -> bool:
    data = _payload(value)
    return bool(data and data.get("parked") is False)


READY_ENTRY_CLASSIFIERS: dict[str, ReadyEntryClassifier] = {
    "created": ReadyEntryClassifier("created", lambda p: _payload(p) is not None and _payload(p).get("status") == "ready"),
    "promoted": ReadyEntryClassifier("promoted", _absent_or_ready),
    "promoted_manual": ReadyEntryClassifier("promoted_manual", _absent_or_ready),
    "unblocked": ReadyEntryClassifier("unblocked", _absent_or_ready),
    "changes_requested": ReadyEntryClassifier("changes_requested", lambda p: _payload(p) is not None and _payload(p).get("status") == "ready"),
    "review_reopened": ReadyEntryClassifier("review_reopened", _absent_or_ready),
    "status": ReadyEntryClassifier("status", lambda p: _payload(p) is not None and _payload(p).get("status") == "ready"),
    "reclaimed": ReadyEntryClassifier("reclaimed", _retry_ready),
    "timed_out": ReadyEntryClassifier("timed_out", _retry_ready),
    "stale": ReadyEntryClassifier("stale", _retry_ready),
    "protocol_violation": ReadyEntryClassifier("protocol_violation", _retry_ready),
    "rate_limited": ReadyEntryClassifier("rate_limited", _retry_ready),
    "crashed": ReadyEntryClassifier("crashed", _retry_ready),
    "spawn_failed": ReadyEntryClassifier("spawn_failed", _retry_ready),
    "reconciled": ReadyEntryClassifier("reconciled", lambda p: True),
    "imported": ReadyEntryClassifier("imported", _manual_import_ready),
}


def canonical_guard_fingerprint(reason: str, evidence: Mapping[str, Any] | None = None) -> str:
    """Hash stable enums and durable identifiers, never prose or credentials."""
    safe = {"protocol": PROTOCOL_VERSION, "reason": str(reason)}
    if evidence:
        safe["evidence"] = {
            str(k): v for k, v in sorted(evidence.items())
            if str(k).endswith(("_id", "_generation", "_digest", "_code"))
        }
    raw = json.dumps(safe, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def classify_ready_state(event_kind: str, payload: Any, committed_status: str) -> bool:
    """Return whether one event proves a committed landing in Ready."""
    classifier = READY_ENTRY_CLASSIFIERS.get(event_kind)
    if committed_status != "ready" or classifier is None:
        return False
    return bool(classifier.predicate(payload))


def ready_generation(conn: sqlite3.Connection, task_id: str) -> tuple[int, int] | None:
    """Return ``(event id, event time)`` for the newest unambiguous Ready entry."""
    task = conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if not task or task["status"] != "ready":
        return None
    rows = conn.execute(
        "SELECT id, kind, payload, created_at FROM task_events WHERE task_id = ? ORDER BY id DESC",
        (task_id,),
    )
    for row in rows:
        if row["kind"] in READY_ENTRY_CLASSIFIERS and classify_ready_state(row["kind"], row["payload"], task["status"]):
            return int(row["id"]), int(row["created_at"])
    return None


MONITOR_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS kanban_board_identity (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), board_scope_id TEXT NOT NULL,
 created_at INTEGER NOT NULL, identity_version INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS kanban_ready_state (
 task_id TEXT PRIMARY KEY, ready_generation INTEGER, ready_since INTEGER,
 run_generation INTEGER NOT NULL DEFAULT 0, first_observed_at INTEGER NOT NULL,
 last_observed_at INTEGER NOT NULL, controller_ref_digest TEXT,
 controller_generation TEXT, profile TEXT, decision TEXT, guard_reason TEXT,
 guard_fingerprint TEXT, eligible_miss_count INTEGER NOT NULL DEFAULT 0,
 episode_id TEXT);
CREATE TABLE IF NOT EXISTS kanban_ready_episodes (
 id TEXT PRIMARY KEY, task_id TEXT NOT NULL, ready_generation INTEGER NOT NULL,
 profile TEXT, controller_ref_digest TEXT, controller_generation TEXT,
 run_generation INTEGER NOT NULL DEFAULT 0, guard_reason TEXT, guard_fingerprint TEXT,
 first_observed_at INTEGER NOT NULL, last_observed_at INTEGER NOT NULL,
 opened_at INTEGER NOT NULL, closed_at INTEGER, eligible_miss_count INTEGER NOT NULL DEFAULT 0,
 intent_count INTEGER NOT NULL DEFAULT 0, resolver_stage TEXT NOT NULL DEFAULT 'unavailable',
 routing_generation INTEGER NOT NULL DEFAULT 0, next_intent_at INTEGER,
 state TEXT NOT NULL DEFAULT 'open', outcome TEXT,
 UNIQUE(task_id, ready_generation));
CREATE TABLE IF NOT EXISTS kanban_ready_escalation_intents (
 id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, routing_generation INTEGER NOT NULL,
 stage TEXT NOT NULL, ordinal INTEGER NOT NULL, scheduled_at INTEGER NOT NULL,
 state TEXT NOT NULL, resolver_ref_digest TEXT, authority_set_digest TEXT,
 diagnostic_code TEXT, UNIQUE(episode_id, routing_generation, stage, ordinal));
CREATE TABLE IF NOT EXISTS kanban_ready_tick_observations (
 activation_epoch TEXT NOT NULL, controller_ref_digest TEXT NOT NULL,
 controller_generation TEXT NOT NULL, tick_id TEXT NOT NULL, task_id TEXT NOT NULL,
 created_at INTEGER NOT NULL, UNIQUE(activation_epoch, controller_ref_digest,
 controller_generation, tick_id, task_id));
CREATE INDEX IF NOT EXISTS idx_ready_episode_task ON kanban_ready_episodes(task_id);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(MONITOR_SCHEMA_SQL)
    row = conn.execute("SELECT board_scope_id FROM kanban_board_identity WHERE singleton=1").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO kanban_board_identity VALUES (1, ?, ?, 1)",
            (secrets.token_hex(16), int(time.time())),
        )
    conn.commit()


def observe_ready(conn: sqlite3.Connection, task_id: str, *, now: int | None = None,
                  decision: str = "eligible_missed", guard_reason: str | None = None,
                  guard_evidence: Mapping[str, Any] | None = None, profile: str | None = None,
                  controller_ref_digest: str | None = None, controller_generation: str | None = None,
                  activation_epoch: str = "disabled", tick_id: str | None = None,
                  min_age_seconds: int = 1800, min_eligible_misses: int = 3) -> dict[str, Any]:
    """Reconcile one observation. Disabled/default callers only observe UNKNOWN."""
    ensure_schema(conn)
    now = int(time.time()) if now is None else int(now)
    generation = ready_generation(conn, task_id)
    if generation is None:
        return {"task_id": task_id, "ready_generation": None, "state": "UNKNOWN"}
    gen, ready_since = generation
    fingerprint = canonical_guard_fingerprint(guard_reason or decision, guard_evidence)
    row = conn.execute("SELECT * FROM kanban_ready_state WHERE task_id=?", (task_id,)).fetchone()
    count = int(row["eligible_miss_count"]) if row and row["ready_generation"] == gen else 0
    if decision == "eligible_missed" and activation_epoch != "disabled":
        count += 1
    conn.execute("""INSERT INTO kanban_ready_state
        (task_id,ready_generation,ready_since,first_observed_at,last_observed_at,profile,
         controller_ref_digest,controller_generation,decision,guard_reason,guard_fingerprint,eligible_miss_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET
        ready_generation=excluded.ready_generation,ready_since=excluded.ready_since,
        last_observed_at=excluded.last_observed_at,profile=excluded.profile,
        controller_ref_digest=excluded.controller_ref_digest,controller_generation=excluded.controller_generation,
        decision=excluded.decision,guard_reason=excluded.guard_reason,guard_fingerprint=excluded.guard_fingerprint,
        eligible_miss_count=excluded.eligible_miss_count""",
        (task_id, gen, ready_since, row["first_observed_at"] if row and row["ready_generation"] == gen else now,
         now, profile, controller_ref_digest, controller_generation, decision, guard_reason, fingerprint, count))
    episode = None
    age = max(0, now - ready_since)
    if activation_epoch != "disabled" and age >= min_age_seconds and count >= min_eligible_misses:
        episode = _open_episode(conn, task_id, gen, now, count, profile, controller_ref_digest, controller_generation, guard_reason, fingerprint)
    conn.commit()
    return {"task_id": task_id, "ready_generation": gen, "ready_since": ready_since,
            "age_seconds": age, "eligible_miss_count": count, "episode_id": episode,
            "decision": decision, "guard_reason": guard_reason, "guard_fingerprint": fingerprint}


def _open_episode(conn: sqlite3.Connection, task_id: str, generation: int, now: int, count: int,
                  profile: str | None, controller_ref_digest: str | None, controller_generation: str | None,
                  reason: str | None, fingerprint: str) -> str:
    row = conn.execute("SELECT id FROM kanban_ready_episodes WHERE task_id=? AND ready_generation=?", (task_id, generation)).fetchone()
    if row:
        conn.execute("UPDATE kanban_ready_episodes SET last_observed_at=?, eligible_miss_count=?, guard_reason=?, guard_fingerprint=? WHERE id=?", (now, count, reason, fingerprint, row["id"]))
        return row["id"]
    episode_id = "ep_" + secrets.token_hex(12)
    conn.execute("INSERT INTO kanban_ready_episodes (id,task_id,ready_generation,profile,controller_ref_digest,controller_generation,first_observed_at,last_observed_at,opened_at,eligible_miss_count,guard_reason,guard_fingerprint) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (episode_id,task_id,generation,profile,controller_ref_digest,controller_generation,now,now,now,count,reason,fingerprint))
    conn.execute("UPDATE kanban_ready_state SET episode_id=? WHERE task_id=?", (episode_id, task_id))
    return episode_id


def close_episode(conn: sqlite3.Connection, task_id: str, outcome: str, *, run_id: int | None = None, final_status: str | None = None, now: int | None = None) -> bool:
    ensure_schema(conn)
    now = int(time.time()) if now is None else int(now)
    row = conn.execute("SELECT episode_id FROM kanban_ready_state WHERE task_id=?", (task_id,)).fetchone()
    if not row or not row["episode_id"]:
        return False
    conn.execute("UPDATE kanban_ready_episodes SET state='closed', outcome=?, closed_at=? WHERE id=? AND state='open'", (outcome, now, row["episode_id"]))
    conn.execute("UPDATE kanban_ready_state SET episode_id=NULL WHERE task_id=?", (task_id,))
    conn.commit()
    return True


def ready_health_snapshot(conn: sqlite3.Connection, *, now: int | None = None, stale_after_seconds: int = 180) -> dict[str, Any]:
    ensure_schema(conn)
    now = int(time.time()) if now is None else int(now)
    cards = []
    for row in conn.execute("SELECT * FROM kanban_ready_state ORDER BY task_id"):
        fresh = now - int(row["last_observed_at"]) <= stale_after_seconds
        cards.append({"task_id": row["task_id"], "ready_generation": row["ready_generation"], "ready_age_seconds": max(0, now - row["ready_since"]) if row["ready_since"] is not None else None, "decision": row["decision"], "guard_reason": row["guard_reason"], "guard_fingerprint": row["guard_fingerprint"], "eligible_miss_count": row["eligible_miss_count"], "freshness": "FRESH" if fresh else "UNKNOWN"})
    return {"cards": cards, "activation": "OFF", "profiles": [], "owners": []}
