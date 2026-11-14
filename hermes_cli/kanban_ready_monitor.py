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
import uuid
import dataclasses
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Mapping

PROTOCOL_VERSION = 1
COMPATIBILITY_IDENTITY = "aged-ready-v1"


class ReadyDecision(str, Enum):
    ELIGIBLE_MISSED = "eligible_missed"
    CLAIMED = "claimed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class MonitorWriterContext:
    board_scope_id: str
    profile: str
    writer_ref: str
    controller_generation: str
    activation_epoch: str
    roster_generation: str
    roster_digest: str
    installed_revision: str
    protocol_version: int = PROTOCOL_VERSION
    compatibility_identity: str = COMPATIBILITY_IDENTITY
    tick_id: str = ""

    def for_tick(self) -> MonitorWriterContext:
        return dataclasses.replace(self, tick_id=str(uuid.uuid4()))


SUPPRESSION_REASONS = (
    "active_claim", "parents_open", "review_hold", "release_or_pr_hold",
    "scheduled_or_blocked", "unknown_block_identity", "unassigned",
    "nonspawnable_profile", "profile_cap", "board_cap", "host_cap",
    "review_reservation", "shared_budget_exhausted", "memory_pressure",
    "dispatch_profile_denied", "dispatcher_lock_lost", "rate_limit_cooldown",
    "infrastructure_cooldown", "recent_success", "auth_or_quota_guard",
    "guard_unknown", "dry_run",
)

TRUTHFUL_HOLD_STATUSES = frozenset({
    "todo", "blocked", "review", "scheduled", "triage", "done", "archived",
})
DIAGNOSTIC_CODES = frozenset({
    "resolver_missing", "manager_missing", "ack_authority_missing",
    "delivery_consumer_unavailable", "controller_stale", "controller_conflict",
    "writer_scope_incomplete",
})


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
CREATE TABLE IF NOT EXISTS kanban_monitor_scope_activation (
 id INTEGER PRIMARY KEY AUTOINCREMENT, board_scope_id TEXT NOT NULL, profile TEXT NOT NULL,
 activation_epoch TEXT NOT NULL, roster_generation TEXT NOT NULL, roster_digest TEXT NOT NULL,
 protocol_version INTEGER NOT NULL, compatibility_identity TEXT NOT NULL,
 legacy_exclusion_control_digest TEXT, legacy_exclusion_proof_state TEXT NOT NULL DEFAULT 'unproven',
 state TEXT NOT NULL DEFAULT 'pending', activated_at INTEGER NOT NULL, disabled_at INTEGER,
 UNIQUE(board_scope_id, profile, activation_epoch));
CREATE UNIQUE INDEX IF NOT EXISTS idx_monitor_scope_current
 ON kanban_monitor_scope_activation(board_scope_id, profile) WHERE disabled_at IS NULL;
CREATE TABLE IF NOT EXISTS kanban_dispatch_attempts (
 id TEXT PRIMARY KEY, board_scope_id TEXT NOT NULL, profile TEXT NOT NULL,
 activation_epoch TEXT NOT NULL, writer_ref_digest TEXT NOT NULL, controller_generation TEXT NOT NULL,
 tick_id TEXT NOT NULL, surface TEXT NOT NULL, protocol_version INTEGER NOT NULL,
 compatibility_identity TEXT NOT NULL, installed_revision TEXT NOT NULL,
 roster_generation TEXT NOT NULL, roster_digest TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'open',
 started_at INTEGER NOT NULL, completed_at INTEGER, decision_count INTEGER NOT NULL DEFAULT 0,
 diagnostic_code TEXT, UNIQUE(activation_epoch, writer_ref_digest, controller_generation, tick_id, board_scope_id));
CREATE TABLE IF NOT EXISTS kanban_claim_admissions (
 token TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, board_scope_id TEXT NOT NULL, profile TEXT NOT NULL,
 activation_epoch TEXT NOT NULL, writer_ref_digest TEXT NOT NULL, controller_generation TEXT NOT NULL,
 task_id TEXT NOT NULL, ready_generation INTEGER NOT NULL, run_generation INTEGER NOT NULL,
 state TEXT NOT NULL DEFAULT 'armed', created_at INTEGER NOT NULL, consumed_at INTEGER);
CREATE TABLE IF NOT EXISTS kanban_writer_faults (
 id INTEGER PRIMARY KEY AUTOINCREMENT, board_scope_id TEXT NOT NULL, profile TEXT,
 activation_epoch TEXT, diagnostic_code TEXT NOT NULL, writer_ref_digest TEXT,
 created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS kanban_controller_observations (
 board_scope_id TEXT NOT NULL, profile TEXT NOT NULL, writer_ref_digest TEXT NOT NULL,
 controller_generation TEXT NOT NULL, activation_epoch TEXT, last_seen_at INTEGER NOT NULL,
 state TEXT NOT NULL, diagnostic_code TEXT, PRIMARY KEY(board_scope_id, writer_ref_digest, controller_generation, profile));
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
 routing_generation INTEGER NOT NULL DEFAULT 0, authority_set_digest TEXT, next_intent_at INTEGER,
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
CREATE INDEX IF NOT EXISTS idx_monitor_fault_scope ON kanban_writer_faults(board_scope_id, profile, activation_epoch);
CREATE INDEX IF NOT EXISTS idx_monitor_admission_attempt ON kanban_claim_admissions(attempt_id, state);
CREATE TRIGGER IF NOT EXISTS kanban_ready_claim_admission
BEFORE UPDATE OF status ON tasks
WHEN OLD.status = 'ready' AND NEW.status = 'running'
 AND EXISTS (SELECT 1 FROM kanban_monitor_scope_activation s
             WHERE s.profile = COALESCE(OLD.assignee, '') AND s.disabled_at IS NULL
               AND s.state IN ('pending','active','faulted'))
BEGIN
 INSERT INTO kanban_writer_faults(board_scope_id, profile, activation_epoch, diagnostic_code, created_at)
 SELECT s.board_scope_id, s.profile, s.activation_epoch, 'unadmitted_ready_claim_refused', strftime('%s','now')
 FROM kanban_monitor_scope_activation s
 WHERE s.profile = COALESCE(OLD.assignee, '') AND s.disabled_at IS NULL
   AND s.state IN ('pending','active','faulted')
   AND NOT EXISTS (SELECT 1 FROM kanban_claim_admissions a
                   WHERE a.token = NEW.dispatch_admission_id AND a.state = 'armed'
                     AND a.task_id = OLD.id AND a.profile = OLD.assignee
                     AND a.activation_epoch = s.activation_epoch
                     AND a.attempt_id IN (SELECT id FROM kanban_dispatch_attempts WHERE state = 'open'));
 SELECT RAISE(IGNORE)
 WHERE NOT EXISTS (SELECT 1 FROM kanban_claim_admissions a
                   WHERE a.token = NEW.dispatch_admission_id AND a.state = 'armed'
                     AND a.task_id = OLD.id AND a.profile = OLD.assignee
                     AND a.activation_epoch = (SELECT activation_epoch FROM kanban_monitor_scope_activation
                                               WHERE profile=OLD.assignee AND disabled_at IS NULL
                                               ORDER BY id DESC LIMIT 1)
                     AND a.attempt_id IN (SELECT id FROM kanban_dispatch_attempts WHERE state = 'open'));
 UPDATE kanban_claim_admissions SET state='consumed', consumed_at=strftime('%s','now')
 WHERE token = NEW.dispatch_admission_id AND state='armed';
END;
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(MONITOR_SCHEMA_SQL)
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(kanban_ready_episodes)")}
    if "authority_set_digest" not in columns:
        conn.execute("ALTER TABLE kanban_ready_episodes ADD COLUMN authority_set_digest TEXT")
    row = conn.execute("SELECT board_scope_id FROM kanban_board_identity WHERE singleton=1").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO kanban_board_identity VALUES (1, ?, ?, 1)",
            (str(uuid.uuid4()), int(time.time())),
        )
    conn.commit()


def _scope(conn: sqlite3.Connection, profile: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM kanban_monitor_scope_activation WHERE profile=? AND disabled_at IS NULL "
        "ORDER BY id DESC LIMIT 1", (profile,)
    ).fetchone()


def activate_scope(conn: sqlite3.Connection, *, profile: str, state: str = "pending",
                   board_scope_id: str | None = None, roster_generation: str = "roster",
                   roster_digest: str = "roster-digest", legacy_exclusion_proof_state: str = "unproven",
                   now: int | None = None) -> dict[str, Any]:
    """Create a protected epoch. Activation never infers legacy exclusion."""
    ensure_schema(conn)
    now = int(time.time()) if now is None else int(now)
    board_scope_id = board_scope_id or conn.execute(
        "SELECT board_scope_id FROM kanban_board_identity WHERE singleton=1"
    ).fetchone()[0]
    prior = _scope(conn, profile)
    if prior:
        conn.execute("UPDATE kanban_monitor_scope_activation SET disabled_at=? WHERE id=?", (now, prior["id"]))
    epoch = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO kanban_monitor_scope_activation "
        "(board_scope_id,profile,activation_epoch,roster_generation,roster_digest,protocol_version,"
        "compatibility_identity,legacy_exclusion_proof_state,state,activated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (board_scope_id, profile, epoch, roster_generation, roster_digest, PROTOCOL_VERSION,
         COMPATIBILITY_IDENTITY, legacy_exclusion_proof_state, state, now),
    )
    return dict(conn.execute("SELECT * FROM kanban_monitor_scope_activation WHERE activation_epoch=?", (epoch,)).fetchone())


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def begin_dispatch_attempt(conn: sqlite3.Connection, *, controller: MonitorWriterContext,
                           surface: str, now: int | None = None) -> dict[str, Any] | None:
    ensure_schema(conn)
    now = int(time.time()) if now is None else int(now)
    scope = _scope(conn, controller.profile)
    if not scope or scope["board_scope_id"] != controller.board_scope_id:
        return None
    if (scope["activation_epoch"] != controller.activation_epoch or
            scope["roster_generation"] != controller.roster_generation or
            scope["roster_digest"] != controller.roster_digest or
            scope["protocol_version"] != controller.protocol_version or
            scope["compatibility_identity"] != controller.compatibility_identity or
            not controller.installed_revision):
        conn.execute("INSERT INTO kanban_writer_faults(board_scope_id,profile,activation_epoch,diagnostic_code,created_at) VALUES(?,?,?,?,?)",
                     (scope["board_scope_id"], controller.profile, scope["activation_epoch"], "incompatible_writer_context", now))
        return None
    attempt_id = str(uuid.uuid4())
    conn.execute("INSERT INTO kanban_dispatch_attempts(id,board_scope_id,profile,activation_epoch,writer_ref_digest,controller_generation,tick_id,surface,protocol_version,compatibility_identity,installed_revision,roster_generation,roster_digest,started_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (attempt_id, controller.board_scope_id, controller.profile, controller.activation_epoch,
                  _digest(controller.writer_ref), controller.controller_generation, controller.tick_id or str(uuid.uuid4()), surface,
                  controller.protocol_version, controller.compatibility_identity, controller.installed_revision,
                  controller.roster_generation, controller.roster_digest, now))
    return dict(conn.execute("SELECT * FROM kanban_dispatch_attempts WHERE id=?", (attempt_id,)).fetchone())


def publish_controller_observation(conn: sqlite3.Connection, *, controller: MonitorWriterContext,
                                   state: str = "fresh", diagnostic_code: str | None = None,
                                   now: int | None = None) -> None:
    """Publish a redacted lease heartbeat for the controller's explicit scope."""
    ensure_schema(conn)
    now = int(time.time()) if now is None else int(now)
    conn.execute(
        "INSERT INTO kanban_controller_observations "
        "(board_scope_id,profile,writer_ref_digest,controller_generation,activation_epoch,last_seen_at,state,diagnostic_code) "
        "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(board_scope_id,writer_ref_digest,controller_generation,profile) DO UPDATE SET "
        "activation_epoch=excluded.activation_epoch,last_seen_at=excluded.last_seen_at,state=excluded.state,diagnostic_code=excluded.diagnostic_code",
        (controller.board_scope_id, controller.profile, _digest(controller.writer_ref), controller.controller_generation,
         controller.activation_epoch, now, state, diagnostic_code),
    )


def arm_claim_admission(conn: sqlite3.Connection, *, attempt_id: str, task_id: str,
                        ready_generation: int, run_generation: int) -> str:
    attempt = conn.execute("SELECT * FROM kanban_dispatch_attempts WHERE id=? AND state='open'", (attempt_id,)).fetchone()
    if not attempt:
        raise ValueError("dispatch attempt is not open")
    token = str(uuid.uuid4())
    conn.execute("INSERT INTO kanban_claim_admissions(token,attempt_id,board_scope_id,profile,activation_epoch,writer_ref_digest,controller_generation,task_id,ready_generation,run_generation,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                 (token, attempt_id, attempt["board_scope_id"], attempt["profile"], attempt["activation_epoch"],
                  attempt["writer_ref_digest"], attempt["controller_generation"], task_id, ready_generation, run_generation, int(time.time())))
    return token


def has_protected_scope(conn: sqlite3.Connection) -> bool:
    ensure_schema(conn)
    return conn.execute(
        "SELECT 1 FROM kanban_monitor_scope_activation WHERE disabled_at IS NULL "
        "AND state IN ('pending','active','faulted') LIMIT 1"
    ).fetchone() is not None


def record_dispatch_tick(conn: sqlite3.Connection, *, controller: MonitorWriterContext,
                         attempt_id: str, decisions: list[Mapping[str, Any]], now: int | None = None) -> bool:
    """Persist a tick exactly once; replayed task decisions do not increment."""
    ensure_schema(conn)
    now = int(time.time()) if now is None else int(now)
    attempt = conn.execute("SELECT * FROM kanban_dispatch_attempts WHERE id=?", (attempt_id,)).fetchone()
    if not attempt or attempt["state"] != "open":
        return False
    scope = _scope(conn, controller.profile)
    projection_allowed = bool(scope and scope["state"] == "active" and
                              scope["legacy_exclusion_proof_state"] == "proven")
    for decision in decisions:
        task_id = str(decision["task_id"])
        duplicate = conn.execute(
            "SELECT 1 FROM kanban_ready_tick_observations WHERE activation_epoch=? AND controller_ref_digest=? "
            "AND controller_generation=? AND tick_id=? AND task_id=?",
            (controller.activation_epoch, _digest(controller.writer_ref), controller.controller_generation,
             attempt["tick_id"], task_id),
        ).fetchone()
        if duplicate:
            continue
        conn.execute(
            "INSERT INTO kanban_ready_tick_observations VALUES(?,?,?,?,?,?)",
            (controller.activation_epoch, _digest(controller.writer_ref), controller.controller_generation,
             attempt["tick_id"], task_id, now),
        )
        observe_ready(conn, task_id, now=now, decision=(str(decision.get("decision", "unknown"))
                      if projection_allowed else "unknown"),
                      guard_reason=decision.get("guard_reason"), profile=controller.profile,
                      controller_ref_digest=_digest(controller.writer_ref),
                      controller_generation=controller.controller_generation,
                      activation_epoch=controller.activation_epoch, tick_id=attempt["tick_id"],
                      min_age_seconds=int(decision.get("min_age_seconds", 1800)),
                      min_eligible_misses=int(decision.get("min_eligible_misses", 3)))
    conn.execute("UPDATE kanban_dispatch_attempts SET state='complete', completed_at=?, decision_count=? WHERE id=?",
                 (now, len(decisions), attempt_id))
    conn.commit()
    return True


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


def _close_episode_in_txn(conn: sqlite3.Connection, task_id: str, outcome: str,
                          *, run_id: int | None = None, final_status: str | None = None,
                          now: int | None = None) -> bool:
    """Close the current episode without committing; callers own the transaction."""
    now = int(time.time()) if now is None else int(now)
    row = conn.execute("SELECT episode_id FROM kanban_ready_state WHERE task_id=?", (task_id,)).fetchone()
    if not row or not row["episode_id"]:
        return False
    changed = conn.execute(
        "UPDATE kanban_ready_episodes SET state='closed', outcome=?, closed_at=? "
        "WHERE id=? AND state='open'", (outcome, now, row["episode_id"]),
    ).rowcount
    if changed:
        conn.execute("UPDATE kanban_ready_state SET episode_id=NULL WHERE task_id=?", (task_id,))
        return True
    return False


def close_for_native_ready_claim(conn: sqlite3.Connection, task_id: str, run_id: int,
                                 *, now: int | None = None) -> bool:
    """Close only the implementation Ready episode, inside the claim transaction."""
    closed = _close_episode_in_txn(conn, task_id, "native_claim", run_id=run_id, now=now)
    if closed:
        conn.execute("INSERT INTO task_events(task_id,kind,payload,run_id,created_at) VALUES(?,?,?,?,?)",
                     (task_id, "aged_ready_closed", json.dumps({"outcome": "native_claim", "run_id": run_id}),
                      run_id, int(time.time()) if now is None else int(now)))
    return closed


def reconcile_truthful_hold(conn: sqlite3.Connection, task_id: str, *, status: str | None = None,
                            now: int | None = None) -> bool:
    """Close an episode only after re-reading a genuine non-dispatchable status."""
    row = conn.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
    final = status or (row["status"] if row else None)
    if final not in TRUTHFUL_HOLD_STATUSES:
        return False
    closed = _close_episode_in_txn(conn, task_id, "truthful_hold", final_status=final, now=now)
    if closed:
        when = int(time.time()) if now is None else int(now)
        conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
                     (task_id, "aged_ready_closed", json.dumps({"outcome": "truthful_hold", "status": final}), when))
    return closed


def _binding_digest(binding: Mapping[str, Any] | None) -> str | None:
    if not binding:
        return None
    safe = {str(k): str(v) for k, v in binding.items() if str(k).endswith(("_id", "_digest", "_alias"))}
    return hashlib.sha256(json.dumps(safe, sort_keys=True).encode()).hexdigest()


def project_escalation_intents(conn: sqlite3.Connection, episode_id: str, *, bindings: Mapping[str, Any] | None,
                               owner_retry_seconds: tuple[int, ...] = (300, 900, 3600),
                               manager_retry_seconds: tuple[int, ...] = (900, 3600), now: int | None = None) -> dict[str, Any]:
    """Create bounded, metadata-only escalation facts; never sends or claims."""
    now = int(time.time()) if now is None else int(now)
    episode = conn.execute("SELECT * FROM kanban_ready_episodes WHERE id=?", (episode_id,)).fetchone()
    if not episode or episode["state"] != "open":
        return {"episode_id": episode_id, "state": "closed", "intents": []}
    binding = dict(bindings or {})
    missing = next((key for key in ("owner_id", "manager_id", "ack_authority_id") if not binding.get(key)), None)
    if missing:
        code = {"owner_id": "resolver_missing", "manager_id": "manager_missing",
                "ack_authority_id": "ack_authority_missing"}[missing]
        conn.execute("UPDATE kanban_ready_episodes SET resolver_stage=?, outcome=? WHERE id=?",
                     ("unavailable", code, episode_id))
        return {"episode_id": episode_id, "state": "unavailable", "diagnostic": code, "intents": []}
    digest = _binding_digest(binding)
    old = episode["authority_set_digest"] if "authority_set_digest" in episode.keys() else None
    routing = int(episode["routing_generation"] or 0) + (1 if old and old != digest else 0)
    if old and old != digest:
        conn.execute("UPDATE kanban_ready_escalation_intents SET state='superseded' WHERE episode_id=? AND state IN ('due','planned')", (episode_id,))
    conn.execute("UPDATE kanban_ready_episodes SET resolver_stage='owner', routing_generation=?, authority_set_digest=? WHERE id=?",
                 (routing, digest, episode_id))
    schedule = [("owner", i + 1, 0 if i == 0 else delay, "due" if i == 0 else "planned")
                for i, delay in enumerate(owner_retry_seconds)]
    schedule += [("manager", i + 1, delay, "planned") for i, delay in enumerate(manager_retry_seconds)]
    for stage, ordinal, delay, state in schedule:
        conn.execute("INSERT OR IGNORE INTO kanban_ready_escalation_intents "
                     "(id,episode_id,routing_generation,stage,ordinal,scheduled_at,state,authority_set_digest) VALUES(?,?,?,?,?,?,?,?)",
                     (f"intent_{secrets.token_hex(10)}", episode_id, routing, stage, ordinal, now + delay, state, digest))
    conn.execute("UPDATE kanban_ready_episodes SET intent_count=(SELECT COUNT(*) FROM kanban_ready_escalation_intents WHERE episode_id=? AND routing_generation=?) WHERE id=?", (episode_id, routing, episode_id))
    rows = conn.execute("SELECT stage,ordinal,scheduled_at,state FROM kanban_ready_escalation_intents WHERE episode_id=? AND routing_generation=? ORDER BY CASE stage WHEN 'owner' THEN 0 ELSE 1 END, ordinal", (episode_id, routing)).fetchall()
    return {"episode_id": episode_id, "routing_generation": routing, "resolver_stage": "owner", "resolver_digest": digest,
            "intents": [dict(row) for row in rows]}


def ready_health_snapshot(conn: sqlite3.Connection, *, now: int | None = None, stale_after_seconds: int = 180) -> dict[str, Any]:
    ensure_schema(conn)
    now = int(time.time()) if now is None else int(now)
    cards = []
    for row in conn.execute("SELECT * FROM kanban_ready_state ORDER BY task_id"):
        fresh = now - int(row["last_observed_at"]) <= stale_after_seconds
        episode = conn.execute("SELECT state,outcome,resolver_stage,routing_generation FROM kanban_ready_episodes WHERE id=?", (row["episode_id"],)).fetchone() if row["episode_id"] else None
        cards.append({"task_id": row["task_id"], "ready_generation": row["ready_generation"],
                      "ready_age_seconds": max(0, now - row["ready_since"]) if row["ready_since"] is not None else None,
                      "decision": row["decision"], "guard_reason": row["guard_reason"],
                      "guard_fingerprint": row["guard_fingerprint"], "eligible_miss_count": row["eligible_miss_count"],
                      "episode_state": episode["state"] if episode else None, "episode_outcome": episode["outcome"] if episode else None,
                      "resolver_stage": episode["resolver_stage"] if episode else None,
                      "routing_generation": episode["routing_generation"] if episode else None,
                      "controller_digest": row["controller_ref_digest"], "controller_generation": row["controller_generation"],
                      "profile": row["profile"], "freshness": "FRESH" if fresh else "UNKNOWN"})
    scopes = conn.execute("SELECT * FROM kanban_monitor_scope_activation WHERE disabled_at IS NULL").fetchall()
    profiles = []
    for scope in scopes:
        observations = conn.execute(
            "SELECT * FROM kanban_controller_observations WHERE board_scope_id=? AND profile=?",
            (scope["board_scope_id"], scope["profile"]),
        ).fetchall()
        fresh = [row for row in observations if now - int(row["last_seen_at"]) <= stale_after_seconds]
        states = {row["state"] for row in fresh}
        if scope["state"] != "active" or scope["legacy_exclusion_proof_state"] != "proven":
            state = "UNKNOWN"
        elif len({row["controller_generation"] for row in fresh}) > 1:
            state = "CONFLICT"
        elif not fresh or "faulted" in states:
            state = "STALE"
        else:
            state = "FRESH"
        running = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='running' AND assignee=?", (scope["profile"],)).fetchone()[0]
        ready = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='ready' AND assignee=?", (scope["profile"],)).fetchone()[0]
        profiles.append({"profile": scope["profile"], "running_wip": running, "ready_count": ready,
                         "controller_digests": sorted({row["writer_ref_digest"] for row in fresh}), "state": state})
    activation = "OFF" if not scopes else ("UNKNOWN" if any(p["state"] != "FRESH" for p in profiles) else "FRESH")
    return {"cards": cards, "activation": activation, "profiles": profiles, "owners": [], "read_only": True}
