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
    dispatch_profiles: tuple[str, ...] = ()
    min_age_seconds: int = 1800
    min_eligible_misses: int = 3

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
    if value is None:
        return True
    data = _payload(value)
    return bool(data and data.get("status") == "ready")


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
        # A recognized Ready-entry event is authoritative.  If its payload is
        # malformed, an older valid event cannot resurrect the card's age.
        if row["kind"] in READY_ENTRY_CLASSIFIERS:
            if classify_ready_state(row["kind"], row["payload"], task["status"]):
                return int(row["id"]), int(row["created_at"])
            return None
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
 state TEXT NOT NULL, diagnostic_code TEXT,
 profile_scope_digest TEXT, monitor_protocol_version INTEGER, compatibility_identity TEXT,
 installed_revision TEXT, roster_generation TEXT, roster_digest TEXT,
 dispatch_profiles_fingerprint TEXT, board_running INTEGER, board_cap INTEGER,
 host_running INTEGER, host_cap INTEGER, profile_running INTEGER, profile_cap INTEGER,
 ready_count INTEGER, eligible_ready_count INTEGER, suppressed_ready_count INTEGER,
 PRIMARY KEY(board_scope_id, writer_ref_digest, controller_generation, profile));
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
                   JOIN kanban_dispatch_attempts d ON d.id = a.attempt_id
                   JOIN kanban_monitor_scope_activation s
                     ON s.board_scope_id = a.board_scope_id AND s.profile = a.profile
                    AND s.activation_epoch = a.activation_epoch AND s.disabled_at IS NULL
                   WHERE a.token = NEW.dispatch_admission_id AND a.state = 'armed'
                     AND d.state = 'open' AND a.task_id = OLD.id AND a.profile = OLD.assignee
                     AND d.board_scope_id = a.board_scope_id AND d.profile = a.profile
                     AND d.activation_epoch = a.activation_epoch
                     AND d.writer_ref_digest = a.writer_ref_digest
                     AND d.controller_generation = a.controller_generation
                     AND a.ready_generation = COALESCE((SELECT MAX(id) FROM task_events
                         WHERE task_id=OLD.id AND kind IN ('created','promoted','promoted_manual',
                         'unblocked','changes_requested','review_reopened','status','reclaimed',
                         'timed_out','stale','protocol_violation','rate_limited','crashed',
                         'spawn_failed','reconciled','imported')), 0)
                     AND a.run_generation = COALESCE((SELECT MAX(id) FROM task_runs WHERE task_id=OLD.id), 0));
 UPDATE kanban_claim_admissions SET state='consumed', consumed_at=strftime('%s','now')
 WHERE token = NEW.dispatch_admission_id AND state='armed';
END;
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TRIGGER IF EXISTS kanban_ready_claim_admission")
    conn.executescript(MONITOR_SCHEMA_SQL)
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(kanban_ready_episodes)")}
    if "authority_set_digest" not in columns:
        conn.execute("ALTER TABLE kanban_ready_episodes ADD COLUMN authority_set_digest TEXT")
    observation_columns = {row["name"] for row in conn.execute("PRAGMA table_info(kanban_controller_observations)")}
    for name, definition in {
        "profile_scope_digest": "TEXT", "monitor_protocol_version": "INTEGER",
        "compatibility_identity": "TEXT", "installed_revision": "TEXT",
        "roster_generation": "TEXT", "roster_digest": "TEXT",
        "dispatch_profiles_fingerprint": "TEXT", "board_running": "INTEGER",
        "board_cap": "INTEGER", "host_running": "INTEGER", "host_cap": "INTEGER",
        "profile_running": "INTEGER", "profile_cap": "INTEGER", "ready_count": "INTEGER",
        "eligible_ready_count": "INTEGER", "suppressed_ready_count": "INTEGER",
    }.items():
        if name not in observation_columns:
            conn.execute(f"ALTER TABLE kanban_controller_observations ADD COLUMN {name} {definition}")
    row = conn.execute("SELECT board_scope_id FROM kanban_board_identity WHERE singleton=1").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO kanban_board_identity VALUES (1, ?, ?, 1)",
            (str(uuid.uuid4()), int(time.time())),
        )
    conn.commit()


def _scope(conn: sqlite3.Connection, profile: str) -> sqlite3.Row | None:
    rows = conn.execute(
        "SELECT * FROM kanban_monitor_scope_activation WHERE profile=? AND disabled_at IS NULL "
        "ORDER BY id", (profile,)
    ).fetchall()
    return rows[0] if len(rows) == 1 else None


def _uuid(value: Any) -> bool:
    try:
        uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        return False
    return True


def _profiles(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple, set)):
        return ()
    return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))


def parse_expected_writer_scopes(value: Any) -> dict[str, dict[str, Any]]:
    """Parse and canonicalize the deployment-owned writer roster."""
    if not isinstance(value, Mapping):
        raise ValueError("expected_writer_scopes must be a mapping")
    parsed: dict[str, dict[str, Any]] = {}
    for board_id, raw in value.items():
        if not _uuid(board_id) or not isinstance(raw, Mapping) or not _uuid(raw.get("roster_generation")):
            raise ValueError("invalid writer scope")
        members = raw.get("members")
        if not isinstance(members, list) or not members:
            raise ValueError("writer scope requires members")
        refs: set[str] = set()
        profiles: set[str] = set()
        canonical = []
        for member in members:
            if not isinstance(member, Mapping) or not _uuid(member.get("writer_ref")):
                raise ValueError("invalid writer member")
            member_profiles = _profiles(member.get("profiles"))
            if not member_profiles or member["writer_ref"] in refs or profiles.intersection(member_profiles):
                raise ValueError("duplicate or overlapping writer member")
            try:
                protocol = int(member.get("protocol_version"))
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid writer protocol") from exc
            compatible = member.get("compatible_revisions")
            if protocol != PROTOCOL_VERSION or not isinstance(compatible, list) or not compatible:
                raise ValueError("invalid writer compatibility")
            refs.add(str(member["writer_ref"])); profiles.update(member_profiles)
            canonical.append({"writer_ref": str(member["writer_ref"]), "profiles": member_profiles,
                              "protocol_version": protocol, "compatible_revisions": tuple(sorted(map(str, compatible)))})
        parsed[str(board_id)] = {"roster_generation": str(raw["roster_generation"]),
                                 "roster_digest": raw.get("roster_digest"),
                                 "members": tuple(sorted(canonical, key=lambda item: item["writer_ref"]))}
    return parsed


def validate_writer_context(conn: sqlite3.Connection, controller: MonitorWriterContext,
                            config: Mapping[str, Any] | None = None) -> str | None:
    """Validate a writer against the configured roster and one exact scope."""
    scope = conn.execute(
        "SELECT * FROM kanban_monitor_scope_activation WHERE board_scope_id=? AND profile=? "
        "AND disabled_at IS NULL", (controller.board_scope_id, controller.profile),
    ).fetchall()
    if len(scope) != 1:
        return "writer_scope_incomplete"
    scope = scope[0]
    cfg = dict(config or {})
    aged = cfg.get("aged_ready", cfg)
    try:
        expected = parse_expected_writer_scopes(aged.get("expected_writer_scopes", {})) if isinstance(aged, Mapping) else {}
    except ValueError:
        return "writer_scope_incomplete"
    board = expected.get(controller.board_scope_id)
    members = board.get("members", []) if isinstance(board, Mapping) else []
    declared_profiles = _profiles(cfg.get("dispatch_profiles", aged.get("dispatch_profiles", ()))) if isinstance(aged, Mapping) else ()
    matches = []
    for member in members if isinstance(members, (list, tuple)) else ():
        if not isinstance(member, Mapping):
            continue
        member_profiles = _profiles(member.get("profiles"))
        # A writer owns its configured partition.  ``dispatch_profiles`` is
        # deliberately exact: a writer must not silently widen its lease.
        if (member.get("writer_ref") == controller.writer_ref
                and member_profiles == declared_profiles):
            matches.append(member)
    if len(matches) != 1:
        return "writer_scope_incomplete"
    member = matches[0]
    if declared_profiles != (controller.profile,) and controller.profile not in declared_profiles:
        return "incompatible_writer_context"
    try:
        member_protocol = int(member.get("protocol_version", -1))
    except (TypeError, ValueError):
        return "incompatible_writer_context"
    compatible = member.get("compatible_revisions", ())
    if not isinstance(compatible, (list, tuple, set)):
        return "incompatible_writer_context"
    if (scope["activation_epoch"] != controller.activation_epoch or
            scope["roster_generation"] != controller.roster_generation or
            scope["roster_digest"] != controller.roster_digest or
            scope["board_scope_id"] != controller.board_scope_id or
            (isinstance(board, Mapping) and board.get("roster_generation") not in (None, controller.roster_generation)) or
            (isinstance(board, Mapping) and board.get("roster_digest") not in (None, controller.roster_digest)) or
            member_protocol != controller.protocol_version or
            controller.compatibility_identity not in compatible or
            controller.compatibility_identity != COMPATIBILITY_IDENTITY or
            not controller.installed_revision or
            controller.profile not in _profiles(member.get("profiles"))):
        return "incompatible_writer_context"
    return None


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
                           surface: str, now: int | None = None,
                           config: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
    ensure_schema(conn)
    now = int(time.time()) if now is None else int(now)
    # Callers commonly publish the heartbeat immediately before opening the
    # attempt.  That write must not become an outer transaction around the
    # real claim (write_txn intentionally refuses that composition).
    if conn.in_transaction:
        conn.commit()
    scope = _scope(conn, controller.profile)
    if not scope or scope["board_scope_id"] != controller.board_scope_id:
        return None
    context_error = (validate_writer_context(conn, controller, config)
                     if config is not None else None)
    if (context_error or scope["activation_epoch"] != controller.activation_epoch or
            scope["roster_generation"] != controller.roster_generation or
            scope["roster_digest"] != controller.roster_digest or
            scope["protocol_version"] != controller.protocol_version or
            scope["compatibility_identity"] != controller.compatibility_identity or
            not controller.installed_revision):
        conn.execute("INSERT INTO kanban_writer_faults(board_scope_id,profile,activation_epoch,diagnostic_code,created_at) VALUES(?,?,?,?,?)",
                     (scope["board_scope_id"], controller.profile, scope["activation_epoch"], context_error or "incompatible_writer_context", now))
        conn.commit()
        return None
    attempt_id = str(uuid.uuid4())
    conn.execute("INSERT INTO kanban_dispatch_attempts(id,board_scope_id,profile,activation_epoch,writer_ref_digest,controller_generation,tick_id,surface,protocol_version,compatibility_identity,installed_revision,roster_generation,roster_digest,started_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (attempt_id, controller.board_scope_id, controller.profile, controller.activation_epoch,
                  _digest(controller.writer_ref), controller.controller_generation, controller.tick_id or str(uuid.uuid4()), surface,
                  controller.protocol_version, controller.compatibility_identity, controller.installed_revision,
                  controller.roster_generation, controller.roster_digest, now))
    conn.commit()
    return dict(conn.execute("SELECT * FROM kanban_dispatch_attempts WHERE id=?", (attempt_id,)).fetchone())


def publish_controller_observation(conn: sqlite3.Connection, *, controller: MonitorWriterContext,
                                   state: str = "fresh", diagnostic_code: str | None = None,
                                   now: int | None = None) -> None:
    """Publish a redacted lease heartbeat for the controller's explicit scope."""
    ensure_schema(conn)
    now = int(time.time()) if now is None else int(now)
    conn.execute(
        "INSERT INTO kanban_controller_observations "
        "(board_scope_id,profile,writer_ref_digest,controller_generation,activation_epoch,last_seen_at,state,diagnostic_code,"
        "profile_scope_digest,monitor_protocol_version,compatibility_identity,installed_revision,roster_generation,roster_digest,dispatch_profiles_fingerprint) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(board_scope_id,writer_ref_digest,controller_generation,profile) DO UPDATE SET "
        "activation_epoch=excluded.activation_epoch,last_seen_at=excluded.last_seen_at,state=excluded.state,diagnostic_code=excluded.diagnostic_code,"
        "profile_scope_digest=excluded.profile_scope_digest,monitor_protocol_version=excluded.monitor_protocol_version,"
        "compatibility_identity=excluded.compatibility_identity,installed_revision=excluded.installed_revision,"
        "roster_generation=excluded.roster_generation,roster_digest=excluded.roster_digest,"
        "dispatch_profiles_fingerprint=excluded.dispatch_profiles_fingerprint",
        (controller.board_scope_id, controller.profile, _digest(controller.writer_ref), controller.controller_generation,
         controller.activation_epoch, now, state, diagnostic_code,
         _digest(controller.profile), controller.protocol_version, controller.compatibility_identity,
         controller.installed_revision, controller.roster_generation, controller.roster_digest,
         _digest(",".join(controller.dispatch_profiles or (controller.profile,)))),
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


def validate_claim_admission(conn: sqlite3.Connection, token: str, task_id: str,
                             profile: str, ready_gen: int, run_gen: int) -> bool:
    row = conn.execute(
        "SELECT a.*, d.state AS d_state, d.board_scope_id AS d_board, d.profile AS d_profile, "
        "d.activation_epoch AS d_epoch, d.writer_ref_digest AS d_writer, "
        "d.controller_generation AS d_controller, d.tick_id AS d_tick "
        "FROM kanban_claim_admissions a JOIN kanban_dispatch_attempts d ON d.id=a.attempt_id "
        "WHERE a.token=? AND a.state='armed' AND a.task_id=? AND a.profile=? "
        "AND a.ready_generation=? AND a.run_generation=?", (token, task_id, profile, ready_gen, run_gen),
    ).fetchone()
    if not row or row["d_state"] != "open":
        scope = _scope(conn, profile)
        if scope:
            conn.execute(
                "INSERT INTO kanban_writer_faults(board_scope_id,profile,activation_epoch,diagnostic_code,created_at) VALUES(?,?,?,?,?)",
                (scope["board_scope_id"], profile, scope["activation_epoch"],
                 "invalid_claim_admission", int(time.time())),
            )
        return False
    current = ready_generation(conn, task_id)
    valid = bool(current and current[0] == ready_gen and row["d_board"] == row["board_scope_id"]
                and row["d_profile"] == profile and row["d_epoch"] == row["activation_epoch"]
                and row["d_writer"] == row["writer_ref_digest"]
                and row["d_controller"] == row["controller_generation"]
                and row["d_tick"] == conn.execute(
                    "SELECT tick_id FROM kanban_dispatch_attempts WHERE id=?", (row["attempt_id"],)
                ).fetchone()[0])
    if not valid:
        conn.execute(
            "INSERT INTO kanban_writer_faults(board_scope_id,profile,activation_epoch,diagnostic_code,created_at) VALUES(?,?,?,?,?)",
            (row["board_scope_id"], profile, row["activation_epoch"],
             "invalid_claim_admission", int(time.time())),
        )
    return valid


def has_protected_scope(conn: sqlite3.Connection) -> bool:
    ensure_schema(conn)
    return conn.execute(
        "SELECT 1 FROM kanban_monitor_scope_activation WHERE disabled_at IS NULL "
        "AND state IN ('pending','active','faulted')"
    ).fetchone() is not None


def ensure_ready_decisions(conn: sqlite3.Connection, result: Any, reason: str) -> None:
    seen = {entry["task_id"] for entry in result.monitor_decisions}
    for row in conn.execute("SELECT id, assignee FROM tasks WHERE status='ready' AND claim_lock IS NULL"):
        if row["id"] not in seen:
            result.monitor_decisions.append({"task_id": row["id"], "decision": "unassigned" if not row["assignee"] else reason})


def runtime_context(conn: sqlite3.Connection, writer_ref: str, controller_generation: str) -> MonitorWriterContext | None:
    scopes = conn.execute("SELECT * FROM kanban_monitor_scope_activation WHERE disabled_at IS NULL AND state IN ('pending','active','faulted') ORDER BY id").fetchall()
    if len(scopes) != 1:
        return None
    scope = scopes[0]
    try:
        from hermes_cli.config import load_config
        cfg = load_config().get("kanban", {})
    except (OSError, TypeError, ValueError, KeyError):
        cfg = {}
    aged = cfg.get("aged_ready") or {}
    from hermes_cli.version_info import get_version_info
    info = get_version_info()
    dispatch_profiles = _profiles(aged.get("dispatch_profiles", cfg.get("dispatch_profiles", ())))
    selected_writer = str(aged.get("controller_id") or writer_ref)
    if not aged.get("controller_id"):
        board = (aged.get("expected_writer_scopes", {}).get(scope["board_scope_id"], {})
                 if isinstance(aged.get("expected_writer_scopes", {}), Mapping) else {})
        for member in board.get("members", ()) if isinstance(board, Mapping) else ():
            if scope["profile"] in _profiles(member.get("profiles")):
                selected_writer = str(member.get("writer_ref"))
                dispatch_profiles = _profiles(member.get("profiles"))
                break
    return MonitorWriterContext(
        board_scope_id=scope["board_scope_id"], profile=scope["profile"],
        writer_ref=selected_writer, controller_generation=controller_generation,
        activation_epoch=scope["activation_epoch"], roster_generation=scope["roster_generation"],
        roster_digest=scope["roster_digest"], installed_revision=info.commit or info.base_version,
        dispatch_profiles=dispatch_profiles or (scope["profile"],), min_age_seconds=int(aged.get("min_age_seconds", 1800)),
        min_eligible_misses=int(aged.get("min_eligible_misses", 3)),
    )


def record_dispatch_tick(conn: sqlite3.Connection, *, controller: MonitorWriterContext,
                         attempt_id: str, decisions: list[Mapping[str, Any]], now: int | None = None,
                         config: Mapping[str, Any] | None = None) -> bool:
    """Persist a tick exactly once; replayed task decisions do not increment."""
    ensure_schema(conn)
    now = int(time.time()) if now is None else int(now)
    conn.execute("BEGIN IMMEDIATE")
    attempt = conn.execute("SELECT * FROM kanban_dispatch_attempts WHERE id=?", (attempt_id,)).fetchone()
    if not attempt or attempt["state"] != "open":
        conn.rollback()
        return False
    if any((attempt[key] != value) for key, value in {
        "board_scope_id": controller.board_scope_id,
        "profile": controller.profile,
        "activation_epoch": controller.activation_epoch,
        "writer_ref_digest": _digest(controller.writer_ref),
        "controller_generation": controller.controller_generation,
        "roster_generation": controller.roster_generation,
        "roster_digest": controller.roster_digest,
    }.items()):
        conn.rollback()
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
        try:
            observed = observe_ready(conn, task_id, now=now, decision=(str(decision.get("decision", "unknown"))
                          if projection_allowed else "unknown"),
                          guard_reason=decision.get("guard_reason"), profile=controller.profile,
                          controller_ref_digest=_digest(controller.writer_ref),
                          controller_generation=controller.controller_generation,
                          activation_epoch=controller.activation_epoch, tick_id=attempt["tick_id"],
                          min_age_seconds=controller.min_age_seconds,
                          min_eligible_misses=controller.min_eligible_misses, commit=False)
            if observed.get("episode_id") and config:
                aged = config.get("aged_ready", config)
                bindings = aged.get("resolver_bindings", {}).get(controller.profile, {}) if isinstance(aged, Mapping) else {}
                project_escalation_intents(
                    conn, observed["episode_id"], bindings=bindings,
                    owner_retry_seconds=tuple(aged.get("owner_retry_seconds", (300, 900, 3600))),
                    manager_retry_seconds=tuple(aged.get("manager_retry_seconds", (900, 3600))), now=now,
                )
        except Exception:
            conn.rollback()
            raise
    conn.execute("UPDATE kanban_dispatch_attempts SET state='complete', completed_at=?, decision_count=? WHERE id=?",
                 (now, len(decisions), attempt_id))
    conn.commit()
    return True


def observe_ready(conn: sqlite3.Connection, task_id: str, *, now: int | None = None,
                  decision: str = "eligible_missed", guard_reason: str | None = None,
                  guard_evidence: Mapping[str, Any] | None = None, profile: str | None = None,
                  controller_ref_digest: str | None = None, controller_generation: str | None = None,
                  activation_epoch: str = "disabled", tick_id: str | None = None,
                  min_age_seconds: int = 1800, min_eligible_misses: int = 3,
                  commit: bool = True) -> dict[str, Any]:
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
    if commit:
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
    conn.execute(
        "INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
        (task_id, "aged_ready_opened", json.dumps({"episode_id": episode_id,
         "ready_generation": generation, "eligible_miss_count": count}), now),
    )
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
        changed = conn.execute("UPDATE kanban_ready_episodes SET resolver_stage=?, outcome=? WHERE id=? AND (resolver_stage != ? OR outcome IS NOT ?)",
                               ("unavailable", code, episode_id, "unavailable", code)).rowcount
        if changed:
            conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) SELECT task_id, 'aged_ready_diagnostic', ?, ? FROM kanban_ready_episodes WHERE id=?",
                         (json.dumps({"code": code, "episode_id": episode_id}), now, episode_id))
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
    if not old or old != digest:
        conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) SELECT task_id, 'aged_ready_intents_projected', ?, ? FROM kanban_ready_episodes WHERE id=?",
                     (json.dumps({"episode_id": episode_id, "routing_generation": routing}), now, episode_id))
    rows = conn.execute("SELECT stage,ordinal,scheduled_at,state FROM kanban_ready_escalation_intents WHERE episode_id=? AND routing_generation=? ORDER BY CASE stage WHEN 'owner' THEN 0 ELSE 1 END, ordinal", (episode_id, routing)).fetchall()
    return {"episode_id": episode_id, "routing_generation": routing, "resolver_stage": "owner", "resolver_digest": digest,
            "intents": [dict(row) for row in rows]}


def _projection_proven(conn: sqlite3.Connection, scope: sqlite3.Row, *, now: int,
                       stale_after_seconds: int, config: Mapping[str, Any] | None) -> bool:
    if not config or scope["state"] != "active" or scope["legacy_exclusion_proof_state"] != "proven":
        return False
    aged = config.get("aged_ready", config)
    expected = aged.get("expected_writer_scopes", {}) if isinstance(aged, Mapping) else {}
    board = expected.get(scope["board_scope_id"]) if isinstance(expected, Mapping) else None
    members = board.get("members", []) if isinstance(board, Mapping) else []
    expected_refs = {m.get("writer_ref") for m in members if isinstance(m, Mapping)
                    and scope["profile"] in _profiles(m.get("profiles"))}
    expected_digests = {_digest(str(ref)) for ref in expected_refs}
    if not expected_refs or any(not _uuid(ref) for ref in expected_refs):
        return False
    observations = conn.execute(
        "SELECT * FROM kanban_controller_observations WHERE board_scope_id=? AND profile=?",
        (scope["board_scope_id"], scope["profile"]),
    ).fetchall()
    fresh = [row for row in observations if now - int(row["last_seen_at"]) <= stale_after_seconds]
    if {row["writer_ref_digest"] for row in fresh} != expected_digests:
        return False
    if any(row["state"] != "fresh" or row["activation_epoch"] != scope["activation_epoch"]
           or row["roster_generation"] != scope["roster_generation"]
           or row["roster_digest"] != scope["roster_digest"]
           or row["monitor_protocol_version"] != scope["protocol_version"]
           or row["compatibility_identity"] != scope["compatibility_identity"]
           or not row["installed_revision"] for row in fresh):
        return False
    if conn.execute(
        "SELECT 1 FROM kanban_writer_faults WHERE board_scope_id=? AND profile=? "
        "AND activation_epoch=? LIMIT 1",
        (scope["board_scope_id"], scope["profile"], scope["activation_epoch"]),
    ).fetchone():
        return False
    attempts = conn.execute(
        "SELECT writer_ref_digest, controller_generation, state FROM kanban_dispatch_attempts "
        "WHERE board_scope_id=? AND profile=? AND activation_epoch=?",
        (scope["board_scope_id"], scope["profile"], scope["activation_epoch"]),
    ).fetchall()
    if any(row["state"] in ("open", "aborted") for row in attempts):
        return False
    fresh_pairs = {(row["writer_ref_digest"], row["controller_generation"]) for row in fresh}
    admitted = {(row["writer_ref_digest"], row["controller_generation"])
                for row in attempts if row["state"] == "complete"}
    if not fresh_pairs <= admitted or {writer for writer, _generation in fresh_pairs} != expected_digests:
        return False
    # Every protected running card must carry a live admission correlated to
    # this scope; otherwise the numeric projection is not trustworthy.
    return not conn.execute(
        "SELECT 1 FROM tasks t WHERE t.status='running' AND t.assignee=? "
        "AND (t.dispatch_admission_id IS NULL OR NOT EXISTS ("
        "SELECT 1 FROM kanban_claim_admissions a JOIN kanban_dispatch_attempts d ON d.id=a.attempt_id "
        "WHERE a.token=t.dispatch_admission_id AND a.profile=? AND a.activation_epoch=?)) LIMIT 1",
        (scope["profile"], scope["profile"], scope["activation_epoch"]),
    ).fetchone()


def ready_health_snapshot(conn: sqlite3.Connection, *, now: int | None = None,
                          stale_after_seconds: int = 180,
                          config: Mapping[str, Any] | None = None) -> dict[str, Any]:
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
        proven = _projection_proven(conn, scope, now=now,
                                    stale_after_seconds=stale_after_seconds, config=config)
        conflicting_generations = len({row["controller_generation"] for row in fresh}) > 1
        if conflicting_generations:
            state = "CONFLICT"
        elif not proven:
            state = "UNKNOWN"
        elif not fresh or "faulted" in states:
            state = "STALE"
        else:
            state = "FRESH"
        running = (conn.execute("SELECT COUNT(*) FROM tasks WHERE status='running' AND assignee=?", (scope["profile"],)).fetchone()[0]
                   if proven else None)
        ready = (conn.execute("SELECT COUNT(*) FROM tasks WHERE status='ready' AND assignee=?", (scope["profile"],)).fetchone()[0]
                 if proven else None)
        profiles.append({"profile": scope["profile"], "running_wip": running, "ready_count": ready,
                         "controller_digests": sorted({row["writer_ref_digest"] for row in fresh}), "state": state})
    activation = "OFF" if not scopes else ("UNKNOWN" if any(p["state"] != "FRESH" for p in profiles) else "FRESH")
    return {"cards": cards, "activation": activation, "profiles": profiles, "owners": [], "read_only": True}
