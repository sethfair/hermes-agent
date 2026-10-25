"""Profile-local durable audit ledger for cron execution attempts.

The ledger records what is known about each attempt; it is not a retry queue. Interrupted attempts
become ``unknown`` only after their owner process is proved gone — a start-time reading that fails
to match the claim-time fingerprint is not proof of death. Terminal states are immutable.
"""

from __future__ import annotations

import logging
import math
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from hermes_constants import get_hermes_home
from hermes_time import now as _hermes_now
from cron.constants import CLAIM_TTL_INACTIVITY_HEADROOM
from hermes_cli.observability.shared_metrics_gateway import record_cron_finish

logger = logging.getLogger(__name__)

# Optional test override. Production resolves the path at transaction time so dashboard operations
# that temporarily enter another profile cannot leak that profile's records into the import-time
# home.
EXECUTIONS_FILE: Optional[Path] = None
MAX_TERMINAL_EXECUTIONS = 1000
# Unresolved target receipts cannot be pruned; deny new fires before an unbounded pin pile-up.
MAX_TARGET_UNRESOLVED_BINDINGS = 1000
HANDOFF_ADOPTION_GRACE_SECONDS = 30.0
# Floor for the live-owner stale-claim bound (#115692); see _live_owner_stale_after_seconds.
LIVE_OWNER_STALE_CLAIM_FLOOR_SECONDS = 7200.0
_TERMINAL_STATES = ("completed", "failed", "unknown")
_lock = threading.RLock()
_PROCESS_ID = uuid.uuid4().hex


# --- executions ledger --------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    # Late imports: a scheduler daemon that outlives an on-disk upgrade already has the OLD
    # ``hermes_cli.sqlite_util`` / ``cron.jobs`` cached, so new names must be resolved at call time,
    # not at import time (the guarantee cron/ledger.py used to carry, see e24c8499).
    from cron.jobs import _ensure_cron_dir
    from hermes_cli.sqlite_util import open_db

    path = EXECUTIONS_FILE or (get_hermes_home().resolve() / "cron" / "executions.db")
    _ensure_cron_dir(path.parent)
    return open_db(path, db_label="cron/executions.db", synchronous_full=True, initialize=_initialize_schema)


def _initialize_schema(conn: sqlite3.Connection) -> None:
    from hermes_cli.sqlite_util import add_column_if_missing

    conn.execute(
        """CREATE TABLE IF NOT EXISTS executions (
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL,
             source TEXT NOT NULL,
             process_id TEXT NOT NULL,
             pid INTEGER NOT NULL,
             process_started_at INTEGER,
             status TEXT NOT NULL CHECK(status IN
               ('claimed','running','completed','failed','unknown')),
             handoff_pending INTEGER NOT NULL DEFAULT 0,
             handoff_started_at REAL,
             claimed_at TEXT NOT NULL,
             started_at TEXT,
             finished_at TEXT,
             error TEXT
           )"""
    )
    add_column_if_missing(
        conn, "executions", "handoff_pending",
        "handoff_pending INTEGER NOT NULL DEFAULT 0",
    )
    add_column_if_missing(
        conn, "executions", "handoff_started_at", "handoff_started_at REAL"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_job_claimed "
        "ON executions(job_id, claimed_at DESC, id DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_status_claimed "
        "ON executions(status, claimed_at DESC, id DESC)"
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS cron_prerun_bindings (
             execution_id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL,
             profile_home TEXT NOT NULL,
             process_id TEXT NOT NULL,
             pid INTEGER NOT NULL,
             process_started_at INTEGER,
             fire_claim_by TEXT NOT NULL,
             unresolved_pin INTEGER NOT NULL DEFAULT 1,
             bind_state TEXT NOT NULL CHECK(bind_state IN
               ('awaiting_canonical','preparing','bound','unknown')),
             nonce_digest TEXT,
             generation INTEGER,
             epoch_id TEXT,
             issuance_digest TEXT,
             output_sha256 TEXT,
             admission_id TEXT,
             created_at TEXT NOT NULL,
             FOREIGN KEY(execution_id) REFERENCES executions(id)
           )"""
    )
    add_column_if_missing(conn, "cron_prerun_bindings", "unresolved_pin",
                          "unresolved_pin INTEGER NOT NULL DEFAULT 1")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS cron_local_finalizations (
             execution_id TEXT PRIMARY KEY,
             state TEXT NOT NULL CHECK(state IN ('pending','succeeded','unknown','not_applicable')),
             outcome_digest TEXT,
             created_at TEXT NOT NULL,
             FOREIGN KEY(execution_id) REFERENCES executions(id)
           )"""
    )
    add_column_if_missing(conn, "executions", "delivery_outcome", "delivery_outcome TEXT")
    add_column_if_missing(conn, "executions", "scheduled_instant", "scheduled_instant TEXT")
    add_column_if_missing(conn, "executions", "progress_at", "progress_at TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_occurrence "
        "ON executions(job_id, scheduled_instant) WHERE status='completed'"
    )


@contextmanager
def _transaction() -> Iterator[sqlite3.Connection]:
    from hermes_cli.sqlite_util import transaction

    with _lock, transaction(_connect()) as conn:
        yield conn


def _fetch(conn: sqlite3.Connection, execution_id: str) -> Optional[dict[str, Any]]:
    row = conn.execute("SELECT * FROM executions WHERE id=?", (execution_id,)).fetchone()
    return dict(row) if row is not None else None


def _emit_execution_state(
    record: Optional[dict[str, Any]], *, delivery_outcome: Optional[str] = None
) -> None:
    """Project durable state to monitoring without affecting ledger behavior."""
    try:
        from agent.monitoring.cron_health import emit_execution_state

        emit_execution_state(record, delivery_outcome=delivery_outcome)
    except Exception:
        pass


def _process_start_time(pid: int) -> Optional[int]:
    try:
        from gateway.status import get_process_start_time
        return get_process_start_time(pid)
    except Exception:
        return None


def _owner_is_live(pid: int, started_at: Optional[int]) -> bool:
    try:
        from gateway.status import _pid_exists
        if not _pid_exists(pid):
            return False
    except Exception:
        return True  # fail safe: inability to prove death must not rewrite state
    if started_at is None:
        return pid == os.getpid()
    current = _process_start_time(pid)
    if current is None:
        return True  # cannot compare -> cannot prove death; a misread must not rewrite state
    # Drifted same-host readings (#117505) are not proof of death; a live misread is still
    # bounded by the stale-claim sweep below.
    from gateway.status import start_time_fingerprints_match
    return start_time_fingerprints_match(started_at, current)


def _live_owner_stale_after_seconds() -> Optional[float]:
    """Age past which a claimed/running row with a LIVE owner is treated as wedged.

    Derived from the existing knobs, never a bare wall-clock constant:
    ``max(3 × HERMES_CRON_TIMEOUT, cron script timeout, 7200)``. Returns ``None`` (never reclaim
    live owners — today's behaviour) when the inactivity timeout is 0/unlimited or not a finite
    positive number: with no bound to derive from, fail closed.
    """
    from cron.scheduler import _cron_inactivity_seconds
    from cron.scheduler_script import _get_script_timeout

    inactivity = float(_cron_inactivity_seconds())
    if not math.isfinite(inactivity) or inactivity <= 0:
        return None
    return max(
        inactivity * CLAIM_TTL_INACTIVITY_HEADROOM,
        float(_get_script_timeout()),
        LIVE_OWNER_STALE_CLAIM_FLOOR_SECONDS,
    )


def _claim_age_seconds(claimed_at: str) -> float:
    """Seconds since ``claimed_at`` (NOT NULL, always the aware ISO string from hermes_time.now)."""
    return (_hermes_now() - datetime.fromisoformat(claimed_at)).total_seconds()


def _stale_age_seconds(claimed_at: str, progress_at: Optional[str]) -> float:
    """Seconds since the owner last proved it was making progress.

    A wedged worker (#115692) stops stamping ``progress_at``; a long but healthy run keeps
    stamping it, so the derived stale bound measures silence, not run length. Rows written
    before the column existed have no stamp and fall back to claim age.
    """
    return _claim_age_seconds(progress_at or claimed_at)


def touch_execution_progress(execution_id: str) -> bool:
    """Stamp ``progress_at`` on a running attempt this process owns. Returns False when the row
    is no longer ours or no longer running (the caller treats that as informational only)."""
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions SET progress_at=?
               WHERE id=? AND status IN ('claimed','running') AND process_id=? AND pid=?""",
            (now, execution_id, _PROCESS_ID, os.getpid()),
        )
        return cur.rowcount == 1


def _prune_unlocked(conn: sqlite3.Connection) -> None:
    conn.execute(
        """DELETE FROM executions WHERE id IN (
             SELECT id FROM executions
             WHERE status IN ('completed','failed','unknown')
               AND NOT EXISTS (
                 SELECT 1 FROM cron_prerun_bindings b
                 WHERE b.execution_id=executions.id AND b.unresolved_pin=1)
             ORDER BY julianday(finished_at) DESC, finished_at DESC,
                      julianday(claimed_at) DESC, claimed_at DESC, id DESC LIMIT -1 OFFSET ?
           )""",
        (max(0, int(MAX_TERMINAL_EXECUTIONS)),),
    )


def create_execution(
    job_id: str, *, source: str, scheduled_instant: Optional[str] = None,
) -> dict[str, Any]:
    """Persist a claimed attempt before executor/provider dispatch."""
    from cron.occurrences import scheduled_instant as canonical_instant

    now = _hermes_now().isoformat()
    execution_id = uuid.uuid4().hex
    pid = os.getpid()
    with _transaction() as conn:
        conn.execute(
            """INSERT INTO executions
               (id, job_id, source, process_id, pid, process_started_at,
                status, claimed_at, scheduled_instant)
               VALUES (?, ?, ?, ?, ?, ?, 'claimed', ?, ?)""",
            (execution_id, str(job_id), str(source), _PROCESS_ID, pid,
             _process_start_time(pid), now, canonical_instant(scheduled_instant)),
        )
        record = _fetch(conn, execution_id)
    _emit_execution_state(record)
    return record  # type: ignore[return-value]


def set_execution_occurrence(execution_id: str, instant: Optional[str]) -> None:
    """Bind the store-claimed snapshot before a provider hands it to a worker."""
    from cron.occurrences import scheduled_instant

    with _transaction() as conn:
        cur = conn.execute(
            "UPDATE executions SET scheduled_instant=? WHERE id=? AND status='claimed' "
            "AND handoff_pending=0 AND process_id=? AND pid=?",
            (scheduled_instant(instant), execution_id, _PROCESS_ID, os.getpid()),
        )
        if cur.rowcount != 1:
            raise RuntimeError("Cron occurrence could not be bound before dispatch")


def mark_execution_handoff_pending(execution_id: str) -> Optional[dict[str, Any]]:
    """Fence restart recovery while an external worker is adopting a claim."""
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions
               SET handoff_pending=1, handoff_started_at=?
               WHERE id=? AND status='claimed'
                 AND process_id=? AND pid=?""",
            (time.time(), execution_id, _PROCESS_ID, os.getpid()),
        )
        if cur.rowcount != 1:
            return None
        record = _fetch(conn, execution_id)
    _emit_execution_state(record)
    return record


def adopt_claimed_execution(execution_id: str) -> Optional[dict[str, Any]]:
    """Atomically transfer and start an attempt in its worker process.

    The dispatching gateway creates the row before spawning a restart-safe
    worker.  Adoption is the single ``claimed`` → ``running`` gate: only the
    winner may acknowledge ownership or run side effects.
    """
    pid = os.getpid()
    process_started_at = _process_start_time(pid)
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions
               SET process_id=?, pid=?, process_started_at=?,
                   status='running', started_at=?, handoff_pending=0,
                   handoff_started_at=NULL
               WHERE id=? AND status='claimed' AND handoff_pending=1""",
            (_PROCESS_ID, pid, process_started_at, now, execution_id),
        )
        if cur.rowcount != 1:
            return None
        record = _fetch(conn, execution_id)
    _emit_execution_state(record)
    return record


def mark_execution_running(execution_id: str) -> Optional[dict[str, Any]]:
    """Transition one claimed attempt to running exactly once."""
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions
               SET status='running', started_at=?, handoff_pending=0,
                   handoff_started_at=NULL
               WHERE id=? AND status='claimed' AND handoff_pending=0
                 AND process_id=? AND pid=?""",
            (now, execution_id, _PROCESS_ID, os.getpid()),
        )
        if cur.rowcount != 1:
            return None
        record = _fetch(conn, execution_id)
    _emit_execution_state(record)
    return record


def reserve_prerun_binding(execution_id: str, job_id: str, fire_claim_by: str) -> dict[str, Any]:
    """Original firer only: pin the running owner before canonical submission.

    The caller holds the real job's fire fence. This never delegates execution ownership;
    a separate canonical admission must validate the pin before script effects.
    """
    from cron import jobs

    job = jobs.get_job(job_id)
    if (not job or not isinstance(job.get("fire_claim"), dict)
            or job["fire_claim"].get("by") != fire_claim_by):
        raise RuntimeError("Target cron fire claim is absent or changed")
    with _transaction() as conn:
        row = _fetch(conn, execution_id)
        if (not row or row["job_id"] != job_id or row["status"] != "running"
                or row["process_id"] != _PROCESS_ID or row["pid"] != os.getpid()
                or not _owner_is_live(row["pid"], row["process_started_at"])):
            raise RuntimeError("Target cron original running owner is absent or changed")
        profile_home = str(get_hermes_home().resolve())
        previous = conn.execute(
            "SELECT * FROM cron_prerun_bindings WHERE execution_id=?", (execution_id,)
        ).fetchone()
        if previous is not None:
            if (previous["job_id"] != job_id or previous["profile_home"] != profile_home
                    or previous["process_id"] != row["process_id"]
                    or previous["pid"] != row["pid"]
                    or previous["process_started_at"] != row["process_started_at"]
                    or previous["fire_claim_by"] != fire_claim_by
                    or previous["bind_state"] != "awaiting_canonical"):
                raise RuntimeError("Target cron reservation is already decided or mismatched")
            return dict(previous)
        unresolved = conn.execute(
            "SELECT count(*) FROM cron_prerun_bindings WHERE unresolved_pin=1"
        ).fetchone()[0]
        if unresolved >= MAX_TARGET_UNRESOLVED_BINDINGS:
            raise RuntimeError("Target cron unresolved binding quota reached; reconcile before a new fire")
        conn.execute(
            """INSERT INTO cron_prerun_bindings
               (execution_id, job_id, profile_home, process_id, pid,
                process_started_at, fire_claim_by, bind_state, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'awaiting_canonical', ?)""",
            (execution_id, job_id, profile_home, row["process_id"], row["pid"],
             row["process_started_at"], fire_claim_by, _hermes_now().isoformat()),
        )
        saved = conn.execute(
            "SELECT * FROM cron_prerun_bindings WHERE execution_id=?", (execution_id,)
        ).fetchone()
    if saved is None:
        raise RuntimeError("Target cron reservation readback failed")
    return dict(saved)


def prepare_prerun_binding(execution_id: str, job_id: str, admission_id: str,
                           nonce_digest: str, fire_claim_by: str) -> dict[str, Any]:
    """Canonical owner may prepare only the live original firer's reserved fire."""
    from cron import jobs

    job = jobs.get_job(job_id)
    if (not job or not isinstance(job.get("fire_claim"), dict)
            or job["fire_claim"].get("by") != fire_claim_by):
        raise RuntimeError("Target cron claim changed before canonical preparation")
    with _transaction() as conn:
        row = conn.execute(
            """SELECT e.status AS execution_status, e.process_id AS execution_process_id,
                      e.pid AS execution_pid, e.process_started_at AS execution_birth, b.*
               FROM cron_prerun_bindings b JOIN executions e ON e.id=b.execution_id
               WHERE b.execution_id=? AND b.job_id=? AND e.job_id=?""",
            (execution_id, job_id, job_id),
        ).fetchone()
        if (row is None or row["execution_status"] != "running"
                or row["bind_state"] != "awaiting_canonical"
                or row["process_id"] != row["execution_process_id"]
                or row["pid"] != row["execution_pid"]
                or row["process_started_at"] != row["execution_birth"]
                or row["profile_home"] != str(get_hermes_home().resolve())
                or row["fire_claim_by"] != fire_claim_by
                or not _owner_is_live(row["pid"], row["process_started_at"])):
            raise RuntimeError("Target cron original firer is not live and reserved")
        cur = conn.execute(
            """UPDATE cron_prerun_bindings SET bind_state='preparing',
               nonce_digest=?, admission_id=?
               WHERE execution_id=? AND bind_state='awaiting_canonical'""",
            (nonce_digest, admission_id, execution_id),
        )
        if cur.rowcount != 1:
            raise RuntimeError("Target cron canonical preparation lost the reservation")
        saved = conn.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?",
                             (execution_id,)).fetchone()
    if saved is None:
        raise RuntimeError("Target cron canonical preparation readback failed")
    return dict(saved)


def bind_prerun_output(execution_id: str, job_id: str, admission_id: str,
                       fire_claim_by: str, *, nonce_digest: str, generation: int, epoch_id: str,
                       issuance_digest: str, output_sha256: str) -> dict[str, Any]:
    """Canonical binding of already-validated post-redaction output, never terminal ownership."""
    from cron import jobs

    current_job = jobs.get_job(job_id)
    if (not current_job or not isinstance(current_job.get("fire_claim"), dict)
            or current_job["fire_claim"].get("by") != fire_claim_by):
        raise RuntimeError("Target cron claim changed before output binding")
    with _transaction() as conn:
        row = conn.execute(
            """SELECT e.status AS execution_status, e.process_id AS execution_process_id,
                      e.pid AS execution_pid, e.process_started_at AS execution_birth, b.*
               FROM cron_prerun_bindings b JOIN executions e ON e.id=b.execution_id
               WHERE b.execution_id=? AND b.job_id=? AND e.job_id=?""",
            (execution_id, job_id, job_id),
        ).fetchone()
        if row is not None and row["nonce_digest"] != nonce_digest:
            raise RuntimeError("Target cron nonce digest differs from canonical preparation")
        if (row is None or row["execution_status"] != "running"
                or row["bind_state"] != "preparing"
                or row["process_id"] != row["execution_process_id"]
                or row["pid"] != row["execution_pid"]
                or row["process_started_at"] != row["execution_birth"]
                or row["admission_id"] != admission_id
                or row["nonce_digest"] != nonce_digest
                or row["fire_claim_by"] != fire_claim_by
                or row["profile_home"] != str(get_hermes_home().resolve())
                or not _owner_is_live(row["pid"], row["process_started_at"])):
            raise RuntimeError("Target cron live original owner or admission changed before binding")
        cursor = conn.execute(
            """UPDATE cron_prerun_bindings SET bind_state='bound', generation=?, epoch_id=?,
               issuance_digest=?, output_sha256=?
               WHERE execution_id=? AND bind_state='preparing' AND admission_id=?""",
            (generation, epoch_id, issuance_digest, output_sha256, execution_id, admission_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Target cron binding lost its compare-and-swap")
        saved = conn.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?",
                             (execution_id,)).fetchone()
    if saved is None or saved["bind_state"] != "bound":
        raise RuntimeError("Target cron binding readback failed")
    return dict(saved)


def target_preterminal(execution_id: str, job_id: str, fire_claim_by: str) -> Optional[dict[str, Any]]:
    """Read and strictly verify the original live owner and bound identity under fire fence."""
    with _transaction() as conn:
        execution = _fetch(conn, execution_id)
        binding = conn.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?",
                               (execution_id,)).fetchone()
    if (not execution or binding is None or execution["job_id"] != job_id
            or execution["status"] != "running" or execution["process_id"] != _PROCESS_ID
            or execution["pid"] != os.getpid()
            or binding["bind_state"] != "bound" or binding["job_id"] != job_id
            or binding["fire_claim_by"] != fire_claim_by
            or binding["profile_home"] != str(get_hermes_home().resolve())
            or binding["process_id"] != execution["process_id"]
            or binding["pid"] != execution["pid"]
            or binding["process_started_at"] != execution["process_started_at"]
            or not _owner_is_live(execution["pid"], execution["process_started_at"])):
        return None
    return dict(binding)


def begin_local_finalization(execution_id: str) -> None:
    with _transaction() as conn:
        conn.execute(
            """INSERT INTO cron_local_finalizations (execution_id,state,created_at)
               VALUES (?, 'pending', ?)""", (execution_id, _hermes_now().isoformat()))


def mirror_local_finalization(execution_id: str, state: str, digest: str) -> bool:
    if state not in {"succeeded", "not_applicable", "unknown"} or len(digest) != 64:
        return False
    with _transaction() as conn:
        result = conn.execute(
            """UPDATE cron_local_finalizations SET state=?, outcome_digest=?
               WHERE execution_id=? AND state='pending'
                 AND EXISTS (SELECT 1 FROM executions
                             WHERE id=? AND status='completed')""",
            (state, digest, execution_id, execution_id),
        )
        return result.rowcount == 1

def reconcile_target_pin(execution_id: str, job_id: str) -> bool:
    """Release a completed target's quota pin only after exact retained v2 proof.

    Explicit reconciliation, never a scheduler or mirror callback. Unknown/failed and
    post-COMMIT/pre-mirror windows retain their pins for operator disposition.
    """
    from cron.scheduler_local_observation import _read_complete_parent_outcome
    from local_observation_v2 import Store

    directory = get_hermes_home().resolve() / "cron" / f"recovery-{job_id}"
    with _transaction() as conn:
        row = conn.execute(
            """SELECT e.*, b.job_id AS bound_job, b.profile_home, b.bind_state,
                      b.unresolved_pin, b.nonce_digest, b.generation, b.epoch_id,
                      b.issuance_digest, b.output_sha256, b.fire_claim_by,
                      f.state AS mirror_state, f.outcome_digest AS mirror_digest
               FROM executions e JOIN cron_prerun_bindings b ON b.execution_id=e.id
               JOIN cron_local_finalizations f ON f.execution_id=e.id
               WHERE e.id=? AND e.job_id=?""", (execution_id, job_id),
        ).fetchone()
        if (row is None or row["status"] != "completed" or row["bound_job"] != job_id
                or row["profile_home"] != str(get_hermes_home().resolve())
                or row["bind_state"] != "bound" or row["unresolved_pin"] != 1
                or row["mirror_state"] not in {"succeeded", "not_applicable"}
                or not row["fire_claim_by"]
                or any(not isinstance(row[k], str) or len(row[k]) != 64
                       for k in ("nonce_digest", "epoch_id", "issuance_digest",
                                 "output_sha256", "mirror_digest"))
                or type(row["generation"]) is not int or row["generation"] < 1):
            return False
        store = Store(directory)
        header = store.header()
        if (header["profile"] != row["profile_home"] or header["job"] != job_id
                or header["execution_id"] != execution_id
                or header["state"] != "finalized"
                or any(header[k] != row[k] for k in
                       ("generation", "epoch_id", "issuance_digest"))):
            return False
        try:
            state, digest = _read_complete_parent_outcome(
                store, {**dict(row), "execution_id": execution_id},
                {"state": row["mirror_state"], "digest": row["mirror_digest"]},
                _outcome_roots(store, execution_id),
            )
        except (KeyError, TypeError, ValueError, RuntimeError):
            return False
        if state != row["mirror_state"] or digest != row["mirror_digest"]:
            return False
        updated = conn.execute(
            """UPDATE cron_prerun_bindings SET unresolved_pin=0
               WHERE execution_id=? AND job_id=? AND unresolved_pin=1
                 AND bind_state='bound'""", (execution_id, job_id),
        )
        return updated.rowcount == 1


def _outcome_roots(store, execution_id: str) -> list[str]:
    """Independent storage root inventory, never a caller or mirrored root count."""
    import json
    with store._connection() as (db, _):
        row = db.execute("SELECT payload FROM outcomes WHERE execution_id=?", (execution_id,)).fetchone()
        if row is None:
            return []
        records = json.loads(row[0])['records']
        return [item['root'] for item in records]


def finish_execution(
    execution_id: str, *, success: bool, error: Optional[str] = None,
    delivery_outcome: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Write a terminal result once; terminal attempts cannot be rewritten.

    Only the owning process may finish its attempt; receipt recovery for a departed owner
    settles through ``recover_receipted_execution``.
    """
    now = _hermes_now().isoformat()
    status = "completed" if success else "failed"
    detail = None if success else (str(error) if error else "unknown failure")
    with _transaction() as conn:
        owner = (_PROCESS_ID, os.getpid())
        cur = conn.execute(
            """UPDATE executions
               SET status=?, finished_at=?, error=?, handoff_pending=0,
                   handoff_started_at=NULL, delivery_outcome=?
               WHERE id=? AND status IN ('claimed','running')
                 AND process_id=? AND pid=?""",
            (status, now, detail, delivery_outcome, execution_id, *owner),
        )
        if cur.rowcount != 1:
            return None
        _prune_unlocked(conn)
        record = _fetch(conn, execution_id)
    _emit_execution_state(record, delivery_outcome=delivery_outcome)
    record_cron_finish(record, delivery_outcome)
    return record


def recover_receipted_execution(execution_id, job_id, *, success, error=None, delivery_outcome=None):
    """Settle a verified canonical receipt after owner loss, including its unknown audit row.

    Terminal verdicts stay immutable. A live firer still owns its bookkeeping
    (``execution_owner_live``).
    """
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        row = _recoverable_row(conn, execution_id, job_id)
        if row is None or _row_owner_live(row):
            return None
        conn.execute(
            """UPDATE executions SET status=?, finished_at=?, error=?, handoff_pending=0,
               handoff_started_at=NULL, delivery_outcome=? WHERE id=?""",
            ('completed' if success else 'failed', now, None if success else (str(error) if error else 'unknown failure'),
             delivery_outcome, execution_id))
        _prune_unlocked(conn)
        record = _fetch(conn, execution_id)
    _emit_execution_state(record, delivery_outcome=delivery_outcome)
    record_cron_finish(record, delivery_outcome)
    return record


def _recoverable_row(conn, execution_id, job_id):
    return conn.execute(
        """SELECT status, process_id, pid, process_started_at FROM executions
           WHERE id=? AND job_id=? AND status IN ('claimed','running','unknown')""",
        (str(execution_id), str(job_id))).fetchone()


def _row_owner_live(row) -> bool:
    if row['process_id'] == _PROCESS_ID:
        # The in-gateway ticker fires on pool threads: a claimed/running row of THIS process is
        # still in flight here. Only an unknown row was given up by its own firer.
        return row['status'] != 'unknown'
    return _owner_is_live(int(row['pid']), row['process_started_at'])


def execution_owner_live(execution_id: str, job_id: str) -> bool:
    """True while the attempt's own firer (this process included) can still finish it, so receipt
    recovery must not act for it: no output re-save, no job mark, no ledger write."""
    with _transaction() as conn:
        row = _recoverable_row(conn, execution_id, job_id)
    return row is not None and _row_owner_live(row)


def settle_delivery_outcome(execution_id: str, outcome: str) -> bool:
    """Replace a terminal attempt's ``queued`` delivery outcome with the drain's real one.

    The only post-terminal rewrite the ledger allows: the attempt's status is untouched and
    only a ``queued`` handoff can settle, so a replay or a late caller is a no-op.
    """
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions SET delivery_outcome=?
               WHERE id=? AND delivery_outcome='queued'
                 AND status IN ('completed','failed','unknown')""",
            (outcome, execution_id),
        )
    return cur.rowcount == 1


_OWNER_GONE_REASON = (
    "Scheduler restarted after this execution's owner exited before a durable "
    "terminal state; whether side effects ran is unknown."
)
_OWNER_WEDGED_REASON = (
    "Owner process is still alive but the claim outlived the derived stale bound; "
    "treated as wedged (#115692). The process was not terminated; whether side effects "
    "ran is unknown."
)


def settle_unstarted_execution(execution_id: str, job_id: str, error: str) -> None:
    """Close the receipt of a run that never started: a ``claimed`` row never resolves. Best-effort
    so a ledger write cannot mask the failure the caller logs or re-raises."""
    try:
        finish_execution(execution_id, success=False, error=error)
    except (sqlite3.Error, OSError) as record_err:
        logger.error("Job '%s': failed to close execution receipt %s (%s): %s",
                     job_id, execution_id, error, record_err)


def recover_interrupted_executions() -> int:
    """Mark abandoned attempts unknown without scheduling retries: rows whose owner is provably
    dead, plus rows whose live owner holds a claim older than the derived stale bound (the
    process is not killed)."""
    now = _hermes_now().isoformat()
    changed = 0
    recovered: list[dict[str, Any]] = []
    # Derived on the first live-owned row only: the bound reads config, and the idle gateway
    # tick must stay config-free (tests/cron/test_idle_tick_config_skip.py).
    stale_after: Optional[float] = None
    stale_after_resolved = False
    with _transaction() as conn:
        rows = conn.execute(
            """SELECT id, status, process_id, pid, process_started_at,
                      handoff_pending, handoff_started_at, claimed_at, progress_at
               FROM executions
               WHERE status IN ('claimed','running')"""
        ).fetchall()
        for row in rows:
            if row["process_id"] == _PROCESS_ID:
                continue
            reason = _OWNER_GONE_REASON
            if _owner_is_live(int(row["pid"]), row["process_started_at"]):
                # A live owner is normally a legitimately running job. A worker permanently
                # deadlocked (e.g. futex_wait behind a route/proxy flip, #115692) also passes
                # this check, so a claim SILENT for longer than the derived bound is treated
                # as wedged and released — the external-worker wait loop polls this ledger
                # for a terminal status, so the job can fire again. Silence is measured from
                # the owner's last ``progress_at`` stamp (the run monitor refreshes it while
                # the agent is active), so a healthy multi-hour run is never reclaimed while
                # it is still working. The wedged worker PROCESS is NOT terminated here
                # (leaked until host restart); rows owned by this process (process_id ==
                # _PROCESS_ID, in-process runs) are skipped above and remain out of scope.
                if not stale_after_resolved:
                    stale_after = _live_owner_stale_after_seconds()
                    stale_after_resolved = True
                if (
                    stale_after is None
                    or _stale_age_seconds(row["claimed_at"], row["progress_at"]) <= stale_after
                ):
                    continue
                reason = _OWNER_WEDGED_REASON
            handoff_started_at = row["handoff_started_at"]
            if (
                row["handoff_pending"]
                and handoff_started_at is not None
                and time.time() - float(handoff_started_at)
                < HANDOFF_ADOPTION_GRACE_SECONDS
            ):
                continue
            # The SELECT ran outside a write transaction and the liveness probe takes time: the
            # owner may have stamped progress since, so act only on the snapshot that was judged.
            cur = conn.execute(
                """UPDATE executions
                   SET status='unknown', finished_at=?, error=?,
                       handoff_pending=0, handoff_started_at=NULL
                   WHERE id=? AND status=? AND process_id=? AND pid=?
                     AND handoff_pending=?
                     AND handoff_started_at IS ?
                     AND claimed_at=? AND progress_at IS ?""",
                (now, reason, row["id"], row["status"], row["process_id"], row["pid"],
                 row["handoff_pending"], row["handoff_started_at"],
                 row["claimed_at"], row["progress_at"]),
            )
            changed += cur.rowcount
            if cur.rowcount:
                record = _fetch(conn, row["id"])
                if record is not None:
                    recovered.append(record)
        if changed:
            _prune_unlocked(conn)
    for record in recovered:
        _emit_execution_state(record)
    return changed


def terminalize_dead_owner(execution_id: str, *, reason: str) -> bool:
    """Record one attempt as ``unknown`` with a cause this process actually observed.

    ``recover_interrupted_executions`` sweeps every attempt whose owner is provably
    dead, and knows nothing but that absence — so all it can write is
    ``_OWNER_GONE_REASON``, which asserts a scheduler restart. A waiter that held the
    worker's ``Popen`` knows more: the owner was that external worker, and it exited
    with a known status. Without this, a manual run whose worker dies is filed as
    "Scheduler restarted ..." (a restart that never happened) and, because the sweep
    leaves the row terminal, the waiter reports success and never records the run —
    the job's ``fire_claim`` then blocks the next manual fire for the whole lease
    (#128509).

    The attempt stays ``unknown``, not ``failed``: whether side effects ran is still
    unknown. Only the CAUSE becomes truthful. Returns False — leaving the caller to
    fall back to the generic sweep — when the row is absent, already terminal, owned by
    this process, inside the handoff adoption grace, or owned by a live process: a
    worker that is still running must never be terminalized out from under itself.
    """
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        row = conn.execute(
            """SELECT id, status, process_id, pid, process_started_at,
                      handoff_pending, handoff_started_at
               FROM executions WHERE id=?""",
            (execution_id,),
        ).fetchone()
        if row is None or row["status"] not in ("claimed", "running"):
            return False
        if row["process_id"] == _PROCESS_ID:
            return False
        if _owner_is_live(int(row["pid"]), row["process_started_at"]):
            return False
        handoff_started_at = row["handoff_started_at"]
        if (
            row["handoff_pending"]
            and handoff_started_at is not None
            and time.time() - float(handoff_started_at) < HANDOFF_ADOPTION_GRACE_SECONDS
        ):
            return False
        cur = conn.execute(
            """UPDATE executions
               SET status='unknown', finished_at=?, error=?,
                   handoff_pending=0, handoff_started_at=NULL
               WHERE id=? AND status=? AND process_id=? AND pid=?""",
            (now, reason, row["id"], row["status"], row["process_id"], row["pid"]),
        )
        if cur.rowcount != 1:
            return False
        record = _fetch(conn, execution_id)
        _prune_unlocked(conn)
    _emit_execution_state(record)
    return True


def give_up_execution(execution_id: str, *, reason: str) -> bool:
    """The firer itself gave up on its in-flight attempt (``CronExecutionUnknown``): record it
    ``unknown`` now, owner-fenced to this process, instead of leaving it ``running`` until a
    restart's dead-owner sweep. Receipt recovery never acts for a same-process running row, so
    the owner's later terminal receipt can settle it only once it is marked given up."""
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions SET status='unknown', finished_at=?, error=?,
                   handoff_pending=0, handoff_started_at=NULL
               WHERE id=? AND status IN ('claimed','running') AND process_id=? AND pid=?""",
            (now, reason, str(execution_id), _PROCESS_ID, os.getpid()))
        if cur.rowcount != 1:
            return False
        record = _fetch(conn, str(execution_id))
        _prune_unlocked(conn)
    _emit_execution_state(record)
    return True


def list_executions(
    *, job_id: Optional[str] = None, limit: int = 50, before_claimed_at: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Return indexed, newest-first execution history with cursor pagination."""
    clauses: list[str] = []
    params: list[Any] = []
    if job_id is not None:
        clauses.append("job_id=?")
        params.append(str(job_id))
    if before_claimed_at is not None:
        # Same (instant, text) key as the ORDER BY, so a page never skips or repeats a row.
        clauses.append("(julianday(claimed_at), claimed_at) < (julianday(?), ?)")
        params.extend([str(before_claimed_at)] * 2)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(max(1, min(int(limit), 500)))
    # Stamps carry the local offset, which changes at DST and on a timezone change, so text order
    # is not time order. julianday() compares instants (ms); the text breaks same-ms ties.
    with _transaction() as conn:
        rows = conn.execute(
            "SELECT * FROM executions" + where
            + " ORDER BY julianday(claimed_at) DESC, claimed_at DESC, id DESC LIMIT ?",
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_execution(execution_id: str) -> Optional[dict[str, Any]]:
    """Return one exact execution attempt, or ``None`` when it is absent."""
    with _transaction() as conn:
        row = conn.execute(
            "SELECT * FROM executions WHERE id=?",
            (str(execution_id),),
        ).fetchone()
    return dict(row) if row is not None else None


def latest_execution(job_id: str) -> Optional[dict[str, Any]]:
    rows = list_executions(job_id=job_id, limit=1)
    return rows[0] if rows else None


def live_inflight_execution(job_id: str) -> Optional[dict[str, Any]]:
    """The job's latest attempt while it is still claimed/running under a LIVE owner, else ``None``.

    This is scheduler OWNERSHIP, not recent activity: a run inside a long tool call writes no
    heartbeat yet stays owned, while a run whose process died (watchdog kill, crash) does not.
    Read-only — unlike ``recover_interrupted_executions`` it never rewrites a row.
    """
    record = latest_execution(job_id)
    if not record or record.get("status") not in ("claimed", "running"):
        return None
    if not _owner_is_live(int(record["pid"]), record.get("process_started_at")):
        return None
    return record


def latest_executions(job_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Load latest execution for many jobs in one query."""
    clean = [str(job_id) for job_id in dict.fromkeys(job_ids) if job_id]
    if not clean:
        return {}
    placeholders = ",".join("?" for _ in clean)
    # One windowed sort: a per-row correlated ORDER BY julianday() cannot use the index and
    # grows quadratically with history (~90 ms at 1000 rows).
    with _transaction() as conn:
        rows = conn.execute(
            f"""SELECT e.* FROM executions e WHERE e.id IN (
                  SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                             PARTITION BY job_id
                             ORDER BY julianday(claimed_at) DESC, claimed_at DESC, id DESC
                           ) AS rn
                    FROM executions WHERE job_id IN ({placeholders}))
                  WHERE rn=1)""",
            clean,
        ).fetchall()
    return {row["job_id"]: dict(row) for row in rows}
