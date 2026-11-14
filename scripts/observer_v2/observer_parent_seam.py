"""Isolated WRI-2276 parent-only observer-v2 finalization candidate.

The scheduler must capture ParentAuthority before clearing its original fire claim and
call finalize_parent from that same process inside its outer per-job fire fence.
This file is not an installed scheduler entry or a child command.
"""
from __future__ import annotations

import hashlib
import os
import pathlib
import sqlite3
import stat
from contextlib import closing, contextmanager, nullcontext
from urllib.parse import quote

import engineering_recovery_observer as legacy
import local_observation_v2 as v2

_DOMAIN = b"hermes.local-observation.stage-evidence.v1\x00"
_TEST_BEFORE_COMMIT = None  # isolated process-loss test hook; not a receipt or production API

_FIELDS = {"version", "execution_id", "epoch_id", "root", "pending_seq", "observation_id",
           "local_fingerprint", "consumed_event_ids", "attachment_bearing", "ledger_sha", "cutover_sha"}


def canonical(raw):
    if not isinstance(raw, bytes):
        raise v2.Stale("JSON payload is not BLOB")
    value = v2.strict_json(raw)
    if v2.encode(value) != raw:
        raise v2.Stale("noncanonical JSON payload")
    return value


def evidence_digest(obj):
    if set(obj) != _FIELDS or type(obj["version"]) is not int or obj["version"] != 1:
        raise v2.Stale("unknown stage evidence shape")
    if (type(obj["pending_seq"]) is not int or obj["pending_seq"] < 1
            or type(obj["attachment_bearing"]) is not bool or obj["attachment_bearing"]
            or not isinstance(obj["execution_id"], str) or not obj["execution_id"]
            or not isinstance(obj["root"], str) or not v2.ROOT.fullmatch(obj["root"])
            or any(not isinstance(obj[k], str) or not v2.HEX.fullmatch(obj[k])
                   for k in ("epoch_id", "observation_id", "local_fingerprint", "ledger_sha", "cutover_sha"))
            or not isinstance(obj["consumed_event_ids"], list)
            or any(type(n) is not int or n < 0 for n in obj["consumed_event_ids"])
            or len(set(obj["consumed_event_ids"])) != len(obj["consumed_event_ids"])):
        raise v2.Stale("invalid typed stage evidence")
    return hashlib.sha256(_DOMAIN + v2.encode(obj)).hexdigest()


class ParentAuthority:
    """Process-local capture of the original running firer; not serialized to child."""
    __slots__ = ("execution_id", "job_id", "claim", "binding", "pid", "process_id")

    def __init__(self, execution_id, job_id, claim, binding, process_id):
        self.execution_id, self.job_id, self.claim = execution_id, job_id, claim
        self.binding, self.pid, self.process_id = dict(binding), os.getpid(), process_id

    def __reduce__(self):
        raise TypeError("parent authority cannot be transferred to proposal child")

    @classmethod
    def capture(cls, execution_id, job_id, claim):
        from cron import executions, jobs
        live = jobs.get_job(job_id)
        active_claim = live.get("fire_claim") if isinstance(live, dict) else None
        if (not isinstance(active_claim, dict) or active_claim.get("by") != claim
                or not isinstance(claim, str) or not claim):
            raise v2.Stale("original fire claim missing")
        binding = executions.target_preterminal(execution_id, job_id, claim)
        if not binding or binding.get("process_id") != executions._PROCESS_ID or binding.get("pid") != os.getpid():
            raise v2.Stale("original running owner/binding missing")
        return cls(execution_id, job_id, claim, binding, executions._PROCESS_ID)


def _producer_terminal(authority):
    if type(authority) is not ParentAuthority or authority.pid != os.getpid():
        raise v2.Stale("parent process authority absent")
    from cron import executions, jobs
    if authority.process_id != executions._PROCESS_ID:
        raise v2.Stale("original process identity changed")
    bound = authority.binding
    if (bound.get("execution_id") != authority.execution_id or bound.get("job_id") != authority.job_id
            or bound.get("fire_claim_by") != authority.claim or bound.get("bind_state") != "bound"
            or bound.get("pid") != authority.pid or bound.get("process_id") != authority.process_id
            or bound.get("unresolved_pin") != 1
            or any(not isinstance(bound.get(k), str) or not v2.HEX.fullmatch(bound[k])
                   for k in ("epoch_id", "issuance_digest", "nonce_digest", "output_sha256"))):
        raise v2.Stale("captured binding invalid")
    # Read producer state afresh, not a caller's terminal object.  A child has a different
    # _PROCESS_ID; ownerless recovery and a fabricated terminal cannot pass this comparison.
    producer_path = legacy._reject_symlink_ancestors(legacy.PROFILE_HOME / "cron" / "executions.db")
    if bound["profile_home"] != str(legacy.PROFILE_HOME) or not producer_path.is_file():
        raise v2.Stale("producer path/identity differs")
    with closing(sqlite3.connect("file:" + quote(str(producer_path)) + "?mode=ro", uri=True)) as db:
        db.execute("BEGIN")
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM executions WHERE id=? AND job_id=?",
                         (authority.execution_id, authority.job_id)).fetchone()
        now = db.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?",
                         (authority.execution_id,)).fetchone()
        disposition = db.execute("SELECT state FROM cron_local_finalizations WHERE execution_id=?",
                                 (authority.execution_id,)).fetchone()
        if (row is None or now is None or disposition is None or disposition[0] != "pending"
                or row["status"] != "completed" or row["source"] == "recovered"
                or row["process_id"] != authority.process_id or row["pid"] != authority.pid
                or row["process_started_at"] != bound.get("process_started_at")
                or row["finished_at"] is None or row["error"] is not None
                or any(now[k] != bound.get(k) for k in now.keys())):
            raise v2.Stale("original parent terminal or binding changed")
    saved = jobs.get_job(authority.job_id)
    if (not saved or saved.get("fire_claim") is not None
            or authority.execution_id not in saved.get("canonical_completions", [])
            or saved.get("last_status") not in {"ok", "delivery_queued"}):
        raise v2.Stale("original job terminal not persisted")
    return bound


@contextmanager
def board_reservation(board):
    """Reserve the canonical native board writer slot until observer commit/rollback."""
    from hermes_cli import kanban_db as kb
    path = pathlib.Path(board)
    canonical_path = legacy._reject_symlink_ancestors(kb.kanban_db_path(board=legacy.BOARD))
    path = legacy._reject_symlink_ancestors(path)
    if path != canonical_path or not path.is_file() or not stat.S_ISREG(path.stat().st_mode):
        raise v2.Stale("board is not the canonical native database")
    before = path.stat()
    db = sqlite3.connect("file:" + quote(str(path)) + "?mode=rw", uri=True, timeout=2)
    try:
        db.execute("PRAGMA busy_timeout=2000")
        db.execute("BEGIN IMMEDIATE")
        db_path = dict((name, filename) for _, name, filename in db.execute("PRAGMA database_list"))["main"]
        if pathlib.Path(db_path).resolve() != path or (before.st_dev, before.st_ino) != (path.stat().st_dev, path.stat().st_ino):
            raise v2.Stale("canonical board identity changed")
        # An actual task/events schema, not a test-local roots table.
        present = {name for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"tasks", "task_events"}.issubset(present):
            raise v2.Stale("native board schema missing")
        yield db
        if (before.st_dev, before.st_ino) != (path.stat().st_dev, path.stat().st_ino):
            raise v2.Stale("board file replaced during reservation")
    finally:
        db.rollback()
        db.close()


def _epoch_and_issuance(db, meta, execution_id, binding):
    raw_meta = db.execute("SELECT payload FROM metadata WHERE id=1").fetchone()
    if (raw_meta is None or canonical(raw_meta[0]) != meta
            or set(meta) != {"schema", "profile", "job", "board", "source_v1_sha", "ledger_sha",
                             "generation", "state", "execution_id", "epoch_id", "issuance_digest",
                             "revision", "v1_unknown"}
            or type(meta["generation"]) is not int or meta["generation"] < 1
            or type(meta["revision"]) is not int or meta["revision"] < 1
            or any(not isinstance(meta[k], str) or not v2.HEX.fullmatch(meta[k])
                   for k in ("source_v1_sha", "ledger_sha", "epoch_id", "issuance_digest"))):
        raise v2.Stale("noncanonical or invalid observer header")
    row = db.execute("SELECT payload FROM epochs WHERE execution_id=?", (execution_id,)).fetchone()
    if row is None:
        raise v2.Stale("epoch missing")
    epoch = canonical(row[0])
    if (set(epoch) != {"execution_id", "generation", "epoch_id", "issuance_digest", "ledger_sha", "issued_count"}
            or epoch["execution_id"] != execution_id or type(epoch["generation"]) is not int
            or epoch["generation"] < 1 or type(epoch["issued_count"]) is not int
            or not 0 <= epoch["issued_count"] <= v2.MAX_CURRENT
            or any(epoch[k] != meta[k] for k in ("generation", "epoch_id", "issuance_digest", "ledger_sha"))
            or any(epoch[k] != binding[k] for k in ("generation", "epoch_id", "issuance_digest"))):
        raise v2.Stale("epoch/producer mismatch")
    mapping = []
    for root, raw in db.execute("SELECT root,payload FROM epoch_issued WHERE execution_id=? ORDER BY root COLLATE BINARY", (execution_id,)):
        item = canonical(raw)
        if (set(item) != {"root", "observation_id", "pending_seq", "attachment_bearing"}
                or root != item["root"] or not v2.ROOT.fullmatch(root)
                or not isinstance(item["observation_id"], str) or not v2.HEX.fullmatch(item["observation_id"])
                or type(item["pending_seq"]) is not int or item["pending_seq"] < 1
                or type(item["attachment_bearing"]) is not bool or len(mapping) >= v2.MAX_CURRENT):
            raise v2.Stale("invalid issuance mapping")
        current = db.execute("SELECT observation_id,pending_seq,execution_id FROM issued WHERE root=?", (root,)).fetchone()
        latest = db.execute("SELECT pending_seq FROM latest WHERE root=?", (root,)).fetchone()
        pending = db.execute("SELECT kind,root,payload FROM records WHERE seq=?", (item["pending_seq"],)).fetchone()
        if (current != (item["observation_id"], item["pending_seq"], execution_id)
                or latest is None or latest[0] != item["pending_seq"] or pending is None
                or pending[:2] != ("pending", root)):
            raise v2.Stale("issued pending pointer changed")
        value = canonical(pending[2])
        if (value.get("observation_id") != item["observation_id"]
                or value.get("attachment_bearing", False) != item["attachment_bearing"]):
            raise v2.Stale("pending evidence mismatch")
        mapping.append(item)
    count = db.execute("SELECT COUNT(*) FROM issued WHERE execution_id=?", (execution_id,)).fetchone()[0]
    if (len(mapping) != epoch["issued_count"] or count != len(mapping)
            or v2.digest({"execution_id": execution_id, "generation": epoch["generation"],
                          "ledger_sha": epoch["ledger_sha"], "mapping": mapping}) != epoch["issuance_digest"]
            or v2.digest([execution_id, epoch["generation"], epoch["issuance_digest"]]) != epoch["epoch_id"]):
        raise v2.Stale("issuance digest/count changed")
    return epoch, {item["root"]: item for item in mapping}


def _native_evidence(directory, board, roots, meta):
    ledger, actual = legacy.read_ledger(directory)
    if ledger is None or ledger.get("version") != 2 or actual != meta["ledger_sha"]:
        raise v2.Stale("ledger identity changed")
    marker_path = legacy._cutover_path(directory, ledger)
    raw = v2._regular_bytes(marker_path, 1048576)
    legacy._verify_cutover(directory, ledger, marker_path, v2.strict_json(raw))
    fingerprints, events, attachments = legacy.board_observation(board, roots)
    return ledger, actual, hashlib.sha256(raw).hexdigest(), fingerprints, events, attachments


def _stage_binding(execution_id):
    from cron import executions, jobs
    path = legacy._reject_symlink_ancestors(legacy.PROFILE_HOME / "cron" / "executions.db")
    if not path.is_file():
        raise v2.Stale("producer ledger absent")
    with closing(sqlite3.connect("file:" + quote(str(path)) + "?mode=ro", uri=True)) as db:
        db.execute("BEGIN")
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM executions WHERE id=?", (execution_id,)).fetchone()
        binding = db.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?", (execution_id,)).fetchone()
        if row is None or binding is None:
            raise v2.Stale("producer reservation absent")
        row, binding = dict(row), dict(binding)
    job = jobs.get_job(legacy.JOB_ID)
    claim = job.get("fire_claim") if isinstance(job, dict) else None
    if (row["status"] != "running" or binding["bind_state"] != "bound"
            or row["job_id"] != legacy.JOB_ID or binding["job_id"] != legacy.JOB_ID
            or binding["profile_home"] != str(legacy.PROFILE_HOME)
            or not claim or claim.get("by") != binding["fire_claim_by"]
            or any(row[k] != binding[k] for k in ("pid", "process_id", "process_started_at"))
            or not executions._owner_is_live(row["pid"], row["process_started_at"])):
        raise v2.Stale("producer original running/bound identity changed")
    return binding


def stage_observation(store, execution_id, root, observation_id, revision, board):
    """Stage-only admitted route; no checked write. Native evidence survives index deletion."""
    if (not isinstance(execution_id, str) or not execution_id or not isinstance(root, str)
            or not v2.ROOT.fullmatch(root) or not isinstance(observation_id, str)
            or not v2.HEX.fullmatch(observation_id) or type(revision) is not int):
        raise v2.Stale("invalid stage selectors")
    expected_directory = legacy.PROFILE_HOME / "cron" / f"recovery-{legacy.JOB_ID}"
    if store is None or store.directory != expected_directory:
        raise v2.Stale("stage store is not target profile directory")
    bound = _stage_binding(execution_id)
    with legacy._observation_lock(store.directory):
        with board_reservation(board):
            with store._connection(write=True) as (db, meta):
                bound = _stage_binding(execution_id)
                if (meta["state"] != "provisional" or meta["execution_id"] != execution_id
                        or meta["revision"] != revision or meta["profile"] != bound["profile_home"]
                        or meta["job"] != bound["job_id"]):
                    raise v2.Stale("current stage metadata changed")
                epoch, issued = _epoch_and_issuance(db, meta, execution_id, bound)
                item = issued.get(root)
                if item is None or item["observation_id"] != observation_id or item["attachment_bearing"]:
                    raise v2.Stale("root not current non-attachment issuance")
                if db.execute("SELECT 1 FROM stages WHERE execution_id=? AND root=?", (execution_id, root)).fetchone():
                    raise v2.Stale("duplicate stage root")
                pending = db.execute("SELECT payload FROM records WHERE seq=?", (item["pending_seq"],)).fetchone()
                if pending is None:
                    raise v2.Stale("pending record absent")
                value = canonical(pending[0])
                _, ledger_sha, cutover_sha, fingerprints, ids, attached = _native_evidence(store.directory, board, [root], meta)
                if (root in attached or fingerprints.get(root) != value.get("fingerprint")
                        or ids.get(root) != value.get("event_ids") or value.get("attachment_bearing", False)):
                    raise v2.Stale("native stage evidence changed")
                obj = {"version": 1, "execution_id": execution_id, "epoch_id": epoch["epoch_id"],
                       "root": root, "pending_seq": item["pending_seq"], "observation_id": observation_id,
                       "local_fingerprint": fingerprints[root], "consumed_event_ids": ids[root],
                       "attachment_bearing": False, "ledger_sha": ledger_sha, "cutover_sha": cutover_sha}
                preimage = db.execute("SELECT checked_seq FROM latest WHERE root=?", (root,)).fetchone()[0]
                stage = {"execution_id": execution_id, "root": root, "pending_seq": item["pending_seq"],
                         "checked_preimage_seq": preimage, "observation_id": observation_id,
                         "epoch_id": epoch["epoch_id"], "stage_revision": revision,
                         "evidence_version": 1, "board_evidence_digest": evidence_digest(obj)}
                db.execute("INSERT INTO stages(execution_id,root,payload) VALUES(?,?,?)", (execution_id, root, v2.encode(stage)))
                seq = db.execute("INSERT INTO events(payload) VALUES(?)", (v2.encode({"event": "stage", **stage,
                                   "board_evidence": obj}),)).lastrowid
                meta["revision"] += 1
                v2._write_metadata(db, meta)
                return {"revision": meta["revision"], "stage_event_seq": seq, "stage": stage}


def record_parent_unknown(store, authority, board, reason):
    """Separate durable nonpositive transaction after terminal, never overwrite success."""
    if (type(authority) is not ParentAuthority or authority.pid != os.getpid()
            or reason not in {"failed_terminal", "uncertain_terminal", "evidence_mismatch", "success_rollback"}):
        raise v2.Stale("parent negative authority/reason absent")
    from cron import executions
    if executions._PROCESS_ID != authority.process_id:
        raise v2.Stale("negative writer not original process")
    bound = authority.binding
    path = legacy._reject_symlink_ancestors(legacy.PROFILE_HOME / "cron" / "executions.db")
    if bound.get("profile_home") != str(legacy.PROFILE_HOME):
        raise v2.Stale("producer profile changed")
    with closing(sqlite3.connect("file:" + quote(str(path)) + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM executions WHERE id=? AND job_id=?",
                         (authority.execution_id, authority.job_id)).fetchone()
        current = db.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?",
                             (authority.execution_id,)).fetchone()
        disposition = db.execute("SELECT state FROM cron_local_finalizations WHERE execution_id=?",
                                 (authority.execution_id,)).fetchone()
        if (row is None or current is None or disposition is None or disposition[0] != "pending"
                or any(current[k] != bound.get(k) for k in current.keys()) or row["process_id"] != authority.process_id
                or row["pid"] != authority.pid or row["process_started_at"] != bound.get("process_started_at")
                or row["status"] not in {"completed", "failed", "unknown"} or row["finished_at"] is None):
            raise v2.Stale("negative terminal/source mismatch")
        if reason == "failed_terminal" and row["status"] != "failed":
            raise v2.Stale("failed terminal reason does not match")
    with legacy._observation_lock(store.directory):
        with board_reservation(board):
            with store._connection(write=True) as (db, meta):
                if (meta["state"] != "provisional" or meta["execution_id"] != authority.execution_id
                        or meta["epoch_id"] != bound.get("epoch_id") or meta["job"] != authority.job_id
                        or meta["profile"] != bound.get("profile_home")):
                    raise v2.Stale("negative observer identity changed")
                if db.execute("SELECT 1 FROM outcomes WHERE execution_id=?", (authority.execution_id,)).fetchone():
                    raise v2.Stale("immutable terminal outcome already committed")
                epoch, _ = _epoch_and_issuance(db, meta, authority.execution_id, bound)
                outcome = {"execution_id": authority.execution_id, "state": "unknown", "records": [],
                           "epoch_id": epoch["epoch_id"]}
                outcome["digest"] = v2.digest(outcome)
                db.execute("INSERT INTO outcomes(execution_id,payload) VALUES(?,?)",
                           (authority.execution_id, v2.encode(outcome)))
                db.execute("INSERT INTO events(payload) VALUES(?)", (v2.encode({"event": "finalize_unknown",
                           "evidence_version": 1, "execution_id": authority.execution_id,
                           "epoch_id": epoch["epoch_id"], "outcome_digest": outcome["digest"],
                           "reason_class": reason}),))
                db.execute("DELETE FROM stages WHERE execution_id=?", (authority.execution_id,))
                db.execute("DELETE FROM issued WHERE execution_id=?", (authority.execution_id,))
                meta["state"] = "unknown"
                meta["revision"] += 1
                v2._write_metadata(db, meta)
                return outcome


def _current_finalized_proof(db, meta, epoch, outcome, binding, directory):
    """Validate current mirror eligibility separately from historical outcome access."""
    execution_id = binding["execution_id"]
    raw = db.execute("SELECT payload FROM metadata WHERE id=1").fetchone()
    if (raw is None or canonical(raw[0]) != meta
            or set(meta) != {"schema", "profile", "job", "board", "source_v1_sha", "ledger_sha",
                             "generation", "state", "execution_id", "epoch_id", "issuance_digest",
                             "revision", "v1_unknown"}
            or meta["schema"] != 2 or meta["state"] != "finalized"
            or meta["execution_id"] != execution_id or meta["profile"] != binding["profile_home"]
            or meta["job"] != binding["job_id"] or meta["board"] != legacy.BOARD
            or type(meta["generation"]) is not int or type(meta["revision"]) is not int
            or meta["generation"] != binding["generation"] or meta["revision"] < 1
            or any(meta[k] != binding[k] for k in ("epoch_id", "issuance_digest"))
            or set(epoch) != {"execution_id", "generation", "epoch_id", "issuance_digest", "ledger_sha", "issued_count"}
            or epoch["execution_id"] != execution_id
            or any(epoch[k] != meta[k] for k in ("generation", "epoch_id", "issuance_digest", "ledger_sha"))
            or type(epoch["issued_count"]) is not int or not 0 <= epoch["issued_count"] <= v2.MAX_CURRENT
            or outcome["state"] not in {"succeeded", "not_applicable"}):
        raise v2.Stale("current finalized producer/observer identity changed")
    _, actual_ledger_sha = legacy.read_ledger(directory)
    if actual_ledger_sha != epoch["ledger_sha"]:
        raise v2.Stale("current observer ledger changed")
    mapping = []
    for root, payload in db.execute("SELECT root,payload FROM epoch_issued WHERE execution_id=? ORDER BY root COLLATE BINARY", (execution_id,)):
        item = canonical(payload)
        if (set(item) != {"root", "observation_id", "pending_seq", "attachment_bearing"}
                or item["root"] != root or not v2.ROOT.fullmatch(root)
                or type(item["pending_seq"]) is not int or item["pending_seq"] < 1
                or not isinstance(item["observation_id"], str) or not v2.HEX.fullmatch(item["observation_id"])
                or type(item["attachment_bearing"]) is not bool or len(mapping) >= v2.MAX_CURRENT):
            raise v2.Stale("immutable issuance mapping changed")
        pending = db.execute("SELECT kind,root,payload FROM records WHERE seq=?", (item["pending_seq"],)).fetchone()
        if pending is None or pending[:2] != ("pending", root):
            raise v2.Stale("immutable pending record missing")
        value = canonical(pending[2])
        if (value.get("observation_id") != item["observation_id"]
                or value.get("attachment_bearing", False) != item["attachment_bearing"]):
            raise v2.Stale("immutable pending issuance changed")
        mapping.append(item)
    if (len(mapping) != epoch["issued_count"]
            or v2.digest({"execution_id": execution_id, "generation": epoch["generation"],
                          "ledger_sha": epoch["ledger_sha"], "mapping": mapping}) != epoch["issuance_digest"]
            or v2.digest([execution_id, epoch["generation"], epoch["issuance_digest"]]) != epoch["epoch_id"]):
        raise v2.Stale("immutable issuance digest changed")
    staged = {item["root"] for item in outcome["records"]}
    if len(staged) != len(outcome["records"]) or not staged.issubset({item["root"] for item in mapping}):
        raise v2.Stale("checked roots not issued in current epoch")
    for item in mapping:
        root = item["root"]
        current = db.execute("SELECT observation_id,pending_seq,execution_id FROM issued WHERE root=?", (root,)).fetchone()
        if root in staged:
            if current is not None:
                raise v2.Stale("finalized root remains issued")
        elif current != (item["observation_id"], item["pending_seq"], execution_id):
            raise v2.Stale("unstaged current issuance changed")
        else:
            latest = db.execute("SELECT pending_seq,checked_seq FROM latest WHERE root=?", (root,)).fetchone()
            if latest is None or latest[0] != item["pending_seq"]:
                raise v2.Stale("unstaged current pending pointer changed")
            if latest[1] is not None:
                checked = db.execute("SELECT kind,root FROM records WHERE seq=?", (latest[1],)).fetchone()
                if (checked != ("checked", root) or latest[1] >= item["pending_seq"]):
                    raise v2.Stale("unstaged current checked pointer changed")
    if db.execute("SELECT COUNT(*) FROM issued").fetchone()[0] != len(mapping) - len(staged):
        raise v2.Stale("current issuance cardinality changed")
    if db.execute("SELECT 1 FROM stages WHERE execution_id=? LIMIT 1", (execution_id,)).fetchone():
        raise v2.Stale("finalized execution retains a current stage index")

def _proof_shell(epoch, outcome, final, count, last_root, has_more, stage_count):
    return {"epoch": epoch,
            "outcome": {"execution_id": outcome["execution_id"], "state": outcome["state"],
                        "epoch_id": outcome["epoch_id"], "digest": outcome["digest"],
                        "record_count": count},
            "final_event": {"event": final["event"], "evidence_version": final["evidence_version"],
                            "execution_id": outcome["execution_id"], "epoch_id": outcome["epoch_id"],
                            "outcome_digest": outcome["digest"],
                            "stage_event_seq_count": stage_count},
            "items": [], "last_root": last_root, "has_more": has_more}


def read_parent_outcome(store, execution_id, after_root=None, max_items=64, max_bytes=32768,
                        *, _snapshot=None, _cache=None, _current_binding=None):
    """Read one immutable outcome plus a bounded stage/checked proof page.

    This is a storage proof, not producer mirror authority; caller separately checks
    original terminal, nonce, board and ledger before mirroring.
    """
    if (not isinstance(execution_id, str) or not execution_id
            or (after_root is not None and (not isinstance(after_root, str) or not v2.ROOT.fullmatch(after_root)))
            or type(max_items) is not int or not 1 <= max_items <= 128
            or type(max_bytes) is not int or not 512 <= max_bytes <= 32768):
        raise ValueError("invalid outcome page")
    if _cache is not None and _snapshot is None:
        raise ValueError("proof cache requires held database snapshot")
    with (store._connection() if _snapshot is None else nullcontext(_snapshot)) as (db, meta):
        if _cache is not None and "proof" in _cache:
            if _cache.get("source") != (id(db), execution_id) or _cache.get("binding") != _current_binding:
                raise v2.Stale("proof cache does not belong to this snapshot/binding")
            outcome, epoch, final, stage_events, by_root, records = _cache["proof"]
        else:
            row = db.execute("SELECT payload FROM outcomes WHERE execution_id=?", (execution_id,)).fetchone()
            epoch_row = db.execute("SELECT payload FROM epochs WHERE execution_id=?", (execution_id,)).fetchone()
            if row is None or epoch_row is None:
                raise v2.Stale("terminal storage evidence absent")
            outcome, epoch = canonical(row[0]), canonical(epoch_row[0])
            if (set(outcome) != {"execution_id", "state", "records", "epoch_id", "digest"}
                    or outcome["execution_id"] != execution_id or outcome["epoch_id"] != epoch.get("epoch_id")
                    or outcome["state"] not in {"succeeded", "not_applicable", "unknown"}
                    or not isinstance(outcome["records"], list)
                    or v2.digest({k: outcome[k] for k in ("execution_id", "state", "records", "epoch_id")}) != outcome["digest"]):
                raise v2.Stale("terminal storage digest/shape mismatch")
            stage_events, final_events = [], []
            for seq, raw in db.execute("SELECT seq,payload FROM events ORDER BY seq"):
                event = canonical(raw)
                if event.get("execution_id") != execution_id or event.get("epoch_id") != outcome["epoch_id"]:
                    continue
                if event.get("event") == "stage":
                    stage_events.append((seq, event))
                elif event.get("event") in ("finalize", "finalize_unknown"):
                    final_events.append((seq, event))
            if (len(final_events) != 1 or final_events[0][1].get("outcome_digest") != outcome["digest"]
                    or final_events[0][1].get("evidence_version") != 1
                    or (outcome["state"] == "unknown") != (final_events[0][1]["event"] == "finalize_unknown")):
                raise v2.Stale("terminal event missing or ambiguous")
            final = final_events[0][1]
            if outcome["state"] != "unknown" and final.get("stage_event_seqs") != [seq for seq, _ in stage_events]:
                raise v2.Stale("terminal stage event order changed")
            if outcome["state"] == "unknown" and outcome["records"]:
                raise v2.Stale("unknown contains checked records")
            if outcome["state"] == "not_applicable" and (stage_events or outcome["records"]):
                raise v2.Stale("not applicable has stage receipts")
            by_root = {}
            for seq, event in stage_events:
                root = event.get("root")
                if root in by_root or not isinstance(root, str) or not v2.ROOT.fullmatch(root):
                    raise v2.Stale("duplicate stage event root")
                obj = event.get("board_evidence")
                if not isinstance(obj, dict) or event.get("board_evidence_digest") != evidence_digest(obj):
                    raise v2.Stale("retained board evidence changed")
                by_root[root] = {"seq": seq, "event": event}
            records = outcome["records"]
            if outcome["state"] == "succeeded" and (not records or len(records) != len(by_root)):
                raise v2.Stale("incomplete successful stage join")
            if len(records) > v2.MAX_CURRENT:
                raise v2.Stale("terminal root quota exceeded")
            if _current_binding is not None:
                _current_finalized_proof(db, meta, epoch, outcome, _current_binding, store.directory)
            if _cache is not None:
                _cache["source"] = id(db), execution_id
                _cache["binding"] = _current_binding
                _cache["proof"] = outcome, epoch, final, stage_events, by_root, records
        page = []
        page_full = False
        previous = ""
        for item in records:
            if (not isinstance(item, dict) or set(item) != {"root", "pending_seq", "checked_seq", "receipt_digest"}
                    or not isinstance(item["root"], str) or not v2.ROOT.fullmatch(item["root"])
                    or item["root"] <= previous or item["root"] not in by_root
                    or any(type(item[k]) is not int or item[k] < 1 for k in ("pending_seq", "checked_seq"))):
                raise v2.Stale("outcome item shape/order changed")
            root = item["root"]
            previous = root
            # The digest-bound complete record and stage-event lists are checked
            # above. Each page verifies its own durable receipt/preimage joins;
            # the producer consumes every page before mirroring success.
            if page_full or (after_root is not None and root <= after_root):
                continue
            event = by_root[root]["event"]
            prior = event.get("checked_preimage_seq")
            checked = db.execute("SELECT kind,root,payload,predecessor_seq FROM records WHERE seq=?",
                                 (item["checked_seq"],)).fetchone()
            pending = db.execute("SELECT kind,root,payload FROM records WHERE seq=?", (item["pending_seq"],)).fetchone()
            if (checked is None or checked[:2] != ("checked", root) or checked[3] != prior
                    or pending is None or pending[:2] != ("pending", root)
                    or event.get("pending_seq") != item["pending_seq"]
                    or db.execute("SELECT pending_seq,checked_seq FROM latest WHERE root=?", (root,)).fetchone()
                       != (None, item["checked_seq"])
                    or db.execute("SELECT 1 FROM issued WHERE root=? AND execution_id=?", (root, execution_id)).fetchone()
                    or db.execute("SELECT 1 FROM stages WHERE root=? AND execution_id=?", (root, execution_id)).fetchone()):
                raise v2.Stale("checked/pending current join changed")
            receipt, pending_value = canonical(checked[2]), canonical(pending[2])
            if (item["receipt_digest"] != v2.digest(receipt)
                    or receipt.get("local_observation_id") != pending_value.get("observation_id")
                    or receipt.get("local_cursor_sha") != legacy._digest({k: v for k, v in receipt.items()
                                                                          if k != "local_cursor_sha"})):
                raise v2.Stale("checked receipt bytes changed")
            candidate = {"outcome_item": item, "stage_event": by_root[root],
                         "checked": receipt, "pending": pending_value}
            shell = _proof_shell(epoch, outcome, final, len(records), root, False, len(stage_events))
            shell["items"] = [*page, candidate]
            if len(v2.encode(shell)) <= max_bytes:
                page.append(candidate)
                page_full = len(page) >= max_items
            elif not page:
                raise v2.Overflow("single outcome proof item exceeds page")
            else:
                page_full = True
        result = _proof_shell(epoch, outcome, final, len(records),
                              page[-1]["outcome_item"]["root"] if page else after_root,
                              bool(records and (after_root is None or records[-1]["root"] >
                                   (page[-1]["outcome_item"]["root"] if page else after_root))), len(stage_events))
        result["items"] = page
        if len(v2.encode(result)) > max_bytes:
            raise v2.Overflow("outcome proof page exceeds bound")
        return result


def finalize_parent(store, authority, proposal, board):
    """One original-parent in-process success transaction; proposal is root selectors only.

    An isolated test hook can pause the parent immediately before SQLite COMMIT.
    """
    if type(authority) is not ParentAuthority or not isinstance(proposal, dict):
        raise v2.Stale("parent authority or proposal shape absent")
    compact = set(proposal) == {"execution_id", "epoch_id", "count", "digest"}
    if compact:
        if (proposal["execution_id"] != authority.execution_id
                or not isinstance(proposal["epoch_id"], str) or not v2.HEX.fullmatch(proposal["epoch_id"])
                or type(proposal["count"]) is not int or not 0 <= proposal["count"] <= v2.MAX_CURRENT
                or not isinstance(proposal["digest"], str) or not v2.HEX.fullmatch(proposal["digest"])):
            raise v2.Stale("invalid epoch-bound stage commitment")
    elif set(proposal) == {"roots"}:
        roots = proposal["roots"]
        if (not isinstance(roots, list) or len(roots) > v2.MAX_CURRENT or len(set(roots)) != len(roots)
                or any(not isinstance(root, str) or not v2.ROOT.fullmatch(root) for root in roots)):
            raise v2.Stale("invalid proposed root selectors")
    else:
        raise v2.Stale("invalid proposal shape")
    binding = _producer_terminal(authority)
    with legacy._observation_lock(store.directory):
        with board_reservation(board):
            with store._connection(write=True) as (db, meta):
                binding = _producer_terminal(authority)
                if (meta["state"] != "provisional" or meta["execution_id"] != authority.execution_id
                        or meta["epoch_id"] != binding["epoch_id"] or meta["issuance_digest"] != binding["issuance_digest"]
                        or meta["profile"] != binding["profile_home"] or meta["job"] != authority.job_id):
                    raise v2.Stale("current parent/observer identity mismatch")
                epoch, issued = _epoch_and_issuance(db, meta, authority.execution_id, binding)
                events = []
                for seq, raw in db.execute("SELECT seq,payload FROM events ORDER BY seq"):
                    obj = canonical(raw)
                    if obj.get("event") == "stage" and obj.get("execution_id") == authority.execution_id and obj.get("epoch_id") == epoch["epoch_id"]:
                        events.append((seq, obj))
                staged = {root: canonical(raw) for root, raw in db.execute("SELECT root,payload FROM stages WHERE execution_id=?", (authority.execution_id,))}
                staged_roots = sorted(staged)
                if compact:
                    if proposal != v2.stage_proposal(authority.execution_id, epoch["epoch_id"], staged_roots):
                        raise v2.Stale("epoch stage commitment differs from stored set")
                    roots = staged_roots
                if set(staged) != set(roots) or len(events) != len(staged):
                    raise v2.Stale("proposal/stage event set differs")
                by_root = {}
                for seq, event in events:
                    root = event.get("root")
                    if root in by_root or root not in staged or event.get("event") != "stage":
                        raise v2.Stale("duplicate or extra stage event")
                    stage = staged[root]
                    if any(event.get(k) != value for k, value in stage.items()):
                        raise v2.Stale("stage event/index mismatch")
                    obj = event.get("board_evidence")
                    if (not isinstance(obj, dict) or evidence_digest(obj) != event.get("board_evidence_digest")
                            or event.get("evidence_version") != 1
                            or set(stage) != {"execution_id", "root", "pending_seq", "checked_preimage_seq",
                                              "observation_id", "epoch_id", "stage_revision", "evidence_version",
                                              "board_evidence_digest"}
                            or event != {"event": "stage", **stage, "board_evidence": obj}):
                        raise v2.Stale("stage evidence/event shape mismatch")
                    by_root[root] = (seq, stage, obj)
                ledger, ledger_sha, cutover_sha, fingerprints, ids, attachments = _native_evidence(store.directory, board, roots, meta)
                results = []
                largest_proof_item = (0, None)
                checked_count = db.execute("SELECT COUNT(*) FROM latest WHERE checked_seq IS NOT NULL").fetchone()[0]
                if checked_count + sum(staged[r]["checked_preimage_seq"] is None for r in staged) > v2.MAX_CURRENT:
                    raise v2.Overflow("checked current cardinality exceeded")
                for root in sorted(staged):
                    seq, stage, obj = by_root[root]
                    mapping = issued.get(root)
                    pending = db.execute("SELECT kind,root,payload FROM records WHERE seq=?", (stage["pending_seq"],)).fetchone()
                    latest = db.execute("SELECT pending_seq,checked_seq FROM latest WHERE root=?", (root,)).fetchone()
                    if (mapping is None or pending is None or pending[:2] != ("pending", root)
                            or latest != (stage["pending_seq"], stage["checked_preimage_seq"])
                            or mapping["pending_seq"] != stage["pending_seq"]
                            or mapping["observation_id"] != stage["observation_id"] or mapping["attachment_bearing"]
                            or root in attachments or fingerprints.get(root) != obj["local_fingerprint"]
                            or ids.get(root) != obj["consumed_event_ids"]):
                        raise v2.Stale("staged native/pending evidence changed")
                    value = canonical(pending[2])
                    expected = {"version": 1, "execution_id": authority.execution_id, "epoch_id": epoch["epoch_id"],
                                "root": root, "pending_seq": stage["pending_seq"], "observation_id": value["observation_id"],
                                "local_fingerprint": value["fingerprint"], "consumed_event_ids": value["event_ids"],
                                "attachment_bearing": False, "ledger_sha": ledger_sha, "cutover_sha": cutover_sha}
                    if expected != obj or evidence_digest(expected) != stage["board_evidence_digest"]:
                        raise v2.Stale("stage preimage not independently reproducible")
                    prior = stage["checked_preimage_seq"]
                    if prior is not None:
                        old = db.execute("SELECT kind,root,payload FROM records WHERE seq=?", (prior,)).fetchone()
                        if old is None or old[:2] != ("checked", root):
                            raise v2.Stale("prior checked preimage absent")
                    effect = (ledger.get("roots", {}).get(root) or {}).get("receipt")
                    receipt = {"local_fingerprint": value["fingerprint"], "consumed_event_ids": value["event_ids"],
                               "local_observed_at": value["observed_at"], "local_observation_id": value["observation_id"],
                               "ledger_effect_receipt_id_or_null": effect.get("receipt_id") if effect else None}
                    receipt["local_cursor_sha"] = legacy._digest(receipt)
                    checked = v2._insert_record(db, "checked", root, receipt, prior)
                    db.execute("UPDATE latest SET pending_seq=NULL,checked_seq=? WHERE root=?", (checked, root))
                    db.execute("DELETE FROM issued WHERE root=? AND execution_id=?", (root, authority.execution_id))
                    results.append({"root": root, "pending_seq": stage["pending_seq"], "checked_seq": checked,
                                    "receipt_digest": v2.digest(receipt)})
                    # Measure the prospective proof item before COMMIT. Oversized
                    # evidence rolls back this transaction, retaining prior history
                    # and pending/stages for a fail-closed disposition.
                    proof_item = {"outcome_item": results[-1],
                                  "stage_event": {"seq": seq, "event": {"event": "stage", **stage,
                                                                          "board_evidence": obj}},
                                  "checked": receipt, "pending": value}
                    size = len(v2.encode(proof_item))
                    if size > largest_proof_item[0]:
                        largest_proof_item = size, root
                db.execute("DELETE FROM stages WHERE execution_id=?", (authority.execution_id,))
                outcome = {"execution_id": authority.execution_id, "state": "succeeded" if results else "not_applicable",
                           "records": results, "epoch_id": epoch["epoch_id"]}
                outcome["digest"] = v2.digest(outcome)
                db.execute("INSERT INTO outcomes(execution_id,payload) VALUES(?,?)", (authority.execution_id, v2.encode(outcome)))
                db.execute("INSERT INTO events(payload) VALUES(?)", (v2.encode({"event": "finalize", "evidence_version": 1,
                    "execution_id": authority.execution_id, "epoch_id": epoch["epoch_id"], "outcome_digest": outcome["digest"],
                    "stage_event_seqs": [seq for seq, _ in events]}),))
                if results:
                    final = {"event": "finalize", "evidence_version": 1, "stage_event_seqs": events,
                             "outcome_digest": outcome["digest"]}
                    shell = _proof_shell(epoch, outcome, final, len(results), largest_proof_item[1],
                                         False, len(events))
                    if len(v2.encode(shell)) + largest_proof_item[0] > 32768:
                        raise v2.Overflow("single outcome proof item exceeds page")
                meta["state"] = "finalized"
                meta["revision"] += 1
                v2._write_metadata(db, meta)
                # _connection commits on exit, while native board writer reservation is still held.
                _producer_terminal(authority)
                if _TEST_BEFORE_COMMIT is not None:
                    _TEST_BEFORE_COMMIT()
                return outcome
