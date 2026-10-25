"""Opt-in producer binding for one profile-local recovery cron job.

No target is enabled by default; this module never promotes a checked receipt.
"""

from __future__ import annotations

from hermes_constants import get_hermes_home


def is_target_job(job: dict) -> bool:
    from hermes_cli.config_effective import load_user_config_effective

    configured = load_user_config_effective(get_hermes_home() / "config.yaml")
    cron = configured.get("cron", {}) if isinstance(configured, dict) else {}
    target = cron.get("local_observation_target_job_id") if isinstance(cron, dict) else None
    return (isinstance(target, str) and bool(target)
            and target == job.get("id") and not job.get("no_agent")
            and isinstance(job.get("script"), str) and bool(job["script"]))


def verify_target_script(job: dict) -> tuple[int, int, str]:
    """Pin one opt-in source file's owner, path and bytes before scoped nonce exposure."""
    import hashlib
    import os
    import stat
    from pathlib import Path
    from cron.scheduler_script import _resolve_script_path
    from hermes_cli.config_effective import load_user_config_effective

    config = load_user_config_effective(get_hermes_home() / "config.yaml")
    cron = config.get("cron", {}) if isinstance(config, dict) else {}
    expected = cron.get("local_observation_prerun_sha256") if isinstance(cron, dict) else None
    if (not isinstance(expected, str) or len(expected) != 64
            or any(c not in "0123456789abcdef" for c in expected)):
        raise RuntimeError("Target cron script has no exact expected digest")
    raw = Path(job["script"])
    path, error = _resolve_script_path(job["script"])
    if error or path is None or raw.is_symlink():
        raise RuntimeError("Target cron script path refused")
    home = get_hermes_home().resolve()
    if home not in path.parents:
        raise RuntimeError("Target cron script is outside its profile")
    current = path
    while True:
        info = current.lstat()
        if (stat.S_ISLNK(info.st_mode) or info.st_uid != getattr(os, "getuid", lambda: -1)()
                or info.st_mode & 0o022):
            raise RuntimeError("Target cron script or ancestor is mutable or symlinked")
        if current == home:
            break
        current = current.parent
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise RuntimeError("Target cron script source changed")
    return info.st_dev, info.st_ino, expected


def prepare_canonical(job: dict) -> tuple[dict[str, str], tuple[int, int, str]]:
    """Return scoped script overlay and pin only for admitted owner and live firer."""
    import hashlib
    import secrets
    from gateway.session_cron import current_admission, current_execution
    from cron import executions, jobs

    admitted = current_admission()
    context = current_execution()
    execution_id = job.get("execution_id")
    if (admitted is None or context is None or not execution_id
            or admitted[3:5] != (job["id"], execution_id)
            or context[2:4] != (job["id"], execution_id)
            or admitted[0] is not context[0] or admitted[1] != context[1]):
        raise RuntimeError("Target cron canonical admission identity is absent or mismatched")
    live_job = jobs.get_job(job["id"])
    claim = live_job.get("fire_claim") if isinstance(live_job, dict) else None
    owner = claim.get("by") if isinstance(claim, dict) else None
    if not owner or job.get("fire_claim", {}).get("by") != owner:
        raise RuntimeError("Target cron canonical claim absent or changed")
    pin = verify_target_script(job)
    nonce = secrets.token_bytes(32)
    with jobs.fire_claim_fence(job["id"], expected_owner=owner) as held:
        if not held or jobs.get_job(job["id"]) is None:
            raise RuntimeError("Target cron canonical fence unavailable")
        executions.prepare_prerun_binding(execution_id, job["id"], admitted[2],
                                          hashlib.sha256(nonce).hexdigest(), owner)
    return ({"HERMES_LOCAL_OBSERVATION_NONCE": nonce.hex(),
             "HERMES_LOCAL_OBSERVATION_EXECUTION_ID": execution_id,
             "HERMES_LOCAL_OBSERVATION_JOB_ID": job["id"]}, pin)


def _verify_provisional_store(db_path, job_id: str, execution_id: str, output: dict) -> None:
    """Reconstruct archived-v2 issuance in one read snapshot; never trust output as authority."""
    import hashlib
    import json
    import sqlite3

    def encoded(value):
        return json.dumps(value, sort_keys=True, ensure_ascii=True,
                          separators=(",", ":"), allow_nan=False).encode("utf-8")

    def digest(value):
        return hashlib.sha256(encoded(value)).hexdigest()

    def decoded(raw):
        if not isinstance(raw, bytes):
            raise RuntimeError("Target cron observer payload is not a BLOB")
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise RuntimeError("Target cron observer duplicate JSON member")
                result[key] = value
            return result
        value = json.loads(raw, object_pairs_hook=unique,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
        if encoded(value) != raw or not isinstance(value, dict):
            raise RuntimeError("Target cron observer payload is not canonical")
        return value

    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as db:
        db.execute("BEGIN")
        meta_row = db.execute("SELECT payload FROM metadata WHERE id=1").fetchone()
        epoch_row = db.execute("SELECT payload FROM epochs WHERE execution_id=?",
                               (execution_id,)).fetchone()
        if not meta_row or not epoch_row:
            raise RuntimeError("Target cron observer provisional identity absent")
        meta, epoch = decoded(meta_row[0]), decoded(epoch_row[0])
        expected_meta = {"schema": 2, "profile": str(get_hermes_home().resolve()),
                         "job": job_id, "state": "provisional", "execution_id": execution_id}
        if (any(meta.get(k) != v for k, v in expected_meta.items())
                or type(meta.get("generation")) is not int or meta["generation"] < 1
                or type(meta.get("revision")) is not int or meta["revision"] < 1
                or not isinstance(meta.get("board"), str) or not meta["board"]
                or any(not isinstance(meta.get(k), str) or len(meta[k]) != 64
                       for k in ("source_v1_sha", "ledger_sha", "epoch_id", "issuance_digest"))
                or any(meta.get(k) != output[k] for k in ("generation", "epoch_id", "issuance_digest"))):
            raise RuntimeError("Target cron observer metadata identity changed")
        backup = db_path.with_name("local-observation-v1.backup")
        if (backup.is_symlink() or not backup.is_file() or
                hashlib.sha256(backup.read_bytes()).hexdigest() != meta["source_v1_sha"]):
            raise RuntimeError("Target cron observer source backup changed")
        if (set(epoch) != {"execution_id", "generation", "epoch_id", "issuance_digest",
                            "ledger_sha", "issued_count"}
                or epoch["execution_id"] != execution_id
                or any(epoch[k] != meta[k] for k in
                       ("generation", "epoch_id", "issuance_digest", "ledger_sha"))
                or type(epoch["issued_count"]) is not int
                or not 0 <= epoch["issued_count"] <= 10000):
            raise RuntimeError("Target cron observer epoch identity changed")
        mapping = []
        for root, payload in db.execute(
                "SELECT root,payload FROM epoch_issued WHERE execution_id=? ORDER BY root",
                (execution_id,)):
            item = decoded(payload)
            if (set(item) != {"root", "observation_id", "pending_seq", "attachment_bearing"}
                    or item["root"] != root or type(item["pending_seq"]) is not int
                    or item["pending_seq"] < 1 or type(item["attachment_bearing"]) is not bool
                    or not isinstance(item["observation_id"], str)
                    or len(item["observation_id"]) != 64 or len(mapping) >= 10000):
                raise RuntimeError("Target cron observer issuance mapping changed")
            issued = db.execute("SELECT observation_id,pending_seq,execution_id FROM issued "
                                "WHERE root=?", (root,)).fetchone()
            latest = db.execute("SELECT pending_seq FROM latest WHERE root=?", (root,)).fetchone()
            pending = db.execute("SELECT kind,root,payload FROM records WHERE seq=?",
                                 (item["pending_seq"],)).fetchone()
            if (issued != (item["observation_id"], item["pending_seq"], execution_id)
                    or latest is None or latest[0] != item["pending_seq"] or pending is None
                    or pending[:2] != ("pending", root)):
                raise RuntimeError("Target cron observer issued pointer changed")
            observation = decoded(pending[2])
            if (observation.get("observation_id") != item["observation_id"]
                    or observation.get("attachment_bearing", False) != item["attachment_bearing"]):
                raise RuntimeError("Target cron observer pending identity changed")
            mapping.append(item)
        count = db.execute("SELECT COUNT(*) FROM issued WHERE execution_id=?",
                           (execution_id,)).fetchone()[0]
        if (len(mapping) != epoch["issued_count"] or count != len(mapping)
                or digest({"execution_id": execution_id, "generation": epoch["generation"],
                           "ledger_sha": epoch["ledger_sha"], "mapping": mapping}) != epoch["issuance_digest"]
                or digest([execution_id, epoch["generation"], epoch["issuance_digest"]]) != epoch["epoch_id"]):
            raise RuntimeError("Target cron observer issuance digest changed")


def bind_canonical_output(job: dict, output: str) -> dict:
    """Bind only strict post-redaction output to independently committed v2 identity."""
    import hashlib
    import json
    import stat
    from cron import executions, jobs
    from gateway.session_cron import current_admission

    admission = current_admission()
    execution_id = job.get("execution_id")
    if not admission or admission[3:5] != (job["id"], execution_id):
        raise RuntimeError("Target cron output lacks canonical admission")
    data = output.encode("utf-8", errors="strict")
    if not data or len(data) > 16384 or "\ufffd" in output:
        raise RuntimeError("Target cron output is not a bounded lossless UTF-8 envelope")

    def unique(pairs):
        decoded = {}
        for key, value in pairs:
            if key in decoded:
                raise ValueError("Duplicate target cron JSON member")
            decoded[key] = value
        return decoded

    envelope = json.loads(output, object_pairs_hook=unique)
    fields = {"version", "execution_id", "nonce_digest", "generation", "epoch_id",
              "issuance_digest", "wakeAgent"}
    if (not isinstance(envelope, dict) or set(envelope) != fields
            or type(envelope["version"]) is not int or envelope["version"] != 2
            or type(envelope["generation"]) is not int or envelope["generation"] < 1
            or envelope["wakeAgent"] is not True
            or envelope["execution_id"] != execution_id
            or any(not isinstance(envelope[k], str) or len(envelope[k]) != 64
                   or any(c not in "0123456789abcdef" for c in envelope[k])
                   for k in ("nonce_digest", "epoch_id", "issuance_digest"))):
        raise RuntimeError("Target cron output identity or grammar mismatch")
    db_path = get_hermes_home() / "cron" / f"recovery-{job['id']}" / "local-observation-v2.sqlite"
    # A same-UID trusted deployment still refuses a changed path or symlink at readback.
    if (db_path.is_symlink() or db_path.parent.is_symlink()
            or not stat.S_ISREG(db_path.stat().st_mode)):
        raise RuntimeError("Target cron observer store is not a regular restricted file")
    _verify_provisional_store(db_path, job["id"], execution_id, envelope)
    live = jobs.get_job(job["id"])
    claim = live.get("fire_claim") if isinstance(live, dict) else None
    owner = claim.get("by") if isinstance(claim, dict) else None
    if not owner or job.get("fire_claim", {}).get("by") != owner:
        raise RuntimeError("Target cron claim lost before output binding")
    with jobs.fire_claim_fence(job["id"], expected_owner=owner) as held:
        if not held or jobs.get_job(job["id"]) is None:
            raise RuntimeError("Target cron binding fire fence lost")
        return executions.bind_prerun_output(
            execution_id, job["id"], admission[2], owner,
            nonce_digest=envelope["nonce_digest"],
            generation=envelope["generation"], epoch_id=envelope["epoch_id"],
            issuance_digest=envelope["issuance_digest"],
            output_sha256=hashlib.sha256(data).hexdigest(),
        )


def finish_target_completed_run(d, fire_owner: str | None, execution_id: str) -> bool:
    """Keep the live original fire fenced through mark, terminal and local disposition."""
    from cron import executions, jobs, scheduler_bookkeeping

    job = d.job
    if not fire_owner or not d.success or d.error or d.delivery_error or d.blocked_config:
        return scheduler_bookkeeping.finish_completed_run(d, fire_owner, execution_id,
                                                          _target_inner=True)
    with jobs.fire_claim_fence(job["id"], expected_owner=fire_owner) as held:
        persisted = jobs.get_job(job["id"])
        if (not held or not persisted or not isinstance(persisted.get("fire_claim"), dict)
                or persisted["fire_claim"].get("by") != fire_owner
                or execution_id in persisted.get("canonical_completions", [])):
            return scheduler_bookkeeping.finish_completed_run(d, fire_owner, execution_id,
                                                              _target_inner=True)
        binding = executions.target_preterminal(execution_id, job["id"], fire_owner)
        if binding is None:
            return scheduler_bookkeeping.finish_completed_run(d, fire_owner, execution_id,
                                                              _target_inner=True)
        from observer_parent_seam import ParentAuthority
        authority = ParentAuthority.capture(execution_id, job["id"], fire_owner)
        executions.begin_local_finalization(execution_id)
        completed = scheduler_bookkeeping.finish_completed_run(d, fire_owner, execution_id,
                                                               _target_inner=True)
        terminal = executions.get_execution(execution_id)
        saved_job = jobs.get_job(job["id"])
        if (terminal is None or terminal["status"] != "completed"
                or not saved_job or execution_id not in saved_job.get("canonical_completions", [])
                or saved_job.get("fire_claim") is not None
                or saved_job.get("last_status") not in {"ok", "delivery_queued"}):
            return completed  # pending/UNKNOWN is durable; no checked promotion
        try:
            state, digest = _finalize_with_observer(job, binding, authority)
            if not executions.mirror_local_finalization(execution_id, state, digest):
                raise RuntimeError("Target cron finalization mirror refused")
        except (OSError, ValueError, RuntimeError) as exc:
            import logging
            logging.getLogger(__name__).warning("Target cron local finalization UNKNOWN: %s", exc)
        return completed


def _finalize_with_observer(job: dict, binding: dict, authority) -> tuple[str, str]:
    import hashlib
    import json
    import os
    import stat
    from cron.scheduler_script import _run_job_script, _resolve_script_path
    from hermes_cli.config_effective import load_user_config_effective
    from hermes_cli import kanban_db
    import engineering_recovery_observer as legacy
    from local_observation_v2 import Store

    configured = load_user_config_effective(get_hermes_home() / "config.yaml")
    cron = configured.get("cron", {}) if isinstance(configured, dict) else {}
    script = cron.get("local_observation_finalizer_script") if isinstance(cron, dict) else None
    expected = cron.get("local_observation_finalizer_sha256") if isinstance(cron, dict) else None
    if not isinstance(script, str) or not script or not isinstance(expected, str) or len(expected) != 64:
        raise RuntimeError("Target cron finalizer is not configured")
    path, error = _resolve_script_path(script)
    if error or path is None or path.is_symlink():
        raise RuntimeError("Target cron finalizer path is untrusted")
    home = get_hermes_home().resolve()
    if home not in path.parents:
        raise RuntimeError("Target cron finalizer is outside its profile")
    current = path
    while True:
        info = current.lstat()
        if (stat.S_ISLNK(info.st_mode) or info.st_uid != getattr(os, "getuid", lambda: -1)()
                or info.st_mode & 0o022):
            raise RuntimeError("Target cron finalizer or ancestor is mutable or symlinked")
        if current == home:
            break
        current = current.parent
    if not stat.S_ISREG(path.stat().st_mode):
        raise RuntimeError("Target cron finalizer is not a regular file")
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise RuntimeError("Target cron finalizer source changed")
    ok, output = _run_job_script(str(path), scoped_env={
        "HERMES_LOCAL_OBSERVATION_JOB_ID": job["id"],
        "HERMES_LOCAL_OBSERVATION_EXECUTION_ID": binding["execution_id"],
        "HERMES_LOCAL_OBSERVATION_EPOCH_ID": binding["epoch_id"],
    })
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected or not ok or len(output) > 4096:
        raise RuntimeError("Target cron finalizer failed or changed while running")
    result = json.loads(output, object_pairs_hook=lambda pairs: _unique_proposal_members(pairs))
    if (not isinstance(result, dict) or set(result) != {"roots"}
            or not isinstance(result["roots"], list) or len(result["roots"]) > 10000
            or any(not isinstance(root, str) or not root.startswith("t_") for root in result["roots"])
            or result["roots"] != sorted(set(result["roots"]))):
        raise RuntimeError("Target cron finalizer proposal is not a bounded root set")
    directory = get_hermes_home() / "cron" / f"recovery-{job['id']}"
    db_path = directory / "local-observation-v2.sqlite"
    if db_path.is_symlink() or directory.is_symlink():
        raise RuntimeError("Target cron observer outcome path changed")
    store = Store(directory)
    board = kanban_db.kanban_db_path(board=legacy.BOARD)
    committed = store.parent_finalize(authority, result, board)
    return _read_complete_parent_outcome(store, binding, committed, result["roots"])

def _read_complete_parent_outcome(store, binding, committed, roots):
    """Read every bounded immutable proof page before reflecting success into the producer."""
    execution_id = binding["execution_id"]
    cursor = None
    seen = []
    baseline = None
    for _ in range(10001):
        proof = store.read_parent_outcome(execution_id, after_root=cursor)
        outcome = proof["outcome"]
        current = (proof["epoch"], outcome, proof["final_event"])
        if (baseline is not None and current != baseline
                or outcome["execution_id"] != execution_id
                or outcome["epoch_id"] != binding["epoch_id"]
                or outcome["state"] not in {"succeeded", "not_applicable"}
                or outcome["state"] != committed["state"]
                or outcome["digest"] != committed["digest"]
                or outcome["record_count"] != len(roots)
                or proof["epoch"]["issuance_digest"] != binding["issuance_digest"]
                or proof["epoch"]["generation"] != binding["generation"]
                or proof["final_event"]["event"] != "finalize"
                or proof["final_event"]["outcome_digest"] != outcome["digest"]):
            raise RuntimeError("Target cron parent receipt readback mismatch")
        baseline = current
        page_roots = [item["outcome_item"]["root"] for item in proof["items"]]
        if (any(not isinstance(root, str) for root in page_roots)
                or page_roots != sorted(set(page_roots))
                or (page_roots and cursor is not None and page_roots[0] <= cursor)
                or len(seen) + len(page_roots) > len(roots)
                or proof["last_root"] != (page_roots[-1] if page_roots else cursor)
                or (proof["has_more"] and not page_roots)):
            raise RuntimeError("Target cron parent proof page did not advance")
        seen.extend(page_roots)
        if not proof["has_more"]:
            if seen != roots:
                raise RuntimeError("Target cron parent proof root set incomplete")
            return outcome["state"], outcome["digest"]
        cursor = proof["last_root"]
    raise RuntimeError("Target cron parent proof exceeded page bound")


def _unique_proposal_members(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RuntimeError("Target cron duplicate proposal member")
        result[key] = value
    return result


def reserve_original_firer(job: dict, execution_id: str, fire_owner: str | None) -> None:
    from cron import executions, jobs

    if not fire_owner:
        raise RuntimeError("Target cron fire has no original claim owner")
    with jobs.fire_claim_fence(job["id"], expected_owner=fire_owner) as held:
        if not held:
            raise RuntimeError("Target cron fire claim fence lost before admission")
        # fire_claim_fence also allows a self-removed job; target explicitly forbids that.
        persisted = jobs.get_job(job["id"])
        if not persisted or persisted.get("fire_claim", {}).get("by") != fire_owner:
            raise RuntimeError("Target cron job or claim missing before admission")
        executions.reserve_prerun_binding(execution_id, job["id"], fire_owner)
