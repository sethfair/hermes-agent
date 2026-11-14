"""Bounded provisional native output for an isolated observer-v2 candidate.

This module is consumed by the job's verified pre-run script; the core producer
must separately bind the resulting output before any stage is actionable.
"""
from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path

import engineering_recovery_observer as legacy
import local_observation_v2 as v2
from local_observation_v2 import Store, encode


def native_prerun(directory, native_db, board, *, enforce_board=False):
    directory = Path(directory)
    execution = os.environ.get("HERMES_LOCAL_OBSERVATION_EXECUTION_ID")
    job = os.environ.get("HERMES_LOCAL_OBSERVATION_JOB_ID")
    nonce_hex = os.environ.get("HERMES_LOCAL_OBSERVATION_NONCE")
    if (not execution or job != legacy.JOB_ID or not isinstance(nonce_hex, str)
            or len(nonce_hex) != 64 or any(c not in "0123456789abcdef" for c in nonce_hex)):
        raise ValueError("scheduler-scoped observation identity required")
    if enforce_board and Path(board).resolve() != Path("/home/macpro/.hermes/kanban.db"):
        raise ValueError("unexpected default board path")
    store = Store(directory)
    if store.header()["state"] not in {"invalidating", "provisional", "finalized"}:
        raise ValueError("unknown observer-v2 admission state")
    # Invalidate before any read whose failure could leave a prior epoch selectable.
    prior_ledger, ledger_sha = legacy.read_ledger(directory)
    store.begin(execution, ledger_sha)
    if not prior_ledger or prior_ledger["version"] != 2:
        raise ValueError("v2 ledger required")
    marker = legacy._cutover_path(directory, prior_ledger)
    legacy._verify_cutover(directory, prior_ledger, marker,
                           legacy.json.loads(marker.read_bytes()))
    ack, _ = legacy.read_ack(directory)
    native_changes = []
    prior_sha = ledger_sha
    ledger_sha, native_changes = legacy.reconcile_native(directory, native_db, ledger_sha)
    if ledger_sha != prior_sha:
        store.rebind_invalidating_ledger(execution, prior_sha, ledger_sha)
    ledger, actual = legacy.read_ledger(directory)
    if actual != ledger_sha:
        raise legacy.StaleAck("native ledger changed")
    prior = legacy.read_legacy(native_db)
    roots, _ = legacy.discover_roots({}, prior, ack, ledger)
    local, _, missing = legacy.board_snapshot(board, roots, include_hints=True,
                                              allow_missing=True)
    checked = {}
    for root in sorted(roots):
        current = store.current(root)
        if current and current["checked"]:
            checked[root] = current["checked"].get("local_fingerprint")
    changed = {root for root in roots if root in local and local[root] != checked.get(root)}
    if len(changed) > 10000:
        raise ValueError("current observation cardinality exceeded")
    # No root IDs or event lists enter the native stdout. The immutable epoch
    # mapping is read with page_current after the producer binds these bytes.
    epoch = store.native_commit(execution, ledger_sha, board, sorted(changed))
    due_now = time.time()
    externally_due = any(
        item.get("unknown") is True
        or not item.get("sources", {}).get("external_sources")
        or (isinstance(item.get("due_at"), (int, float)) and not isinstance(item.get("due_at"), bool)
            and item["due_at"] <= due_now)
        or any(source.get("state") != "checked" or source.get("next_due") is None
               or source["next_due"] <= due_now
               for source in item.get("sources", {}).get("external_sources", {}).values())
        for item in ledger["roots"].values()
    )
    envelope = {"version": 2, "execution_id": execution,
                "nonce_digest": hashlib.sha256(bytes.fromhex(nonce_hex)).hexdigest(),
                "generation": epoch["generation"], "epoch_id": epoch["epoch_id"],
                "issuance_digest": epoch["issuance_digest"],
                "wakeAgent": bool(changed or missing or native_changes or externally_due)}
    if len(encode(envelope)) > 16384:
        raise ValueError("native observer output exceeds envelope")
    header = store.header()
    if (header["state"] != "provisional" or header["epoch_id"] != epoch["epoch_id"]
            or header["issuance_digest"] != epoch["issuance_digest"]):
        raise legacy.StaleAck("native observer commit readback mismatch")
    return envelope


def select_current(directory, board, execution_id, epoch_id, revision, *, output_dir, limit=1,
                   targets=None, after_root=None):
    """Offer a bounded advisory batch; stage rechecks all authority and native evidence."""
    import stat
    import tempfile

    import observer_parent_seam as seam
    from hermes_cli import kanban_db

    if type(revision) is not int or type(limit) is not int or not 1 <= limit <= 128:
        raise ValueError("invalid selector revision or batch size")
    if after_root is not None and (not isinstance(after_root, str) or not v2.ROOT.fullmatch(after_root)):
        raise ValueError("invalid page cursor")
    if targets is not None and (not isinstance(targets, list) or not targets
            or len(targets) > limit or len(set(targets)) != len(targets)
            or any(not isinstance(root, str) or not v2.ROOT.fullmatch(root) for root in targets)):
        raise ValueError("invalid bounded explicit roots")
    directory = Path(directory)
    board = legacy._reject_symlink_ancestors(Path(board))
    canonical = legacy._reject_symlink_ancestors(kanban_db.kanban_db_path(board=legacy.BOARD))
    if board != canonical or not board.is_file() or not stat.S_ISREG(board.stat().st_mode):
        raise v2.Stale("selector board is not canonical")
    binding = seam._stage_binding(execution_id)
    store = v2.Store(directory)
    header = store.header()
    if (header["execution_id"] != execution_id or header["epoch_id"] != epoch_id
            or header["revision"] != revision or header["state"] != "provisional"
            or header["profile"] != binding["profile_home"]
            or header["job"] != binding["job_id"]
            or header["generation"] != binding["generation"]
            or header["issuance_digest"] != binding["issuance_digest"]):
        raise v2.Stale("selector binding/epoch changed")
    ledger, ledger_sha = legacy.read_ledger(directory)
    if not ledger or ledger_sha != header["ledger_sha"]:
        raise v2.Stale("selector ledger changed")
    marker = legacy._cutover_path(directory, ledger)
    legacy._verify_cutover(directory, ledger, marker, v2.strict_json(v2._regular_bytes(marker, 1048576)))
    selected = {}
    cursor = after_root
    last = after_root
    more = False
    wanted = set(targets) if targets is not None else None
    while True:
        page = store.page_current(execution_id, epoch_id, header["generation"],
                                  header["issuance_digest"], cursor)
        for item in page["items"]:
            root = item["root"]
            last = root
            if wanted is not None and root not in wanted:
                continue
            if item["attachment_bearing"]:
                if wanted is not None:
                    raise v2.Stale("explicit root is attachment-bearing")
                continue
            current = store.current(root)
            if not current or current["pending_seq"] != item["pending_seq"]:
                raise v2.Stale("selector pending pointer changed")
            with store._connection() as (db, _):
                staged = db.execute("SELECT 1 FROM stages WHERE execution_id=? AND root=?",
                                    (execution_id, root)).fetchone()
            if staged:
                if wanted is not None:
                    raise v2.Stale("explicit root already staged")
                continue
            try:
                actual, ids, attachments = legacy.board_observation(board, [root])
            except legacy.StaleAck:
                if wanted is not None:
                    raise
                continue
            pending = current["pending"]
            if (root in attachments or actual.get(root) != pending.get("fingerprint")
                    or ids.get(root) != pending.get("event_ids")):
                if wanted is not None:
                    raise v2.Stale("explicit root native evidence changed")
                continue
            selected[root] = item["observation_id"]
            if len(selected) >= limit:
                break
        more = page["has_more"] or (bool(page["items"]) and last != page["last_root"])
        if len(selected) >= limit or not more or wanted is None:
            break
        if not page["items"] or page["last_root"] == cursor:
            raise v2.Stale("selector page did not advance")
        cursor = page["last_root"]
    if wanted is not None and set(selected) != wanted:
        raise v2.Stale("explicit root not in current issuance")
    if store.header() != header or seam._stage_binding(execution_id) != binding:
        raise v2.Stale("selector identity changed before artifact")
    if not selected:
        return {"status": "more" if more else "none", "revision": revision, "count": 0,
                "last_root": last, "has_more": more}
    data = v2.encode({"execution_id": execution_id, "epoch_id": epoch_id,
                      "generation": header["generation"], "issuance_digest": header["issuance_digest"],
                      "revision": revision, "roots": selected})
    if len(data) > 32768:
        raise v2.Overflow("selection artifact exceeds byte bound")
    output_dir = legacy._reject_symlink_ancestors(Path(output_dir))
    if not output_dir.is_dir() or output_dir.stat().st_mode & 0o077:
        raise ValueError("selection directory must be private")
    fd, name = tempfile.mkstemp(prefix="wri-2276-selection-", suffix=".json", dir=output_dir)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        with open(name, "rb") as stream:
            if stream.read(32769) != data:
                raise v2.Stale("selection artifact readback changed")
        return {"status": "selected", "selection_path": name,
                "selection_sha256": hashlib.sha256(data).hexdigest(),
                "revision": revision, "count": len(selected), "last_root": last, "has_more": more}
    except BaseException:
        if fd >= 0:
            os.close(fd)
        os.unlink(name)
        raise
