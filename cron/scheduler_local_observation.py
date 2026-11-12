"""Original-worker admission for the private local observation producer.

This module has no gateway or child authority. Later issuance and parent terminal
steps must consume the same pinned execution row; a script result is not approval.
"""

from __future__ import annotations

import hashlib
import importlib.util
import importlib.abc
import stat
import threading
import types
import json
import os
import secrets
import sqlite3
import subprocess
import sys
from pathlib import Path

from hermes_constants import get_hermes_home
from hermes_time import now as _hermes_now

TARGET_JOB_ID = "61a6b39fba27"
TARGET_PROFILE_NAME = "rowan"
_REFERENCE_SHA = {
    "engineering_recovery_observer": "b13bf6d9b00381c915c60a3f9017e94b3914a6225872e9a4d266d2476a2a1134",
    "native_status_policy": "60cbdfa950d25873aa7b82247b7dd1d857d364b4e3a6d15aa43a0783ab665c5a",
    "observer_native_v2": "04add5c2c28ad50672bb1a0e160b22d26b3efbd1d89ad042fa6ccb9cd29648af",
    "observer_private_entry": "245a053b8010c80cb679c2e069cf2005a2e1a15f257859152175845d307f9e10",
    "local_observation_v2": "e00e8c99a2e20f9fd2df5b65fb59f6c981649e183c639fea9e1c91933f08a152",
    "observer_parent_seam": "b9e762b53dc820cb47b0833bf81dfb6bb3322f64c1b31c96f2c90b74bdc302e2",
    "observer_proposal_v2": "db6987006def99fadddc4d8b6943a8a0a872fd34c3babca9a42cc51fc2ac9316",
}
_TEST_AFTER_RESERVE = None  # owner-process synchronization for native integration tests
_TEST_AFTER_BIND = None
_TEST_AFTER_COMMIT = None


def _runtime_root() -> Path:
    return Path(__file__).resolve().parents[1]


def is_target(job: dict) -> bool:
    return (job.get("id") == TARGET_JOB_ID
            and get_hermes_home().resolve().name == TARGET_PROFILE_NAME)


def _require_current_manifest() -> None:
    from cron.local_observation_install import validate_runtime_manifest

    validate_runtime_manifest(get_hermes_home().resolve(), _runtime_root())


def begin_owned(job: dict, execution_id: str, fire_owner: str | None) -> None:
    if not is_target(job):
        return
    _require_current_manifest()
    if not fire_owner:
        raise ValueError("target fire requires an original claim")
    job["_local_observation_nonce"] = reserve(job, execution_id, fire_owner)


def reserve(job: dict, execution_id: str, fire_owner: str) -> str:
    """Reserve the original running attempt before child script admission."""
    from cron import executions, jobs
    from cron.scheduler_script import _resolve_script_path

    if not fire_owner or job.get("no_agent") is False and not job.get("script"):
        raise ValueError("target requires fire claim and a pre-run script")
    path = _admit_script(job)
    script_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    source_backup = get_hermes_home().resolve() / "cron" / f"recovery-{TARGET_JOB_ID}" / "local-observation-v1.backup"
    source_sha = (hashlib.sha256(source_backup.read_bytes()).hexdigest()
                  if source_backup.is_file() and not source_backup.is_symlink() else "0" * 64)
    saved = jobs.get_job(job["id"])
    if not saved or saved.get("fire_claim", {}).get("by") != fire_owner:
        raise ValueError("original fire claim changed")
    with executions._transaction() as db:
        row = executions._fetch(db, execution_id)
        if (not row or row["job_id"] != job["id"] or row["status"] != "running"
                or row["process_id"] != executions._PROCESS_ID or row["pid"] != os.getpid()
                or row["process_started_at"] is None):
            raise ValueError("original running execution absent")
        existing = db.execute(
            "SELECT execution_id FROM cron_prerun_bindings WHERE execution_id=?", (execution_id,)
        ).fetchone()
        if existing:
            raise ValueError("attempt already reserved; read-only reconciliation required")
        db.execute("""INSERT INTO cron_prerun_bindings
            (execution_id,job_id,profile_home,process_id,pid,process_started_at,
             fire_claim_by,bind_state,admission_id,source_sha256,script_sha256)
            VALUES (?,?,?,?,?,?,?,'awaiting_canonical',?,?,?)""",
            (execution_id, job["id"], str(get_hermes_home().resolve()),
             executions._PROCESS_ID, os.getpid(), row["process_started_at"],
             fire_owner, secrets.token_hex(16), source_sha, script_sha),
        )
    if _TEST_AFTER_RESERVE is not None:
        _TEST_AFTER_RESERVE()
    nonce = secrets.token_hex(32)
    digest = hashlib.sha256(bytes.fromhex(nonce)).hexdigest()
    with executions._transaction() as db:
        changed = db.execute("""UPDATE cron_prerun_bindings
            SET bind_state='preparing',nonce_digest=?
            WHERE execution_id=? AND job_id=? AND fire_claim_by=?
              AND process_id=? AND pid=? AND bind_state='awaiting_canonical'
              AND nonce_digest IS NULL AND output_sha256 IS NULL""",
            (digest, execution_id, job["id"], fire_owner,
             executions._PROCESS_ID, os.getpid()),
        )
        if changed.rowcount != 1:
            raise ValueError("lost reservation before nonce preparation")
    return nonce


def _strict_json(raw: bytes) -> dict:
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate observer output field")
            value[key] = item
        return value
    result = json.loads(raw, object_pairs_hook=unique,
                        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
    if not isinstance(result, dict):
        raise ValueError("observer output must be an object")
    return result


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _current_issuance(db: sqlite3.Connection, output: dict, meta: dict) -> None:
    execution_id = output["execution_id"]
    rows = db.execute("""SELECT i.root,i.observation_id,i.pending_seq,r.payload,l.pending_seq,e.payload
        FROM issued i JOIN records r ON r.seq=i.pending_seq
        JOIN latest l ON l.root=i.root JOIN epoch_issued e
        ON e.execution_id=i.execution_id AND e.root=i.root
        WHERE i.execution_id=? ORDER BY i.root COLLATE BINARY""", (execution_id,)).fetchall()
    if len(rows) > 10000:
        raise ValueError("observer issuance exceeds cardinality")
    mapping = []
    for root, ident, seq, payload, current, epoch_payload in rows:
        item = {"root": root, "observation_id": ident, "pending_seq": seq,
                "attachment_bearing": _strict_json(payload)["attachment_bearing"]}
        if seq != current or _strict_json(payload)["observation_id"] != ident or _strict_json(epoch_payload) != item:
            raise ValueError("observer issuance pointer differs")
        mapping.append(item)
    epoch_row = db.execute("SELECT payload FROM epochs WHERE execution_id=?", (execution_id,)).fetchone()
    if not epoch_row or db.execute("SELECT COUNT(*) FROM issued").fetchone()[0] != len(mapping):
        raise ValueError("observer epoch or issued set differs")
    expected = _digest({"execution_id": execution_id, "generation": meta["generation"],
                        "ledger_sha": meta["ledger_sha"], "mapping": mapping})
    epoch = _strict_json(epoch_row[0])
    if (expected != output["issuance_digest"] or epoch.get("issuance_digest") != expected
            or epoch.get("issued_count") != len(mapping) or epoch.get("epoch_id") != output["epoch_id"]):
        raise ValueError("observer issuance digest differs")


def bind_output(job: dict, ok: bool, output_text: str) -> None:
    """Compare redacted child output with the owned nonce and committed v2 epoch."""
    if not is_target(job):
        return
    from cron import executions
    execution_id = job["execution_id"]
    raw = output_text.strip().encode("utf-8")
    if not ok or len(raw) > 16384:
        raise ValueError("observer script failed or output exceeds bound")
    output = _strict_json(raw)
    if (set(output) != {"version", "execution_id", "nonce_digest", "generation",
                        "epoch_id", "issuance_digest", "wakeAgent"}
            or type(output["version"]) is not int or output["version"] != 2
            or output["execution_id"] != execution_id
            or type(output["generation"]) is not int or output["generation"] < 1
            or type(output["wakeAgent"]) is not bool
            or any(not isinstance(output[k], str) or len(output[k]) != 64
                   or any(ch not in "0123456789abcdef" for ch in output[k])
                   for k in ("nonce_digest", "epoch_id", "issuance_digest"))):
        raise ValueError("observer script envelope invalid")
    directory = get_hermes_home().resolve() / "cron" / f"recovery-{TARGET_JOB_ID}"
    db_file = directory / "local-observation-v2.sqlite"
    backup = directory / "local-observation-v1.backup"
    if any(p.is_symlink() for p in (directory, db_file, backup)) or not db_file.is_file() or not backup.is_file():
        raise ValueError("observer storage unavailable")
    source_sha = hashlib.sha256(backup.read_bytes()).hexdigest()
    with sqlite3.connect(f"file:{db_file}?mode=ro", uri=True) as db:
        db.execute("BEGIN")
        meta_row = db.execute("SELECT payload FROM metadata WHERE id=1").fetchone()
        if not meta_row:
            raise ValueError("observer metadata absent")
        meta = _strict_json(meta_row[0])
        if (meta.get("schema") != 2 or meta.get("job") != TARGET_JOB_ID
                or meta.get("profile") != str(get_hermes_home().resolve())
                or meta.get("source_v1_sha") != source_sha or meta.get("state") != "provisional"
                or any(meta.get(k) != output[k] for k in ("execution_id", "epoch_id", "issuance_digest", "generation"))):
            raise ValueError("observer current epoch differs")
        _current_issuance(db, output, meta)
    script = _admit_script(job)
    with executions._transaction() as db:
        db.row_factory = sqlite3.Row
        bound = db.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?", (execution_id,)).fetchone()
        row = executions._fetch(db, execution_id)
        if (not bound or not row or row["status"] != "running"
                or bound["source_sha256"] != source_sha
                or bound["script_sha256"] != hashlib.sha256(Path(script).read_bytes()).hexdigest()
                or bound["profile_home"] != str(get_hermes_home().resolve())
                or bound["process_id"] != executions._PROCESS_ID
                or bound["pid"] != os.getpid()
                or bound["process_started_at"] != row["process_started_at"]
                or bound["fire_claim_by"] != job.get("fire_claim", {}).get("by")
                or bound["nonce_digest"] != output["nonce_digest"]):
            raise ValueError("original producer pin differs")
        changed = db.execute("""UPDATE cron_prerun_bindings
            SET bind_state='bound', generation=?,epoch_id=?,issuance_digest=?,output_sha256=?
            WHERE execution_id=? AND bind_state='preparing' AND output_sha256 IS NULL""",
            (output["generation"], output["epoch_id"], output["issuance_digest"],
             hashlib.sha256(raw).hexdigest(), execution_id))
        if changed.rowcount != 1:
            raise ValueError("observer output already bound or quarantined")
    if _TEST_AFTER_BIND is not None:
        _TEST_AFTER_BIND()


def deny_unbound(job: dict) -> None:
    """Record a failed preparation without modifying an already captured bound pin."""
    from cron import executions
    execution_id = job["execution_id"]
    with executions._transaction() as db:
        changed = db.execute("""UPDATE cron_prerun_bindings SET bind_state='unknown'
            WHERE execution_id=? AND process_id=? AND pid=? AND unresolved_pin=1
              AND bind_state IN ('awaiting_canonical','preparing')""",
            (execution_id, executions._PROCESS_ID, os.getpid()))
        if changed.rowcount == 1:
            db.execute("INSERT OR IGNORE INTO cron_local_finalizations VALUES(?,'unknown',NULL,?)",
                       (execution_id, _hermes_now().isoformat()))


def prepare_terminal(job: dict, execution_id: str, owner: str | None,
                     *, success: bool, delivery_error: str | None, status: str | None,
                     mark_kwargs: dict):
    if not is_target(job) or not success or delivery_error or status not in (None, "delivery_queued"):
        return None
    from cron import executions
    if not owner:
        raise ValueError("original target claim missing before terminal")
    from cron.jobs import get_job
    active = get_job(job["id"])
    if not active or active.get("fire_claim", {}).get("by") != owner:
        raise ValueError("target claim changed before terminal")
    if not executions.target_preterminal(execution_id, job["id"], owner):
        raise ValueError("bound target required")
    _admit_script(job)
    source, v2, seam = _retained_sources()
    authority = seam.ParentAuthority.capture(execution_id, job["id"], owner)
    if not executions.start_target_finalization(execution_id, job["id"], owner):
        raise ValueError("bound target and pending finalization required")
    mark_kwargs["execution_id"] = execution_id
    return authority, source, v2, seam


def allow_self_removal_shortcut(job: dict) -> bool:
    if is_target(job):
        return False
    from cron.jobs import self_removal_delivery_allowed
    return self_removal_delivery_allowed(job["id"])


def selector_prompt(job: dict) -> str:
    """Build the target's current, stage-only instructions after output binding."""
    if not is_target(job):
        return ''
    import shlex
    entry = _admit_script(job)
    _directory, v2, seam = _retained_sources()
    execution_id = job['execution_id']
    binding = seam._stage_binding(execution_id)
    store = v2.Store(get_hermes_home().resolve() / 'cron' / f'recovery-{TARGET_JOB_ID}')
    header = store.header()
    if (header['state'] != 'provisional' or header['execution_id'] != execution_id
            or any(header[key] != binding[key]
                   for key in ('epoch_id', 'generation', 'issuance_digest'))):
        raise ValueError('selector prompt current binding changed')
    command = f'{shlex.quote(sys.executable)} {shlex.quote(str(entry))}'
    select = f'{command} local-select {shlex.quote(execution_id)} {binding["epoch_id"]}'
    return (
        '\n\nPrivate current observation (stage-only):\n'
        f'{select} {header["revision"]} 8\n'
        'Read the bounded selection artifact and verify its returned SHA256. '
        'Stage only current returned root and observation IDs with:\n'
        f'{command} local-stage {shlex.quote(execution_id)} <root> <observation-id> <current-revision>\n'
        'Use the returned revision after each stage or selection. '
        'For another bounded batch use local-select with that revision and the returned after-root cursor. '
        'A stage stays pending; only the original successful terminal parent can produce checked proof. '
        'Do not finalize, globally ACK, clear UNKNOWN or missing roots, or run reconcile-status automatically. '
        'Native status is local history, not recovery completion. Explicit status observation needs its separate authority.\n')


class _VerifiedByteLoader(importlib.abc.Loader):
    def exec_module(self, module):
        raise ImportError('private package requires verified-byte admission')


_PACKAGE_LOCK = threading.RLock()
_PACKAGE_REGISTRY: tuple[Path, Path, dict[str, types.ModuleType]] | None = None
_PACKAGE_LOADER = _VerifiedByteLoader()
_LOAD_ORDER = ('native_status_policy', 'engineering_recovery_observer',
               'local_observation_v2', 'observer_parent_seam', 'observer_proposal_v2',
               'observer_native_v2', 'observer_private_entry')


def _source_identity(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _regular_path(path, *, directory=False):
    for part in (path, *path.parents):
        if stat.S_ISLNK(part.lstat().st_mode):
            raise ValueError('symlinked private package ancestor')
    mode = path.lstat().st_mode
    if not (stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode)):
        raise ValueError('private package member type differs')
    return path


def _package_bytes(directory):
    home = get_hermes_home().absolute()
    expected = home / 'scripts' / 'observer_v2'
    if home.name != TARGET_PROFILE_NAME or Path(directory) != expected:
        raise ValueError('private package path or owning profile differs')
    _regular_path(expected, directory=True)
    before = expected.stat()
    sources = {}
    for name in _LOAD_ORDER:
        path = _regular_path(expected / (name + '.py'))
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as stream:
            initial = os.fstat(stream.fileno())
            if not stat.S_ISREG(initial.st_mode) or initial.st_size > 2097152:
                raise ValueError('invalid private package source')
            raw = stream.read(2097153)
            final = os.fstat(stream.fileno())
        if (_source_identity(initial) != _source_identity(final)
                or _source_identity(final) != _source_identity(path.lstat())
                or hashlib.sha256(raw).hexdigest() != _REFERENCE_SHA[name]):
            raise ValueError('private package source pin or identity differs')
        sources[name] = raw
    if _source_identity(before) != _source_identity(expected.stat()):
        raise ValueError('private package directory changed during admission')
    return expected, home, sources


def load_private_package(directory):
    """Execute the complete admitted bytes; foreign canonical caches fail closed.

    The repository is the trust root. This is not a hostile same-UID Python sandbox.
    Initialization finishes before any state operation or child launch.
    """
    global _PACKAGE_REGISTRY
    with _PACKAGE_LOCK:
        directory, home, sources = _package_bytes(directory)
        if _PACKAGE_REGISTRY is not None:
            registered_directory, registered_home, modules = _PACKAGE_REGISTRY
            if directory != registered_directory or home != registered_home:
                raise ValueError('cross-profile private package reuse refused')
            for name, module in modules.items():
                expected = str(directory / (name + '.py'))
                if (sys.modules.get(name) is not module or module.__file__ != expected
                        or module.__loader__ is not _PACKAGE_LOADER
                        or module.__spec__ is None or module.__spec__.origin != expected
                        or module.__spec__.loader is not _PACKAGE_LOADER):
                    raise ValueError('private package cached object or origin changed')
            return dict(modules)
        if any(name in sys.modules for name in _LOAD_ORDER):
            raise ValueError('untrusted canonical private package cache')
        created = {}
        try:
            for name in _LOAD_ORDER:
                path = str(directory / (name + '.py'))
                module = types.ModuleType(name)
                module.__file__ = path
                module.__loader__ = _PACKAGE_LOADER
                module.__spec__ = importlib.util.spec_from_loader(name, _PACKAGE_LOADER, origin=path)
                module.__package__ = ''
                created[name] = module
                sys.modules[name] = module
                exec(compile(sources[name], path, 'exec'), module.__dict__)
            created['engineering_recovery_observer'].PROFILE_HOME = home
            _PACKAGE_REGISTRY = directory, home, created
            return dict(created)
        except BaseException:
            for name, module in created.items():
                if sys.modules.get(name) is module:
                    del sys.modules[name]
            _PACKAGE_REGISTRY = None
            raise


def _admit_script(job):
    from cron.scheduler_script import _resolve_script_path
    home = get_hermes_home().absolute()
    canonical = home / 'scripts/observer_v2/observer_private_entry.py'
    requested = Path(str(job.get('script') or ''))
    if not requested.is_absolute():
        requested = home / 'scripts' / requested
    if requested != canonical:
        raise ValueError('target requires canonical private entry script')
    _regular_path(requested)
    path, error = _resolve_script_path(str(requested))
    if path is None or Path(path) != canonical:
        raise ValueError(error or 'private entry path differs')
    load_private_package(canonical.parent)
    return canonical


def _retained_sources():
    directory = get_hermes_home().absolute() / 'scripts' / 'observer_v2'
    modules = load_private_package(directory)
    return directory, modules['local_observation_v2'], modules['observer_parent_seam']


def _read_proposal(source: Path, directory: Path, execution_id: str, epoch_id: str) -> dict:
    from tools.environments.local import build_subprocess_env
    env = build_subprocess_env(strip_launch_profile=True)
    env["HERMES_HOME"] = str(get_hermes_home().resolve())
    source_text = (
        "import pathlib,sys; "
        f"sys.path.insert(0,{str(Path(__file__).resolve().parents[1])!r}); "
        "from cron.scheduler_local_observation import load_private_package; "
        f"modules=load_private_package(pathlib.Path({str(source)!r})); "
        "v2=modules['local_observation_v2'];proposal=modules['observer_proposal_v2']; "
        f"print(v2.encode(proposal.read_proposal(pathlib.Path({str(directory)!r}),"
        f"{execution_id!r},{epoch_id!r})).decode('ascii'))"
    )
    child = subprocess.run([sys.executable, "-c", source_text],
                           cwd=str(source), env=env, capture_output=True,
                           text=True, timeout=20, check=False)
    if child.returncode or len(child.stdout.encode()) > 16384:
        raise ValueError("read-only proposal child failed or exceeded bound")
    return _strict_json(child.stdout.strip().encode())


def complete_terminal(job: dict, execution_id: str, captured,
                      finished: dict | None) -> None:
    """Retained parent alone commits a checked outcome, then mirrors its proof."""
    if captured is None:
        return
    authority, source, v2, seam = captured
    bound = authority.binding
    from cron import executions, jobs
    current = jobs.get_job(job["id"])
    if (not finished or finished.get("status") != "completed"
            or finished.get("source") == "recovered" or finished.get("error") is not None
            or not finished.get("finished_at")
            or any(finished.get(k) != bound[k] for k in ("process_id", "pid", "process_started_at"))
            or not current or current.get("fire_claim") is not None
            or execution_id not in current.get("canonical_completions", [])
            or current.get("last_status") not in {"ok", "delivery_queued"}):
        return
    directory = get_hermes_home().resolve() / "cron" / f"recovery-{TARGET_JOB_ID}"
    with executions._transaction() as db:
        now = db.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?", (execution_id,)).fetchone()
        if not now or dict(now) != bound:
            return
    script = _admit_script(current)
    backup = directory / "local-observation-v1.backup"
    if (not script or Path(script).is_symlink() or backup.is_symlink()
            or hashlib.sha256(Path(script).read_bytes()).hexdigest() != bound["script_sha256"]
            or hashlib.sha256(backup.read_bytes()).hexdigest() != bound["source_sha256"]):
        raise ValueError("target script or source changed after binding")
    _retained_sources()
    proposal = _read_proposal(source, directory, execution_id, bound["epoch_id"])
    from hermes_cli import kanban_db
    store = v2.Store(directory)
    with kanban_db.pin_first_board_resolution():
        board = kanban_db.kanban_db_path(board="default")
        outcome = seam.finalize_parent(store, authority, proposal, board)
    if _TEST_AFTER_COMMIT is not None:
        _TEST_AFTER_COMMIT()
    count = 0
    with store._connection() as snapshot:
        cache = {}
        after = None
        while True:
            page = seam.read_parent_outcome(store, execution_id, after,
                                            _snapshot=snapshot, _cache=cache,
                                            _current_binding=bound)
            if (page["outcome"]["digest"] != outcome["digest"]
                    or page["outcome"]["record_count"] != len(outcome["records"])):
                return
            count += len(page["items"])
            if not page["has_more"]:
                break
            after = page["last_root"]
    if count != len(outcome["records"]):
        return
    if not executions.finish_target_finalization(execution_id, outcome["state"],
                                                  outcome["digest"], expected_binding=bound):
        raise ValueError("bound producer pin changed before mirror")


def settle_terminal(job: dict, execution_id: str, captured, finished: dict | None) -> None:
    """An owner-observed terminal denial stays pinned and externally UNKNOWN."""
    try:
        complete_terminal(job, execution_id, captured, finished)
    except Exception:
        if captured is not None:
            from cron import executions
            executions.finish_target_finalization(execution_id, "unknown", None)
        raise
