"""Bounded paused installer for the WRI-2276 private observer generation."""

from __future__ import annotations

import base64
import hashlib
import importlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

JOB_ID = "61a6b39fba27"
PROFILE_NAME = "rowan"
SCHEMA_GENERATION = 2
SOURCE_REPOSITORY = "writerflow/hermes-kanban"
APPROVAL_NAMESPACE = "wri-2276-release-v1"
APPROVAL_PRINCIPAL = "wri-2276-release"
TRUST_ROOT = Path("/etc/hermes/release-trust/wri-2276")
APPROVAL_ROOT = Path("/var/lib/hermes/release-approvals/wri-2276")
TRUST_PATH_ANCHOR = Path("/")
TRUST_PATH_ANCHOR_MODE = 0o755
TRUST_OWNER_UID = 0
TRUST_OWNER_GID = 0
SIGNATURE_VERIFIER = Path("/usr/bin/ssh-keygen")
SIGNATURE_VERIFIER_ENV = {"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"}
PACKAGE_MEMBERS = (
    "native_status_policy.py",
    "engineering_recovery_observer.py",
    "local_observation_v2.py",
    "observer_parent_seam.py",
    "observer_proposal_v2.py",
    "observer_native_v2.py",
    "observer_private_entry.py",
)
RUNTIME_MEMBERS = (
    "cron/executions.py",
    "cron/jobs.py",
    "cron/scheduler.py",
    "cron/scheduler_script.py",
    "cron/scheduler_local_observation.py",
    "cron/local_observation_install.py",
)
JOB_BINDING_FIELDS = (
    "enabled",
    "schedule",
    "script",
    "workdir",
)


class InstallRefused(RuntimeError):
    pass


class CurrentnessError(InstallRefused):
    pass


class MixedVersionError(InstallRefused):
    pass


class ForwardRepairRequired(InstallRefused):
    pass


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _strict_json(raw: bytes):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise InstallRefused("duplicate JSON member")
            value[key] = item
        return value

    return json.loads(raw, object_pairs_hook=unique)


def _regular_bytes(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file() or not stat.S_ISREG(path.lstat().st_mode):
        raise InstallRefused(f"regular file required: {path}")
    return path.read_bytes()


def _installer_can_write(path: Path) -> bool:
    try:
        return os.access(path, os.W_OK, effective_ids=True)
    except (NotImplementedError, TypeError):
        return True


def _trusted_node(
    path: Path,
    *,
    directory: bool,
    expected_mode: int | None = None,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> os.stat_result:
    try:
        current = path.lstat()
    except OSError as exc:
        raise InstallRefused(f"release approval trust path unavailable: {path}") from exc
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    required_mode = expected_mode if expected_mode is not None else (0o755 if directory else 0o644)
    required_uid = TRUST_OWNER_UID if owner_uid is None else owner_uid
    required_gid = TRUST_OWNER_GID if owner_gid is None else owner_gid
    if (
        stat.S_ISLNK(current.st_mode)
        or not expected_type(current.st_mode)
        or current.st_uid != required_uid
        or current.st_gid != required_gid
        or stat.S_IMODE(current.st_mode) != required_mode
        or (not directory and current.st_nlink != 1)
        or _installer_can_write(path)
    ):
        raise InstallRefused(f"release approval trust path is not protected: {path}")
    return current


def _trusted_chain(
    path: Path,
    *,
    anchor: Path,
    file_mode: int,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
    anchor_mode: int = 0o755,
) -> os.stat_result:
    anchor = anchor.absolute()
    path = path.absolute()
    try:
        relative = path.relative_to(anchor)
    except ValueError as exc:
        raise InstallRefused("release approval path escapes fixed root") from exc
    current = anchor
    _trusted_node(
        current,
        directory=True,
        expected_mode=anchor_mode,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )
    for part in relative.parts[:-1]:
        current /= part
        _trusted_node(current, directory=True, owner_uid=owner_uid, owner_gid=owner_gid)
    return _trusted_node(
        path,
        directory=False,
        expected_mode=file_mode,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
    )


def _trusted_file(path: Path, root: Path) -> bytes:
    root = root.absolute()
    path = path.absolute()
    if not path.is_relative_to(root):
        raise InstallRefused("release approval path escapes fixed root")
    before = _trusted_chain(
        path,
        anchor=TRUST_PATH_ANCHOR,
        anchor_mode=TRUST_PATH_ANCHOR_MODE,
        file_mode=0o644,
    )
    raw = path.read_bytes()
    after = _trusted_chain(
        path,
        anchor=TRUST_PATH_ANCHOR,
        anchor_mode=TRUST_PATH_ANCHOR_MODE,
        file_mode=0o644,
    )
    identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)
    if identity(before) != identity(after):
        raise InstallRefused(f"release approval trust path changed during read: {path}")
    return raw


def _trusted_signature_verifier() -> None:
    _trusted_chain(
        SIGNATURE_VERIFIER,
        anchor=Path("/"),
        file_mode=0o755,
        owner_uid=0,
        owner_gid=0,
    )


def _key_fingerprint(key_blob: str) -> str:
    try:
        decoded = base64.b64decode(key_blob.encode("ascii"), validate=True)
    except (UnicodeError, ValueError) as exc:
        raise InstallRefused("release approval public key is malformed") from exc
    digest = base64.b64encode(hashlib.sha256(decoded).digest()).decode("ascii").rstrip("=")
    return f"SHA256:{digest}"


def _hash_map(value, members: tuple[str, ...], label: str) -> dict[str, str]:
    if (
        not isinstance(value, dict)
        or set(value) != set(members)
        or any(
            not isinstance(item, str)
            or len(item) != 64
            or any(ch not in "0123456789abcdef" for ch in item)
            for item in value.values()
        )
    ):
        raise InstallRefused(f"release approval {label} hashes differ")
    return value


def _verified_approval(commit: str, package: dict[str, bytes], runtime: dict[str, bytes]) -> dict:
    allowed_path = TRUST_ROOT / "allowed_signers"
    policy_path = TRUST_ROOT / "policy.json"
    allowed_raw = _trusted_file(allowed_path, TRUST_ROOT)
    policy_raw = _trusted_file(policy_path, TRUST_ROOT)
    try:
        allowed_text = allowed_raw.decode("ascii")
    except UnicodeError as exc:
        raise InstallRefused("release approval allowed signers is malformed") from exc
    lines = [line.split() for line in allowed_text.splitlines() if line.strip()]
    if (
        len(lines) != 1
        or len(lines[0]) != 3
        or lines[0][0] != APPROVAL_PRINCIPAL
        or lines[0][1] != "ssh-ed25519"
    ):
        raise InstallRefused("release approval allowed signers differs")
    fingerprint = _key_fingerprint(lines[0][2])
    policy = _strict_json(policy_raw)
    expected_policy = {
        "schema": 1,
        "repository": SOURCE_REPOSITORY,
        "profile": PROFILE_NAME,
        "job": JOB_ID,
        "namespace": APPROVAL_NAMESPACE,
        "principal": APPROVAL_PRINCIPAL,
        "key_fingerprint": fingerprint,
    }
    if policy != expected_policy:
        raise InstallRefused("release approval policy differs")

    approval_dir = APPROVAL_ROOT / commit
    approval_path = approval_dir / "approval.json"
    signature_path = approval_dir / "approval.json.sig"
    approval_raw = _trusted_file(approval_path, APPROVAL_ROOT)
    signature_raw = _trusted_file(signature_path, APPROVAL_ROOT)
    _trusted_signature_verifier()
    try:
        verified = subprocess.run(
            [
                str(SIGNATURE_VERIFIER), "-Y", "verify", "-f", str(allowed_path),
                "-I", APPROVAL_PRINCIPAL, "-n", APPROVAL_NAMESPACE,
                "-s", str(signature_path),
            ],
            input=approval_raw,
            capture_output=True,
            env=SIGNATURE_VERIFIER_ENV,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise InstallRefused("release approval signature verifier unavailable") from exc
    if verified.returncode != 0:
        raise InstallRefused("release approval signature differs")
    _trusted_signature_verifier()
    for trusted_path, trusted_root, expected in (
        (allowed_path, TRUST_ROOT, allowed_raw),
        (policy_path, TRUST_ROOT, policy_raw),
        (approval_path, APPROVAL_ROOT, approval_raw),
        (signature_path, APPROVAL_ROOT, signature_raw),
    ):
        if _trusted_file(trusted_path, trusted_root) != expected:
            raise InstallRefused("release approval trust material changed during verification")
    approval = _strict_json(approval_raw)
    expected_fields = {
        "approved_at", "commit", "job", "package_hashes", "pr", "profile", "repository",
        "review_receipt_sha256", "review_verdict", "runtime_hashes", "schema",
    }
    if (
        not isinstance(approval, dict)
        or set(approval) != expected_fields
        or json.dumps(approval, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        + b"\n" != approval_raw
        or approval.get("schema") != 1
        or approval.get("repository") != SOURCE_REPOSITORY
        or approval.get("profile") != PROFILE_NAME
        or approval.get("job") != JOB_ID
        or approval.get("pr") != 7
        or approval.get("commit") != commit
        or approval.get("review_verdict") not in {"PASS", "PASS WITH ADVISORIES"}
        or not isinstance(approval.get("approved_at"), str)
        or not approval["approved_at"].endswith("Z")
        or not isinstance(approval.get("review_receipt_sha256"), str)
        or len(approval["review_receipt_sha256"]) != 64
        or any(ch not in "0123456789abcdef" for ch in approval["review_receipt_sha256"])
    ):
        raise InstallRefused("release approval payload differs")
    package_hashes = _hash_map(approval["package_hashes"], PACKAGE_MEMBERS, "package")
    runtime_hashes = _hash_map(approval["runtime_hashes"], RUNTIME_MEMBERS, "runtime")
    if package_hashes != {name: _sha(raw) for name, raw in package.items()}:
        raise InstallRefused("release approval package hashes differ")
    if runtime_hashes != {name: _sha(raw) for name, raw in runtime.items()}:
        raise InstallRefused("release approval runtime hashes differ")
    return {
        "approval_sha256": _sha(approval_raw),
        "signature_sha256": _sha(signature_raw),
        "commit": commit,
        "key_fingerprint": fingerprint,
        "principal": APPROVAL_PRINCIPAL,
        "namespace": APPROVAL_NAMESPACE,
    }


def _inside(path: Path, root: Path) -> Path:
    path = path.absolute()
    root = root.absolute()
    if not path.is_relative_to(root):
        raise InstallRefused(f"path escapes owned root: {path}")
    current = path
    while current != root.parent:
        if current.exists() or current.is_symlink():
            if current.is_symlink():
                raise InstallRefused(f"symlinked install path: {current}")
        if current == root:
            break
        current = current.parent
    return path


def _atomic(path: Path, raw: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".wri-2276-", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        if path.read_bytes() != raw:
            raise InstallRefused(f"write readback differs: {path}")
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def manifest_path(profile_home: Path) -> Path:
    return Path(profile_home) / "cron" / f"recovery-{JOB_ID}" / "install-manifest.json"


def unknown_path(profile_home: Path) -> Path:
    return Path(profile_home) / "cron" / f"recovery-{JOB_ID}" / "install-unknown.json"


def _store(path: Path):
    raw = _regular_bytes(path)
    value = _strict_json(raw)
    if (
        not isinstance(value, dict)
        or not set(value).issubset({"jobs", "updated_at"})
        or "jobs" not in value
        or not isinstance(value["jobs"], list)
    ):
        raise InstallRefused("unexpected jobs store shape")
    matches = [item for item in value["jobs"] if isinstance(item, dict) and item.get("id") == JOB_ID]
    if len(matches) != 1:
        raise InstallRefused("target job is not unique")
    return value, matches[0], raw


def _paused(job: dict) -> None:
    if job.get("enabled") is not False:
        raise InstallRefused("target job must remain disabled")
    for field in ("fire_claim", "run_claim", "pending_slot", "manual_run_at"):
        if job.get(field) is not None:
            raise InstallRefused(f"target {field} is active")
    schedule = job.get("schedule")
    if schedule != {"kind": "interval", "minutes": 5}:
        raise InstallRefused("target schedule differs")


def _no_active_execution(path: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise InstallRefused("execution database unavailable")
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
        row = db.execute(
            "SELECT 1 FROM executions WHERE job_id=? AND status IN ('claimed','running') LIMIT 1",
            (JOB_ID,),
        ).fetchone()
    if row:
        raise InstallRefused("target execution is pending or active")


def _candidate(source_root: Path):
    package_root = source_root / "scripts" / "observer_v2"
    package = {name: _regular_bytes(package_root / name) for name in PACKAGE_MEMBERS}
    if {path.name for path in package_root.glob("*.py")} != set(PACKAGE_MEMBERS):
        raise InstallRefused("private package set is incomplete or has foreign members")
    runtime = {name: _regular_bytes(source_root / name) for name in RUNTIME_MEMBERS}
    return package, runtime


def _git(source_root: Path, *args: str) -> bytes:
    try:
        return subprocess.run(
            ["git", "-C", str(source_root), *args],
            check=True,
            capture_output=True,
            timeout=20,
        ).stdout
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise InstallRefused("source Git identity unavailable") from exc


def _repository_identity(remote: str) -> str:
    value = remote.strip()
    prefixes = (
        "https://github.com/",
        "ssh://git@github.com/",
        "git@github.com:",
    )
    prefix = next((item for item in prefixes if value.startswith(item)), None)
    if prefix is None:
        raise InstallRefused("source repository differs")
    return value.removeprefix(prefix).removesuffix(".git")


def _source_identity(source_root: Path, corrected_commit: str, package, runtime):
    top = Path(_git(source_root, "rev-parse", "--show-toplevel").decode().strip()).resolve()
    if top != source_root.resolve():
        raise InstallRefused("source root differs from Git worktree")
    commit = _git(source_root, "rev-parse", "HEAD").decode().strip()
    if commit != corrected_commit:
        raise InstallRefused("source commit differs from reviewed commit")
    repository = _repository_identity(
        _git(source_root, "config", "--get", "remote.origin.url").decode()
    )
    if repository != SOURCE_REPOSITORY:
        raise InstallRefused("source repository differs")
    candidates = {
        **{f"scripts/observer_v2/{name}": raw for name, raw in package.items()},
        **runtime,
    }
    for relative, raw in candidates.items():
        if _git(source_root, "show", f"{commit}:{relative}") != raw:
            raise InstallRefused(f"source bytes differ from reviewed commit: {relative}")
    binding = {
        "repository": repository,
        "commit": commit,
        "package_hashes": {name: _sha(raw) for name, raw in package.items()},
        "runtime_hashes": {name: _sha(raw) for name, raw in runtime.items()},
    }
    digest = _sha(json.dumps(binding, sort_keys=True, separators=(",", ":")).encode())
    return {"repository": repository, "commit": commit, "digest": digest}


def _job_binding(job: dict) -> dict:
    prompt = job.get("prompt")
    if not isinstance(prompt, str):
        raise InstallRefused("target prompt must be text")
    return {
        "fields": {field: job.get(field) for field in JOB_BINDING_FIELDS},
        "prompt_sha256": _sha(prompt.encode("utf-8")),
    }


def _backup(paths: list[Path], backup_root: Path, roots: tuple[Path, ...]):
    records = {}
    for path in paths:
        owner = next((root for root in roots if path.is_relative_to(root)), None)
        if owner is None:
            raise InstallRefused(f"backup path is not owned: {path}")
        key = f"{roots.index(owner)}:{path.relative_to(owner).as_posix()}"
        if path.exists() or path.is_symlink():
            raw = _regular_bytes(path)
            destination = backup_root / str(roots.index(owner)) / path.relative_to(owner)
            _atomic(destination, raw, stat.S_IMODE(path.stat().st_mode))
            records[key] = {"sha256": _sha(raw), "backup": str(destination.relative_to(backup_root))}
        else:
            records[key] = {"sha256": None, "backup": None}
    return records


def _load_and_migrate(package_root: Path, recovery: Path, profile_home: Path) -> None:
    names = [name.removesuffix(".py") for name in PACKAGE_MEMBERS]
    if any(name in sys.modules for name in names):
        raise InstallRefused("private package import cache is occupied")
    sys.path.insert(0, str(package_root))
    try:
        legacy = importlib.import_module("engineering_recovery_observer")
        legacy.PROFILE_HOME = profile_home  # type: ignore[attr-defined]
        v2 = importlib.import_module("local_observation_v2")
        v2.migrate(recovery)
    finally:
        sys.path.remove(str(package_root))
        for name in names:
            sys.modules.pop(name, None)


def _write_unknown(profile_home: Path, transaction: str, reason: str, lost: list[str]) -> None:
    payload = {"version": 1, "state": "UNKNOWN", "job": JOB_ID, "profile": PROFILE_NAME,
               "transaction": transaction, "reason": reason, "lost_compare_and_swap": lost}
    _atomic(unknown_path(profile_home), json.dumps(payload, sort_keys=True).encode() + b"\n")


def _restore(changes, profile_home: Path, transaction: str, cause: BaseException) -> None:
    lost = []
    for path, old, candidate, mode in reversed(changes):
        current = path.read_bytes() if path.is_file() and not path.is_symlink() else None
        if current == candidate:
            if old is None:
                path.unlink()
            else:
                _atomic(path, old, mode)
        elif current != old:
            lost.append(str(path))
    if lost:
        _write_unknown(profile_home, transaction, str(cause), lost)
        raise MixedVersionError("mixed generation; compare-and-swap rollback ownership lost") from cause
    pycache = profile_home / "scripts" / "observer_v2" / "__pycache__"
    if pycache.is_dir() and not pycache.is_symlink():
        shutil.rmtree(pycache)
    for directory in (profile_home / "scripts" / "observer_v2", profile_home / "scripts"):
        try:
            directory.rmdir()
        except OSError:
            pass


def install_paused(*, source_root: Path, runtime_root: Path, profile_home: Path,
                   jobs_path: Path, executions_path: Path, corrected_commit: str,
                   candidate_prompt: str, dry_run: bool = False, fail_after: str | None = None,
                   phase_hook=None):
    arguments = {
        "source_root": source_root,
        "runtime_root": runtime_root,
        "profile_home": profile_home,
        "jobs_path": jobs_path,
        "executions_path": executions_path,
        "corrected_commit": corrected_commit,
        "candidate_prompt": candidate_prompt,
        "dry_run": dry_run,
        "fail_after": fail_after,
        "phase_hook": phase_hook,
    }
    if dry_run:
        return _install_paused_verified(**arguments)
    from cron import jobs

    profile_home = Path(profile_home)
    if Path(jobs_path) != profile_home / "cron" / "jobs.json":
        raise InstallRefused("jobs store is not the fixed profile store")
    if Path(executions_path) != profile_home / "cron" / "executions.db":
        raise InstallRefused("execution ledger is not the fixed profile store")
    with jobs.use_cron_store(profile_home):
        with jobs._fire_job_lock(JOB_ID) as acquired:
            if not acquired:
                raise InstallRefused("target execution ownership fence unavailable")
            return _install_paused_verified(**arguments)


def _install_paused_verified(*, source_root: Path, runtime_root: Path, profile_home: Path,
                             jobs_path: Path, executions_path: Path, corrected_commit: str,
                             candidate_prompt: str, dry_run: bool = False,
                             fail_after: str | None = None, phase_hook=None):
    source_root, runtime_root, profile_home = map(Path, (source_root, runtime_root, profile_home))
    jobs_path, executions_path = Path(jobs_path), Path(executions_path)
    if profile_home.name != PROFILE_NAME:
        raise InstallRefused("foreign profile refused")
    if len(corrected_commit) != 40 or any(ch not in "0123456789abcdef" for ch in corrected_commit):
        raise InstallRefused("corrected commit must be a full lowercase SHA")
    for path, root in ((runtime_root, runtime_root), (profile_home, profile_home),
                       (jobs_path, profile_home), (executions_path, profile_home)):
        _inside(path, root)
    _value, job, jobs_raw = _store(jobs_path)
    _paused(job)
    _no_active_execution(executions_path)
    package, runtime = _candidate(source_root)
    source_identity = _source_identity(source_root, corrected_commit, package, runtime)
    approval = _verified_approval(corrected_commit, package, runtime)
    receipt = {"mode": "dry-run" if dry_run else "install", "job": JOB_ID, "profile": PROFILE_NAME,
               "commit": corrected_commit, "source_identity": source_identity,
               "approval": approval,
               "package_hashes": {name: _sha(raw) for name, raw in package.items()},
               "runtime_hashes": {name: _sha(raw) for name, raw in runtime.items()}}
    if dry_run:
        return receipt

    transaction = uuid.uuid4().hex
    recovery = profile_home / "cron" / f"recovery-{JOB_ID}"
    package_root = profile_home / "scripts" / "observer_v2"
    manifest = manifest_path(profile_home)
    v2_path = recovery / "local-observation-v2.sqlite"
    v1_backup = recovery / "local-observation-v1.backup"
    v1_path = recovery / "local-observation-v1.json"
    v1_source = _regular_bytes(v1_path)
    targets = [*(runtime_root / name for name in RUNTIME_MEMBERS),
               *(package_root / name for name in PACKAGE_MEMBERS), v1_path, v2_path,
               v1_backup, jobs_path, manifest]
    for path in targets:
        _inside(path, runtime_root if path.is_relative_to(runtime_root) else profile_home)
    backup_root = recovery / "install-backups" / transaction
    prestate = _backup(targets, backup_root, (runtime_root, profile_home))
    _atomic(backup_root / "prestate.json", json.dumps(prestate, sort_keys=True).encode() + b"\n")
    changes = []
    committed = False

    def stage(path: Path, raw: bytes, mode=0o600):
        old = path.read_bytes() if path.is_file() and not path.is_symlink() else None
        changes.append((path, old, raw, mode))
        _atomic(path, raw, mode)

    def phase(name: str):
        if phase_hook is not None:
            phase_hook(name, {"transaction": transaction, "profile_home": profile_home})
        if fail_after == name:
            raise RuntimeError(f"injected failure after {name}")

    def require_migration_source():
        if _regular_bytes(v1_path) != v1_source:
            raise InstallRefused("migration source drifted")

    def require_commit_fence(expected_jobs: bytes):
        _current, current_job, current_raw = _store(jobs_path)
        if current_raw != expected_jobs:
            raise InstallRefused("jobs candidate drifted")
        _paused(current_job)
        _no_active_execution(executions_path)
        require_migration_source()

    try:
        for name, raw in runtime.items():
            stage(runtime_root / name, raw, 0o644)
        phase("runtime")
        require_migration_source()
        for name, raw in package.items():
            stage(package_root / name, raw, 0o600)
        phase("package")
        require_migration_source()
        _load_and_migrate(package_root, recovery, profile_home)
        if _regular_bytes(v1_backup) != v1_source:
            raise InstallRefused("migration backup differs from captured source")
        changes.extend([(v1_backup, None, v1_backup.read_bytes(), 0o600),
                        (v2_path, None, v2_path.read_bytes(), 0o600)])
        phase("migration")
        require_migration_source()
        current, current_job, current_raw = _store(jobs_path)
        if current_raw != jobs_raw:
            raise InstallRefused("jobs prestate drifted")
        _paused(current_job)
        current_job["script"] = "observer_v2/observer_private_entry.py"
        current_job["workdir"] = str(profile_home)
        current_job["prompt"] = candidate_prompt
        candidate_jobs = json.dumps(current, ensure_ascii=False, indent=2).encode() + b"\n"
        stage(jobs_path, candidate_jobs)
        phase("job")
        require_commit_fence(candidate_jobs)
        migration_inputs = {
            name: _sha(_regular_bytes(recovery / name))
            for name in ("local-observation-v1.json", "local-observation-v1.backup")
        }
        payload = {
            "version": 1, "state": "staged", "transaction": transaction,
            "commit": corrected_commit, "profile": PROFILE_NAME, "job": JOB_ID,
            "source_identity": source_identity, "approval": approval,
            "job_binding": _job_binding(current_job),
            "schema_generation": SCHEMA_GENERATION,
            "runtime_hashes": receipt["runtime_hashes"], "package_hashes": receipt["package_hashes"],
            "migration_inputs": migration_inputs, "prestate": prestate,
            "backup_root": str(backup_root.relative_to(profile_home)),
        }
        staged = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        stage(manifest, staged)
        phase("manifest")
        require_commit_fence(candidate_jobs)
        payload["state"] = "committed"
        committed_raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        _atomic(manifest, committed_raw)
        changes[-1] = (manifest, changes[-1][1], committed_raw, 0o600)
        committed = True
        validate_runtime_manifest(profile_home, runtime_root)
        if fail_after == "commit":
            raise RuntimeError("injected failure after commit")
        return {**receipt, "state": "committed", "transaction": transaction,
                "manifest_sha256": _sha(committed_raw)}
    except BaseException as cause:
        if committed:
            raise ForwardRepairRequired("committed V2 generation requires forward repair") from cause
        _restore(changes, profile_home, transaction, cause)
        raise


def _validate_installed_approval(value: dict, profile_home: Path, runtime_root: Path) -> None:
    runtime_bytes = {name: _regular_bytes(runtime_root / name) for name in RUNTIME_MEMBERS}
    package_root = profile_home / "scripts" / "observer_v2"
    if {path.name for path in package_root.glob("*.py")} != set(PACKAGE_MEMBERS):
        raise MixedVersionError("private package member set is mixed")
    package_bytes = {name: _regular_bytes(package_root / name) for name in PACKAGE_MEMBERS}
    if (
        value.get("runtime_hashes") != {name: _sha(raw) for name, raw in runtime_bytes.items()}
        or value.get("package_hashes") != {name: _sha(raw) for name, raw in package_bytes.items()}
    ):
        raise MixedVersionError("runtime or package generation is mixed")
    try:
        approval = _verified_approval(str(value.get("commit")), package_bytes, runtime_bytes)
    except InstallRefused as exc:
        raise CurrentnessError("release approval differs") from exc
    if value.get("approval") != approval:
        raise CurrentnessError("manifest release approval differs")


def validate_runtime_manifest(profile_home: Path, runtime_root: Path):
    profile_home, runtime_root = Path(profile_home), Path(runtime_root)
    if profile_home.name != PROFILE_NAME or profile_home.is_symlink() or runtime_root.is_symlink():
        raise CurrentnessError("foreign profile refused")
    path = manifest_path(profile_home)
    if not path.is_file() or path.is_symlink():
        raise CurrentnessError("currentness manifest absent")
    value = _strict_json(path.read_bytes())
    if (value.get("version") != 1 or value.get("state") != "committed"
            or value.get("profile") != PROFILE_NAME or value.get("job") != JOB_ID
            or value.get("schema_generation") != SCHEMA_GENERATION):
        raise CurrentnessError("currentness manifest is stale or foreign")
    source_identity = value.get("source_identity")
    source_binding = {
        "repository": SOURCE_REPOSITORY,
        "commit": value.get("commit"),
        "package_hashes": value.get("package_hashes"),
        "runtime_hashes": value.get("runtime_hashes"),
    }
    source_digest = _sha(
        json.dumps(source_binding, sort_keys=True, separators=(",", ":")).encode()
    )
    if source_identity != {
        "repository": SOURCE_REPOSITORY,
        "commit": value.get("commit"),
        "digest": source_digest,
    }:
        raise CurrentnessError("manifest source identity differs")
    _jobs, current_job, _jobs_raw = _store(profile_home / "cron" / "jobs.json")
    manifest_job = value.get("job_binding")
    current_binding = _job_binding(current_job)
    if (
        not isinstance(manifest_job, dict)
        or set(manifest_job.get("fields", {})) != set(JOB_BINDING_FIELDS)
        or current_binding != manifest_job
    ):
        _write_unknown(
            profile_home,
            str(value.get("transaction")),
            "runtime job generation is mixed",
            [str(profile_home / "cron" / "jobs.json")],
        )
        raise MixedVersionError("runtime job generation is mixed")
    _validate_installed_approval(value, profile_home, runtime_root)
    backup_root = _inside(profile_home / value["backup_root"], profile_home)
    prestate_raw = _regular_bytes(backup_root / "prestate.json")
    if _strict_json(prestate_raw) != value.get("prestate"):
        raise CurrentnessError("manifest prestate binding differs")
    for record in value["prestate"].values():
        expected = record.get("sha256")
        relative = record.get("backup")
        if expected is None:
            if relative is not None:
                raise CurrentnessError("absent prestate has a backup path")
            continue
        if not isinstance(relative, str):
            raise CurrentnessError("prestate backup path absent")
        if _sha(_regular_bytes(_inside(backup_root / relative, backup_root))) != expected:
            raise CurrentnessError("prestate backup hash differs")
    recovery = profile_home / "cron" / f"recovery-{JOB_ID}"
    for name, expected in value.get("migration_inputs", {}).items():
        if _sha(_regular_bytes(recovery / name)) != expected:
            raise MixedVersionError("migration input generation differs")
    with sqlite3.connect(f"file:{recovery / 'local-observation-v2.sqlite'}?mode=ro", uri=True) as db:
        row = db.execute("SELECT payload FROM metadata WHERE id=1").fetchone()
    metadata = _strict_json(row[0] if isinstance(row[0], bytes) else row[0].encode()) if row else None
    source_sha = _sha(_regular_bytes(recovery / "local-observation-v1.json"))
    backup_sha = _sha(_regular_bytes(recovery / "local-observation-v1.backup"))
    if (
        source_sha != backup_sha
        or value.get("migration_inputs")
        != {
            "local-observation-v1.json": source_sha,
            "local-observation-v1.backup": source_sha,
        }
    ):
        raise MixedVersionError("migration source and backup generation differ")
    if (not metadata or metadata.get("schema") != SCHEMA_GENERATION
            or metadata.get("profile") != str(profile_home)
            or metadata.get("source_v1_sha") != source_sha):
        raise MixedVersionError("V2 schema or profile generation differs")
    return value
