"""WRI-2276 corrected paused installer and manifest-bound admission."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import threading
from pathlib import Path

import pytest

from cron import local_observation_install as release


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


@pytest.fixture
def install_fixture(tmp_path, monkeypatch):
    tmp_path.chmod(0o755)
    repository = Path(__file__).resolve().parents[2]
    source = tmp_path / "source"
    for relative in release.RUNTIME_MEMBERS:
        destination = source / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repository / relative, destination)
    for name in release.PACKAGE_MEMBERS:
        destination = source / "scripts" / "observer_v2" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repository / "scripts" / "observer_v2" / name, destination)
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    subprocess.run(["git", "-C", str(source), "add", "."], check=True)
    subprocess.run(
        [
            "git", "-C", str(source), "-c", "user.name=Test",
            "-c", "user.email=test@example.invalid", "commit", "-qm", "candidate",
        ],
        check=True,
    )
    subprocess.run(
        [
            "git", "-C", str(source), "remote", "add", "origin",
            "https://github.com/writerflow/hermes-kanban.git",
        ],
        check=True,
    )
    commit = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    trust_root = tmp_path / "fixed-trust" / "wri-2276"
    approval_root = tmp_path / "fixed-approvals" / "wri-2276"
    trust_root.mkdir(parents=True, mode=0o755)
    (approval_root / commit).mkdir(parents=True, mode=0o755)
    key = tmp_path / "release-approval-key"
    subprocess.run(
        ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)],
        check=True,
    )
    public_key = key.with_suffix(".pub").read_text(encoding="ascii").split()
    allowed_signers = trust_root / "allowed_signers"
    allowed_signers.write_text(
        f"{release.APPROVAL_PRINCIPAL} {public_key[0]} {public_key[1]}\n",
        encoding="ascii",
    )
    fingerprint = subprocess.run(
        ["ssh-keygen", "-lf", str(key.with_suffix(".pub")), "-E", "sha256"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()[1]
    policy = {
        "schema": 1,
        "repository": release.SOURCE_REPOSITORY,
        "profile": release.PROFILE_NAME,
        "job": release.JOB_ID,
        "namespace": release.APPROVAL_NAMESPACE,
        "principal": release.APPROVAL_PRINCIPAL,
        "key_fingerprint": fingerprint,
    }
    (trust_root / "policy.json").write_text(
        json.dumps(policy, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    package_hashes = {
        name: _sha((source / "scripts" / "observer_v2" / name).read_bytes())
        for name in release.PACKAGE_MEMBERS
    }
    runtime_hashes = {
        name: _sha((source / name).read_bytes()) for name in release.RUNTIME_MEMBERS
    }
    approval = {
        "approved_at": "2026-11-11T00:00:00Z",
        "commit": commit,
        "job": release.JOB_ID,
        "package_hashes": package_hashes,
        "pr": 7,
        "profile": release.PROFILE_NAME,
        "repository": release.SOURCE_REPOSITORY,
        "review_receipt_sha256": "a" * 64,
        "review_verdict": "PASS",
        "runtime_hashes": runtime_hashes,
        "schema": 1,
    }
    approval_path = approval_root / commit / "approval.json"
    approval_path.write_text(
        json.dumps(approval, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    subprocess.run(
        [
            "ssh-keygen", "-Y", "sign", "-q", "-f", str(key),
            "-n", release.APPROVAL_NAMESPACE, str(approval_path),
        ],
        check=True,
    )
    for directory in (
        trust_root.parent,
        trust_root,
        approval_root.parent,
        approval_root,
        approval_root / commit,
    ):
        directory.chmod(0o755)
    for path in (
        allowed_signers,
        trust_root / "policy.json",
        approval_path,
        approval_path.with_suffix(".json.sig"),
    ):
        path.chmod(0o644)
    monkeypatch.setattr(release, "TRUST_ROOT", trust_root)
    monkeypatch.setattr(release, "APPROVAL_ROOT", approval_root)
    monkeypatch.setattr(release, "TRUST_PATH_ANCHOR", tmp_path)
    monkeypatch.setattr(release, "TRUST_OWNER_UID", os.getuid())
    monkeypatch.setattr(release, "TRUST_OWNER_GID", os.getgid())
    monkeypatch.setattr(release, "_installer_can_write", lambda _path: False)
    runtime = tmp_path / "runtime"
    profile = tmp_path / "profiles" / "rowan"
    recovery = profile / "cron" / f"recovery-{release.JOB_ID}"
    recovery.mkdir(parents=True, mode=0o700)
    runtime.mkdir()
    for relative in release.RUNTIME_MEMBERS:
        target = runtime / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"old:" + relative.encode())
    jobs = profile / "cron" / "jobs.json"
    jobs.parent.mkdir(parents=True, exist_ok=True)
    other = {"id": "other", "last_status": "keep"}
    target = {
        "id": release.JOB_ID,
        "enabled": False,
        "schedule": {"kind": "interval", "minutes": 5},
        "script": "engineering_recovery_observer.py",
        "workdir": "old-workdir",
        "prompt": "old prompt",
        "fire_claim": None,
        "run_claim": None,
        "pending_slot": None,
        "manual_run_at": None,
    }
    jobs.write_text(json.dumps({"jobs": [target, other]}), encoding="utf-8")
    executions = profile / "cron" / "executions.db"
    with sqlite3.connect(executions) as db:
        db.execute("CREATE TABLE executions(job_id TEXT, status TEXT)")
    v1 = {
        "version": 1,
        "board_id": "default",
        "job_id": release.JOB_ID,
        "ledger_sha": "0" * 64,
        "admission_generation": 0,
        "admission_state": "invalidating",
        "pending": {},
        "roots": {},
        "current_issued": {},
    }
    (recovery / "local-observation-v1.json").write_text(
        json.dumps(v1, sort_keys=True, separators=(",", ":")), encoding="ascii"
    )
    return source, runtime, profile, jobs, executions, other


def _install(fixture, **kwargs):
    source, runtime, profile, jobs, executions, _other = fixture
    source = Path(kwargs.pop("source_root", source))
    corrected_commit = kwargs.pop(
        "corrected_commit",
        subprocess.run(
            ["git", "-C", str(source), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
    )
    return release.install_paused(
        source_root=source,
        runtime_root=runtime,
        profile_home=profile,
        jobs_path=jobs,
        executions_path=executions,
        corrected_commit=corrected_commit,
        candidate_prompt="corrected prompt",
        **kwargs,
    )


def _write_target_job(jobs: Path, mutate) -> None:
    value = json.loads(jobs.read_text(encoding="utf-8"))
    mutate(next(item for item in value["jobs"] if item["id"] == release.JOB_ID))
    jobs.write_text(json.dumps(value), encoding="utf-8")


def test_dry_run_reads_complete_transaction_without_writes(install_fixture):
    _source, runtime, profile, jobs, _executions, _other = install_fixture
    before = {path: path.read_bytes() for path in [jobs, *(runtime / p for p in release.RUNTIME_MEMBERS)]}
    receipt = _install(install_fixture, dry_run=True)
    assert receipt["mode"] == "dry-run"
    assert set(receipt["package_hashes"]) == set(release.PACKAGE_MEMBERS)
    assert set(receipt["runtime_hashes"]) == set(release.RUNTIME_MEMBERS)
    assert not release.manifest_path(profile).exists()
    assert before == {path: path.read_bytes() for path in before}


def test_source_commit_and_repository_are_derived_not_trusted_from_caller(
    install_fixture, tmp_path
):
    source, _runtime, _profile, _jobs, _executions, _other = install_fixture
    with pytest.raises(release.InstallRefused, match="source commit differs"):
        _install(install_fixture, corrected_commit="b" * 40, dry_run=True)
    committed = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    changed = source / release.RUNTIME_MEMBERS[0]
    changed.write_bytes(changed.read_bytes() + b"\n# foreign bytes\n")
    with pytest.raises(release.InstallRefused, match="source bytes differ"):
        _install(install_fixture, corrected_commit=committed, dry_run=True)

    foreign = tmp_path / "foreign-source"
    for relative in release.RUNTIME_MEMBERS:
        destination = foreign / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / relative, destination)
    for name in release.PACKAGE_MEMBERS:
        destination = foreign / "scripts" / "observer_v2" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / "scripts" / "observer_v2" / name, destination)
    subprocess.run(["git", "init", "-q", str(foreign)], check=True)
    subprocess.run(["git", "-C", str(foreign), "add", "."], check=True)
    subprocess.run(
        [
            "git", "-C", str(foreign), "-c", "user.name=Test",
            "-c", "user.email=test@example.invalid", "commit", "-qm", "foreign",
        ],
        check=True,
    )
    subprocess.run(
        [
            "git", "-C", str(foreign), "remote", "add", "origin",
            "https://github.com/foreign/hermes-kanban.git",
        ],
        check=True,
    )
    foreign_commit = subprocess.run(
        ["git", "-C", str(foreign), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    with pytest.raises(release.InstallRefused, match="source repository differs"):
        _install(
            install_fixture,
            source_root=foreign,
            corrected_commit=foreign_commit,
            dry_run=True,
        )


def test_arbitrary_local_commit_with_expected_remote_is_refused_before_write(install_fixture):
    source, runtime, profile, jobs, _executions, _other = install_fixture
    subprocess.run(
        [
            "git", "-C", str(source), "-c", "user.name=Test",
            "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-qm",
            "unreviewed identity",
        ],
        check=True,
    )
    arbitrary = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    before = {
        path: path.read_bytes()
        for path in [jobs, *(runtime / member for member in release.RUNTIME_MEMBERS)]
    }

    with pytest.raises(release.InstallRefused, match="approval"):
        _install(install_fixture, corrected_commit=arbitrary)

    assert before == {path: path.read_bytes() for path in before}
    assert not release.manifest_path(profile).exists()


@pytest.mark.parametrize("mutation", ["signature", "approval", "policy", "writable"])
def test_release_approval_trust_drift_is_refused_before_write(install_fixture, mutation):
    source, runtime, profile, jobs, _executions, _other = install_fixture
    commit = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    approval = release.APPROVAL_ROOT / commit / "approval.json"
    if mutation == "signature":
        approval.with_suffix(".json.sig").write_bytes(b"not a signature")
    elif mutation == "approval":
        approval.write_bytes(approval.read_bytes().replace(b'"pr":7', b'"pr":8'))
    elif mutation == "policy":
        policy = release.TRUST_ROOT / "policy.json"
        policy.write_bytes(policy.read_bytes().replace(
            release.APPROVAL_NAMESPACE.encode(), b"foreign-namespace"
        ))
    else:
        approval.chmod(0o666)
    before = {
        path: path.read_bytes()
        for path in [jobs, *(runtime / member for member in release.RUNTIME_MEMBERS)]
    }

    with pytest.raises(release.InstallRefused, match="approval"):
        _install(install_fixture, dry_run=True)

    assert before == {path: path.read_bytes() for path in before}
    assert not release.manifest_path(profile).exists()


def test_path_substitution_cannot_replace_signature_verifier(
    install_fixture, tmp_path, monkeypatch
):
    source, runtime, profile, jobs, _executions, _other = install_fixture
    commit = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    signature = release.APPROVAL_ROOT / commit / "approval.json.sig"
    signature.write_bytes(b"not a signature")
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    marker = tmp_path / "substituted-verifier-ran"
    fake = fake_bin / "ssh-keygen"
    fake.write_text(f"#!/bin/sh\ntouch {marker}\nexit 0\n", encoding="utf-8")
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ.get('PATH', '')}")
    before = {
        path: path.read_bytes()
        for path in [jobs, *(runtime / member for member in release.RUNTIME_MEMBERS)]
    }

    with pytest.raises(release.InstallRefused, match="approval"):
        _install(install_fixture, dry_run=True)

    assert not marker.exists()
    assert before == {path: path.read_bytes() for path in before}
    assert not release.manifest_path(profile).exists()


@pytest.mark.parametrize("admission", ["install", "runtime"])
@pytest.mark.parametrize(
    "node",
    [
        "allowed_signers",
        "policy",
        "approval",
        "signature",
        "trust_ancestor",
        "approval_ancestor",
    ],
)
def test_effective_write_acl_drift_refuses_each_admission(
    install_fixture, monkeypatch, admission, node
):
    source, runtime, profile, _jobs, _executions, _other = install_fixture
    if admission == "runtime":
        _install(install_fixture)
    commit = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    approval = release.APPROVAL_ROOT / commit / "approval.json"
    targets = {
        "allowed_signers": release.TRUST_ROOT / "allowed_signers",
        "policy": release.TRUST_ROOT / "policy.json",
        "approval": approval,
        "signature": approval.with_suffix(".json.sig"),
        "trust_ancestor": release.TRUST_ROOT.parent,
        "approval_ancestor": release.APPROVAL_ROOT.parent,
    }
    target = targets[node]
    monkeypatch.setattr(
        release, "_installer_can_write", lambda path: Path(path) == target
    )

    with pytest.raises(
        (release.InstallRefused, release.CurrentnessError), match="approval"
    ):
        if admission == "install":
            _install(install_fixture, dry_run=True)
        else:
            release.validate_runtime_manifest(profile, runtime)


def test_paused_install_stages_package_migrates_and_preserves_unrelated_fields(install_fixture):
    _source, _runtime, profile, jobs, _executions, other = install_fixture
    receipt = _install(install_fixture)
    assert receipt["state"] == "committed"
    store = json.loads(jobs.read_text(encoding="utf-8"))
    job = next(item for item in store["jobs"] if item["id"] == release.JOB_ID)
    assert job["enabled"] is False
    assert job["script"] == "observer_v2/observer_private_entry.py"
    assert job["workdir"] == str(profile)
    assert next(item for item in store["jobs"] if item["id"] == "other") == other
    package = profile / "scripts" / "observer_v2"
    assert {path.name for path in package.glob("*.py")} == set(release.PACKAGE_MEMBERS)
    recovery = profile / "cron" / f"recovery-{release.JOB_ID}"
    assert (recovery / "local-observation-v1.backup").is_file()
    with sqlite3.connect(recovery / "local-observation-v2.sqlite") as db:
        payload = json.loads(db.execute("SELECT payload FROM metadata WHERE id=1").fetchone()[0])
    assert payload["schema"] == release.SCHEMA_GENERATION
    assert payload["profile"] == str(profile)
    release.validate_runtime_manifest(profile, _runtime)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("enabled", True),
        ("script", "engineering_recovery_observer.py"),
        ("workdir", "foreign-workdir"),
        ("schedule", {"kind": "interval", "minutes": 10}),
        ("prompt", "foreign prompt"),
    ],
)
def test_runtime_admission_rejects_each_mixed_job_or_prompt_field(
    install_fixture, field, replacement
):
    _source, runtime, profile, jobs, _executions, _other = install_fixture
    _install(install_fixture)
    _write_target_job(jobs, lambda job: job.__setitem__(field, replacement))

    with pytest.raises(release.MixedVersionError, match="job generation is mixed"):
        release.validate_runtime_manifest(profile, runtime)

    evidence = json.loads(release.unknown_path(profile).read_text(encoding="utf-8"))
    assert evidence["state"] == "UNKNOWN"
    assert evidence["job"] == release.JOB_ID


def test_runtime_admission_rejects_manifest_commit_rebinding(install_fixture):
    _source, runtime, profile, _jobs, _executions, _other = install_fixture
    _install(install_fixture)
    manifest = release.manifest_path(profile)
    value = json.loads(manifest.read_text(encoding="utf-8"))
    value["commit"] = "b" * 40
    source_binding = {
        "repository": release.SOURCE_REPOSITORY,
        "commit": value["commit"],
        "package_hashes": value["package_hashes"],
        "runtime_hashes": value["runtime_hashes"],
    }
    value["source_identity"] = {
        "repository": release.SOURCE_REPOSITORY,
        "commit": value["commit"],
        "digest": _sha(json.dumps(
            source_binding, sort_keys=True, separators=(",", ":")
        ).encode()),
    }
    manifest.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(release.CurrentnessError, match="approval"):
        release.validate_runtime_manifest(profile, runtime)


@pytest.mark.parametrize("mutation", ["enabled", "fire_claim", "run_claim", "pending_slot", "manual_run_at", "active"])
def test_nonpaused_or_active_state_refuses_without_write(install_fixture, mutation):
    _source, runtime, _profile, jobs, executions, _other = install_fixture
    if mutation == "active":
        with sqlite3.connect(executions) as db:
            db.execute("INSERT INTO executions VALUES(?, 'running')", (release.JOB_ID,))
    else:
        data = json.loads(jobs.read_text(encoding="utf-8"))
        job = data["jobs"][0]
        job[mutation] = True if mutation == "enabled" else {"by": "other"}
        jobs.write_text(json.dumps(data), encoding="utf-8")
    before = {path: path.read_bytes() for path in [jobs, *(runtime / p for p in release.RUNTIME_MEMBERS)]}
    with pytest.raises(release.InstallRefused):
        _install(install_fixture)
    assert before == {path: path.read_bytes() for path in before}


@pytest.mark.parametrize("phase", ["runtime", "package", "migration", "job", "manifest"])
def test_each_precommit_failure_rolls_back_candidate_owned_bytes(install_fixture, phase):
    _source, runtime, profile, jobs, _executions, _other = install_fixture
    before_runtime = {p: (runtime / p).read_bytes() for p in release.RUNTIME_MEMBERS}
    before_jobs = jobs.read_bytes()
    with pytest.raises(RuntimeError, match="injected"):
        _install(install_fixture, fail_after=phase)
    assert before_runtime == {p: (runtime / p).read_bytes() for p in release.RUNTIME_MEMBERS}
    assert jobs.read_bytes() == before_jobs
    assert not (profile / "scripts" / "observer_v2").exists()
    recovery = profile / "cron" / f"recovery-{release.JOB_ID}"
    assert not (recovery / "local-observation-v2.sqlite").exists()
    assert not release.manifest_path(profile).exists()


def test_lost_compare_and_swap_preserves_concurrent_bytes_and_marks_unknown(install_fixture):
    _source, runtime, profile, _jobs, _executions, _other = install_fixture

    def race(phase, _context):
        if phase == "job":
            (runtime / release.RUNTIME_MEMBERS[0]).write_bytes(b"concurrent-owner")
            raise RuntimeError("injected race")

    with pytest.raises(release.MixedVersionError):
        _install(install_fixture, phase_hook=race)
    assert (runtime / release.RUNTIME_MEMBERS[0]).read_bytes() == b"concurrent-owner"
    evidence = json.loads(release.unknown_path(profile).read_text(encoding="utf-8"))
    assert evidence["state"] == "UNKNOWN"


@pytest.mark.parametrize("phase", ["runtime", "package", "migration", "job", "manifest"])
def test_execution_appearing_mid_install_prevents_commit(install_fixture, phase):
    _source, _runtime, profile, _jobs, executions, _other = install_fixture

    def race(current, _context):
        if current == phase:
            with sqlite3.connect(executions) as db:
                db.execute("INSERT INTO executions VALUES(?, 'running')", (release.JOB_ID,))

    with pytest.raises(release.InstallRefused, match="execution is pending or active"):
        _install(install_fixture, phase_hook=race)

    assert not release.manifest_path(profile).exists()


def test_execution_admission_and_install_commit_share_one_ownership_fence(
    install_fixture, monkeypatch
):
    from cron import executions, jobs as cron_jobs

    _source, _runtime, profile, _jobs, executions_path, _other = install_fixture
    executions_path.unlink()
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", executions_path)
    with cron_jobs.use_cron_store(profile):
        with executions._transaction():
            pass
    entered_manifest = threading.Event()
    release_manifest = threading.Event()
    outcome = {}

    def pause_before_commit(phase, _context):
        if phase == "manifest":
            entered_manifest.set()
            assert release_manifest.wait(20)

    def run_install():
        outcome["install"] = _install(install_fixture, phase_hook=pause_before_commit)

    installer = threading.Thread(target=run_install)
    installer.start()
    assert entered_manifest.wait(20)

    def admit_execution():
        with cron_jobs.use_cron_store(profile):
            outcome["execution"] = executions.create_execution(release.JOB_ID, source="direct")

    contender = threading.Thread(target=admit_execution)
    contender.start()
    assert contender.is_alive()
    release_manifest.set()
    installer.join(30)
    contender.join(30)

    assert not installer.is_alive()
    assert not contender.is_alive()
    assert outcome["install"]["state"] == "committed"
    assert outcome["execution"]["status"] == "claimed"
    with sqlite3.connect(executions_path) as db:
        active = db.execute(
            "SELECT COUNT(*) FROM executions WHERE job_id=? AND status IN ('claimed','running')",
            (release.JOB_ID,),
        ).fetchone()[0]
    assert active == 1


@pytest.mark.parametrize("phase", ["runtime", "package", "migration", "job", "manifest"])
def test_claim_appearing_mid_install_prevents_commit(install_fixture, phase):
    _source, _runtime, profile, jobs, _executions, _other = install_fixture

    def race(current, _context):
        if current == phase:
            _write_target_job(
                jobs, lambda job: job.__setitem__("fire_claim", {"by": "concurrent"})
            )

    with pytest.raises(release.InstallRefused):
        _install(install_fixture, phase_hook=race)

    assert not release.manifest_path(profile).exists()


@pytest.mark.parametrize("phase", ["runtime", "package", "migration", "job", "manifest"])
def test_v1_source_drift_at_each_material_phase_prevents_commit(install_fixture, phase):
    _source, runtime, profile, _jobs, _executions, _other = install_fixture
    source = profile / "cron" / f"recovery-{release.JOB_ID}" / "local-observation-v1.json"

    def race(current, _context):
        if current == phase:
            source.write_bytes(source.read_bytes() + b"\n")

    with pytest.raises(release.InstallRefused, match="migration source drifted"):
        _install(install_fixture, phase_hook=race)

    assert not release.manifest_path(profile).exists()
    assert source.read_bytes().endswith(b"\n")
    assert all(
        (runtime / name).read_bytes().startswith(b"old:")
        for name in release.RUNTIME_MEMBERS
    )


def test_absent_stale_foreign_and_mixed_manifests_fail_closed(install_fixture):
    _source, runtime, profile, _jobs, _executions, _other = install_fixture
    with pytest.raises(release.CurrentnessError):
        release.validate_runtime_manifest(profile, runtime)
    _install(install_fixture)
    manifest = release.manifest_path(profile)
    data = json.loads(manifest.read_text(encoding="utf-8"))
    data["profile"] = "foreign"
    manifest.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(release.CurrentnessError):
        release.validate_runtime_manifest(profile, runtime)
    data["profile"] = "rowan"
    manifest.write_text(json.dumps(data), encoding="utf-8")
    (runtime / release.RUNTIME_MEMBERS[0]).write_bytes(b"mixed")
    with pytest.raises(release.MixedVersionError):
        release.validate_runtime_manifest(profile, runtime)


def test_post_commit_failure_requires_forward_repair(install_fixture):
    _source, runtime, profile, _jobs, _executions, _other = install_fixture
    with pytest.raises(release.ForwardRepairRequired):
        _install(install_fixture, fail_after="commit")
    assert release.manifest_path(profile).is_file()
    assert (runtime / release.RUNTIME_MEMBERS[0]).read_bytes() != b"old:" + release.RUNTIME_MEMBERS[0].encode()
    release.validate_runtime_manifest(profile, runtime)


def test_scheduler_admits_rowan_only_with_current_manifest(install_fixture, monkeypatch):
    from cron import scheduler_local_observation as producer

    _source, runtime, profile, _jobs, _executions, _other = install_fixture
    monkeypatch.setenv("HERMES_HOME", str(profile))
    assert producer.is_target({"id": release.JOB_ID})
    with pytest.raises(release.CurrentnessError):
        producer._require_current_manifest()
    _install(install_fixture)
    monkeypatch.setattr(producer, "_runtime_root", lambda: runtime)
    producer._require_current_manifest()
    foreign = profile.parent / "foreign"
    foreign.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(foreign))
    assert not producer.is_target({"id": release.JOB_ID})
