"""Contained native fixtures for WRI-2276; importing support runs no suite."""
from contextlib import contextmanager
from pathlib import Path
import json
import os
import stat
import tempfile
import shutil
import subprocess
import sys
import hashlib

import psutil
import pytest

from agent.delegation_context import kanban_path_is_fenced


@pytest.fixture(name='tmp_path')
def native_tmp_path(monkeypatch):
    # run_tests.sh deliberately scrubs lineage. Restore the actual descendant root
    # for synthetic native mutation, without changing the production guard.
    monkeypatch.setenv('HERMES_DELEGATED_CHILD_CONTEXT', '/home/macpro/.hermes')
    with native_fixture() as root:
        monkeypatch.setenv('TMPDIR', str(root))
        try:
            yield root
        finally:
            from cron import scheduler_local_observation as producer
            if producer._PACKAGE_REGISTRY is not None:
                for name, module in producer._PACKAGE_REGISTRY[2].items():
                    if sys.modules.get(name) is module:
                        del sys.modules[name]
                producer._PACKAGE_REGISTRY = None


def copy_package(source):
    from cron import scheduler_local_observation as producer
    fixture = Path(__file__).parent / 'fixtures/observer-v2-reference'
    for name in producer._LOAD_ORDER:
        (source / (name + '.py')).write_bytes((fixture / (name + '.py.fixture')).read_bytes())


def write_current_manifest(home, package, job):
    """Bind a contained fixture to its exact package, runtime and job bytes."""
    from cron import local_observation_install as release
    runtime = Path(__file__).resolve().parents[2]
    recovery = home / 'cron' / f'recovery-{release.JOB_ID}'
    backup = recovery / 'install-backups/test-fixture'
    backup.mkdir(parents=True, exist_ok=True)
    prestate = {}
    (backup / 'prestate.json').write_text(json.dumps(prestate), encoding='utf-8')
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    runtime_hashes = {name: digest(runtime / name) for name in release.RUNTIME_MEMBERS}
    package_hashes = {name: digest(package / name) for name in release.PACKAGE_MEMBERS}
    commit = '0' * 40
    trust_root = home / 'test-release-trust'
    approval_root = home / 'test-release-approvals'
    approval_dir = approval_root / commit
    home.chmod(0o700)
    trust_root.mkdir(parents=True, mode=0o755)
    approval_dir.mkdir(parents=True, mode=0o755)
    key = home / 'test-release-approval-key'
    subprocess.run(
        ['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)], check=True)
    public_key = key.with_suffix('.pub').read_text(encoding='ascii').split()
    allowed_signers = trust_root / 'allowed_signers'
    allowed_signers.write_text(
        f'{release.APPROVAL_PRINCIPAL} {public_key[0]} {public_key[1]}\n', encoding='ascii')
    fingerprint = release._key_fingerprint(public_key[1])
    policy = {
        'schema': 1, 'repository': release.SOURCE_REPOSITORY,
        'profile': release.PROFILE_NAME, 'job': release.JOB_ID,
        'namespace': release.APPROVAL_NAMESPACE, 'principal': release.APPROVAL_PRINCIPAL,
        'key_fingerprint': fingerprint,
    }
    (trust_root / 'policy.json').write_text(
        json.dumps(policy, sort_keys=True, separators=(',', ':')) + '\n', encoding='utf-8')
    approval = {
        'approved_at': '2026-11-11T00:00:00Z', 'commit': commit, 'job': release.JOB_ID,
        'package_hashes': package_hashes, 'pr': 7, 'profile': release.PROFILE_NAME,
        'repository': release.SOURCE_REPOSITORY, 'review_receipt_sha256': 'a' * 64,
        'review_verdict': 'PASS', 'runtime_hashes': runtime_hashes, 'schema': 1,
    }
    approval_path = approval_dir / 'approval.json'
    approval_raw = json.dumps(
        approval, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode() + b'\n'
    approval_path.write_bytes(approval_raw)
    subprocess.run(
        ['ssh-keygen', '-Y', 'sign', '-q', '-f', str(key),
         '-n', release.APPROVAL_NAMESPACE, str(approval_path)], check=True)
    signature_path = approval_path.with_suffix('.json.sig')
    for directory in (trust_root, approval_root, approval_dir):
        directory.chmod(0o755)
    for path in (allowed_signers, trust_root / 'policy.json', approval_path, signature_path):
        path.chmod(0o644)
    approval_binding = {
        'approval_sha256': hashlib.sha256(approval_raw).hexdigest(),
        'signature_sha256': digest(signature_path), 'commit': commit,
        'key_fingerprint': fingerprint, 'principal': release.APPROVAL_PRINCIPAL,
        'namespace': release.APPROVAL_NAMESPACE,
    }
    source_binding = {
        'repository': release.SOURCE_REPOSITORY,
        'commit': commit,
        'package_hashes': package_hashes,
        'runtime_hashes': runtime_hashes,
    }
    source_digest = hashlib.sha256(
        json.dumps(source_binding, sort_keys=True, separators=(',', ':')).encode()
    ).hexdigest()
    source_sha = digest(recovery / 'local-observation-v1.json')
    assert source_sha == digest(recovery / 'local-observation-v1.backup')
    payload = {
        'version': 1, 'state': 'committed', 'transaction': 'test-fixture',
        'commit': commit, 'profile': release.PROFILE_NAME, 'job': release.JOB_ID,
        'source_identity': {
            'repository': release.SOURCE_REPOSITORY,
            'commit': commit,
            'digest': source_digest,
        },
        'approval': approval_binding,
        'job_binding': release._job_binding(job),
        'schema_generation': release.SCHEMA_GENERATION,
        'runtime_hashes': runtime_hashes,
        'package_hashes': package_hashes,
        'migration_inputs': {
            'local-observation-v1.json': source_sha,
            'local-observation-v1.backup': source_sha,
        },
        'prestate': prestate,
        'backup_root': str(backup.relative_to(home)),
    }
    recovery.mkdir(parents=True, exist_ok=True)
    release.manifest_path(home).write_text(
        json.dumps(payload, sort_keys=True, separators=(',', ':')) + '\n', encoding='utf-8')
    return (
        'from cron import local_observation_install as _release_trust\n'
        f'_release_trust.TRUST_ROOT=pathlib.Path({str(trust_root)!r})\n'
        f'_release_trust.APPROVAL_ROOT=pathlib.Path({str(approval_root)!r})\n'
        f'_release_trust.TRUST_PATH_ANCHOR=pathlib.Path({str(home)!r})\n'
        f'_release_trust.TRUST_PATH_ANCHOR_MODE={stat.S_IMODE(home.stat().st_mode)!r}\n'
        f'_release_trust.TRUST_OWNER_UID={os.getuid()}\n'
        f'_release_trust.TRUST_OWNER_GID={os.getgid()}\n'
        '_release_trust._installer_can_write=lambda _path: False\n'
    )


def bootstrap_text(source):
    return (
        'import pathlib,sys\n'
        f'sys.path.insert(0,{str(Path(__file__).resolve().parents[2])!r})\n'
        'from cron.scheduler_local_observation import load_private_package\n'
        f'modules=load_private_package(pathlib.Path({str(source)!r}))\n'
        'legacy=modules["engineering_recovery_observer"]\n'
        'v2=modules["local_observation_v2"]\n'
        'seam=modules["observer_parent_seam"]\n'
        'entry=modules["observer_private_entry"]\n')


def prerun_bootstrap(source):
    return (bootstrap_text(source)
            + 'original=entry.native_prerun\n'
            + 'entry.native_prerun=lambda d,n,b,**kw: original(d,n,b,enforce_board=False)\n'
            + 'entry.main([])\n')


def close_owned_descendants(before):
    """Settle this serial fixture's new descendants before removing its files."""
    owned = [p for p in psutil.Process().children(recursive=True)
             if (p.pid, p.create_time()) not in before]
    for process in reversed(owned):
        try:
            process.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs(owned, timeout=5)
    for process in alive:
        process.kill()
    _, alive = psutil.wait_procs(alive, timeout=5)
    if alive:
        raise RuntimeError('UNKNOWN fixture descendants remain: ' + repr([p.pid for p in alive]))

LIVE_BOARD = Path('/home/macpro/.hermes/kanban.db')


def check_board(path, invocation):
    path = Path(path)
    for part in (path, *path.parents):
        if part.exists() or part.is_symlink():
            if stat.S_ISLNK(part.lstat().st_mode):
                raise ValueError('symlink fixture ancestor')
    if not path.is_relative_to(invocation) or path.resolve() == LIVE_BOARD.absolute():
        raise ValueError('board outside owned invocation')
    if kanban_path_is_fenced(path):
        raise ValueError('native board is fenced')


@contextmanager
def native_fixture():
    """Disjoint per-invocation ownership: no shared parent, no cross-process lock.

    Every invocation creates its own random parent under the runner's temp root, so
    concurrent files, jobs and checkouts cannot contend, and the fixture needs no
    host-specific path. Fence, symlink, descendant-settlement and cleanup guards stay.
    """
    root = Path(tempfile.gettempdir()).resolve()
    parent = Path(tempfile.mkdtemp(prefix='wri-2276-native-', dir=root))
    child = None
    before = {(p.pid, p.create_time()) for p in psutil.Process().children(recursive=True)}
    try:
        if parent.is_symlink() or parent.resolve() != parent or kanban_path_is_fenced(parent / 'board.db'):
            raise ValueError('fixture parent identity or fence denial')
        child = Path(tempfile.mkdtemp(prefix='invocation-', dir=parent))
        check_board(child / 'board.db', child)
        yield child
    finally:
        try:
            close_owned_descendants(before)
            shutil.rmtree(parent)
        except BaseException:
            print(json.dumps({'fixture_parent': str(parent), 'residue': str(child),
                              'cleanup': 'UNKNOWN'}))
            raise
        print(json.dumps({'fixture_parent': str(parent), 'cleanup': 'removed',
                          'lineage': os.environ.get('HERMES_DELEGATED_CHILD_CONTEXT'),
                          'board_pin': os.environ.get('HERMES_KANBAN_DB')}))
