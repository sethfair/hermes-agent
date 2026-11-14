"""WRI-2276 package admission at the trusted loader, not importlib caches."""
from contextlib import contextmanager
import hashlib
import importlib.util
from pathlib import Path
import sys
import types

import pytest

from cron import scheduler_local_observation as producer
from tests.cron.local_observation_support import native_fixture


@contextmanager
def package(monkeypatch):
    from tools.environments import local as local_env

    monkeypatch.setattr(local_env, '_resolve_hermes_bin_dir', lambda: None)
    with native_fixture() as root:
        home = root / 'rowan'
        directory = home / 'scripts/observer_v2'
        directory.mkdir(parents=True)
        fixture = Path(__file__).parent / 'fixtures/observer-v2-reference'
        for name in producer._LOAD_ORDER:
            (directory / (name + '.py')).write_bytes((fixture / (name + '.py.fixture')).read_bytes())
        monkeypatch.setenv('HERMES_HOME', str(home))
        assert producer._PACKAGE_REGISTRY is None
        before = {name: sys.modules.get(name) for name in producer._LOAD_ORDER}
        assert not any(before.values()), 'foreign setup cache is PRETEST'
        try:
            yield home, directory, root
        finally:
            # Each test owns only the canonical objects it created in this fresh file process.
            if producer._PACKAGE_REGISTRY is not None:
                for name in producer._LOAD_ORDER:
                    sys.modules.pop(name, None)
                producer._PACKAGE_REGISTRY = None


def test_complete_package_reuses_exact_objects_and_ignores_stale_bytecode(monkeypatch):
    with package(monkeypatch) as (_, directory, _):
        (directory / 'native_status_policy.pyc').write_bytes(b'not trusted executable code')
        modules = producer.load_private_package(directory)
        again = producer.load_private_package(directory)
        assert set(modules) == set(producer._LOAD_ORDER)
        assert all(again[name] is module for name, module in modules.items())
        assert modules['observer_parent_seam'].v2 is modules['local_observation_v2']
        assert modules['engineering_recovery_observer'].NATIVE_STATUS_POLICY
        assert producer._admit_script({'script':'observer_v2/observer_private_entry.py'}) == directory / 'observer_private_entry.py'
        assert not list(directory.rglob('*.sqlite'))


@pytest.mark.parametrize('name', producer._LOAD_ORDER)
@pytest.mark.parametrize('fault', ['missing', 'tampered', 'symlink'])
def test_each_member_denies_incomplete_package(monkeypatch, name, fault):
    with package(monkeypatch) as (_, directory, root):
        path = directory / (name + '.py')
        if fault == 'tampered':
            path.write_bytes(path.read_bytes() + b'\n# changed\n')
        else:
            raw = path.read_bytes()
            path.unlink()
            if fault == 'symlink':
                target = root / 'outside.py'
                target.write_bytes(raw)
                path.symlink_to(target)
        with pytest.raises((ValueError, FileNotFoundError)):
            producer.load_private_package(directory)
        assert not any(name in sys.modules for name in producer._LOAD_ORDER)
        assert not list(root.rglob('*.sqlite'))


@pytest.mark.parametrize('name', ['native_status_policy', 'engineering_recovery_observer',
                                  'observer_proposal_v2', 'observer_private_entry'])
def test_forged_expected_origin_is_still_foreign_cache(monkeypatch, name):
    with package(monkeypatch) as (_, directory, _):
        foreign = types.ModuleType(name)
        foreign.__file__ = str(directory / (name + '.py'))
        foreign.__spec__ = importlib.util.spec_from_loader(name, producer._PACKAGE_LOADER, origin=foreign.__file__)
        foreign.__loader__ = producer._PACKAGE_LOADER
        monkeypatch.setitem(sys.modules, name, foreign)
        with pytest.raises(ValueError, match='untrusted canonical'):
            producer.load_private_package(directory)
        assert sys.modules[name] is foreign


def test_all_members_verified_before_first_execution(monkeypatch):
    with package(monkeypatch) as (_, directory, root):
        policy = directory / 'native_status_policy.py'
        sentinel = root / 'executed'
        raw = policy.read_bytes() + f'\nopen({str(sentinel)!r},"w").write("executed")\n'.encode()
        policy.write_bytes(raw)
        monkeypatch.setitem(producer._REFERENCE_SHA, 'native_status_policy', hashlib.sha256(raw).hexdigest())
        (directory / 'observer_native_v2.py').write_bytes(b'raise RuntimeError("tampered later source")')
        with pytest.raises(ValueError, match='source pin'):
            producer.load_private_package(directory)
        assert not sentinel.exists(), 'policy executed before complete-package verification'


@pytest.mark.parametrize('fault', ['bytes', 'object', 'origin', 'profile'])
def test_trusted_reuse_rechecks_identity(monkeypatch, fault):
    with package(monkeypatch) as (_home, directory, root):
        modules = producer.load_private_package(directory)
        if fault == 'bytes':
            path = directory / 'observer_private_entry.py'
            path.write_bytes(path.read_bytes() + b'\n# stale\n')
        elif fault == 'object':
            sys.modules['native_status_policy'] = types.ModuleType('native_status_policy')
        elif fault == 'origin':
            modules['native_status_policy'].__spec__.origin = str(root / 'wrong.py')
        else:
            home2 = root / 'other/rowan'
            other = home2 / 'scripts/observer_v2'
            other.mkdir(parents=True)
            for path in directory.glob('*.py'):
                (other / path.name).write_bytes(path.read_bytes())
            monkeypatch.setenv('HERMES_HOME', str(home2))
            directory = other
        with pytest.raises(ValueError):
            producer.load_private_package(directory)


def test_wrong_path_identical_entry_denied_before_any_reservation(monkeypatch):
    with package(monkeypatch) as (home, directory, root):
        path = home / 'scripts/copy.py'
        path.write_bytes((directory / 'observer_private_entry.py').read_bytes())
        with pytest.raises(ValueError, match='canonical private entry'):
            producer.reserve({'id': producer.TARGET_JOB_ID, 'script': 'copy.py', 'no_agent': True}, 'attempt', 'claim')
        assert not list(root.rglob('executions.db'))
        assert producer._admit_script({'script': 'observer_v2/observer_private_entry.py'}) == directory / 'observer_private_entry.py'


def test_preterminal_rechecks_actual_script_before_authority_capture(monkeypatch):
    from cron import executions, jobs
    with package(monkeypatch) as (home, directory, root):
        modules = producer.load_private_package(directory)
        admitted = []
        monkeypatch.setattr(jobs, 'get_job', lambda _: {'fire_claim': {'by': 'owner'}})
        monkeypatch.setattr(executions, 'target_preterminal', lambda *a: {'bind_state': 'bound'})
        monkeypatch.setattr(executions, 'start_target_finalization', lambda *a: True)
        monkeypatch.setattr(modules['observer_parent_seam'].ParentAuthority, 'capture',
                            lambda *a: admitted.append('capture'))
        monkeypatch.setattr(producer, 'is_target', lambda candidate: candidate.get('id') == producer.TARGET_JOB_ID)
        job = {'id': producer.TARGET_JOB_ID, 'script': 'copy.py'}
        (home / 'scripts/copy.py').write_bytes((directory / 'observer_private_entry.py').read_bytes())
        with pytest.raises(ValueError, match='canonical private entry'):
            producer.prepare_terminal(job, 'attempt', 'owner', success=True,
                                      delivery_error=None, status=None, mark_kwargs={})
        assert admitted == [], 'wrong actual script captured parent authority'
        assert not list(root.rglob('*.sqlite'))


def _activate_empty_v2(directory, modules):
    legacy, v2 = modules['engineering_recovery_observer'], modules['local_observation_v2']
    old = {'version':1,'board_id':legacy.BOARD,'job_id':legacy.JOB_ID,
           'ledger_sha':'0'*64,'admission_generation':0,'admission_state':'invalidating',
           'pending':{},'roots':{},'current_issued':{}}
    (directory/'local-observation-v1.json').write_bytes(v2.encode(old))
    v2.migrate(directory)
    assert v2.Store(directory).header()['state'] == 'invalidating'


def test_active_entry_retires_legacy_writers_but_not_explicit_status(monkeypatch):
    from hermes_cli import kanban_db
    with package(monkeypatch) as (home, source, root):
        modules = producer.load_private_package(source)
        legacy, entry = modules['engineering_recovery_observer'], modules['observer_private_entry']
        directory = home / 'cron' / ('recovery-' + legacy.JOB_ID)
        directory.mkdir(parents=True, mode=0o700)
        _activate_empty_v2(directory, modules)
        before = {p.name:p.read_bytes() for p in directory.iterdir() if p.is_file()}
        monkeypatch.setenv('HERMES_KANBAN_DB', str(root/'board.db'))
        reached = []

        def old_dispatch(argv):
            reached.append(argv[0])
            if argv[0] in {'observe-local', 'local-check'}:
                (directory/'forbidden-legacy-write').write_text(argv[0])
            return 'explicit-status-sentinel'

        monkeypatch.setattr(legacy, 'main', old_dispatch)
        for command in ('observe-local', 'local-check'):
            with pytest.raises(ValueError, match='legacy writer retired'):
                entry.main([command])
        assert reached == []
        assert not (directory/'forbidden-legacy-write').exists()
        assert entry.main(['reconcile-status']) == 'explicit-status-sentinel'
        assert reached == ['reconcile-status']
        assert before == {p.name:p.read_bytes() for p in directory.iterdir() if p.is_file()}
        assert not kanban_db.kanban_db_path(board='default').exists()


@pytest.mark.parametrize('active,argv', [
    (False, ['local-select','attempt','epoch','1','2']),
    (False, ['local-stage','attempt','root','observation','1']),
    (True, ['local-select','attempt']),
    (True, ['local-stage','attempt']),
    (True, ['local-finalize','attempt']),
    (True, ['unexpected-command']),
])
def test_entry_denies_inactive_malformed_and_child_finalize(monkeypatch, active, argv):
    with package(monkeypatch) as (home, source, root):
        modules = producer.load_private_package(source)
        directory = home/'cron'/('recovery-'+producer.TARGET_JOB_ID)
        directory.mkdir(parents=True, mode=0o700)
        if active:
            _activate_empty_v2(directory, modules)
        before = {p.name:p.read_bytes() for p in directory.iterdir() if p.is_file()}
        monkeypatch.setenv('HERMES_KANBAN_DB', str(root/'board.db'))
        with pytest.raises(ValueError):
            modules['observer_private_entry'].main(argv)
        assert before == {p.name:p.read_bytes() for p in directory.iterdir() if p.is_file()}
        assert not (root/'board.db').exists()


def test_actual_proposal_child_denies_forged_canonical_cache(monkeypatch):
    with package(monkeypatch) as (home, source, root):
        producer.load_private_package(source)
        original_run = producer.subprocess.run
        executions = []

        def poisoned_child(argv, **kwargs):
            assert argv[1] == '-c'
            forged = ('import types,sys,importlib.util; '
                      'from cron.scheduler_local_observation import _PACKAGE_LOADER; '
                      'm=types.ModuleType("native_status_policy"); '
                      f'm.__file__={str(source/"native_status_policy.py")!r}; '
                      'm.__loader__=_PACKAGE_LOADER; '
                      'm.__spec__=importlib.util.spec_from_loader(m.__name__,_PACKAGE_LOADER,origin=m.__file__); '
                      'sys.modules[m.__name__]=m; ')
            # Insert after the trusted repository path, but before actual package use.
            code = argv[2].replace('modules=load_private_package(', forged+'modules=load_private_package(', 1)
            result = original_run([*argv[:2],code], **kwargs)
            executions.append((result.returncode,result.stderr))
            return result

        monkeypatch.setattr(producer.subprocess, 'run', poisoned_child)
        with pytest.raises(ValueError, match='read-only proposal child failed'):
            producer._read_proposal(source, home/'unused-recovery', 'attempt', 'epoch')
        assert len(executions) == 1 and executions[0][0] != 0
        assert 'untrusted canonical private package cache' in executions[0][1]
        assert not list(root.rglob('*.sqlite'))
