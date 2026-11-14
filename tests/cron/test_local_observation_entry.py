"""WRI-2276: default script reaches a bound epoch and stage-only child."""
from contextlib import closing
import hashlib
import importlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import pytest

from tests.cron.local_observation_support import native_fixture, check_board, write_current_manifest
from tests.waiting import wait_until


@pytest.mark.parametrize('stage_count', [2, 0])
def test_default_script_binds_epoch_and_real_stage_child(monkeypatch, stage_count):
    from cron import executions, jobs
    from hermes_cli import kanban_db as kb

    # Restore the real path-valued descendant fence after the runner's documented scrub.
    monkeypatch.setenv('HERMES_DELEGATED_CHILD_CONTEXT', '/home/macpro/.hermes')
    with native_fixture() as root:
        home = root / 'rowan'
        source = home / 'scripts/observer_v2'
        source.mkdir(parents=True)
        (home / 'cache/scratch').mkdir(parents=True, mode=0o700)
        fixtures = Path(__file__).parent / 'fixtures'
        for path in (fixtures / 'observer-v2-reference').glob('*.py.fixture'):
            (source / path.name.removesuffix('.fixture')).write_bytes(path.read_bytes())
        for path in (fixtures / 'private-entry-component').glob('*.py.fixture'):
            (source / path.name.removesuffix('.fixture')).write_bytes(path.read_bytes())
        names = ['engineering_recovery_observer', 'native_status_policy', 'local_observation_v2',
                 'observer_parent_seam', 'observer_proposal_v2', 'observer_native_v2', 'observer_private_entry']
        monkeypatch.syspath_prepend(str(source))
        monkeypatch.setenv('HERMES_HOME', str(home))
        board = root / 'board.db'
        check_board(board, root)
        monkeypatch.setenv('HERMES_KANBAN_DB', str(board))
        with closing(sqlite3.connect(board)) as db:
            db.executescript(kb.SCHEMA_SQL)
            db.executemany("INSERT INTO tasks(id,title,body,assignee,status,created_at) VALUES(?,'fixture','','fixture','blocked',1)", [('t_11111111',), ('t_22222222',)])
            db.commit()
        legacy = importlib.import_module('engineering_recovery_observer')
        legacy.PROFILE_HOME = home
        v2 = importlib.import_module('local_observation_v2')
        directory = home / 'cron' / ('recovery-' + legacy.JOB_ID)
        directory.mkdir(parents=True, mode=0o700)
        native = home / 'cron/notepad.db'
        with closing(sqlite3.connect(native)) as db:
            db.execute('CREATE TABLE cron_notepad(job_id TEXT,key TEXT,value TEXT,updated_at TEXT)')
            db.execute('INSERT INTO cron_notepad VALUES(?,?,?,?)',
                       (legacy.JOB_ID, 'recovery_root_t_11111111', '{"status":"blocked"}', 'original'))
            db.execute('INSERT INTO cron_notepad VALUES(?,?,?,?)',
                       (legacy.JOB_ID, 'recovery_root_t_22222222', '{"status":"blocked"}', 'original'))
            db.commit()
        ledger = {'version': 2, 'board': legacy.BOARD, 'job': legacy.JOB_ID,
                  'legacy_raw': legacy.read_legacy(native)['raw'], 'roots': {}}
        (directory / 'ledger.json').write_bytes(legacy._bytes(ledger))
        sha = legacy.read_ledger(directory)[1]
        legacy.commit_cutover_marker(directory, sha, 2, 'missing')
        old = {'version': 1, 'board_id': legacy.BOARD, 'job_id': legacy.JOB_ID,
               'ledger_sha': sha, 'admission_generation': 0, 'admission_state': 'invalidating',
               'pending': {}, 'roots': {}, 'current_issued': {}}
        (directory / 'local-observation-v1.json').write_bytes(v2.encode(old))
        v2.migrate(directory)

        entry = source / 'observer_private_entry.py'
        script = entry if entry.exists() else source / 'engineering_recovery_observer.py'
        # Isolated bootstrap binds the established synthetic profile and library-only board seam.
        bootstrap = root / 'bootstrap.py'
        bootstrap.write_text(
            'import importlib,pathlib,sys\n'
            f'sys.path.insert(0,{str(Path.cwd())!r})\n'
            f'sys.path.insert(0,{str(source)!r})\n'
            'from cron.scheduler_local_observation import load_private_package\n'
            f'modules=load_private_package(pathlib.Path({str(source)!r}))\n'
            'module=modules[pathlib.Path(sys.argv[1]).stem]\n'
            'legacy=modules["engineering_recovery_observer"]\n'
            f'legacy.PROFILE_HOME=pathlib.Path({str(home)!r})\n'
            'original=module.native_prerun\n'
            'module.native_prerun=lambda d,n,b,**kw: original(d,n,b,enforce_board=False)\n'
            'module.main([])\n')
        with jobs.use_cron_store(home):
            original_uuid = jobs.uuid.uuid4
            class TargetId:
                hex = legacy.JOB_ID
            monkeypatch.setattr(jobs.uuid, 'uuid4', lambda: TargetId())
            job = jobs.create_job(prompt=None, schedule='every 1h', name='entry tracer',
                                  script=str(script.relative_to(home / 'scripts')), no_agent=True)
            monkeypatch.setattr(jobs.uuid, 'uuid4', original_uuid)
            claimed = jobs.claim_job_for_fire(job['id'], manual=True, return_job=True)
            execution = executions.create_execution(job['id'], source='direct')
            assert executions.mark_execution_handoff_pending(execution['id'])
        assert isinstance(claimed, dict)
        trust_patch = write_current_manifest(home, source, claimed)
        claimed['execution_id'] = execution['id']
        payload = root / 'worker.json'
        payload.write_text(json.dumps({'job': claimed, 'profile_home': str(home)}))
        bound, release = root / 'bound', root / 'release'
        worker_code = (
            'import pathlib,sys,time\n'
            + trust_patch
            + 'from cron import scheduler,scheduler_script as scripts,scheduler_local_observation as producer\n'
            f'scripts._posix_cron_script_argv=lambda script: ([sys.executable,{str(bootstrap)!r},str(script)],{{}})\n'
            'def after_bind():\n'
            f' pathlib.Path({str(bound)!r}).write_text("bound")\n'
            ' deadline=time.monotonic()+30\n'
            f' while not pathlib.Path({str(release)!r}).exists() and time.monotonic()<deadline: time.sleep(0.02)\n'
            f' if not pathlib.Path({str(release)!r}).exists(): raise RuntimeError("stage timeout")\n'
            'producer._TEST_AFTER_BIND=after_bind\n'
            f'assert scheduler._run_external_worker_payload(pathlib.Path({str(payload)!r}),pathlib.Path({str(root / "ready")!r}))\n')
        env = dict(os.environ, HERMES_HOME=str(home), TMPDIR=str(root))
        worker = subprocess.Popen([sys.executable, '-c', worker_code], cwd=Path.cwd(), env=env,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        diagnostics = {}
        try:
            assert wait_until(lambda: bound.exists() or worker.poll() is not None, timeout=60), 'PRETEST: worker did not settle'
            if bound.exists():
                with closing(sqlite3.connect(home / 'cron/executions.db')) as db:
                    captured_binding = db.execute('SELECT * FROM cron_prerun_bindings WHERE execution_id=?', (execution['id'],)).fetchone()
                for index in range(stage_count):
                    with closing(sqlite3.connect(directory / 'local-observation-v2.sqlite')) as db:
                        header = json.loads(db.execute('SELECT payload FROM metadata').fetchone()[0])
                        issued = db.execute('SELECT root,observation_id FROM issued ORDER BY root LIMIT 1 OFFSET ?', (index,)).fetchone()
                    assert issued is not None, 'bound epoch has no observable positive root'
                    stage_code = (
                        'import importlib,pathlib,sys\n'
                        f'sys.path.insert(0,{str(Path.cwd())!r})\n'
                        'from cron.scheduler_local_observation import load_private_package\n'
                        f'modules=load_private_package(pathlib.Path({str(source)!r}))\n'
                        f'module=modules[{script.stem!r}]\n'
                        'legacy=modules["engineering_recovery_observer"]\n'
                        f'legacy.PROFILE_HOME=pathlib.Path({str(home)!r})\n'
                        f'module.main(["local-stage",{execution["id"]!r},{issued[0]!r},{issued[1]!r},{str(header["revision"])!r}])\n')
                    child = subprocess.run([sys.executable, '-c', stage_code], cwd=Path.cwd(), env=env,
                                           capture_output=True, text=True, timeout=20, check=False)
                    diagnostics['stage_exit'] = child.returncode
                    diagnostics['stage_stderr'] = child.stderr
                    assert child.returncode == 0, child.stderr
            with closing(sqlite3.connect(home / 'cron/executions.db')) as db:
                row = db.execute('SELECT bind_state,epoch_id FROM cron_prerun_bindings WHERE execution_id=?', (execution['id'],)).fetchone()
            with closing(sqlite3.connect(directory / 'local-observation-v2.sqlite')) as db:
                epoch = db.execute('SELECT COUNT(*) FROM epochs WHERE execution_id=?', (execution['id'],)).fetchone()[0]
                stages = db.execute('SELECT COUNT(*) FROM stages WHERE execution_id=?', (execution['id'],)).fetchone()[0]
                checked = db.execute("SELECT COUNT(*) FROM records WHERE kind='checked'").fetchone()[0]
            assert (row[0], epoch, stages, checked) == ('bound', 1, stage_count, 0), (
                f'real default ingress lacks bound V2 epoch and child stage: row={row}, epoch={epoch}, stages={stages}, diagnostics={diagnostics}')
        finally:
            release.write_text('go')
            try:
                out, err = worker.communicate(timeout=60)
            except subprocess.TimeoutExpired:
                worker.terminate()
                out, err = worker.communicate(timeout=10)
            print(json.dumps({'worker_exit': worker.returncode, 'stdout': out, 'stderr': err, **diagnostics}))
            for name in names:
                sys.modules.pop(name, None)
        assert worker.returncode == 0, err
        with closing(sqlite3.connect(home / 'cron/executions.db')) as db:
            assert db.execute('SELECT status,error,pid FROM executions WHERE id=?', (execution['id'],)).fetchone() == ('completed', None, worker.pid)
            assert db.execute('SELECT * FROM cron_prerun_bindings WHERE execution_id=?', (execution['id'],)).fetchone() == captured_binding
            expected = 'succeeded' if stage_count else 'not_applicable'
            final = db.execute('SELECT state,outcome_digest FROM cron_local_finalizations WHERE execution_id=?', (execution['id'],)).fetchone()
            assert final[0] == expected and final[1]
        with closing(sqlite3.connect(directory / 'local-observation-v2.sqlite')) as db:
            assert db.execute("SELECT COUNT(*) FROM records WHERE kind='checked'").fetchone()[0] == stage_count
            outcome = json.loads(db.execute('SELECT payload FROM outcomes WHERE execution_id=?', (execution['id'],)).fetchone()[0])
            assert outcome['state'] == expected and outcome['digest'] == final[1]
            assert len(outcome['records']) == stage_count
        assert execution['id'] in jobs.get_job(job['id'])['canonical_completions']
