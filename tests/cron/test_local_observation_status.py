"""Status CLI tracer through a real isolated board and independent durable read."""
import contextlib
import copy
import io
import json
import sqlite3
import unittest
import multiprocessing
import os
import stat
from hermes_cli import kanban_db as kb
from unittest.mock import patch
from tests.cron.local_observation_component_support import obs, native_component_scope
from tests.cron import local_observation_ledger_cases as baseline
from tests.cron import local_observation_retained_route as route

def gc_fixture(board, invocation=None):
    route.check_board(board,invocation)
    with baseline.fixture_db(board) as db:
        return kb.gc_events(db,older_than_seconds=1)

class StatusTests(unittest.TestCase):
    def setUp(self):
        self.f=baseline.LedgerRegression(); self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.target=baseline.A
        with baseline.fixture_db(self.f.board) as db:
            db.execute("UPDATE tasks SET status='archived' WHERE id=?",(self.target,))
            db.execute("INSERT INTO task_runs(id,task_id,profile,status,outcome,started_at,ended_at,summary) VALUES(1605,?,'fixture','done','completed',1,2,'historical planning only')",(self.target,))
            db.execute("INSERT INTO task_events(id,task_id,kind,created_at,run_id) VALUES(22645,?,'completed',2,1605)",(self.target,))
            db.execute("INSERT INTO task_events(id,task_id,kind,created_at) VALUES(38819,?,'archived',3)",(self.target,))
        self.before=self.f.read()
        self.sha=obs.read_ledger(self.f.directory)[1]
        for name in ('ack.json','local-observation-v1.json'):
            (self.f.directory/name).write_bytes(b'preserved independent plane')
        self.policy=None
        if hasattr(obs,'NATIVE_STATUS_POLICY'):
            with contextlib.closing(sqlite3.connect(self.f.board)) as db:
                db.row_factory=sqlite3.Row
                events=[dict(r) for r in db.execute('SELECT id,task_id,kind,payload,created_at,run_id FROM task_events WHERE task_id=? ORDER BY id',(self.target,))]
                run=dict(db.execute('SELECT * FROM task_runs WHERE id=1605').fetchone())
            st=self.f.board.stat()
            self.policy={**copy.deepcopy(obs.NATIVE_STATUS_POLICY),
                'board_path':str(self.f.board),'device':st.st_dev,'inode':st.st_ino,
                'root':self.target,'prior_sha':self.sha,'expires_at':2000,
                'events_sha':obs._digest(events),'event_rows':{str(e['id']):obs._digest(e) for e in events},
                'run_sha':obs._digest(run),'run_columns':list(run)}
            self.addCleanup(patch.stopall)
            patch.object(obs,'NATIVE_STATUS_POLICY',self.policy).start()
            patch.object(obs.time,'time',return_value=1000).start()

    def test_cli_archival_is_not_clearing(self):
        self.f.cli('reconcile-status',self.sha)
        actual=json.loads((self.f.directory/'ledger.json').read_bytes())
        self.assertEqual('archived',actual['roots'][self.target]['status'])
        root=actual['roots'][self.target]
        expected=copy.deepcopy(self.before)
        expected['roots'][self.target]['status']='archived'
        mutation=root['mutation_history'][-1]
        self.assertEqual([{'field':'status','previous':'triage','new':'archived'}],mutation['changes'])
        self.assertEqual('native-status-observer',mutation['actor'])
        expected['roots'][self.target]['mutation_history'].append(mutation)
        self.assertEqual(expected,actual)
        self.assertTrue(root['unknown']); self.assertIsNone(root['receipt'])
        for name in ('ack.json','local-observation-v1.json'):
            self.assertEqual(b'preserved independent plane',(self.f.directory/name).read_bytes())
        with baseline.fixture_db(self.f.board) as db:
            self.assertEqual('archived',db.execute('SELECT status FROM tasks WHERE id=?',(self.target,)).fetchone()[0])
        self.assertEqual(obs.ledger_due(self.before,self.sha,None,1000),
                         obs.ledger_due(actual,obs.read_ledger(self.f.directory)[1],None,1000))

    def apply(self, sha=None, board=None):
        return obs.reconcile_native_status(self.f.directory,board or self.f.board,sha or self.sha)

    def refused(self, text, action=None):
        before=(self.f.directory/'ledger.json').read_bytes()
        with self.assertRaisesRegex((obs.StaleAck,ValueError,RuntimeError),text):
            (action or self.apply)()
        self.assertEqual(before,(self.f.directory/'ledger.json').read_bytes())

    def test_real_source_denials(self):
        with baseline.fixture_db(self.f.board) as db:
            db.row_factory=sqlite3.Row
            originals={table:[dict(r) for r in db.execute('SELECT * FROM '+table)]
                       for table in ('tasks','task_runs','task_events')}
        changes=[("INSERT INTO task_events(task_id,kind,created_at) VALUES(?, 'heartbeat',4)",(self.target,),'native-revision-changed'),
                 ("UPDATE task_runs SET summary='changed' WHERE id=1605",(),'native-task-run-mismatch'),
                 ("UPDATE task_runs SET metadata='{}' WHERE id=1605",(),'native-task-run-mismatch'),
                 ("DELETE FROM task_events WHERE id=22645",(),'native-history-incomplete'),
                 ("DELETE FROM task_runs WHERE id=1605",(),'native-task-run-mismatch'),
                 ("DELETE FROM tasks WHERE id=?",(self.target,),'native-task-run-mismatch'),
                 ("UPDATE tasks SET status='done' WHERE id=?",(self.target,),'native-task-run-mismatch'),
                 ("UPDATE task_events SET payload='changed' WHERE id=38819",(),'native-revision-changed')]
        for sql,args,message in changes:
            with self.subTest(sql=sql):
                with baseline.fixture_db(self.f.board) as db: db.execute(sql,args)
                self.refused(message)
                with baseline.fixture_db(self.f.board) as db:
                    for table,rows in originals.items():
                        db.execute('DELETE FROM '+table)
                        for row in rows:
                            columns=','.join(row)
                            db.execute('INSERT INTO '+table+' ('+columns+') VALUES ('+
                                       ','.join('?' for key in row)+')',list(row.values()))

    def test_native_attachments_and_schema_drift_refuse(self):
        with baseline.fixture_db(self.f.board) as db:
            db.execute('INSERT INTO task_attachments(task_id,filename,content_type,size,stored_path,uploaded_by,created_at) VALUES(?,?,?,?,?,?,?)',
                       (self.target,'synthetic.txt','text/plain',0,'synthetic','fixture',1))
        self.refused('native-attachments-present')
        with baseline.fixture_db(self.f.board) as db:
            db.execute('DELETE FROM task_attachments')
            db.execute('ALTER TABLE task_events ADD COLUMN unsupported TEXT')
        self.refused('native-schema-mismatch')

    def test_wrong_path_inode_symlink_and_unrelated_root(self):
        other=self.f.root/'other.db'; other.write_bytes(self.f.board.read_bytes())
        self.refused('board-identity-mismatch',lambda:self.apply(board=other))
        link=self.f.root/'linked.db'; link.symlink_to(self.f.board)
        self.refused('symlinked',lambda:self.apply(board=link))
        original=self.f.board.with_suffix('.original'); self.f.board.rename(original)
        self.f.board.write_bytes(original.read_bytes())
        self.refused('board-identity-mismatch')
        self.f.board.unlink(); original.rename(self.f.board)
        wrong=copy.deepcopy(self.before); wrong['roots'][self.target]['status']='done'
        self.f.save(wrong); self.refused('prior-mismatch')
        self.f.save(self.before)
        self.refused('prior-mismatch',lambda:self.apply('0'*64))
        self.assertEqual('status-observed',self.apply()['outcome'])

    def test_gc_fixture_rejects_outside_before_native_open(self):
        before=self.f.board.read_bytes()
        with self.assertRaisesRegex(ValueError,'outside'):
            gc_fixture(self.f.board,self.f.root/'narrow-invocation')
        self.assertEqual(before,self.f.board.read_bytes())

    def test_real_gc_diagnostics_and_horizon(self):
        self.assertEqual(2,gc_fixture(self.f.board))
        self.refused('native-history-incomplete.*pruning-compatible')
        with baseline.fixture_db(self.f.board) as db:
            db.execute("INSERT INTO task_events(task_id,kind,created_at) VALUES(?,'heartbeat',1000)",(self.target,))
        self.refused('native-history-incomplete.*native-revision-changed')
        with patch.object(obs.time,'time',return_value=2000):
            self.refused('policy-window-expired')

    def test_replay_currentness_expiry_and_preserved_state(self):
        with patch.object(obs.time,'time',return_value=1999):
            result=self.apply(); self.assertEqual('status-observed',result['outcome'])
        before=(self.f.directory/'ledger.json').read_bytes()
        self.assertEqual('already-observed',self.apply()['outcome'])
        self.assertEqual(before,(self.f.directory/'ledger.json').read_bytes())
        with patch.object(obs.time,'time',return_value=2000):
            self.refused('policy-window-expired')
        self.f.cli('ledger',self.write_candidate(self.f.changed()),obs.read_ledger(self.f.directory)[1])
        self.refused('replay-mismatch')

    def write_candidate(self,value):
        path=self.f.root/'candidate.json'; path.write_bytes(obs._bytes(value)); return str(path)

    def test_ordinary_forged_status_provenance_stays_frozen(self):
        self.apply(); trusted=self.f.read()
        self.f.save(self.before)
        self.refused('provisional root',lambda:self.f.submit(trusted))
        self.assertEqual('status-observed',self.apply()['outcome'])

    def test_board_writer_before_fence_refuses(self):
        ctx=multiprocessing.get_context('fork')
        done=ctx.Event()
        def writer():
            with baseline.fixture_db(self.f.board) as db:
                db.execute('BEGIN IMMEDIATE')
                db.execute("INSERT INTO task_events(task_id,kind,created_at) VALUES(?,'heartbeat',4)",(self.target,))
            done.set()
        child=ctx.Process(target=writer); child.start()
        self.assertTrue(done.wait(10)); child.join(10); self.assertEqual(0,child.exitcode)
        self.refused('native-revision-changed')

    def test_fence_excludes_board_writer_until_durable_readback(self):
        ctx=multiprocessing.get_context('fork'); held=ctx.Event(); release=ctx.Event(); results=ctx.Queue()
        original=obs.read_ledger
        def owner():
            def read(directory):
                value=original(directory)
                if value[0]['roots'][self.target]['status']=='archived':
                    held.set()
                    if not release.wait(10): raise RuntimeError('barrier timeout')
                return value
            try:
                with patch.object(obs,'read_ledger',side_effect=read): results.put(self.apply()['outcome'])
            except Exception as exc: results.put(repr(exc))
        child=ctx.Process(target=owner); child.start()
        try:
            self.assertTrue(held.wait(10))
            with contextlib.closing(sqlite3.connect(self.f.board,timeout=0)) as writer:
                writer.execute('PRAGMA busy_timeout=0')
                with self.assertRaises(sqlite3.OperationalError) as caught: writer.execute('BEGIN IMMEDIATE')
                self.assertEqual(sqlite3.SQLITE_BUSY,caught.exception.sqlite_errorcode)
                self.assertEqual('archived',self.f.read()['roots'][self.target]['status'])
                release.set(); child.join(10); self.assertEqual(0,child.exitcode)
                self.assertEqual('status-observed',results.get(timeout=2))
                writer.execute('BEGIN IMMEDIATE')
                writer.execute("INSERT INTO task_events(task_id,kind,created_at) VALUES(?,'heartbeat',4)",(self.target,)); writer.commit()
            self.refused('native-revision-changed')
        finally:
            release.set()
            if child.is_alive(): child.terminate()
            child.join(10)

    def test_competing_ledger_mutations_one_winner(self):
        ctx=multiprocessing.get_context('fork'); start=ctx.Event(); results=ctx.Queue()
        def writer(label):
            candidate=self.f.changed('phase',label)
            if not start.wait(10): return
            try:
                obs.commit_ledger(self.f.directory,candidate,self.sha)
                results.put('ok')
            except obs.StaleAck: results.put('stale')
        children=[ctx.Process(target=writer,args=(label,)) for label in ('one','two')]
        for child in children: child.start()
        start.set()
        answers=[results.get(timeout=10) for child in children]
        for child in children: child.join(10); self.assertEqual(0,child.exitcode)
        self.assertEqual(['ok','stale'],sorted(answers))
        actual=self.f.read(); self.assertIn(actual['roots'][self.target]['phase'],('one','two'))
        self.assertEqual(len(self.before['roots'][self.target]['mutation_history'])+1,
                         len(actual['roots'][self.target]['mutation_history']))

    def replay_during_ledger_validation(self, validated_at):
        assert self.policy is not None
        self.assertEqual('status-observed',self.apply()['outcome'])
        before=(self.f.directory/'ledger.json').read_bytes()
        real_read=obs.read_ledger
        clock={'now':1999}
        reads=[]
        output=io.StringIO()
        def read_then_advance(directory):
            result=real_read(directory)
            reads.append(result[1])
            clock['now']=validated_at
            return result
        with patch.object(obs.time,'time',side_effect=lambda:clock['now']), \
             patch.object(obs,'read_ledger',side_effect=read_then_advance), \
             contextlib.redirect_stdout(output):
            if validated_at < self.policy['expires_at']:
                result=self.apply()
                self.assertEqual('already-observed',result['outcome'])
                self.assertEqual(reads[-1],result['old_sha'])
                self.assertEqual(result['old_sha'],result['new_sha'])
            else:
                with self.assertRaisesRegex(obs.StaleAck,'policy-window-expired'):
                    self.apply()
        self.assertEqual(1,len(reads))
        self.assertEqual(validated_at,clock['now'])
        self.assertEqual('',output.getvalue())
        self.assertEqual(before,(self.f.directory/'ledger.json').read_bytes())
        self.assertEqual(len(self.before['roots'][self.target]['mutation_history'])+1,
                         len(self.f.read()['roots'][self.target]['mutation_history']))

    def test_replay_just_before_cutoff_after_ledger_validation(self):
        self.replay_during_ledger_validation(1999)

    def test_replay_crossing_cutoff_during_ledger_validation_refuses(self):
        self.replay_during_ledger_validation(2000)

    def test_horizon_crossing_before_replace_refuses(self):
        with patch.object(obs.time,'time',side_effect=[1999,2000,2000]):
            self.refused('policy-window-expired')

    def test_faults_and_uncertain_old_sha_replay(self):
        real_fsync=obs.os.fsync
        real_read=obs.read_ledger
        for point in ('temp','file-fsync','replace','directory-fsync','readback'):
            with self.subTest(point=point):
                self.f.save(self.before)
                def fsync(fd):
                    is_directory=stat.S_ISDIR(os.fstat(fd).st_mode)
                    if (point=='directory-fsync' and is_directory) or (point=='file-fsync' and not is_directory):
                        raise OSError('fixture fsync failure')
                    return real_fsync(fd)
                def read(directory):
                    result=real_read(directory)
                    if point=='readback' and result[0]['roots'][self.target]['status']=='archived':
                        raise OSError('fixture readback failure')
                    return result
                with contextlib.ExitStack() as stack:
                    stack.enter_context(patch.object(obs.os,'fsync',side_effect=fsync))
                    stack.enter_context(patch.object(obs,'read_ledger',side_effect=read))
                    if point=='temp': stack.enter_context(patch.object(obs.tempfile,'mkstemp',side_effect=OSError('fixture temp failure')))
                    if point=='replace': stack.enter_context(patch.object(obs.os,'replace',side_effect=OSError('fixture replace failure')))
                    with self.assertRaises((OSError,RuntimeError)):
                        self.f.cli('reconcile-status',self.sha)
                actual=self.f.read()
                if point in ('directory-fsync','readback'):
                    self.assertEqual('archived',actual['roots'][self.target]['status'])
                    self.assertEqual('already-observed',self.apply()['outcome'])
                    self.assertEqual(len(self.before['roots'][self.target]['mutation_history'])+1,
                                     len(self.f.read()['roots'][self.target]['mutation_history']))
                else: self.assertEqual(self.before,actual)

    def test_process_loss_before_and_after_replace_then_fresh_retry(self):
        ctx=multiprocessing.get_context('fork')
        for after in (False,True):
            with self.subTest(after=after):
                self.f.save(self.before)
                real_replace=obs.os.replace
                reached=ctx.Event()
                def crashing():
                    def replace(source,dest):
                        if after: real_replace(source,dest)
                        reached.set()
                        os._exit(77)
                    with patch.object(obs.os,'replace',side_effect=replace): self.apply()
                child=ctx.Process(target=crashing); child.start()
                self.assertTrue(reached.wait(10)); child.join(10); self.assertEqual(77,child.exitcode)
                observed=self.f.read()
                self.assertEqual('archived' if after else 'triage',observed['roots'][self.target]['status'])
                results=ctx.Queue()
                def retry(): results.put(self.apply())
                child=ctx.Process(target=retry); child.start(); answer=results.get(timeout=10)
                child.join(10); self.assertEqual(0,child.exitcode)
                self.assertEqual('already-observed' if after else 'status-observed',answer['outcome'])
                self.assertEqual(len(self.before['roots'][self.target]['mutation_history'])+1,
                                 len(self.f.read()['roots'][self.target]['mutation_history']))

    def test_invalid_ledger_never_evicts_or_writes(self):
        path=self.f.directory/'ledger.json'
        for raw in (b'{',b'[]',b'x'*(obs.MAX_LEDGER_BYTES+1)):
            with self.subTest(raw_size=len(raw)):
                path.write_bytes(raw)
                with self.assertRaises((ValueError,TypeError)): self.apply()
                self.assertEqual(raw,path.read_bytes())
        path.unlink(); other=self.f.root/'ledger-target'; other.write_bytes(obs._bytes(self.before)); path.symlink_to(other)
        with self.assertRaisesRegex(ValueError,'symlink'): self.apply()
        self.assertEqual(obs._bytes(self.before),other.read_bytes())

if __name__=='__main__': unittest.main()
