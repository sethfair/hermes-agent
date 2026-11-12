"""C0: characterized installed writer routes; all storage is disposable."""
import copy
import contextlib
import hashlib
import io
import json
import pathlib
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from hermes_cli import kanban_db as kb
from tests.cron.local_observation_component_support import obs, component_temporary_directory

A, B, MISSING = 't_11111111', 't_22222222', 't_33333333'

@contextlib.contextmanager
def fixture_db(path):
    with contextlib.closing(sqlite3.connect(path)) as db:
        with db:
            yield db

class LedgerRegression(unittest.TestCase):
    def setUp(self):
        self.tmp = component_temporary_directory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.home = self.root / 'profile'
        self.directory = self.home / 'cron' / ('recovery-' + obs.JOB_ID)
        self.directory.mkdir(parents=True)
        self.board = self.root / 'board.db'
        self.native = self.home / 'cron/notepad.db'
        with fixture_db(self.board) as db:
            db.executescript(kb.SCHEMA_SQL)
            for task in (A, B):
                db.execute('INSERT INTO tasks(id,title,body,assignee,status,created_at) VALUES(?,?,?,?,?,?)',
                           (task, 'WRI-2276 fixture', '', 'fixture', 'blocked', 1))
        with fixture_db(self.native) as db:
            db.execute('CREATE TABLE cron_notepad(job_id TEXT,key TEXT,value TEXT,updated_at TEXT)')
            for key, value in [('recovery_index', json.dumps({'version':1,'active':['recovery_root_'+A,'recovery_root_'+MISSING]})),
                               ('recovery_root_'+A, ' {"issue":"WRI-2276", "status":"triage", "due_at":0} '),
                               ('recovery_root_'+MISSING, 'not JSON: preserve raw')]:
                db.execute('INSERT INTO cron_notepad VALUES(?,?,?,?)',(obs.JOB_ID,key,value,'original'))
        self.seed = obs.migration_candidate(obs.read_legacy(self.native))
        for item in self.seed['roots'].values():
            item.update(receipt_history=[{'old':'receipt'}], source_history=[{'old':'source'}],
                        action_result_history=[{'old':'effect'}], mutation_history=[{'old':'mutation'}])
        self.save(self.seed)

    def save(self, value):
        obs._validate_ledger(value)
        (self.directory/'ledger.json').write_bytes(obs._bytes(value))
        return obs.read_ledger(self.directory)[1]

    def read(self):
        return json.loads((self.directory/'ledger.json').read_bytes())

    def changed(self, field='phase', value='characterized'):
        result=copy.deepcopy(self.read()); old=result['roots'][A].get(field)
        result['roots'][A][field]=value
        result['roots'][A].setdefault('mutation_history',[]).append({
            'changes':[{'field':field,'previous':old,'new':value}],
            'actor':'fixture','evidence_locator':'fixture update'})
        return result

    def cli(self, *args):
        path_factory=pathlib.Path
        def mapped(path):
            return self.board if str(path)=='/home/macpro/.hermes/kanban.db' else path_factory(path)
        real_connect=sqlite3.connect
        def connect(path,*a,**kw):
            raw=str(path).removeprefix('file:').split('?')[0]
            self.assertTrue(path_factory(raw).is_relative_to(self.root),raw)
            return real_connect(path,*a,**kw)
        with patch.object(obs,'_checked_home',return_value=self.home), \
             patch.object(obs.pathlib,'Path',side_effect=mapped), \
             patch.object(obs.sqlite3,'connect',side_effect=connect), \
             contextlib.redirect_stdout(io.StringIO()):
            return obs.main(list(args))

    def submit(self,candidate,sha=None):
        path=self.root/'candidate.json'; path.write_bytes(obs._bytes(candidate))
        self.cli('ledger',str(path),sha or obs.read_ledger(self.directory)[1])

    def test_ordinary_mutable_positive(self):
        candidate=self.changed()
        self.submit(candidate)
        self.assertEqual(candidate,self.read())

    def test_ordinary_denials_preserve_bytes(self):
        baseline=copy.deepcopy(self.seed)
        def revise(c):
            c['native_revisions']={'recovery_index':[self.revision('recovery_index','new')]}
        def raw(c):
            c['legacy_raw']['recovery_index']['updated_at']='changed'
        def sources(c):
            c['roots'][A]['sources']['external_sources']={}
        def archive(c):
            c['legacy_v1_ledger']={'version':1,'board':obs.BOARD,'job':obs.JOB_ID,'legacy_raw':{},'roots':{}}
            c['legacy_v1_sha']=obs._digest(c['legacy_v1_ledger'])
        def receipt(c): c['roots'][A]['receipt']={'forged':True}
        def cursor(c): c.pop('legacy_cursor_absent')
        edits=[lambda c:c.update(roots={}),raw,cursor,archive,revise,sources,receipt,
               lambda c:c['roots'][A].update(receipt_history=[]),
               lambda c:c['roots'][A].update(source_history=[]),
               lambda c:c['roots'][A].update(action_result_history=[]),
               lambda c:c['roots'][A].update(mutation_history=[]),
               lambda c:c['roots'][A].update(unknown=False)]
        for edit in edits:
            with self.subTest(edit=edit):
                self.save(baseline); before=(self.directory/'ledger.json').read_bytes()
                candidate=self.changed(); edit(candidate)
                with self.assertRaises((ValueError,obs.StaleAck,KeyError)): self.submit(candidate)
                self.assertEqual(before,(self.directory/'ledger.json').read_bytes())
        self.save(baseline)
        with self.assertRaises(obs.StaleAck): self.submit(self.changed(),'0'*64)
        self.assertEqual(baseline,self.read())

    def test_frozen_status_guard(self):
        before=(self.directory/'ledger.json').read_bytes()
        with self.assertRaisesRegex(ValueError,'provisional root'): self.submit(self.changed('status','archived'))
        self.assertEqual(before,(self.directory/'ledger.json').read_bytes())
        self.submit(self.changed())

    def test_new_root_positive_and_forged_history(self):
        c=copy.deepcopy(self.seed)
        c['roots'][B]={'unknown':True,'receipt':None,'sources':{'external_sources':{}}}
        self.submit(c); self.assertIn(B,self.read()['roots'])
        self.save(self.seed); c['roots'][B]['mutation_history']=[{'forged':True}]
        before=(self.directory/'ledger.json').read_bytes()
        with self.assertRaises(ValueError): self.submit(c)
        self.assertEqual(before,(self.directory/'ledger.json').read_bytes())

    def revision(self,key,value,prior=None):
        return {'key':key,'value_utf8_or_null':value,'present':isinstance(value,str),
                'sha256_or_absent':hashlib.sha256(value.encode()).hexdigest() if isinstance(value,str) else 'absent',
                'prior_revision_sha':prior or self.seed['legacy_raw'].get(key,{}).get('sha256','absent'),
                'updated_at_or_null':'new','observed_at':1,'evidence_locator':'fixture'}

    def test_native_revision_authority(self):
        c=copy.deepcopy(self.seed); c['native_revisions']={'recovery_index':[self.revision('recovery_index','new')]}
        obs._validate_ledger(c)
        before=(self.directory/'ledger.json').read_bytes()
        with self.assertRaisesRegex(ValueError,'trusted native read'): self.submit(c)
        self.assertEqual(before,(self.directory/'ledger.json').read_bytes())
        obs.commit_ledger(self.directory,c,obs.read_ledger(self.directory)[1],_lock_held=True)
        self.assertEqual(c,self.read())

    def test_revision_prefix_guard(self):
        first=self.revision('recovery_index','first')
        c=copy.deepcopy(self.seed); c['native_revisions']={'recovery_index':[first]}
        sha=self.save(c)
        c['native_revisions']['recovery_index']=[self.revision('recovery_index','different')]
        obs._validate_ledger(c)
        before=(self.directory/'ledger.json').read_bytes()
        with self.assertRaisesRegex(ValueError,'discard native revision'):
            obs.commit_ledger(self.directory,c,sha,_lock_held=True)
        self.assertEqual(before,(self.directory/'ledger.json').read_bytes())
        c['native_revisions']['recovery_index']=[first,self.revision('recovery_index','second',first['sha256_or_absent'])]
        obs.commit_ledger(self.directory,c,sha,_lock_held=True)
        self.assertEqual(c,self.read())

    def v1(self):
        return {'version':1,'board':obs.BOARD,'job':obs.JOB_ID,'legacy_raw':{},
                'roots':{B:{'unknown':True,'sources':{'odd-id':{'historical':True}},
                            'receipt_history':[{'old':1}]}}}

    def test_migration_archive_guard(self):
        v1=self.v1(); sha=self.save(v1)
        valid=obs.migration_candidate(obs.read_legacy(self.native),prior_ledger=v1,prior_sha=sha)
        forged=copy.deepcopy(valid)
        forged['legacy_v1_ledger']['roots'][B]['receipt_history']=[]
        forged['legacy_v1_sha']=obs._digest(forged['legacy_v1_ledger'])
        obs._validate_ledger(forged)
        before=(self.directory/'ledger.json').read_bytes()
        with self.assertRaisesRegex(ValueError,'entire predecessor'):
            obs.commit_ledger(self.directory,forged,sha,_migration=True)
        self.assertEqual(before,(self.directory/'ledger.json').read_bytes())
        for field in ('root','source'):
            bad=copy.deepcopy(valid)
            if field=='root': del bad['roots'][B]
            else: bad['roots'][B]['sources']['external_sources']={}
            with self.subTest(field=field), self.assertRaises(ValueError):
                obs.commit_ledger(self.directory,bad,sha,_migration=True)
            self.assertEqual(before,(self.directory/'ledger.json').read_bytes())
        obs.commit_ledger(self.directory,valid,sha,_migration=True)
        self.assertEqual(v1,self.read()['legacy_v1_ledger'])
        self.assertEqual(sha,self.read()['legacy_v1_sha'])
        self.assertIn('legacy-v1:'+b'odd-id'.hex(),self.read()['roots'][B]['sources']['external_sources'])

    def test_real_migrate_cli_absent_and_v1(self):
        for prior in (None,self.v1()):
            with self.subTest(prior=prior):
                for p in self.directory.glob('cutover*.json'): p.unlink()
                if prior is None:
                    (self.directory/'ledger.json').unlink(); sha='missing'
                else:
                    sha=self.save(prior)
                    obs.commit_cutover_marker(self.directory,sha,0,'missing')
                self.cli('migrate',sha)
                value=self.read()
                self.assertEqual(obs.read_legacy(self.native)['raw'],value['legacy_raw'])
                self.assertTrue(value['legacy_cursor_absent'])
                self.assertIn(MISSING,value['missing_retained'])
                self.assertTrue(value['roots'][MISSING]['unknown'])
                self.assertEqual(' {"issue":"WRI-2276", "status":"triage", "due_at":0} ',value['legacy_raw']['recovery_root_'+A]['value_utf8'])
                if prior: self.assertEqual(prior,value['legacy_v1_ledger'])
                marker_path=obs._cutover_path(self.directory,value)
                obs._verify_cutover(self.directory,value,marker_path,json.loads(marker_path.read_bytes()))

    def test_fresh_v2_requires_migration(self):
        (self.directory/'ledger.json').unlink()
        with self.assertRaisesRegex(ValueError,'trusted native migration'):
            obs.commit_ledger(self.directory,self.seed,'missing')
        obs.commit_ledger(self.directory,self.seed,'missing',_migration=True)
        self.assertEqual(self.seed,self.read())

    def test_real_notepad_changes_and_noop(self):
        original=copy.deepcopy(self.seed)
        sha=obs.read_ledger(self.directory)[1]
        self.assertEqual((sha,[]),obs.reconcile_native(self.directory,self.native,sha))
        for value in ('new value','changed value',None):
            with fixture_db(self.native) as db:
                db.execute('DELETE FROM cron_notepad WHERE key=?',('recovery_root_'+A,))
                if value is not None:
                    db.execute('INSERT INTO cron_notepad VALUES(?,?,?,?)',(obs.JOB_ID,'recovery_root_'+A,value,'later'))
            previous=self.read(); sha,changed=obs.reconcile_native(self.directory,self.native,sha)
            self.assertIn('recovery_root_'+A,changed)
            actual=self.read(); obs._validate_ledger(actual)
            self.assertEqual(original['legacy_raw'],actual['legacy_raw'])
            self.assertEqual(original['roots'],actual['roots'])
            old=previous.get('native_revisions',{}).get('recovery_root_'+A,[])
            self.assertEqual(old,actual['native_revisions']['recovery_root_'+A][:len(old)])
        with fixture_db(self.native) as db:
            db.execute('INSERT INTO cron_notepad VALUES(?,?,?,?)',(obs.JOB_ID,'recovery_root_'+B,'fresh','later'))
        sha,changed=obs.reconcile_native(self.directory,self.native,sha)
        self.assertIn(B,self.read()['roots'])
        self.assertTrue(self.read()['roots'][B]['unknown'])

    def test_native_snapshot_and_postcommit_source_races(self):
        original_read=obs.read_legacy
        for at_call in (2,3):
            with self.subTest(source_change_at_read=at_call):
                self.save(self.seed)
                with fixture_db(self.native) as db:
                    db.execute('UPDATE cron_notepad SET value=?,updated_at=? WHERE key=?',
                               ('first changed value','first','recovery_root_'+A))
                before=(self.directory/'ledger.json').read_bytes()
                calls=0
                def read_with_real_change(path):
                    nonlocal calls
                    calls+=1
                    if calls==at_call:
                        with fixture_db(self.native) as writer:
                            writer.execute('UPDATE cron_notepad SET value=?,updated_at=? WHERE key=?',
                                           ('second changed value','second','recovery_root_'+A))
                    return original_read(path)
                with patch.object(obs,'read_legacy',side_effect=read_with_real_change):
                    with self.assertRaisesRegex(obs.StaleAck,'during revision' if at_call==2 else 'after revision'):
                        obs.reconcile_native(self.directory,self.native,obs.read_ledger(self.directory)[1])
                if at_call==2:
                    self.assertEqual(before,(self.directory/'ledger.json').read_bytes())
                else:
                    actual=self.read()
                    self.assertEqual(self.seed['roots'],actual['roots'])
                    self.assertEqual('first changed value',actual['native_revisions']['recovery_root_'+A][-1]['value_utf8_or_null'])
                    _sha,changed=obs.reconcile_native(self.directory,self.native,obs.read_ledger(self.directory)[1])
                    self.assertIn('recovery_root_'+A,changed)
                    self.assertEqual('second changed value',self.read()['native_revisions']['recovery_root_'+A][-1]['value_utf8_or_null'])

    def test_native_prerun_shadow_missing_and_lost_ledger(self):
        (self.directory/'ledger.json').unlink(); self.cli('migrate','missing')
        for i in range(2):
            result=obs.native_prerun(self.directory,self.native,self.board)
            self.assertTrue(result['wakeAgent'])
            self.assertIn(MISSING+':missing-retained-board-card',result['context']['unknown'])
            state,_sha=obs.read_local_observation(self.directory)
            self.assertEqual(result['context']['ledger_sha'],state['ledger_sha'])
            self.assertIn(A,state['current_issued'])
        with fixture_db(self.native) as db:
            db.execute('UPDATE cron_notepad SET updated_at=? WHERE key=?',('changed','recovery_root_'+A))
        result=obs.native_prerun(self.directory,self.native,self.board)
        self.assertIn('native-change:recovery_root_'+A,result['context']['unknown'])
        (self.directory/'ledger.json').unlink()
        with self.assertRaisesRegex(RuntimeError,'ledger missing after cutover'):
            obs.native_prerun(self.directory,self.native,self.board)
        self.assertEqual({},obs.read_local_observation(self.directory)[0]['current_issued'])

if __name__=='__main__': unittest.main()
