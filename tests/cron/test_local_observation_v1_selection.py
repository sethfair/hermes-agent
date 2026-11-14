import hashlib
import json
import os
import pathlib
import sqlite3
import stat
import tempfile
import unittest
from contextlib import closing
from unittest.mock import patch

from tests.cron.local_observation_component_support import obs, native_component_scope
from tests.cron import local_observation_v1_cases as baseline
A, B, ORPHAN = baseline.A, baseline.B, baseline.ORPHAN

class SelectionTests(unittest.TestCase):
    def setUp(self):
        baseline.IssuedTests.setUp(self)
        self.output = self.dir / 'scratch'
        self.output.mkdir(mode=0o700)
        self.board = self.dir / 'board.db'
        from hermes_cli import kanban_db as kb
        with closing(sqlite3.connect(self.board)) as db:
            db.executescript(kb.SCHEMA_SQL)
            for root in (A, B):
                db.execute('INSERT INTO tasks(id,title,body,assignee,status,created_at) VALUES(?,?,?,?,?,?)',
                           (root, 'WRI-2276 fixture', '', 'fixture', 'blocked', 1))
            db.commit()
        self.snapshot = lambda roots: obs.board_observation(self.board, roots)

    def issue_current(self, roots=(A, B)):
        obs.begin_native_prerun(self.dir)
        return obs.commit_native_prerun(self.dir, self.ledger_sha, self.snapshot, set(roots))

    def select(self, sidecar, targets=None, ledger=None):
        return obs.select_local(self.dir, self.board, sidecar,
                                ledger or self.ledger_sha, targets=targets,
                                output_dir=self.output)

    def assert_file(self, answer, expected):
        self.assertEqual('selected', answer['status'])
        path = pathlib.Path(answer['selection_path'])
        data = path.read_bytes()
        self.assertEqual(expected, obs.parse_selection(data))
        self.assertEqual(hashlib.sha256(data).hexdigest(), answer['selection_sha256'])
        self.assertEqual(0o600, stat.S_IMODE(path.stat().st_mode))
        self.assertEqual(64, len(answer['expected_sidecar_sha']))
        return path

    def test_historical_orphan_excluded_and_two_serial_checks(self):
        _, _old = obs.observe_local(self.dir, self.ledger_sha, lambda roots: (
            {ORPHAN:'orphan'}, {ORPHAN:[1]}, set()), {ORPHAN})
        sidecar, current = self.issue_current()
        before = (self.dir / 'local-observation-v1.json').read_bytes()
        first = self.select(sidecar)
        path = self.assert_file(first, {A:current[A]})
        self.assertEqual(before, (self.dir / 'local-observation-v1.json').read_bytes())
        self.assertIn(ORPHAN, obs.read_local_observation(self.dir)[0]['pending'])
        sidecar = obs.local_check(self.dir, obs.parse_selection(path.read_bytes()), sidecar,
                                  self.ledger_sha, self.snapshot)
        second = self.select(sidecar)
        self.assert_file(second, {B:current[B]})
        sidecar = obs.local_check(self.dir, obs.parse_selection(pathlib.Path(second['selection_path']).read_bytes()),
                                  sidecar, self.ledger_sha, self.snapshot)
        with self.assertRaises((obs.StaleAck, ValueError)):
            obs.local_check(self.dir, obs.parse_selection(path.read_bytes()), sidecar,
                            self.ledger_sha, self.snapshot)
        self.assertEqual('none', self.select(sidecar)['status'])

    def test_invalidating_and_missing_admission_never_idle(self):
        sidecar, _ = self.issue_current(())
        self.assertEqual('none', self.select(sidecar)['status'])
        sidecar = obs.begin_native_prerun(self.dir)
        self.assertEqual('stale', self.select(sidecar)['status'])
        path = self.dir / 'local-observation-v1.json'
        state = json.loads(path.read_bytes()); state.pop('admission_state')
        path.write_bytes(obs._bytes(state))
        self.assertEqual('stale', self.select(obs.read_local_observation(self.dir)[1])['status'])

    def test_explicit_targets_atomic_and_malformed(self):
        sidecar, current = self.issue_current()
        self.assert_file(self.select(sidecar, [B, A]), {A:current[A], B:current[B]})
        for targets in ([ORPHAN], [A, ORPHAN], [A, A], ['bad'], []):
            with self.subTest(targets=targets), self.assertRaises((ValueError, obs.StaleAck)):
                self.select(sidecar, targets)
        for digest in ('x', 'a'*63):
            with self.assertRaises(ValueError):
                self.select(digest)

    def test_stale_ledger_and_sidecar_or_reissue(self):
        sidecar, _ = self.issue_current()
        with self.assertRaises(obs.StaleAck):
            self.select(sidecar, ledger='a'*64)
        with self.assertRaises(obs.StaleAck):
            self.select('a'*64)
        obs.begin_native_prerun(self.dir)
        self.assertEqual('stale', self.select(obs.read_local_observation(self.dir)[1])['status'])

    def test_attachment_missing_changed_and_heartbeat(self):
        sidecar, current = self.issue_current()
        with closing(sqlite3.connect(self.board)) as db:
            db.execute('DELETE FROM tasks WHERE id=?', (A,))
            db.commit()
        self.assert_file(self.select(sidecar), {B:current[B]})
        with self.assertRaises((obs.StaleAck, ValueError)):
            self.select(sidecar, [A, B])
        with closing(sqlite3.connect(self.board)) as db:
            db.execute('INSERT INTO task_events(task_id,kind,created_at) VALUES(?,?,?)',
                       (B, 'commented', 3))
            db.commit()
        self.assertEqual('none', self.select(sidecar)['status'])
        with closing(sqlite3.connect(self.board)) as db:
            db.execute('DELETE FROM task_events WHERE task_id=?', (B,))
            db.execute('INSERT INTO task_events(task_id,kind,created_at) VALUES(?,?,?)',
                       (B, 'heartbeat', 4))
            db.commit()
        self.assert_file(self.select(sidecar), {B:current[B]})

    def test_output_symlink_and_failed_write_no_offer(self):
        sidecar, _ = self.issue_current()
        link = self.dir / 'linked'; link.symlink_to(self.output, target_is_directory=True)
        with self.assertRaises(ValueError):
            obs.select_local(self.dir, self.board, sidecar, self.ledger_sha, output_dir=link)
        before = set(self.output.iterdir())
        with patch.object(obs.os, 'fsync', side_effect=OSError('disk failure')):
            with self.assertRaises(OSError):
                self.select(sidecar)
        self.assertEqual(before, set(self.output.iterdir()))

    def test_consumed_reissuance_not_selected(self):
        sidecar, current = self.issue_current((A,))
        obs.local_check(self.dir, current, sidecar, self.ledger_sha, self.snapshot)
        sidecar, _ = self.issue_current((A,))
        self.assertEqual('none', self.select(sidecar)['status'])

    def test_due_order_and_attachment_at_issuance(self):
        ledger = json.loads((self.dir / 'ledger.json').read_bytes())
        ledger['roots'][B] = {'unknown':True, 'sources':{'external_sources':{}}, 'due_at':1}
        (self.dir / 'ledger.json').write_bytes(obs._bytes(ledger))
        self.ledger_sha = obs.read_ledger(self.dir)[1]
        sidecar, issued = self.issue_current()
        self.assert_file(self.select(sidecar), {B:issued[B]})
        state, _ = obs.read_local_observation(self.dir)
        state['pending'][B]['attachment_bearing'] = True
        (self.dir / 'local-observation-v1.json').write_bytes(obs._bytes(state))
        sidecar = obs.read_local_observation(self.dir)[1]
        self.assert_file(self.select(sidecar), {A:issued[A]})

    def test_cli_v2_cutover_and_target_route(self):
        sidecar, _issued = self.issue_current()
        profile = self.dir / 'profile'
        directory = profile / 'cron' / f'recovery-{obs.JOB_ID}'
        directory.mkdir(parents=True)
        (directory / 'ledger.json').write_bytes((self.dir / 'ledger.json').read_bytes())
        with patch.object(obs, '_checked_home', return_value=profile), \
             patch.object(obs, 'select_local', return_value={'status':'none'}) as selector:
            with self.assertRaises(RuntimeError):
                obs.main(['local-select', self.ledger_sha, sidecar])
            obs.commit_cutover_marker(directory, self.ledger_sha, 0, 'missing')
            from io import StringIO
            with patch('sys.stdout', new_callable=StringIO) as output:
                obs.main(['local-select', self.ledger_sha, sidecar, A])
            self.assertEqual('none', json.loads(output.getvalue())['status'])
            self.assertEqual([A], selector.call_args.kwargs['targets'])
            self.assertEqual(sidecar, selector.call_args.args[2])

    def test_failure_does_not_advance_ledger_or_ack(self):
        sidecar, _issued = self.issue_current()
        ledger_before = (self.dir/'ledger.json').read_bytes()
        ack_before = obs.read_ack(self.dir)[1]
        with patch.object(obs, 'board_snapshot', side_effect=OSError('board unavailable')):
            with self.assertRaises(OSError):
                self.select(sidecar)
        self.assertEqual(ledger_before, (self.dir/'ledger.json').read_bytes())
        self.assertEqual(ack_before, obs.read_ack(self.dir)[1])
        self.assertEqual({}, obs.read_local_observation(self.dir)[0]['roots'])

if __name__ == '__main__':
    unittest.main()
