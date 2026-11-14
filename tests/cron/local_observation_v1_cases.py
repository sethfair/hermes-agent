import json
import multiprocessing
import pathlib
import sqlite3
import tempfile
import unittest
from contextlib import closing
from unittest.mock import patch

from tests.cron.local_observation_component_support import obs, component_temporary_directory

A, B, ORPHAN = 't_11111111', 't_22222222', 't_33333333'

def competing_check(directory, selection, sidecar_sha, ledger_sha, start, results):
    start.wait()
    try:
        result = obs.local_check(directory, selection, sidecar_sha, ledger_sha,
            lambda roots: ({r: 'fingerprint-' + r for r in roots},
                           {r: [1] for r in roots}, set()))
        results.put(('ok', result))
    except Exception as exc:
        results.put((type(exc).__name__, str(exc)))


class IssuedTests(unittest.TestCase):
    def setUp(self):
        self.tmp = component_temporary_directory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = pathlib.Path(self.tmp.name)
        ledger = {'version': 2, 'board': obs.BOARD, 'job': obs.JOB_ID,
                  'legacy_raw': {}, 'roots': {}}
        (self.dir / 'ledger.json').write_bytes(obs._bytes(ledger))
        self.ledger_sha = obs.read_ledger(self.dir)[1]
        self.state = {root: ('fingerprint-' + root, [1]) for root in (A, B, ORPHAN)}

    def snapshot(self, roots):
        return ({r: self.state[r][0] for r in roots if r in self.state},
                {r: self.state[r][1] for r in roots if r in self.state}, set())

    def issue(self, roots):
        obs.begin_native_prerun(self.dir)
        return obs.commit_native_prerun(self.dir, self.ledger_sha, self.snapshot, set(roots))

    def test_current_issue_excludes_historical_pending_and_retains_second_pair(self):
        _, orphan = obs.observe_local(self.dir, self.ledger_sha, self.snapshot, {ORPHAN})
        sha, current = self.issue([A, B])
        before = (self.dir / 'local-observation-v1.json').read_bytes()
        with self.assertRaises((ValueError, obs.StaleAck)):
            obs.local_check(self.dir, orphan, sha, self.ledger_sha, self.snapshot)
        self.assertEqual(before, (self.dir / 'local-observation-v1.json').read_bytes())
        sha = obs.local_check(self.dir, {A: current[A]}, sha, self.ledger_sha, self.snapshot)
        sha = obs.local_check(self.dir, {B: current[B]}, sha, self.ledger_sha, self.snapshot)
        with self.assertRaises((ValueError, obs.StaleAck)):
            obs.local_check(self.dir, {A: current[A]}, sha, self.ledger_sha, self.snapshot)

    def test_second_idle_prerun_revokes_prior_pair(self):
        sha, current = self.issue([A])
        # This is the idle/zero-selectable native admission path, not a historical observe-local call.
        obs.begin_native_prerun(self.dir)
        sha, issued = obs.commit_native_prerun(self.dir, self.ledger_sha, self.snapshot, set())
        self.assertEqual({}, issued)
        before = (self.dir / 'local-observation-v1.json').read_bytes()
        with self.assertRaises((ValueError, obs.StaleAck)):
            obs.local_check(self.dir, current, sha, self.ledger_sha, self.snapshot)
        self.assertEqual(before, (self.dir / 'local-observation-v1.json').read_bytes())

    def test_stale_and_attachment_multi_selection_is_atomic(self):
        sha, current = self.issue([A, B])
        self.state[B] = ('changed', [1, 2])
        before = (self.dir / 'local-observation-v1.json').read_bytes()
        with self.assertRaises(obs.StaleAck):
            obs.local_check(self.dir, current, sha, self.ledger_sha, self.snapshot)
        self.assertEqual(before, (self.dir / 'local-observation-v1.json').read_bytes())
        self.state[B] = ('fingerprint-' + B, [1])
        sha = obs.local_check(self.dir, current, sha, self.ledger_sha, self.snapshot)
        self.assertEqual({A, B}, set(obs.read_local_observation(self.dir)[0]['roots']))

    def test_invalid_selection_and_failed_entry_preserve_history(self):
        sha, current = self.issue([A])
        for invalid in ({'bad': current[A]}, {A: 'x'}, {A: 3}, [], {}):
            with self.subTest(invalid=invalid), self.assertRaises((ValueError, obs.StaleAck)):
                obs.local_check(self.dir, invalid, sha, self.ledger_sha, self.snapshot)
        obs.begin_native_prerun(self.dir)
        state, sha = obs.read_local_observation(self.dir)
        self.assertIn(A, state['pending'])
        with self.assertRaises((ValueError, obs.StaleAck)):
            obs.local_check(self.dir, current, sha, self.ledger_sha, self.snapshot)


    def test_native_path_on_real_isolated_board_and_idle(self):
        from hermes_cli import kanban_db as kb
        board, native = self.dir / 'board.db', self.dir / 'notepad.db'
        with closing(sqlite3.connect(board)) as db:
            db.executescript(kb.SCHEMA_SQL)
            db.execute("INSERT INTO tasks(id,title,body,assignee,status,created_at) VALUES(?,?,?,?,?,?)",
                       (A, 'WRI-2276 fixture', '', 'fixture', 'blocked', 1))
            db.commit()
        with closing(sqlite3.connect(native)) as db:
            db.execute('CREATE TABLE cron_notepad(job_id TEXT,key TEXT,value TEXT,updated_at TEXT)')
            db.commit()
        obs.commit_cutover_marker(self.dir, self.ledger_sha, 0, 'missing')
        result = obs.native_prerun(self.dir, native, board)
        self.assertTrue(result['wakeAgent'])
        issued = result['context']['pending']
        self.assertIn(A, issued)
        current, sha = obs.read_local_observation(self.dir)
        self.assertEqual(issued, current['current_issued'])
        with closing(sqlite3.connect(board)) as db:
            db.execute("INSERT INTO task_events(task_id,kind,created_at) VALUES(?,?,?)",
                       (A, 'heartbeat', 2))
            db.commit()
        sha = obs.local_check(self.dir, issued, sha, self.ledger_sha,
                              lambda roots: obs.board_observation(board, roots))
        with closing(sqlite3.connect(board)) as db:
            db.execute("INSERT INTO task_events(task_id,kind,created_at) VALUES(?,?,?)",
                       (A, 'commented', 3))
            db.commit()
        result = obs.native_prerun(self.dir, native, board)
        issued = result['context']['pending']
        _, sha = obs.read_local_observation(self.dir)
        with closing(sqlite3.connect(board)) as db:
            db.execute("INSERT INTO task_events(task_id,kind,created_at) VALUES(?,?,?)",
                       (A, 'commented', 4))
            db.commit()
        before = (self.dir / 'local-observation-v1.json').read_bytes()
        with self.assertRaises(obs.StaleAck):
            obs.local_check(self.dir, issued, sha, self.ledger_sha,
                            lambda roots: obs.board_observation(board, roots))
        self.assertEqual(before, (self.dir / 'local-observation-v1.json').read_bytes())
        # Another native observation supersedes the stale identity.
        result = obs.native_prerun(self.dir, native, board)
        issued = result['context']['pending']
        _, sha = obs.read_local_observation(self.dir)
        sha = obs.local_check(self.dir, issued, sha, self.ledger_sha,
                              lambda roots: obs.board_observation(board, roots))
        result = obs.native_prerun(self.dir, native, board)
        self.assertFalse(result['wakeAgent'])
        current, _ = obs.read_local_observation(self.dir)
        self.assertEqual({}, current['current_issued'])

    def test_duplicate_json_and_malformed_admission_rejected(self):
        ident = 'a' * 64
        with self.assertRaises(ValueError):
            obs.parse_selection(('{' + json.dumps(A) + ':' + json.dumps(ident) + ',' +
                                 json.dumps(A) + ':' + json.dumps(ident) + '}').encode())
        for bad in (b'[]', b'{}', b'{"t_11111111":"x"}', b'{"oops":12}'):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                obs.parse_selection(bad)
        self.issue([A])
        path = self.dir / 'local-observation-v1.json'
        value = json.loads(path.read_bytes())
        value['current_issued'] = {B: ident}
        path.write_bytes(obs._bytes(value))
        with self.assertRaises(ValueError):
            obs.read_local_observation(self.dir)

    def test_attached_missing_and_heartbeat_boundary(self):
        sha, current = self.issue([A, B])
        before = (self.dir / 'local-observation-v1.json').read_bytes()
        for snapshot in (
            lambda roots: (dict((r, self.state[r][0]) for r in roots),
                           dict((r, self.state[r][1]) for r in roots), {B}),
            lambda roots: ({A: self.state[A][0]}, {A: self.state[A][1]}, set()),
        ):
            with self.assertRaises(obs.StaleAck):
                obs.local_check(self.dir, current, sha, self.ledger_sha, snapshot)
            self.assertEqual(before, (self.dir / 'local-observation-v1.json').read_bytes())
        # Heartbeat does not alter fingerprint/non-heartbeat IDs in the native reader.
        obs.local_check(self.dir, current, sha, self.ledger_sha, self.snapshot)

    def test_failure_after_replace_revokes_admission_without_losing_pending(self):
        self.issue([A])
        obs.begin_native_prerun(self.dir)
        original = obs._replace_local
        seen = [False]
        def replaced_then_failed(directory, value):
            result = original(directory, value)
            if not seen[0]:
                seen[0] = True
                raise OSError('simulated post-replace failure')
            return result
        with patch.object(obs, '_replace_local', side_effect=replaced_then_failed):
            with self.assertRaises(OSError):
                obs.commit_native_prerun(self.dir, self.ledger_sha, self.snapshot, {B})
        sidecar, sha = obs.read_local_observation(self.dir)
        self.assertEqual({}, sidecar['current_issued'])
        self.assertEqual('invalidating', sidecar['admission_state'])
        self.assertIn(A, sidecar['pending'])
        self.assertIn(B, sidecar['pending'])
        with self.assertRaises((ValueError, obs.StaleAck)):
            obs.local_check(self.dir, {B: sidecar['pending'][B]['observation_id']},
                            sha, self.ledger_sha, self.snapshot)

    def test_preflight_failure_and_reissue_revokes_prior_identity(self):
        sha, old = self.issue([A])
        obs.begin_native_prerun(self.dir)
        with self.assertRaises(obs.StaleAck):
            obs.commit_native_prerun(self.dir, '0'*64, self.snapshot, {A})
        sidecar, sha = obs.read_local_observation(self.dir)
        self.assertEqual({}, sidecar['current_issued'])
        with self.assertRaises((ValueError, obs.StaleAck)):
            obs.local_check(self.dir, old, sha, self.ledger_sha, self.snapshot)
        sha, fresh = obs.commit_native_prerun(self.dir, self.ledger_sha, self.snapshot, {A})
        self.assertNotEqual(old, fresh)
        with self.assertRaises((ValueError, obs.StaleAck)):
            obs.local_check(self.dir, old, sha, self.ledger_sha, self.snapshot)
        obs.local_check(self.dir, fresh, sha, self.ledger_sha, self.snapshot)

    def test_two_process_checks_cas_one_winner(self):
        sha, issued = self.issue([A])
        start, results = multiprocessing.Event(), multiprocessing.Queue()
        workers = [multiprocessing.Process(target=competing_check,
                   args=(self.dir, issued, sha, self.ledger_sha, start, results))
                   for _ in range(2)]
        for worker in workers:
            worker.start()
        start.set()
        outcomes = [results.get(timeout=10)[0] for _ in workers]
        for worker in workers:
            worker.join(timeout=10)
            self.assertEqual(0, worker.exitcode)
        self.assertEqual(['StaleAck', 'ok'], sorted(outcomes))
        self.assertEqual({A}, set(obs.read_local_observation(self.dir)[0]['roots']))

    def test_missing_cutover_native_failure_keeps_legacy_pending_but_denies_check(self):
        _, old = self.issue([A])
        ledger_before = (self.dir / 'ledger.json').read_bytes()
        # Native entry must revoke before the cutover and notepad preflight fails.
        with self.assertRaises(RuntimeError):
            obs.native_prerun(self.dir, self.dir / 'missing-notepad.db',
                              self.dir / 'missing-board.db')
        sidecar, sha = obs.read_local_observation(self.dir)
        self.assertEqual({}, sidecar['current_issued'])
        self.assertEqual('invalidating', sidecar['admission_state'])
        self.assertIn(A, sidecar['pending'])
        self.assertEqual(ledger_before, (self.dir / 'ledger.json').read_bytes())
        with self.assertRaises((obs.StaleAck, ValueError)):
            obs.local_check(self.dir, old, sha, self.ledger_sha, self.snapshot)


if __name__ == '__main__':
    unittest.main()
