import datetime
import fcntl
import json
import pathlib
import tempfile
import threading
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from tests.cron import local_observation_retained_installer as install
from tests.cron.local_observation_component_support import native_component_scope, component_temporary_directory

class InstallTests(unittest.TestCase):
    def setUp(self):
        self.tmp = component_temporary_directory()
        self.addCleanup(self.tmp.cleanup)
        d = pathlib.Path(self.tmp.name)
        self.source, self.jobs = d/'observer.py', d/'jobs.json'
        self.source.write_bytes(b'old-source')
        self.config = dict(id=install.JOB_ID, enabled=True, script='engineering_recovery_observer.py',
                           workdir='fixture', deliver='local', failure_deliver='bot-chat:fixture',
                           schedule={'kind':'interval','minutes':5}, provider=None)
        self.job = dict(self.config, prompt='old-prompt', fire_claim=None, pending_slot=None,
                        manual_run_at=None, next_run_at=(datetime.datetime.now(datetime.UTC)+
                        datetime.timedelta(minutes=10)).isoformat())
        self.other = dict(id='other', prompt='unrelated', last_status='ok')
        self.jobs.write_text(json.dumps({'jobs':[self.job, self.other]}))
        self.fire = True
        self.lock = True
        self.started = []
        @contextmanager
        def fire(job_id):
            self.started.append('fire')
            yield self.fire
        def flock(fd, timeout):
            self.started.append('strict-lock')
            if self.lock:
                fcntl.flock(fd, fcntl.LOCK_EX)
            return self.lock
        def release(fd):
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()
        self.module = SimpleNamespace(_fire_job_lock=fire, _jobs_file_lock=threading.RLock(),
                                      _acquire_flock=flock, _release_flock=release,
                                      _JOBS_LOCK_TIMEOUT_SECONDS=1)
    def act(self):
        return install.install(self.source, self.jobs, b'new-source', 'new-prompt',
                               install.sha(b'old-source'), install.sha(b'old-prompt'),
                               self.config, self.module)
    def test_strict_fenced_scoped_install_preserves_unrelated_job(self):
        outcome = self.act()
        self.assertEqual(['fire','strict-lock'], self.started)
        self.assertEqual(install.sha(b'new-source'), outcome['source_sha'])
        self.assertEqual('new-prompt', install.read_store(self.jobs)[1]['prompt'])
        self.assertEqual(self.other, install.read_store(self.jobs)[0]['jobs'][1])
        self.assertEqual(b'new-source', self.source.read_bytes())

    def test_lock_denial_claim_due_and_config_drift_refuse_without_write(self):
        for field, value in [('fire', False), ('lock', False),
                             ('job.fire_claim', {'by':'another'}),
                             ('job.pending_slot', 'due'),
                             ('job.next_run_at', datetime.datetime.now(datetime.UTC).isoformat()),
                             ('job.deliver', 'changed')]:
            with self.subTest(field=field):
                self.setUp()

                if field.startswith('job.'):
                    self.job[field[4:]] = value
                    self.jobs.write_text(json.dumps({'jobs':[self.job,self.other]}))
                else:
                    setattr(self, field, value)
                before = self.jobs.read_bytes()
                with self.assertRaises(install.InstallRefused): self.act()
                self.assertEqual(before, self.jobs.read_bytes())
                self.assertEqual(b'old-source', self.source.read_bytes())

    def test_conflicting_unrelated_writer_aborts_and_restores_only_source(self):
        original = install.read_store
        calls = [0]
        def race(path):
            calls[0] += 1
            if calls[0] == 2:
                value, _, _ = original(path)
                value['jobs'][1]['last_status'] = 'updated-by-other'
                self.jobs.write_text(json.dumps(value))
            return original(path)
        with patch.object(install, 'read_store', side_effect=race):
            with self.assertRaises(install.InstallRefused): self.act()
        self.assertEqual(b'old-source', self.source.read_bytes())
        self.assertEqual('updated-by-other', install.read_store(self.jobs)[0]['jobs'][1]['last_status'])
        self.assertEqual('old-prompt', install.read_store(self.jobs)[1]['prompt'])

    def test_post_replace_failure_rolls_back_only_touched_fields(self):
        original = install.atomic_bytes
        calls = [0]
        def fail_after_replace(path, data):
            calls[0] += 1
            original(path, data)
            if calls[0] == 2: raise OSError('prompt readback failed')
        with patch.object(install, 'atomic_bytes', side_effect=fail_after_replace):
            with self.assertRaises(OSError): self.act()
        self.assertEqual(b'old-source', self.source.read_bytes())
        self.assertEqual('old-prompt', install.read_store(self.jobs)[1]['prompt'])
        self.assertEqual(self.other, install.read_store(self.jobs)[0]['jobs'][1])

    def test_source_replace_failure_still_rolls_back(self):
        original = install.atomic_bytes
        def failed_source(path, data):
            original(path, data)
            if pathlib.Path(path) == self.source and data == b'new-source':
                raise OSError('source post-rename failure')
        with patch.object(install, 'atomic_bytes', side_effect=failed_source):
            with self.assertRaises(OSError): self.act()
        self.assertEqual(b'old-source', self.source.read_bytes())
        self.assertEqual('old-prompt', install.read_store(self.jobs)[1]['prompt'])

    def test_source_rollback_cas_loss_is_mixed_version_unknown(self):
        original = install.atomic_bytes
        calls = [0]
        def raced(path, data):
            calls[0] += 1
            original(path, data)
            if calls[0] == 2:
                self.source.write_bytes(b'other-writer')
                raise OSError('failed after replacement')
        with patch.object(install, 'atomic_bytes', side_effect=raced):
            with self.assertRaisesRegex(install.InstallRefused, 'MIXED VERSION UNKNOWN'):
                self.act()
        self.assertEqual(b'other-writer', self.source.read_bytes())

if __name__ == '__main__': unittest.main()
