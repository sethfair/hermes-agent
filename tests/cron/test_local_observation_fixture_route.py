"""Disposable synthetic-parent tests; never acquire the real task parent."""
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
from tests.cron import local_observation_retained_route as route
from tests.cron.local_observation_component_support import native_component_scope, component_temporary_directory

class FixtureRouteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = component_temporary_directory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.parent = self.root/'common'/'task'
        self.report = self.root/'report.json'
        self.env = patch.dict(os.environ, {'WRI_ROUTE_REPORT':str(self.report), 'WRI_ROUTE_MODE':'ok'})
        self.env.start(); self.addCleanup(self.env.stop)

    def run_child(self, **kw):
        return route._run(self.parent, ['local_observation_fixture_child.ChildTests'], **kw)

    def test_owned_creation_cleanup_and_lineage(self):
        before={k:os.environ.get(k) for k in route.LINEAGE_KEYS}
        self.assertEqual(0,self.run_child())
        data=json.loads(self.report.read_text())
        self.assertEqual(before,data['lineage'])
        invocation=Path(data['tmpdir'])
        self.assertEqual(self.parent,invocation.parent)
        self.assertEqual(0o700,data['mode'])
        self.assertFalse(invocation.exists()); self.assertFalse(self.parent.exists())
        self.assertTrue(self.parent.parent.is_dir())

    def test_existing_parent_refuses_without_touching_sentinel(self):
        self.parent.mkdir(parents=True); sentinel=self.parent/'sentinel'; sentinel.write_text('keep')
        with self.assertRaisesRegex((ValueError,FileExistsError),'exist'):
            self.run_child()
        self.assertEqual('keep',sentinel.read_text()); self.assertFalse(self.report.exists())

    def test_symlink_ancestor_and_outside_invocation_refuse(self):
        self.parent.parent.mkdir(); link=self.root/'linked'; link.symlink_to(self.parent.parent)
        with self.assertRaisesRegex(ValueError,'symlink'):
            route._run(link/'task',['local_observation_fixture_child.ChildTests'])
        with self.assertRaisesRegex(ValueError,'outside'):
            route.check_board(self.root/'board.db', self.root/'invocation')

    def test_real_live_fence_refusal_is_noncreating(self):
        with self.assertRaisesRegex(ValueError,'fenced|live'):
            route.check_path(Path('/home/macpro/.hermes/nonexistent-wri2276-fixture'))

    def test_nonzero_child_forwards_status_and_cleans(self):
        os.environ['WRI_ROUTE_MODE']='fail'
        self.assertNotEqual(0,self.run_child())
        self.assertTrue(self.report.exists()); self.assertFalse(self.parent.exists())

    def test_cleanup_failure_reports_residue_nonzero(self):
        original=shutil.rmtree
        def fail_owned(path,*args,**kw):
            if Path(path).parent==self.parent: raise OSError('injected cleanup failure')
            return original(path,*args,**kw)
        with patch.object(route.shutil,'rmtree',side_effect=fail_owned):
            self.assertNotEqual(0,self.run_child())
        self.assertTrue(self.parent.exists()); self.assertTrue(any(self.parent.iterdir()))

    def test_timeout_kills_grandchild_before_cleanup(self):
        os.environ['WRI_ROUTE_MODE']='timeout'
        self.assertNotEqual(0,self.run_child(timeout=1))
        data=json.loads(self.report.read_text())
        self.assertEqual([],route._active_group(data['pgid']))
        self.assertFalse(self.parent.exists())

    def test_per_board_guard_rejects_fallback_and_accepts_owned(self):
        invocation=self.root/'invocation'; invocation.mkdir()
        owned=invocation/'fresh'/'board.db'; owned.parent.mkdir()
        self.assertEqual(owned,route.check_board(owned,invocation))
        with self.assertRaisesRegex(ValueError,'outside'):
            route.check_board(self.root/'fallback.db',invocation)

if __name__=='__main__': unittest.main()
