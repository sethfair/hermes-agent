"""Contained fixture cleanup must settle descendants before removing data."""
from pathlib import Path
import subprocess
import sys

import psutil

from tests.cron.local_observation_support import native_fixture
from tests.waiting import wait_until


def test_fixture_joins_owned_child_before_removal():
    child = None
    try:
        with native_fixture() as root:
            child = subprocess.Popen(
                [sys.executable, '-c',
                 'import sys,threading; print("ready",flush=True); threading.Event().wait(30)'],
                stdout=subprocess.PIPE, text=True, start_new_session=True)
            assert child.stdout is not None
            assert child.stdout.readline().strip() == 'ready'
            identity = psutil.Process(child.pid).create_time()
        assert wait_until(lambda: child.poll() is not None, timeout=2), (
            'fixture removed while its owned child remained alive')
        assert not root.exists() and not root.parent.exists()
        assert not psutil.pid_exists(child.pid) or psutil.Process(child.pid).create_time() != identity
    finally:
        if child is not None:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
            if child.stdout is not None:
                child.stdout.close()


def _checkout_copy(tmp, name):
    """A distinct checkout root holding its own copy of the fixture module."""
    import shutil
    repo = Path(__file__).resolve().parents[2]
    checkout = tmp / name
    (checkout / 'tests/cron').mkdir(parents=True)
    for package in (checkout / 'tests', checkout / 'tests/cron'):
        (package / '__init__.py').write_text('')
    shutil.copy(repo / 'tests/cron/local_observation_support.py',
                checkout / 'tests/cron/local_observation_support.py')
    return checkout


def test_distinct_checkouts_own_disjoint_parents_without_host_paths(tmp_path, monkeypatch):
    import os
    import select

    repo = Path(__file__).resolve().parents[2]
    runner_tmp = tmp_path / 'runner-tmp'
    runner_tmp.mkdir()
    env = dict(os.environ, HOME=str(tmp_path / 'no-such-home'), TMPDIR=str(runner_tmp),
               HERMES_DELEGATED_CHILD_CONTEXT=str(tmp_path / 'fence-marker'))
    code = ('import sys\n'
            f'sys.path.insert(0,{str(repo)!r})\n'
            'sys.path.insert(0,sys.argv[1])\n'
            'import tests.cron.local_observation_support as s\n'
            'assert s.__file__.startswith(sys.argv[1]),s.__file__\n'
            'with s.native_fixture() as root:\n'
            ' print(str(root),flush=True)\n'
            ' sys.stdin.readline()\n')
    children = []
    try:
        for name in ('checkout-a', 'checkout-b'):
            checkout = _checkout_copy(tmp_path, name)
            children.append(subprocess.Popen(
                [sys.executable, '-c', code, str(checkout)], env=env, stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0))
        roots = []
        for child in children:
            # Both must be inside their fixture at once: contention would block or raise.
            assert select.select([child.stdout], [], [], 60)[0], (
                'fixture child did not enter concurrently: ' + repr(child.stderr.read1()))
            roots.append(Path(child.stdout.readline().decode().strip()))
        assert roots[0].parent != roots[1].parent
        for root in roots:
            assert root.is_dir() and runner_tmp in root.parents
        for child in children:
            child.stdin.write(b'finish\n')
            child.stdin.flush()
            child.wait(timeout=30)
        assert [c.returncode for c in children] == [0, 0]
        assert list(runner_tmp.iterdir()) == []
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            for stream in (child.stdin, child.stdout, child.stderr):
                stream.close()
