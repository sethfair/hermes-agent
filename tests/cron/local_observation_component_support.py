"""Retained component regression helpers; importing defines no running suite.

These component-only imports are separate from private producer admission. Each
file runs in its own native runner process; the private loader tests stay separate.
"""
from contextvars import ContextVar
import hashlib
import importlib.util
import os
from pathlib import Path
import sys
import tempfile

import pytest

from cron import scheduler_local_observation as producer
from tests.cron.local_observation_support import native_fixture

_ACTIVE_ROOT: ContextVar[Path | None] = ContextVar('component_fixture_root', default=None)


def _component():
    source = Path(__file__).resolve().parents[2] / 'scripts/observer_v2'
    loaded = {}
    # Execute source bytes, not a stale bytecode cache or an installed observer.
    for name in ('native_status_policy', 'engineering_recovery_observer'):
        path = source / (name + '.py')
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != producer._REFERENCE_SHA[name]:
            raise ValueError('component source pin changed')
        if name in sys.modules:
            raise ValueError('foreign component cache at collection')
        spec = importlib.util.spec_from_loader(name, loader=None, origin=str(path))
        assert spec is not None
        module = importlib.util.module_from_spec(spec)
        module.__file__ = str(path)
        sys.modules[name] = module
        exec(compile(raw, str(path), 'exec'), module.__dict__)
        loaded[name] = module
    return loaded['engineering_recovery_observer']


obs = _component()


@pytest.fixture(autouse=True)
def native_component_scope(monkeypatch):
    monkeypatch.setenv('HERMES_DELEGATED_CHILD_CONTEXT', '/home/macpro/.hermes')
    with native_fixture() as root:
        monkeypatch.setenv('TMPDIR', str(root))
        token = _ACTIVE_ROOT.set(root)
        try:
            yield root
        finally:
            _ACTIVE_ROOT.reset(token)


def component_temporary_directory():
    root = _ACTIVE_ROOT.get()
    if root is None:
        raise RuntimeError('PRETEST: no contained native component fixture')
    return tempfile.TemporaryDirectory(prefix='component-', dir=root)
