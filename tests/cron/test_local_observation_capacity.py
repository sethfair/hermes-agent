"""WRI-2276: larger native terminal witness through typed stage seeds."""

from tests.cron.local_observation_parent_support import run_parent_fixture
from tests.cron.local_observation_support import native_tmp_path


def test_parent_470_distinct_roots(tmp_path, monkeypatch):
    run_parent_fixture(tmp_path, monkeypatch, crash_phase=None, root_count=470, historical=True)
