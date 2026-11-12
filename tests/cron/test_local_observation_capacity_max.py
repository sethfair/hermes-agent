"""WRI-2276: upper cardinality parent proof through real native board/worker."""

from tests.cron.local_observation_parent_support import run_parent_fixture
from tests.cron.local_observation_support import native_tmp_path


def test_parent_10000_distinct_roots(tmp_path, monkeypatch):
    run_parent_fixture(tmp_path, monkeypatch, crash_phase=None, root_count=10000)
