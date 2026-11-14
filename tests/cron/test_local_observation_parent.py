"""WRI-2276 original worker and separate stage children against native board/v2."""
import pytest
from tests.cron.local_observation_support import native_tmp_path
from tests.cron.local_observation_parent_support import run_parent_fixture


@pytest.mark.parametrize("crash_phase", [None, "before_commit", "after_commit", "writer_first",
                                         "stale_epoch", "script_changed", "binding_changed_postcommit",
                                         "disk_full", "lost_proposal_reply", "attachment_after_stage"])
def test_parent_only_two_roots_complete_after_terminal(tmp_path, monkeypatch, crash_phase):
    run_parent_fixture(tmp_path, monkeypatch, crash_phase=crash_phase, root_count=2)


def test_extra_stage_index_without_event_denies_parent(tmp_path, monkeypatch):
    run_parent_fixture(tmp_path, monkeypatch, crash_phase="extra_stage", root_count=3)


def test_oversized_single_proof_item_rolls_back_before_commit(tmp_path, monkeypatch):
    run_parent_fixture(tmp_path, monkeypatch, crash_phase="event_heavy", root_count=2)


def test_second_fire_does_not_replay_prior_checked_roots(tmp_path, monkeypatch):
    run_parent_fixture(tmp_path, monkeypatch, crash_phase=None, root_count=2, second_fire=True)


def test_stale_claim_cannot_capture_parent_authority(tmp_path, monkeypatch):
    run_parent_fixture(tmp_path, monkeypatch, crash_phase=None, root_count=2,
                       old_claim_attempt=True)
