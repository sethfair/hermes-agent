"""WRI-2276: crash, incomplete proposal, delivery and missing-v2 process cases.

Separate file keeps each CI runner subprocess inside its per-file time bound.
"""

import pytest

from tests.cron.test_target_prerun_binding import (
    test_target_distinct_process_binds_before_stage_and_finalizes_only_after_terminal as _run_case,
)


@pytest.mark.platforms("linux")
@pytest.mark.parametrize(
    "stage_roots,tamper,kill_script,kill_after_commit,hold_finalizer,omit_receipt",
    [(True, False, False, False, True, False),
     (True, False, False, False, False, True),
     (True, False, False, False, "before-v2", False),
     (True, False, False, False, "after-v2", False),
     (False, "absent-v2", False, False, False, False),
     (True, "queued", False, False, False, False)],
    ids=["second-fire-waits-through-finalizer", "missing-root-receipt-refuses-mirror",
         "sigkill-firer-terminal-before-v2", "sigkill-firer-v2-before-mirror",
         "absent-v2-production-fails-closed", "queued-bot-chat-delivery-is-not-checked-proof"],
)
def test_target_parent_finalization_failure_and_recovery(
    tmp_path, monkeypatch, model_peer, stage_roots, tamper, kill_script, kill_after_commit,
    hold_finalizer, omit_receipt,
):
    _run_case(tmp_path, monkeypatch, model_peer, stage_roots, tamper, kill_script,
              kill_after_commit, hold_finalizer, omit_receipt)
