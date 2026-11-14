"""The WRI-2276 health policy has no reviewed health exceptions; the base pin must not gate runs.

CI judges a PR against the base-branch tip, which is no longer the commit the policy was
reviewed at. A run with no findings in the frozen sources has nothing to excuse, so it must
not fail on the base spelling; a health finding in them stays a blocker whatever the base.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from scripts.frozen_import_policy import BASE, OBSERVER, PolicyError, partition_findings

REPO = Path(__file__).resolve().parents[2]
LATER_BASE = "969d4f2ad3f326f5210d8d94877c3046c6553a6a"
FINDING = {"path": OBSERVER, "rule": "CC", "line": 1, "scope": "f", "detail": "x",
           "blocking": True, "allowed_reason": None}


def test_no_frozen_findings_ignore_the_base():
    normal, exceptions = partition_findings(REPO, "health", [], base=LATER_BASE)
    assert (normal, exceptions) == ([], [])


@pytest.mark.parametrize("base", [BASE, LATER_BASE])
def test_a_frozen_health_finding_is_never_excused(base):
    with pytest.raises(PolicyError):
        partition_findings(REPO, "health", [dict(FINDING)], base=base)
