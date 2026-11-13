"""Execute the blocking Ruff workflow command on a disposable private candidate."""
from __future__ import annotations

import shlex
import sys

import pytest
from ruamel.yaml import YAML

from tests.scripts.frozen_policy_support import (
    BASE, OBSERVER, PARENT, POLICY_PASS, candidate_tree, check, command,
)


@pytest.fixture(scope="module")
def candidate(tmp_path_factory):
    return candidate_tree(tmp_path_factory.mktemp("policy-ci") / "candidate")


def test_blocking_workflow_command_matches_native_policy(candidate):
    workflow = YAML(typ="base").load((candidate / ".github/workflows/lint.yml").read_text(encoding="utf-8"))
    job = workflow["jobs"]["lint"]
    step = next(step for step in job["steps"]
                if "scripts.check_frozen_ruff" in step.get("run", ""))
    argv = shlex.split(step["run"])
    if argv[0] == "python":
        argv[0] = sys.executable
    positive = command(candidate, *argv)
    assert positive.returncode == 0 and POLICY_PASS in positive.stdout, positive.stdout + positive.stderr
    for path in (OBSERVER, PARENT):
        assert path in positive.stdout
    local = check(candidate, "ruff", "--base", BASE)
    assert local.returncode == 0 and POLICY_PASS in local.stdout
    negative = candidate / "scripts/workflow_negative.py"
    negative.write_text("def f():\n    workflow_unused = 1\n    return 2\n", encoding="utf-8")
    try:
        denied = command(candidate, *argv)
        assert denied.returncode != 0 and "workflow_negative.py" in denied.stdout
        assert POLICY_PASS not in denied.stdout
    finally:
        negative.unlink()
