"""WRI-2276: exact census, source identity and native command failures."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest

from tests.scripts.frozen_policy_support import (
    BASE, CANDIDATE, OBSERVER, POLICY_PASS, assert_health_clean, assert_treated, candidate_tree,
    check, command, git,
)


@pytest.fixture(scope="module")
def candidate(tmp_path_factory):
    return candidate_tree(tmp_path_factory.mktemp("policy-denials") / "candidate")


@pytest.mark.parametrize("mutation", ["byte", "missing", "symlink"])
def test_changed_source_refuses_treatment(candidate, mutation):
    path = candidate / OBSERVER
    raw = path.read_bytes()
    if mutation == "byte":
        path.write_bytes(raw + b"\n")
    else:
        path.unlink()
        if mutation == "symlink":
            path.symlink_to(candidate / "tests/cron/fixtures/observer-v2-reference/engineering_recovery_observer.py.fixture")
    try:
        result = check(candidate, "ruff", "--base", BASE)
        assert result.returncode != 0, result.stdout
        assert POLICY_PASS not in result.stdout
        assert "policy" in result.stdout.lower(), result.stdout
    finally:
        if path.is_symlink():
            path.unlink()
        path.write_bytes(raw)


@pytest.mark.parametrize("mutation", [
    "missing", "duplicate", "extra", "location", "detail", "severity", "shape", "escape", "wrong-path",
])
def test_structured_census_drift_fails(candidate, mutation):
    from scripts.code_health.ruff_runner import resolve_ruff
    from scripts.frozen_import_policy import PolicyError, partition_findings

    result = command(candidate, *resolve_ruff(candidate), "check", ".", "--output-format", "json")
    records = json.loads(result.stdout)
    assert result.returncode == 1 and len(records) == 4
    retained, exceptions = partition_findings(candidate, "ruff", records)
    assert not retained and len(exceptions) == 4
    bad = copy.deepcopy(records)
    mutations = {
        "missing": lambda: bad.pop(),
        "duplicate": lambda: bad.append(copy.deepcopy(bad[0])),
        "extra": lambda: bad.append({**bad[0], "code": "F841"}),
        "location": lambda: bad[0]["location"].update(column=80),
        "detail": lambda: bad[0].update(message=bad[0]["message"] + " drift"),
        "severity": lambda: bad[0].update(severity="warning"),
        "shape": lambda: bad[0].update(unknown_field=None),
        "escape": lambda: bad[0].update(filename=str(candidate / "../outside.py")),
        "wrong-path": lambda: bad[0].update(filename=str(candidate / "scripts/observer_v2/wrong.py")),
    }
    mutations[mutation]()
    with pytest.raises(PolicyError):
        partition_findings(candidate, "ruff", bad)


def test_replaced_inode_during_scan_fails(candidate):
    from scripts.code_health.ruff_runner import resolve_ruff
    from scripts.frozen_import_policy import PolicyError, capture_sources, partition_findings

    before = capture_sources(candidate)
    result = command(candidate, *resolve_ruff(candidate), "check", ".", "--output-format", "json")
    path = candidate / OBSERVER
    replacement = path.with_suffix(".replacement")
    replacement.write_bytes(path.read_bytes())
    replacement.replace(path)
    with pytest.raises(PolicyError, match="during scan"):
        partition_findings(candidate, "ruff", json.loads(result.stdout), before=before)


@pytest.mark.parametrize("mode", ["--staged", "--commit"])
def test_snapshot_qualifies_judged_bytes_not_live_files(candidate, mode):
    raw = (candidate / OBSERVER).read_bytes()
    head = git(candidate, "rev-parse", "HEAD")
    args = (mode,) if mode == "--staged" else (mode, head)
    (candidate / OBSERVER).write_bytes(raw + b"\n")
    try:
        assert_treated(check(candidate, "ruff", *args, "--base", BASE),
                       (OBSERVER, "scripts/observer_v2/observer_parent_seam.py"))
        assert check(candidate, "ruff", "--base", BASE).returncode != 0
    finally:
        (candidate / OBSERVER).write_bytes(raw)


def test_health_judges_head_bytes_not_live_files(candidate):
    raw = (candidate / OBSERVER).read_bytes()
    head = git(candidate, "rev-parse", "HEAD")
    (candidate / OBSERVER).write_bytes(raw + b"\n")
    try:
        assert_health_clean(command(candidate, sys.executable, "-m", "scripts.code_health",
                                    "--head", head, "--base", BASE))
        live = check(candidate, "health", "--base", "HEAD^1")
        assert live.returncode != 0 and POLICY_PASS not in live.stdout
        assert "frozen source mismatch" in live.stdout, live.stdout + live.stderr
    finally:
        (candidate / OBSERVER).write_bytes(raw)
    tree = git(candidate, "rev-parse", "HEAD^{tree}")
    merge = command(candidate, "git", "commit-tree", tree, "-p", BASE, "-m", "CI-shaped snapshot")
    assert merge.returncode == 0, merge.stderr
    git(candidate, "update-ref", "refs/heads/policy-ci-shape", merge.stdout.strip())
    git(candidate, "checkout", "--detach", merge.stdout.strip())
    try:
        assert git(candidate, "rev-parse", "HEAD^1") == BASE
        assert_health_clean(check(candidate, "health", "--base", "HEAD^1"))
    finally:
        git(candidate, "checkout", "--detach", head)


@pytest.fixture(scope="module")
def health_findings(candidate):
    """Real findings for the frozen sources measured as if they were new code."""
    from dataclasses import asdict
    from scripts.code_health.compare import compare
    from scripts.code_health.gitio import Change
    from scripts.code_health.measure import Measurer
    from scripts.code_health.ruff_runner import resolve_ruff
    from tests.scripts.frozen_policy_support import HASHES

    measured = Measurer(candidate, resolve_ruff(candidate), known_env=set()).measure(None, list(HASHES))
    return [asdict(item) for item in compare({}, measured,
            [Change("A", None, path) for path in HASHES])]


@pytest.mark.parametrize("base", [BASE, CANDIDATE], ids=["policy-base", "later-base"])
def test_inherited_health_findings_are_never_exceptions(candidate, health_findings, base):
    from scripts.frozen_import_policy import PolicyError, partition_findings

    assert health_findings and all(item["blocking"] for item in health_findings)
    assert partition_findings(candidate, "health", [], base=base) == ([], [])
    for finding in health_findings:
        with pytest.raises(PolicyError):
            partition_findings(candidate, "health", [finding], base=base)


def test_invalid_snapshot_cannot_borrow_valid_live_source(candidate):
    path = candidate / OBSERVER
    raw = path.read_bytes()
    head = git(candidate, "rev-parse", "HEAD")
    path.write_bytes(raw + b"\n")
    git(candidate, "add", OBSERVER)
    path.write_bytes(raw)
    try:
        result = check(candidate, "ruff", "--staged", "--base", BASE)
        assert result.returncode != 0 and POLICY_PASS not in result.stdout
        assert_treated(check(candidate, "ruff", "--base", BASE),
                       (OBSERVER, "scripts/observer_v2/observer_parent_seam.py"))
    finally:
        git(candidate, "read-tree", head)


def test_git_symlink_mode_is_not_regular_source(candidate):
    head = git(candidate, "rev-parse", "HEAD")
    blob = git(candidate, "rev-parse", head + ":" + OBSERVER)
    git(candidate, "update-index", "--cacheinfo", f"120000,{blob},{OBSERVER}")
    try:
        tree = git(candidate, "write-tree")
        result = command(candidate, sys.executable, "-m", "scripts.code_health",
                         "--head", tree, "--base", BASE)
        assert result.returncode != 0 and POLICY_PASS not in result.stdout
        assert "nonregular Git source" in result.stderr
    finally:
        git(candidate, "read-tree", head)


@pytest.mark.parametrize("checker,source,excused", [
    ("footguns", "import fcntl\n", True),
    # The health census is empty, so a clean run has nothing to excuse.
    ("health", "def f():\n    try:\n        return 1\n    except Exception:\n        return 2\n", False),
])
def test_direct_native_offscope_twin(candidate, checker, source, excused):
    path = candidate / "scripts/offscope_native.py"
    path.write_text(source, encoding="utf-8")
    argv = ([sys.executable, "scripts/check-windows-footguns.py", "--all"] if checker == "footguns"
            else [sys.executable, "-m", "scripts.code_health", "--base", BASE])
    try:
        result = command(candidate, *argv)
        assert result.returncode != 0 and "offscope_native.py" in result.stdout
        assert POLICY_PASS not in result.stdout
    finally:
        path.unlink()
    positive = command(candidate, *argv)
    assert positive.returncode == 0, positive.stdout + positive.stderr
    assert (POLICY_PASS in positive.stdout) is excused, positive.stdout + positive.stderr


@pytest.mark.parametrize("kind", ["exit2", "empty_failure", "bad_json", "timeout"])
def test_ruff_tool_failures_are_not_treated(candidate, monkeypatch, capsys, kind):
    import subprocess
    from scripts import check_frozen_ruff

    def broken(*args, **kwargs):
        if kind == "timeout":
            raise subprocess.TimeoutExpired(args[0], 600)
        stdout = "truncated[" if kind == "bad_json" else "[]"
        return subprocess.CompletedProcess(args[0], 2 if kind == "exit2" else 1, stdout, "tool error")

    monkeypatch.setattr(check_frozen_ruff, "resolve_ruff", lambda root: ["unused-tool"])
    monkeypatch.setattr(check_frozen_ruff.subprocess, "run", broken)
    assert check_frozen_ruff.run(candidate) != 0
    output = capsys.readouterr()
    assert POLICY_PASS not in output.out + output.err
    assert "failed" in (output.out + output.err).lower()
