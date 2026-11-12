"""WRI-2276: real native reports and failed measurements stay visible."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from tests.scripts.frozen_policy_support import (
    BASE, HASHES, NATIVE, OBSERVER, PARENT, POLICY_PASS, REPO,
    assert_treated, candidate_tree, check, command,
)


@pytest.fixture(scope="module")
def candidate(tmp_path_factory):
    return candidate_tree(tmp_path_factory.mktemp("policy") / "candidate")


@pytest.mark.parametrize("selection,paths,count", [
    ("footguns", (OBSERVER,), 3),
    ("ruff", (OBSERVER, PARENT), 4),
    ("health", (OBSERVER, PARENT, NATIVE), 12),
])
def test_real_aggregate_retains_exception_sites(candidate, selection, paths, count):
    result = check(candidate, selection, "--base", BASE)
    assert_treated(result, paths)
    assert result.stdout.count("WRI-2276 exception:") == count, result.stdout


def test_new_author_violation_still_blocks(candidate):
    path = candidate / "scripts/policy_author_negative.py"
    path.write_text("def f():\n    unused_author = 1\n    return 2\n", encoding="utf-8")
    try:
        result = check(candidate, "ruff", "--base", BASE)
        assert result.returncode != 0, result.stdout
        assert "policy_author_negative.py" in result.stdout and "F841" in result.stdout
        assert POLICY_PASS not in result.stdout
    finally:
        path.unlink()


def _scanner():
    spec = importlib.util.spec_from_file_location("policy_footguns", REPO / "scripts/check-windows-footguns.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("relative", [OBSERVER, "scripts/policy_author_read.py"])
def test_actual_scanner_read_failure_is_visible(candidate, monkeypatch, capsys, relative):
    scanner = _scanner()
    monkeypatch.setattr(scanner, "REPO_ROOT", candidate)
    target = candidate / relative
    created = not target.exists()
    if created:
        target.write_text("value = 1\n", encoding="utf-8")
    read_text = Path.read_text

    def fail_selected(path, *args, **kwargs):
        if path == target:
            raise OSError("injected scanner read failure")
        return read_text(path, *args, **kwargs)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(Path, "read_text", fail_selected)
            result = scanner.main(["--all"])
        output = capsys.readouterr()
        assert result != 0, output
        assert relative in output.out + output.err
        assert "injected scanner read failure" in output.out + output.err
        assert POLICY_PASS not in output.out + output.err
        assert_treated(check(candidate, "footguns", "--base", BASE), (OBSERVER,))
    finally:
        if created:
            target.unlink()
