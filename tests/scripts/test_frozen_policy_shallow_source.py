"""The frozen-policy candidate fixture works from a shallow source checkout.

Regression for WRI-2402: CI's actions/checkout fetches only the tested commit, so
the reviewed WRI-2276 commits were missing from the checkout and every fixture
built on ``candidate_tree`` errored with ``fatal: unable to read tree``.
"""
from __future__ import annotations

import pytest

from tests.scripts import frozen_policy_support as support
from tests.scripts.frozen_policy_support import git


def _commit(repo, message, files):
    for rel, text in files.items():
        target = repo / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "-c", "user.name=Upstream", "-c", "user.email=upstream@example.invalid",
        "commit", "-qm", message)
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def upstream(tmp_path):
    repo = tmp_path / "upstream"
    repo.mkdir()
    git(repo, "init", "-q")
    base = _commit(repo, "base", {"scripts/code_health/__init__.py": "",
                                  ".github/workflows/lint.yml": "jobs: {}\n"})
    candidate = _commit(repo, "reviewed", {"scripts/reviewed.py": "value = 1\n"})
    _commit(repo, "later", {"scripts/later.py": "value = 2\n"})
    return repo, base, candidate


@pytest.mark.parametrize("depth", [(), ("--depth", "1")], ids=["full", "shallow"])
def test_candidate_tree_builds_from_source_checkout(tmp_path, monkeypatch, upstream, depth):
    repo, base, candidate = upstream
    source = tmp_path / "source"
    git(tmp_path, "clone", "-q", *depth, repo.as_uri(), str(source))
    head = git(source, "rev-parse", "HEAD")
    monkeypatch.setattr(support, "REPO", source)
    monkeypatch.setattr(support, "BASE", base)
    monkeypatch.setattr(support, "CANDIDATE", candidate)

    tree = support.candidate_tree(tmp_path / "candidate")

    assert git(tree, "rev-parse", "HEAD^1") == candidate
    assert git(tree, "diff", "--name-only", base, "HEAD").splitlines() == ["scripts/reviewed.py"]
    assert git(source, "rev-parse", "HEAD") == head
    assert git(source, "status", "--porcelain") == ""
