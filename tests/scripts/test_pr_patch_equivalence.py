"""pr_patch_equivalence.py says PASS only when a PR's changes are in the fork ref."""
from pathlib import Path
import subprocess
import sys

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "pr_patch_equivalence.py"
BASE = "alpha\nbeta\ngamma\ndelta\n"


def git(repo, *args, stdin=None):
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
        cwd=repo, input=stdin, capture_output=True, text=True, check=True, timeout=60,
    ).stdout


def commit(repo, text, msg):
    (repo / "f.txt").write_text(text, encoding="utf-8")
    git(repo, "add", "f.txt")
    git(repo, "commit", "-q", "-m", msg)
    return git(repo, "rev-parse", "HEAD").strip()


@pytest.fixture
def repo(tmp_path):
    git(tmp_path, "init", "-q", "-b", "main")
    commit(tmp_path, BASE, "base")
    return tmp_path


def pr_files(repo, tmp_path_factory_dir, new_text):
    """Diff + per-commit mbox for a PR that turns BASE into new_text, built off main."""
    git(repo, "checkout", "-q", "-b", "pr")
    sha = commit(repo, new_text, "pr change")
    diff = git(repo, "diff", "main", "pr")
    mbox = git(repo, "format-patch", "--stdout", "-1", sha)
    git(repo, "checkout", "-q", "main")
    git(repo, "branch", "-q", "-D", "pr")
    d = Path(tmp_path_factory_dir)
    (d / "pr.diff").write_text(diff, encoding="utf-8")
    (d / "pr.mbox").write_text(mbox, encoding="utf-8")
    return str(d / "pr.diff"), str(d / "pr.mbox")


def check(repo, diff, mbox, ref):
    return subprocess.run(
        [sys.executable, str(SCRIPT), "1", ref, "--diff-file", diff, "--commit-patches-file", mbox],
        cwd=repo, capture_output=True, text=True, check=False, timeout=60,
    )


NEW = "alpha\nbeta changed\ngamma\ndelta\nepsilon\n"


def test_rebased_commit_with_new_message_passes_on_patch_id(repo, tmp_path):
    diff, mbox = pr_files(repo, tmp_path, NEW)
    git(repo, "commit", "-q", "--allow-empty", "-m", "unrelated")
    commit(repo, NEW, "different message, same change")
    out = check(repo, diff, mbox, "main")
    assert out.returncode == 0, out.stdout
    assert "PASS" in out.stdout and "patch-id: whole PR diff ==" in out.stdout


def test_squash_with_extra_change_passes_on_hunk_presence(repo, tmp_path):
    diff, mbox = pr_files(repo, tmp_path, NEW)
    commit(repo, NEW + "zeta\n", "squash plus another edit")
    out = check(repo, diff, mbox, "main")
    assert out.returncode == 0, out.stdout
    assert "PASS" in out.stdout and "content: every added block" in out.stdout
    assert "patch-id: whole PR diff" not in out.stdout


def test_missing_change_fails_with_evidence(repo, tmp_path):
    diff, mbox = pr_files(repo, tmp_path, NEW)
    out = check(repo, diff, mbox, "main")
    assert out.returncode == 1
    assert "FAIL" in out.stdout
    assert "added block missing" in out.stdout


def test_unknown_ref_is_an_error_not_a_fail(repo, tmp_path):
    diff, mbox = pr_files(repo, tmp_path, NEW)
    out = check(repo, diff, mbox, "no-such-ref")
    assert out.returncode == 2
    assert "does not resolve" in out.stderr


def make_pr(repo, tmp_path, name, new_data):
    """Like pr_files for an arbitrary file (str or bytes), leaving main untouched."""
    git(repo, "checkout", "-q", "-b", "pr")
    path = repo / name
    path.write_bytes(new_data if isinstance(new_data, bytes) else new_data.encode("utf-8"))
    git(repo, "add", name)
    git(repo, "commit", "-q", "-m", "pr change")
    sha = git(repo, "rev-parse", "HEAD").strip()
    (tmp_path / "pr.diff").write_text(git(repo, "diff", "main", "pr"), encoding="utf-8")
    (tmp_path / "pr.mbox").write_text(git(repo, "format-patch", "--stdout", "-1", sha), encoding="utf-8")
    git(repo, "checkout", "-q", "main")
    return str(tmp_path / "pr.diff"), str(tmp_path / "pr.mbox"), sha


def test_landed_then_reverted_change_fails_despite_patch_id_match(repo, tmp_path):
    (repo / "m.py").write_text("x = 1\n", encoding="utf-8")
    git(repo, "add", "m.py")
    git(repo, "commit", "-q", "-m", "add m")
    diff, mbox, sha = make_pr(repo, tmp_path, "m.py", "x = 1\nSAFETY_CHECK = True\n")
    git(repo, "cherry-pick", sha)
    git(repo, "revert", "--no-edit", "HEAD")
    out = check(repo, diff, mbox, "main")
    assert out.returncode == 1, out.stdout
    assert "PASS" not in out.stdout
    assert "added block missing" in out.stdout


def test_modified_binary_file_fails_when_ref_keeps_old_bytes(repo, tmp_path):
    (repo / "logo.bin").write_bytes(b"\x00\x01OLD\x00")
    git(repo, "add", "logo.bin")
    git(repo, "commit", "-q", "-m", "add logo")
    diff, mbox, _ = make_pr(repo, tmp_path, "logo.bin", b"\x00\x01NEW\x00\xff")
    out = check(repo, diff, mbox, "main")
    assert out.returncode == 1, out.stdout
    assert "PASS" not in out.stdout
    assert "blob" in out.stdout and "MISS" in out.stdout


def test_modified_binary_file_passes_when_ref_has_new_bytes(repo, tmp_path):
    (repo / "logo.bin").write_bytes(b"\x00\x01OLD\x00")
    git(repo, "add", "logo.bin")
    git(repo, "commit", "-q", "-m", "add logo")
    diff, mbox, _ = make_pr(repo, tmp_path, "logo.bin", b"\x00\x01NEW\x00\xff")
    git(repo, "checkout", "-q", "main")
    (repo / "logo.bin").write_bytes(b"\x00\x01NEW\x00\xff")
    git(repo, "add", "logo.bin")
    git(repo, "commit", "-q", "-m", "binary landed")
    out = check(repo, diff, mbox, "main")
    assert out.returncode == 0, out.stdout


def test_block_copied_elsewhere_in_file_is_not_a_landed_change(repo, tmp_path):
    base = "def f(x):\n    if x is None:\n        return None\n    return x\n\ndef g(x):\n    return x * 2\n"
    pr = base.replace("def g(x):\n", "def g(x):\n    if x is None:\n        return None\n")
    (repo / "m.py").write_text(base, encoding="utf-8")
    git(repo, "add", "m.py")
    git(repo, "commit", "-q", "-m", "add m")
    diff, mbox, _ = make_pr(repo, tmp_path, "m.py", pr)
    out = check(repo, diff, mbox, "main")
    assert out.returncode == 1, out.stdout
    assert "PASS" not in out.stdout
    assert "added block missing" in out.stdout
