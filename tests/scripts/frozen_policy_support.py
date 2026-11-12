"""Disposable committed checker trees for the WRI-2276 policy boundary."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from tests.waiting import wait_until

REPO = Path(__file__).resolve().parents[2]
BASE = "6231b760c2325d2dc9f71afce51acf1163fdd908"
CANDIDATE = "2b8e9678ef8ebca08e949ac56e057bc359b80c51"
OBSERVER = "scripts/observer_v2/engineering_recovery_observer.py"
PARENT = "scripts/observer_v2/observer_parent_seam.py"
NATIVE = "scripts/observer_v2/observer_native_v2.py"
HASHES = {
    OBSERVER: "1d24b40cf07b40a31160063f225c2dca0f10fc3d05130fdff453b9478cc0858f",
    PARENT: "b9e762b53dc820cb47b0833bf81dfb6bb3322f64c1b31c96f2c90b74bdc302e2",
    NATIVE: "04add5c2c28ad50672bb1a0e160b22d26b3efbd1d89ad042fa6ccb9cd29648af",
}
POLICY_PASS = "PASS under reviewed WRI-2276 frozen-import policy"


def command(repo: Path, *args: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
    env["PATH"] = str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", "")
    return subprocess.run(args, cwd=repo, env=env, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", stdin=subprocess.DEVNULL,
                          timeout=600, check=False)


def git(repo: Path, *args: str) -> str:
    result = command(repo, "git", *args)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout.strip()


def _has_reviewed_commits(repo: Path) -> bool:
    return all(command(repo, "git", "cat-file", "-e", f"{sha}^{{tree}}").returncode == 0
               for sha in (CANDIDATE, BASE))


def _fetch_reviewed_commits() -> bool:
    if _has_reviewed_commits(REPO):
        return True
    # The checkout's own config and credentials (CI's checkout token, a user's helper) apply.
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env["GIT_TERMINAL_PROMPT"] = "0"
    result = subprocess.run(
        ("git", "fetch", "--quiet", "--no-tags", "--no-write-fetch-head", "--depth=1",
         "origin", CANDIDATE, BASE),
        cwd=REPO, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace",
        stdin=subprocess.DEVNULL, timeout=600, check=False)
    if _has_reviewed_commits(REPO):
        return True
    # Test files fetch concurrently and git refuses a second writer of .git/shallow.lock.
    assert ".lock" in result.stderr, f"cannot fetch reviewed commits {CANDIDATE} and {BASE}: {result.stderr}"
    return False


def provide_reviewed_commits() -> None:
    """Make the reviewed WRI-2276 commits readable in the source checkout.

    A shallow checkout (CI's actions/checkout fetches one commit) lacks them, so fetch
    both from its origin at depth 1. A full clone already has them and is not touched.
    """
    wait_until(_fetch_reviewed_commits, timeout=300, interval=1,
               what=f"another fetch to release the lock on {REPO}")


def candidate_tree(path: Path) -> Path:
    provide_reviewed_commits()
    git(path.parent, "clone", "--shared", "--no-checkout", str(REPO), str(path))
    if not _has_reviewed_commits(path):
        # Git clones a shallow source by transport, which copies only its refs' history.
        git(path, "fetch", "--quiet", "--no-tags", "--no-write-fetch-head", "--depth=1",
            str(REPO), CANDIDATE, BASE)
    git(path, "checkout", "--detach", CANDIDATE)
    git(path, "config", "user.name", "Policy fixture")
    git(path, "config", "user.email", "policy@example.invalid")
    for rel in ("scripts/check", "scripts/check-windows-footguns.py",
                "scripts/frozen_import_policy.py", "scripts/check_frozen_ruff.py",
                ".github/workflows/lint.yml"):
        if (REPO / rel).exists():
            shutil.copy2(REPO / rel, path / rel)
    shutil.copytree(REPO / "scripts/code_health", path / "scripts/code_health",
                    dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
    git(path, "add", "scripts", ".github/workflows/lint.yml")
    git(path, "commit", "--allow-empty", "-qm", "checker candidate")
    return path


def check(repo: Path, selection: str, *args: str) -> subprocess.CompletedProcess:
    return command(repo, sys.executable, "scripts/check", "--only", selection, *args)


def assert_treated(result: subprocess.CompletedProcess, paths: tuple[str, ...]) -> None:
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert POLICY_PASS in output, output
    assert "ok under reported check policy" in output, output
    for path in paths:
        assert path in output and HASHES[path] in output, output
    assert "reason=" in output, output
