"""Disposable committed checker trees for the WRI-2276 policy boundary."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from scripts.code_health.ruff_runner import pinned_version
REPO = Path(__file__).resolve().parents[2]
SEED_PACK = REPO / "tests/fixtures/frozen_policy/reviewed-objects.pack"
# PR #7 rebound the policy: health is measured against its base, the merge of PR #6.
BASE = "c0850d5c34ea4dcd666b42b3e4f571ac9cf29969"
# PR #7's merge commit on main carries the reviewed frozen bytes.
CANDIDATE = "969d4f2ad3f326f5210d8d94877c3046c6553a6a"
OBSERVER = "scripts/observer_v2/engineering_recovery_observer.py"
PARENT = "scripts/observer_v2/observer_parent_seam.py"
NATIVE = "scripts/observer_v2/observer_native_v2.py"
HASHES = {
    OBSERVER: "b13bf6d9b00381c915c60a3f9017e94b3914a6225872e9a4d266d2476a2a1134",
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


def _seed_reviewed_objects(repo: Path) -> None:
    """Install the committed reviewed-object fixture without consulting a remote."""
    git(repo, "init", "-q")
    source_objects = Path(git(REPO, "rev-parse", "--path-format=absolute", "--git-path", "objects"))
    alternates = repo / ".git/objects/info/alternates"
    alternates.parent.mkdir(parents=True, exist_ok=True)
    alternates.write_text(str(source_objects) + "\n", encoding="utf-8")
    with SEED_PACK.open("rb") as seed:
        result = subprocess.run(
            ("git", "index-pack", "--stdin"), cwd=repo, stdin=seed,
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=600, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    git(repo, "sparse-checkout", "init", "--no-cone")
    git(repo, "sparse-checkout", "set", "/scripts/observer_v2/",
        "/scripts/frozen_import_policy.py", "/scripts/check",
        "/scripts/check-windows-footguns.py", "/scripts/check_frozen_ruff.py",
        "/scripts/code_health/", "/scripts/ci/", "/scripts/reviewed.py", "/scripts/later.py",
        "/.github/workflows/lint.yml", "/pyproject.toml", "/ruff.toml",
        "/tests/cron/fixtures/observer-v2-reference/")


def _clear_seed_worktree(repo: Path) -> None:
    for relative in ("scripts", ".github", "tests", "pyproject.toml", "ruff.toml"):
        target = repo / relative
        if target.is_dir():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()


def _reduced_reviewed_tree(repo: Path, revision: str) -> str:
    """Materialize only the files exercised by this fixture, then snapshot them."""
    _clear_seed_worktree(repo)
    wanted = ("scripts/observer_v2", "scripts/frozen_import_policy.py", "scripts/check",
              "scripts/check-windows-footguns.py", "scripts/check_frozen_ruff.py",
              "scripts/code_health", "scripts/ci", "scripts/reviewed.py", "scripts/later.py",
              ".github/workflows/lint.yml",
              "pyproject.toml", "ruff.toml", "tests/cron/fixtures/observer-v2-reference")
    for line in git(repo, "ls-tree", "-r", revision, "--", *wanted).splitlines():
        metadata, relative = line.split("\t", 1)
        mode, kind, oid = metadata.split()
        assert kind == "blob" and mode in {"100644", "100755"}
        result = subprocess.run(("git", "cat-file", "blob", oid), cwd=repo,
                                capture_output=True, timeout=60, check=False)
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(result.stdout)
        target.chmod(int(mode, 8))
    git(repo, "read-tree", "--empty")
    paths = [relative for relative in
             ("scripts", ".github/workflows/lint.yml", "tests", "pyproject.toml", "ruff.toml")
             if (repo / relative).exists()]
    git(repo, "add", "-f", *paths)
    return git(repo, "write-tree")


def _localize_reachable_objects(repo: Path) -> None:
    """Copy reduced-history trees and overlay blobs out of the source alternate."""
    trees = []
    for line in git(repo, "rev-list", "--objects", "HEAD").splitlines():
        oid = line.split(" ", 1)[0]
        if git(repo, "cat-file", "-t", oid) == "tree":
            result = subprocess.run(("git", "cat-file", "tree", oid), cwd=repo,
                                    capture_output=True, timeout=60, check=False)
            assert result.returncode == 0, result.stderr.decode(errors="replace")
            trees.append((oid, result.stdout))
    alternates = repo / ".git/objects/info/alternates"
    detached = alternates.with_suffix(".detached")
    alternates.rename(detached)
    for expected, raw in trees:
        result = subprocess.run(("git", "hash-object", "-t", "tree", "-w", "--stdin"),
                                cwd=repo, input=raw, capture_output=True, timeout=60, check=False)
        assert result.returncode == 0 and result.stdout.decode().strip() == expected
    for path in repo.rglob("*"):
        if path.is_file() and ".git" not in path.relative_to(repo).parts:
            git(repo, "hash-object", "-w", str(path))
    detached.unlink()


def candidate_tree(path: Path) -> Path:
    path.mkdir()
    _seed_reviewed_objects(path)
    git(path, "config", "user.name", "Policy fixture")
    git(path, "config", "user.email", "policy@example.invalid")
    base_tree = _reduced_reviewed_tree(path, BASE)
    reduced_base = git(path, "commit-tree", base_tree, "-m", "reviewed fixture base")
    candidate_tree_oid = _reduced_reviewed_tree(path, CANDIDATE)
    reduced_candidate = git(path, "commit-tree", candidate_tree_oid, "-p", BASE,
                            "-m", "reviewed fixture candidate")
    git(path, "replace", BASE, reduced_base)
    git(path, "replace", CANDIDATE, reduced_candidate)
    git(path, "checkout", "--detach", CANDIDATE)
    git(path, "config", "user.name", "Policy fixture")
    git(path, "config", "user.email", "policy@example.invalid")
    for rel in ("scripts/check", "scripts/check-windows-footguns.py",
                "scripts/frozen_import_policy.py", "scripts/check_frozen_ruff.py",
                ".github/workflows/lint.yml"):
        if (REPO / rel).exists():
            shutil.copy2(REPO / rel, path / rel)
    add_paths = ["scripts", ".github/workflows/lint.yml"]
    if (REPO / "pyproject.toml").exists() and (path / "pyproject.toml").exists():
        reviewed_pin = pinned_version(path)
        current_pin = pinned_version(REPO)
        project = (path / "pyproject.toml").read_text(encoding="utf-8")
        (path / "pyproject.toml").write_text(
            project.replace(f'"ruff=={reviewed_pin}"', f'"ruff=={current_pin}"', 1),
            encoding="utf-8",
        )
        add_paths.insert(0, "pyproject.toml")
    shutil.copytree(REPO / "scripts/code_health", path / "scripts/code_health",
                    dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
    git(path, "add", *add_paths)
    git(path, "commit", "--allow-empty", "-qm", "checker candidate")
    _localize_reachable_objects(path)
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


def assert_health_clean(result: subprocess.CompletedProcess) -> None:
    """The health census is empty: the frozen sources pass with nothing excused."""
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert " 0 blocking" in output, output
    assert "WRI-2276 exception:" not in output and POLICY_PASS not in output, output
