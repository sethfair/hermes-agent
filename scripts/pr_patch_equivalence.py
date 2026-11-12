#!/usr/bin/env python3
"""Check that a pull request's changes are present in a fork ref (read-only).

    scripts/pr_patch_equivalence.py PR_NUMBER FORK_REF [--source-repo OWNER/REPO]

Prints PASS or FAIL with evidence; exit 0 = PASS, 1 = FAIL, 2 = usage/lookup error.

The PR is read from GitHub with ``gh api`` (nothing is fetched into or written to the
local repository). FORK_REF is any commit-ish that resolves in the current clone, for
example ``origin/main`` after the caller's own ``git fetch``. Two checks, strongest first:

1. patch-id (``git patch-id --stable``): the whole-PR diff, or each PR commit, equals a
   commit already in FORK_REF's history. Tolerates rebases, reorders and message changes;
   whole-PR equality also covers a squash.
2. hunk presence: for each hunk of the PR diff, the added lines appear as a contiguous
   block in the file at FORK_REF and the removed lines no longer do. Tolerates squashes,
   cherry-picks with conflict resolution and later unrelated edits to the same file;
   whitespace at line ends is ignored.

Method 2 is a content check, not proof of identical history: a hunk that was later
reverted and re-applied differently could pass or fail on content alone, so the
evidence names the commit that introduced each file's lines where one exists.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field

DEFAULT_SOURCE_REPO = "writerflow/hermes-kanban"
HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@")


class LookupFailure(Exception):
    """A required input (PR, ref, tool) could not be read."""


def run(args: list[str], stdin: str | None = None) -> str:
    try:
        proc = subprocess.run(
            args, input=stdin, capture_output=True, text=True, encoding="utf-8", errors="replace"
        )
    except FileNotFoundError as exc:
        raise LookupFailure(f"required tool not found: {args[0]}") from exc
    if proc.returncode != 0:
        raise LookupFailure(f"{' '.join(args[:3])} failed: {proc.stderr.strip()[:300]}")
    return proc.stdout


def gh_api(path: str, accept: str | None = None) -> str:
    cmd = ["gh", "api"]
    if accept:
        cmd += ["-H", f"Accept: {accept}"]
    return run(cmd + [path])


def patch_ids(patch_text: str) -> list[tuple[str, str]]:
    """Return (patch_id, commit_id) pairs from a stream of patches."""
    out = run(["git", "patch-id", "--stable"], stdin=patch_text)
    return [tuple(line.split()[:2]) for line in out.splitlines() if line.strip()]  # type: ignore[misc]


@dataclass
class FileDiff:
    path: str
    status: str  # added | deleted | modified
    hunks: list[list[str]] = field(default_factory=list)


def parse_diff(diff: str) -> list[FileDiff]:
    files: list[FileDiff] = []
    cur: FileDiff | None = None
    hunk: list[str] | None = None
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            m = re.match(r"diff --git a/(.*) b/(.*)$", line)
            cur = FileDiff(path=m.group(2) if m else line, status="modified")
            files.append(cur)
            hunk = None
        elif cur is None:
            continue
        elif hunk is None and line.startswith("new file mode"):
            cur.status = "added"
        elif hunk is None and line.startswith("deleted file mode"):
            cur.status = "deleted"
        elif HUNK_RE.match(line):
            hunk = []
            cur.hunks.append(hunk)
        elif hunk is not None and line[:1] in "+- ":
            hunk.append(line)
    return files


def runs(hunk: list[str], marker: str) -> list[list[str]]:
    """Consecutive lines with ``marker`` (+/-), as stripped-end text; context breaks a run."""
    out: list[list[str]] = []
    cur: list[str] = []
    for line in hunk:
        if line[0] == marker:
            cur.append(line[1:].rstrip())
        else:
            if cur:
                out.append(cur)
                cur = []
    if cur:
        out.append(cur)
    return out


def contains_block(lines: list[str], block: list[str]) -> bool:
    n = len(block)
    return any(lines[i : i + n] == block for i in range(len(lines) - n + 1))


def file_at(ref: str, path: str) -> list[str] | None:
    proc = subprocess.run(
        ["git", "show", f"{ref}:{path}"], capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    if proc.returncode != 0:
        return None
    return [ln.rstrip() for ln in proc.stdout.splitlines()]


def check_hunks(files: list[FileDiff], ref: str) -> tuple[bool, list[str]]:
    evidence: list[str] = []
    ok = True
    for fd in files:
        content = file_at(ref, fd.path)
        if fd.status == "deleted":
            good = content is None
            evidence.append(f"  {'ok  ' if good else 'MISS'} {fd.path}: deleted file {'absent' if good else 'still present'} at ref")
            ok &= good
            continue
        if content is None:
            evidence.append(f"  MISS {fd.path}: file not present at ref")
            ok = False
            continue
        if not fd.hunks:
            evidence.append(f"  ok   {fd.path}: no content hunks (mode/rename only), file exists")
            continue
        bad: list[str] = []
        for i, hunk in enumerate(fd.hunks, 1):
            for block in runs(hunk, "+"):
                if not any(block):
                    continue  # blank-only runs carry no evidence
                if not contains_block(content, block):
                    bad.append(f"hunk {i}: added block missing: {next(x for x in block if x)[:70]!r}")
            for block in runs(hunk, "-"):
                if any(block) and contains_block(content, block):
                    bad.append(f"hunk {i}: removed block still present: {block[0][:70]!r}")
        if bad:
            ok = False
            evidence.append(f"  MISS {fd.path}: {len(fd.hunks)} hunk(s)")
            evidence.extend(f"       {b}" for b in bad)
        else:
            evidence.append(f"  ok   {fd.path}: {len(fd.hunks)} hunk(s) present")
    return ok, evidence


def fork_patch_index(ref: str, paths: list[str], limit: int) -> dict[str, str]:
    """patch-id -> commit sha for non-merge commits in ref's history touching ``paths``."""
    shas = run(
        ["git", "log", "--no-merges", f"--max-count={limit}", "--format=%H", ref, "--", *paths]
    ).split()
    if not shas:
        return {}
    stream = run(["git", "diff-tree", "--stdin", "-p", "--root"], stdin="\n".join(shas) + "\n")
    # diff-tree --stdin prefixes each patch with the commit id line; patch-id understands it.
    return {pid: sha for pid, sha in patch_ids(stream)}


def pr_commit_patches(repo: str, pr: int) -> list[tuple[str, str]]:
    commits = json.loads(gh_api(f"repos/{repo}/pulls/{pr}/commits?per_page=100"))
    return [
        (c["sha"], gh_api(f"repos/{repo}/commits/{c['sha']}", "application/vnd.github.patch"))
        for c in commits
    ]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    ap.add_argument("pr", type=int, help="pull request number in the source repo")
    ap.add_argument("ref", help="fork commit or branch, resolvable in the current clone")
    ap.add_argument("--source-repo", default=DEFAULT_SOURCE_REPO, help=f"default {DEFAULT_SOURCE_REPO}")
    ap.add_argument("--diff-file", help="read the PR diff from this file instead of GitHub (testing)")
    ap.add_argument("--commit-patches-file", help="mbox of the PR's per-commit patches (testing)")
    ap.add_argument("--max-history", type=int, default=5000, help="newest N fork commits to index")
    args = ap.parse_args(argv)

    try:
        sha = run(["git", "rev-parse", "--verify", f"{args.ref}^{{commit}}"]).strip()
    except LookupFailure:
        print(f"ERROR: ref {args.ref!r} does not resolve in this clone (fetch it first; this tool never fetches).", file=sys.stderr)
        return 2
    try:
        if args.diff_file:
            diff = open(args.diff_file, encoding="utf-8").read()
            commit_mbox = open(args.commit_patches_file, encoding="utf-8").read() if args.commit_patches_file else ""
            commit_patches = [("(file)", commit_mbox)] if commit_mbox else []
        else:
            diff = gh_api(f"repos/{args.source_repo}/pulls/{args.pr}", "application/vnd.github.v3.diff")
            commit_patches = pr_commit_patches(args.source_repo, args.pr)
        files = parse_diff(diff)
        if not files:
            print(f"ERROR: PR #{args.pr} diff has no files.", file=sys.stderr)
            return 2
        paths = [f.path for f in files]
        index = fork_patch_index(sha, paths, args.max_history)
    except LookupFailure as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(f"PR {args.source_repo}#{args.pr} vs {args.ref} ({sha[:12]}): {len(files)} file(s)")

    # 1. patch-id: whole PR, then per commit.
    methods: list[str] = []
    whole = patch_ids(diff)
    if whole and whole[0][0] in index:
        methods.append(f"patch-id: whole PR diff == fork commit {index[whole[0][0]][:12]}")
    per_commit: list[str] = []
    for csha, patch in commit_patches:
        for pid, _ in patch_ids(patch):
            if pid in index:
                per_commit.append(f"  PR commit {csha[:12]} == fork commit {index[pid][:12]}")
            else:
                per_commit.append(f"  PR commit {csha[:12]} has no patch-id match")
    all_commits_match = bool(per_commit) and all("no patch-id match" not in line for line in per_commit)
    if all_commits_match:
        methods.append("patch-id: every PR commit matches a fork commit")
    if per_commit:
        print("per-commit patch-id:")
        print("\n".join(per_commit))

    # 2. hunk presence.
    hunks_ok, evidence = check_hunks(files, sha)
    print("hunk presence at ref:")
    print("\n".join(evidence))
    if hunks_ok:
        methods.append("hunk presence: every added block present and removed block gone")

    if methods:
        print("PASS")
        print("  evidence: " + "; ".join(methods))
        return 0
    print("FAIL")
    print("  evidence: no patch-id match for the whole PR or all its commits, and hunk presence failed (see MISS lines)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
