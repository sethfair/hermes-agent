#!/usr/bin/env python3
"""Check that a pull request's changes are present in a fork ref (read-only).

    scripts/pr_patch_equivalence.py PR_NUMBER FORK_REF [--source-repo OWNER/REPO]

Prints PASS or FAIL with evidence; exit 0 = PASS, 1 = FAIL, 2 = usage/lookup error.

The PR is read from GitHub with ``gh api`` (nothing is fetched into or written to the
local repository). FORK_REF is any commit-ish that resolves in the current clone, for
example ``origin/main`` after the caller's own ``git fetch``. Two checks, strongest first:

1. content at FORK_REF (required for PASS): for each hunk of the PR diff, the added lines
   appear as a contiguous block in the file at FORK_REF *in the hunk's position* (the
   block's adjacent context lines, before or after, must sit next to it, so a copy of the
   same lines elsewhere in the file does not count) and the removed lines no longer do.
   Files without text hunks are checked by blob id (binary), by mode, and for renames by
   the old path being gone. Whitespace at line ends is ignored.
2. patch-id (``git patch-id --stable``): the whole-PR diff, or each PR commit, equals a
   commit already in FORK_REF's history. This is extra evidence only: a commit that landed
   and was later reverted still matches, so a patch-id match never turns a failed content
   check into PASS.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_SOURCE_REPO = "writerflow/hermes-kanban"
HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@")
INDEX_RE = re.compile(r"^index ([0-9a-f]+)\.\.([0-9a-f]+)(?: (\d+))?$")
CONTEXT_LINES = 3
TIMEOUT = 120


class LookupFailure(Exception):
    """A required input (PR, ref, tool) could not be read."""


def run(args: list[str], stdin: str | None = None) -> str:
    try:
        proc = subprocess.run(
            args, input=stdin, capture_output=True, text=True, encoding="utf-8", errors="replace",
            check=False, timeout=TIMEOUT,
        )
    except FileNotFoundError as exc:
        raise LookupFailure(f"required tool not found: {args[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise LookupFailure(f"{' '.join(args[:3])} timed out after {TIMEOUT}s") from exc
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
    old_path: str | None = None  # set for a rename or copy source
    new_blob: str | None = None  # abbreviated post-image blob id from the ``index`` line
    new_mode: str | None = None
    binary: bool = False


def parse_diff(diff: str) -> list[FileDiff]:
    files: list[FileDiff] = []
    cur: FileDiff | None = None
    hunk: list[str] | None = None
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            m = re.match(r"diff --git a/(.*) b/(.*)$", line)
            cur = FileDiff(path=m.group(2) if m else line, status="modified")
            if m and m.group(1) != m.group(2):
                cur.old_path = m.group(1)
            files.append(cur)
            hunk = None
        elif cur is None:
            continue
        elif hunk is None and line.startswith("new file mode"):
            cur.status = "added"
        elif hunk is None and line.startswith("deleted file mode"):
            cur.status = "deleted"
        elif hunk is None and line.startswith("rename from "):
            cur.old_path = line[len("rename from "):]
        elif hunk is None and line.startswith("new mode "):
            cur.new_mode = line[len("new mode "):].strip()
        elif hunk is None and (idx := INDEX_RE.match(line)):
            cur.new_blob = idx.group(2)
            if idx.group(3):
                cur.new_mode = idx.group(3)
        elif hunk is None and (line.startswith("Binary files ") or line.startswith("GIT binary patch")):
            cur.binary = True
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


def added_blocks(hunk: list[str]) -> list[tuple[list[str], list[str], list[str]]]:
    """Added runs of a hunk as (block, before, after).

    ``before``/``after`` are the post-image context lines directly adjacent to the run, at
    most CONTEXT_LINES each; a run touching another change on a side has no context there.
    """
    post = [(ln[0], ln[1:].rstrip()) for ln in hunk if ln[0] in "+ "]
    out = []
    i = 0
    while i < len(post):
        if post[i][0] != "+":
            i += 1
            continue
        j = i
        while j < len(post) and post[j][0] == "+":
            j += 1
        before: list[str] = []
        k = i - 1
        while k >= 0 and post[k][0] == " " and len(before) < CONTEXT_LINES:
            before.insert(0, post[k][1])
            k -= 1
        after: list[str] = []
        k = j
        while k < len(post) and post[k][0] == " " and len(after) < CONTEXT_LINES:
            after.append(post[k][1])
            k += 1
        out.append(([t for _, t in post[i:j]], before, after))
        i = j
    return out


def contains_anchored(lines: list[str], block: list[str], before: list[str], after: list[str]) -> bool:
    """Block present with its leading or trailing hunk context directly beside it.

    One side is enough so later edits to the other side's context do not fail a landed
    change. With no context at all (new file, whole-file rewrite) the plain block counts.
    """
    n = len(block)
    for i in range(len(lines) - n + 1):
        if lines[i : i + n] != block:
            continue
        if not before and not after:
            return True
        if before and i >= len(before) and lines[i - len(before) : i] == before:
            return True
        if after and lines[i + n : i + n + len(after)] == after:
            return True
    return False


def git_text(args: list[str]) -> str | None:
    try:
        proc = subprocess.run(
            args, capture_output=True, text=True, encoding="utf-8", errors="replace",
            check=False, timeout=TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        raise LookupFailure(f"{' '.join(args[:3])} timed out after {TIMEOUT}s") from exc
    return proc.stdout if proc.returncode == 0 else None


def file_at(ref: str, path: str) -> list[str] | None:
    out = git_text(["git", "show", f"{ref}:{path}"])
    return None if out is None else [ln.rstrip() for ln in out.splitlines()]


def blob_at(ref: str, path: str) -> str | None:
    out = git_text(["git", "rev-parse", "--verify", f"{ref}:{path}"])
    return out.strip() if out else None


def mode_at(ref: str, path: str) -> str | None:
    out = git_text(["git", "ls-tree", ref, "--", path])
    return out.split()[0] if out and out.split() else None


def check_no_text(fd: FileDiff, ref: str) -> tuple[bool, list[str]]:
    """Evidence for a file with no text hunks: blob id (binary), mode, rename source."""
    ok = True
    ev: list[str] = []
    if fd.old_path and fd.old_path != fd.path:
        gone = file_at(ref, fd.old_path) is None
        ev.append(f"  {'ok  ' if gone else 'MISS'} {fd.path}: renamed from {fd.old_path}, old path {'gone' if gone else 'still present'} at ref")
        ok &= gone
    if fd.new_blob and set(fd.new_blob) != {"0"}:
        actual = blob_at(ref, fd.path)
        same = bool(actual) and actual.startswith(fd.new_blob)
        ev.append(f"  {'ok  ' if same else 'MISS'} {fd.path}: blob {fd.new_blob} {'matches' if same else 'differs from ' + str(actual)[:12]} at ref")
        ok &= same
    elif fd.binary:
        ev.append(f"  MISS {fd.path}: binary change with no index line, cannot verify content")
        ok = False
    if fd.new_mode:
        mode = mode_at(ref, fd.path)
        good = mode == fd.new_mode
        ev.append(f"  {'ok  ' if good else 'MISS'} {fd.path}: mode {fd.new_mode} {'matches' if good else 'differs (' + str(mode) + ')'} at ref")
        ok &= good
    if not ev:
        ev.append(f"  MISS {fd.path}: nothing in the diff to verify (no hunks, blob id, mode or rename)")
        ok = False
    return ok, ev


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
            good, ev = check_no_text(fd, ref)
            evidence.extend(ev)
            ok &= good
            continue
        bad: list[str] = []
        for i, hunk in enumerate(fd.hunks, 1):
            for block, before, after in added_blocks(hunk):
                if not any(block):
                    continue  # blank-only runs carry no evidence
                if not contains_anchored(content, block, before, after):
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
    pages = json.loads(run(["gh", "api", "--paginate", "--slurp", f"repos/{repo}/pulls/{pr}/commits?per_page=100"]))
    commits = [c for page in pages for c in page]
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
            diff = Path(args.diff_file).read_text(encoding="utf-8-sig")
            commit_mbox = Path(args.commit_patches_file).read_text(encoding="utf-8-sig") if args.commit_patches_file else ""
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

    # 2. content at the ref is required; patch-id above is supporting evidence only.
    hunks_ok, evidence = check_hunks(files, sha)
    print("content at ref:")
    print("\n".join(evidence))
    if hunks_ok:
        methods.append("content: every added block present in position, removed blocks gone")
        print("PASS")
        print("  evidence: " + "; ".join(methods))
        return 0
    print("FAIL")
    note = " (patch-id matched, but a matching commit may have been reverted)" if methods else ""
    print("  evidence: content check failed, see MISS lines" + note)
    return 1


if __name__ == "__main__":
    sys.exit(main())
