"""Closed WRI-2276 inherited-import policy; not a general lint waiver.

Authority: Seth A and affected review aa2a14e222a8ea7fce5af514eeb2e13208b84dbe1dadc92a58636c53cca68059,
Linear Resource 6bcf0416-7b91-460f-8ff0-90b5780bb0dc. Runtime bytes stay frozen.
"""
from __future__ import annotations

import json
import os
import stat
import subprocess
from collections import Counter
from hashlib import sha256
from pathlib import Path, PurePosixPath

BASE = "c0850d5c34ea4dcd666b42b3e4f571ac9cf29969"
OBSERVER = "scripts/observer_v2/engineering_recovery_observer.py"
PARENT = "scripts/observer_v2/observer_parent_seam.py"
NATIVE = "scripts/observer_v2/observer_native_v2.py"
HASHES = {
    OBSERVER: "b13bf6d9b00381c915c60a3f9017e94b3914a6225872e9a4d266d2476a2a1134",
    PARENT: "b9e762b53dc820cb47b0833bf81dfb6bb3322f64c1b31c96f2c90b74bdc302e2",
    NATIVE: "04add5c2c28ad50672bb1a0e160b22d26b3efbd1d89ad042fa6ccb9cd29648af",
}
PASS = "PASS under reviewed WRI-2276 frozen-import policy"
REASON = "reviewed frozen private POSIX import; runtime repair requires affected review"


class PolicyError(RuntimeError):
    """Eligibility or the closed finding census could not be established."""


def _point(row, column):
    return {"row": row, "column": column}


def _edit(row, column, end_row, end_column, content):
    return {"location": _point(row, column), "end_location": _point(end_row, end_column),
            "content": content}


def _ruff(path, rule, row, column, end_column, message, slug, fix):
    return {"filename": path, "code": rule, "location": _point(row, column),
            "end_location": _point(row, end_column), "message": message,
            "cell": None, "noqa_row": row, "severity": "error", "fix": fix,
            "url": "https://docs.astral.sh/ruff/rules/" + slug}


def _regex(row, column):
    return _ruff(OBSERVER, "FURB167", row, column, column + 4,
                 "Use of regular expression alias `re.I`", "regex-flag-alias",
                 {"applicability": "safe", "message": "Replace with `re.IGNORECASE`",
                  "edits": [_edit(14, 1, 14, 10, "import re"),
                            _edit(row, column, row, column + 4, "re.IGNORECASE")]})


def _health(path, rule, line, scope, detail):
    return dict(path=path, rule=rule, line=line, scope=scope, detail=detail,
                blocking=True, allowed_reason=None)


def _cc(path, line, scope, value):
    return _health(path, "CC", line, scope,
                   f"CC {value} > target 20 (new code must meet the target)")


_POSIX_MESSAGE = ("fcntl/pwd/grp/termios/resource/pty/tty do not exist on Windows; a module-level "
                  "import raises ModuleNotFoundError and breaks every module that imports this one.")
_POSIX_FIX = ("Import lazily inside the function that needs it, or\ntry:\n    import fcntl\n"
              "except ImportError:\n    fcntl = None  # and take the Windows path when None")
_FD_MESSAGE = ("os.fdopen() without an explicit encoding= uses the platform default (cp1252/mbcs on "
               "Windows) in text mode — the same mojibake class as bare open(). ruff PLW1514 does "
               "not cover fdopen, so this checker is the only gate.")
_FD_FIX = ("reads: os.fdopen(fd, 'r', encoding='utf-8-sig')  writes: os.fdopen(fd, 'w', "
           "encoding='utf-8')  binary: mode 'rb'/'wb'")
_EXPECTED = {
    "footguns": [
        dict(path=OBSERVER, line=8, text="import fcntl",
             name="module-level import of a POSIX-only stdlib module",
             message=_POSIX_MESSAGE, fix=_POSIX_FIX),
        dict(path=OBSERVER, line=319,
             text='    with closing(os.fd' + 'open(os.open(directory / "ack.lock", os.O_CREAT | os.O_RDWR |',
             name="os.fdopen() without encoding= on text mode", message=_FD_MESSAGE, fix=_FD_FIX),
        dict(path=OBSERVER, line=322,
             text='        with closing(os.fd' + 'open(os.open(directory / "local-observation.lock", os.O_CREAT |',
             name="os.fdopen() without encoding= on text mode", message=_FD_MESSAGE, fix=_FD_FIX),
    ],
    "ruff": [
        _regex(85, 79), _regex(88, 83),
        _ruff(OBSERVER, "F841", 577, 5, 9, "Local variable `path` is assigned to but never used",
              "unused-variable", {"applicability": "unsafe",
              "message": "Remove assignment to unused variable `path`",
              "edits": [_edit(577, 5, 577, 12, "")]}),
        _ruff(PARENT, "RUF023", 54, 17, 84, "`ParentAuthority.__slots__` is not sorted",
              "unsorted-dunder-slots", {"applicability": "safe",
              "message": "Apply a natural sort to `ParentAuthority.__slots__`",
              "edits": [_edit(54, 17, 54, 84,
                '("binding", "claim", "execution_id", "job_id", "pid", "process_id")')]}),
    ],
    "health": [],
}


def read_source(repo: Path, path: str):
    target = repo / path
    for part in (target, *target.parents):
        if part == repo.parent:
            break
        if part.is_symlink():
            raise PolicyError(f"source symlink: {part}")
    try:
        initial = target.lstat()
        if not stat.S_ISREG(initial.st_mode):
            raise PolicyError(f"nonregular source: {path}")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        fd = os.open(target, flags)
        with os.fdopen(fd, "rb") as stream:
            before = os.fstat(stream.fileno())
            raw = stream.read()
            after = os.fstat(stream.fileno())
    except FileNotFoundError:
        return None
    identity = _stat_identity(after)
    if not all(_stat_identity(s) == identity for s in (initial, before, target.lstat())):
        raise PolicyError(f"source replaced during read: {path}")
    return raw, identity


def _stat_identity(value):
    return (value.st_dev, value.st_ino, value.st_mode, value.st_size,
            value.st_mtime_ns, value.st_ctime_ns)


def git_source_reader(repo: Path, tree: str):
    def reader(path):
        entry = _git_bytes(repo, "ls-tree", "-z", tree, "--", path)
        if not entry:
            return None
        meta, name = entry.rstrip(b"\0").split(b"\t", 1)
        mode, kind, oid = meta.split()
        if mode != b"100644" or kind != b"blob" or name.decode() != path:
            raise PolicyError(f"nonregular Git source: {path}")
        raw = _git_bytes(repo, "cat-file", "blob", oid.decode())
        return raw, (tree, mode.decode(), oid.decode())
    return reader


def _git_bytes(repo, *args):
    result = subprocess.run(["git", *args], cwd=repo, capture_output=True,
                            stdin=subprocess.DEVNULL, timeout=120, check=False)
    if result.returncode:
        raise PolicyError(f"Git source read failed: {result.stderr.decode(errors='replace')}")
    return result.stdout


def capture_sources(repo, source_reader=None):
    reader = source_reader or (lambda path: read_source(repo, path))
    try:
        found = {path: reader(path) for path in HASHES}
    except (OSError, ValueError) as exc:
        raise PolicyError(f"source read failed: {exc}") from exc
    if all(value is None for value in found.values()):
        return {}
    for path, value in found.items():
        if value is None or sha256(value[0]).hexdigest() != HASHES[path]:
            raise PolicyError(f"frozen source mismatch: {path}")
    return {path: (sha256(value[0]).hexdigest(), value[1]) for path, value in found.items()}


def _relative(repo, spelling):
    if not isinstance(spelling, str) or "\\" in spelling:
        raise PolicyError("ambiguous diagnostic path")
    path = Path(spelling)
    if ".." in path.parts or "." in PurePosixPath(spelling).parts:
        raise PolicyError(f"escaping diagnostic path: {spelling}")
    if path.is_absolute():
        try:
            spelling = path.relative_to(repo).as_posix()
        except ValueError as exc:
            raise PolicyError(f"outside diagnostic path: {spelling}") from exc
    if not spelling or PurePosixPath(spelling).as_posix() != spelling:
        raise PolicyError(f"ambiguous diagnostic path: {spelling}")
    return spelling


def _normalized(repo, checker, finding):
    if not isinstance(finding, dict):
        raise PolicyError("unsupported diagnostic shape")
    key = "filename" if checker == "ruff" else "path"
    item = dict(finding)
    try:
        item[key] = _relative(repo, item[key])
    except (KeyError, TypeError) as exc:
        raise PolicyError("unsupported diagnostic path") from exc
    return item, key


def _signature(item):
    return json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def partition_findings(repo, checker, findings, *, source_reader=None, before=None, base=None):
    if checker not in _EXPECTED:
        raise PolicyError(f"unsupported checker: {checker}")
    current = capture_sources(repo, source_reader)
    if before is not None and current != before:
        raise PolicyError("source identity changed during scan")
    normal, scoped = [], []
    for finding in findings:
        item, key = _normalized(repo, checker, finding)
        (scoped if item[key] in HASHES else normal).append(item)
    if not current and not scoped:
        return normal, []
    if not current:
        raise PolicyError("scoped diagnostics without frozen sources")
    if checker == "health" and base != BASE:
        raise PolicyError(f"health policy base mismatch: {base}")
    if Counter(map(_signature, scoped)) != Counter(map(_signature, _EXPECTED[checker])):
        raise PolicyError(f"{checker} frozen finding census drift; expected {len(_EXPECTED[checker])}, got {len(scoped)}")
    return normal, scoped


def exception_reason(item):
    rule = item.get("rule", item.get("code", item.get("name")))
    if rule == "BLE001":
        return ("failed durability readback re-raises" if item["line"] == 730 else
                "executable boundary prints CRON_FAILURE and exits 1") + "; inherited missing traceback accepted"
    reasons = {
        "CC": "exact inherited function complexity accepted; target remains 20",
        "F841": "inherited unused local; frozen runtime bytes retained",
        "FURB167": "inherited regex alias; frozen runtime bytes retained",
        "RUF023": "inherited slots ordering; frozen runtime bytes retained",
        "module-level import of a POSIX-only stdlib module": "private POSIX-targeted composition; not Windows support",
        "os.fdopen() without encoding= on text mode": "lock-only stream; no ledger serialization through this stream",
    }
    return reasons[rule]


def report_exceptions(checker, exceptions, *, passed):
    for item in exceptions:
        path = item.get("path", item.get("filename"))
        line = item.get("line", item.get("location", {}).get("row"))
        rule = item.get("rule", item.get("code", item.get("name")))
        column = item.get("location", {}).get("column")
        site = f"{path}:{line}" + (f":{column}" if column is not None else "")
        detail = item.get("detail", item.get("message", ""))
        print(f"WRI-2276 exception: {checker} {site} {rule}; "
              f"scope={item.get('scope', '')}; detail={detail}; "
              f"sha256={HASHES[path]}; reason={exception_reason(item)}")
    if exceptions:
        print(f"{checker}: {len(exceptions)} reviewed exceptions; remaining blockers={'0' if passed else 'present'}")
        if passed:
            print(PASS)
