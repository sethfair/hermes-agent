"""Run pinned full-tree Ruff with the closed frozen-import partition."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from scripts.code_health.ruff_runner import resolve_ruff
from scripts.frozen_import_policy import (
    PolicyError, capture_sources, partition_findings, report_exceptions,
)


def run(repo: Path) -> int:
    try:
        before = capture_sources(repo)
        proc = subprocess.run([*resolve_ruff(repo), "check", ".", "--output-format", "json"],
                              cwd=repo, capture_output=True, text=True, encoding="utf-8",
                              stdin=subprocess.DEVNULL, timeout=600, check=False)
        if proc.returncode not in (0, 1):
            raise PolicyError(f"Ruff tooling failed ({proc.returncode}): {proc.stderr}")
        records = json.loads(proc.stdout)
        if not isinstance(records, list) or (proc.returncode == 1 and not records):
            raise PolicyError("Ruff failed without structured findings")
        if proc.returncode == 0 and records:
            raise PolicyError("Ruff status contradicts structured findings")
        retained, exceptions = partition_findings(repo, "ruff", records, before=before)
    except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        print(f"frozen Ruff policy failed: {exc}", file=sys.stderr)
        return 2
    if proc.stderr:
        print(proc.stderr.rstrip(), file=sys.stderr)
    for item in retained:
        print(json.dumps(item, sort_keys=True))
    report_exceptions("ruff", exceptions, passed=not retained)
    print(f"ruff: {len(records)} raw findings; {len(exceptions)} exceptions; {len(retained)} remaining blockers")
    return 1 if retained else 0


def main() -> int:
    if len(sys.argv) != 1:
        print("usage: python -m scripts.check_frozen_ruff", file=sys.stderr)
        return 2
    return run(Path.cwd())


if __name__ == "__main__":
    raise SystemExit(main())
