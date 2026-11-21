#!/usr/bin/env python3
"""Fail-closed census of production code that can mutate Kanban task state.

Production files come from tracked Git files intersected with setuptools package discovery,
setup.py's root-module contract, and configured entry points. Every SQL sink must resolve to a
finite statement set. Protected effects pass only when the complete deterministic inventory row
matches an exact versioned allowlist entry.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.kanban_storage_census import analyze_repository


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT, help="repository root (default: checkout)")
    parser.add_argument("--allowlist", type=Path, help="versioned exact inventory allowlist JSON")
    parser.add_argument("--json", action="store_true", help="emit the complete machine-readable report")
    args = parser.parse_args(argv)
    repository = args.root.resolve()
    allowlist = args.allowlist
    if allowlist is None and repository == ROOT:
        allowlist = Path(__file__).with_name("kanban_storage_census") / "exact_allowlist.json"
    report = analyze_repository(repository, allowlist)
    if args.json:
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    else:
        for item in report["inventory"]:
            print("INVENTORY " + json.dumps(item, sort_keys=True, separators=(",", ":")))
        for problem in report["problems"]:
            print("REFUSAL " + json.dumps(problem, sort_keys=True, separators=(",", ":")), file=sys.stderr)
        if not report["problems"]:
            print(f"check_kanban_storage_writers: OK ({len(report['inventory'])} allowed sinks)")
    return 1 if report["problems"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
