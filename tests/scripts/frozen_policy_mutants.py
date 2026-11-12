"""Explicit opt-in fault probes for the frozen-import guard tests.

Mutate disposable checker copies and isolated pytest memory, not package/runtime files.
Load with -p tests.scripts.frozen_policy_mutants and --frozen-mutant=<case>.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path


def pytest_addoption(parser):
    parser.addoption("--frozen-mutant", choices=("source", "census", "aggregate"))
    parser.addoption("--frozen-mutant-dir")


def _changed(text, case):
    replacements = {
        "source": ("value is None or sha256(value[0]).hexdigest() != HASHES[path]", "value is None"),
        "census": ("if Counter(map(_signature, scoped)) != Counter(map(_signature, _EXPECTED[checker])):",
                   "if False:"),
        "aggregate": ('name in {"health", "ruff", "footguns"}', 'name == "health"'),
    }
    old, new = replacements[case]
    assert text.count(old) == 1, (case, old)
    changed = text.replace(old, new)
    compile(changed, "frozen-policy-mutant", "exec")
    return changed


def pytest_configure(config):
    from tests.scripts import frozen_policy_support as support

    case = config.getoption("--frozen-mutant")
    directory = config.getoption("--frozen-mutant-dir")
    if case is None or directory is None:
        raise RuntimeError("fault plugin requires an explicit case and owned evidence directory")
    output = Path(directory)
    output.mkdir(parents=True, exist_ok=True)
    original_tree = support.candidate_tree
    relative = "scripts/check" if case == "aggregate" else "scripts/frozen_import_policy.py"

    def candidate_tree(path):
        repo = original_tree(path)
        target = repo / relative
        original = target.read_text(encoding="utf-8")
        changed = _changed(original, case)
        target.write_text(changed, encoding="utf-8")
        receipt = dict(case=case, compiled=True, disposable_path=str(target),
                       original_sha256=hashlib.sha256(original.encode()).hexdigest(),
                       mutant_sha256=hashlib.sha256(changed.encode()).hexdigest(),
                       source_disk_unchanged=(support.REPO / relative).read_text(encoding="utf-8") == original)
        (output / f"{case}-{os.getpid()}.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
        return repo

    support.candidate_tree = candidate_tree
    if case == "census":
        from scripts import frozen_import_policy as policy
        path = Path(policy.__file__)
        original = path.read_text(encoding="utf-8")
        exec(compile(_changed(original, case), str(path), "exec"), policy.__dict__)
