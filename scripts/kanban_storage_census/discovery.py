"""Discover tracked production Python from packaging metadata."""

from __future__ import annotations

import fnmatch
import subprocess
import tomllib
from pathlib import Path


_NONPRODUCTION = ("tests/", "scripts/", "evals/", "examples/", "docs/", "website/")


def production_python_files(root: Path) -> tuple[list[Path], list[str]]:
    """Return tracked Python files shipped as packages, root modules, or entry points."""
    errors: list[str] = []
    try:
        raw = subprocess.run(
            ["git", "ls-files", "-z", "--", "*.py"], cwd=root, capture_output=True,
            timeout=60, check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        return [], [f"cannot enumerate tracked files: {exc}"]
    tracked = sorted(part.decode("utf-8", "surrogateescape") for part in raw.split(b"\0") if part)
    try:
        metadata = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8-sig"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        return [], [f"cannot parse pyproject.toml: {exc}"]

    finder = metadata.get("tool", {}).get("setuptools", {}).get("packages", {}).get("find", {})
    includes = finder.get("include", [])
    if not isinstance(includes, list) or not all(isinstance(item, str) for item in includes):
        return [], ["[tool.setuptools.packages.find].include must be a string list"]
    scripts = metadata.get("project", {}).get("scripts", {})
    entry_modules = {
        target.split(":", 1)[0].replace(".", "/") + ".py"
        for target in scripts.values() if isinstance(target, str)
    }

    selected: list[Path] = []
    for relative in tracked:
        if relative == "setup.py" or relative.startswith(_NONPRODUCTION):
            continue
        parts = relative.split("/")
        if len(parts) == 1:
            selected.append(root / relative)  # setup.py::_root_py_modules()
            continue
        module = relative[:-3].replace("/", ".")
        package = ".".join(parts[:-1])
        if relative in entry_modules or any(
            fnmatch.fnmatchcase(module, pattern) or fnmatch.fnmatchcase(package, pattern)
            for pattern in includes
        ):
            selected.append(root / relative)
    return selected, errors
