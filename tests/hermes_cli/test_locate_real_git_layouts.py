"""``_locate_real_git`` must find git-core in every Git-for-Windows layout.

Git for Windows x64 now ships its MSYS2 environment as ``ucrt64`` (the windows-latest image has no
``mingw64``), so a locator that only knows ``mingw64`` finds no real git and never heals a broken
trampoline. The managed PortableGit tree under the Hermes root follows the same layout.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from hermes_cli import update_cmd


# POSIX only: on Windows the runner's own ``C:\Program Files\Git`` candidates are searched first, so the
# managed tree cannot be isolated there (the Windows live test covers that host).
@pytest.mark.platforms("posix")
@pytest.mark.parametrize("env_dir", ["ucrt64", "clangarm64", "mingw64"])
def test_locate_real_git_finds_managed_git_in_each_layout(tmp_path, monkeypatch, env_dir):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    real = tmp_path / "git" / env_dir / "libexec" / "git-core" / "git.exe"
    real.parent.mkdir(parents=True)
    real.write_bytes(b"")

    with patch("hermes_cli.update_cmd_git._probe_fork_bomb", return_value=False):
        found = update_cmd._locate_real_git()

    assert found is not None and Path(found) == real
