"""Hash-pinned archived observer-v2 source, loaded only in isolated test homes."""

import hashlib
import sys
from pathlib import Path

import pytest


def materialize_archived_v2(source, *, parent=False):
    """Copy hash-pinned inert source into a disposable import directory."""
    fixture = Path(__file__).parent
    source.mkdir()
    store_hash = ("695ee7b66072b5bd539fb18991402b9a938378ed1e317f2e69eb6d4170e69f78"
                  if parent else "3513e5370b3d6bcaa59bda81797b7a9080e19658250a233acff7b017b54b762e")
    sources = [
        ("local_observation_v2.py", store_hash,
         "wri2276_parent_local_observation_v2.fixture" if parent else "wri2276_local_observation_v2.fixture"),
        ("engineering_recovery_observer.py", "0cd560e01986c3e6e145ddad6b7e74bb06f922e84abe627736c45db6f47fda31",
         "wri2276_engineering_recovery_observer.fixture"),
    ]
    if parent:
        sources.append(("observer_parent_seam.py", "a5dab786d8b0b1d828ed7671a5ac8041785c03c5c69bfc91851423444d752768",
                        "wri2276_observer_parent_seam.fixture"))
    for name, expected, filename in sources:
        content = (fixture / filename).read_bytes()
        assert hashlib.sha256(content).hexdigest() == expected
        (source / name).write_bytes(content)
    return source


@pytest.fixture
def real_v2(tmp_path, monkeypatch):
    source = materialize_archived_v2(tmp_path / "archived-source")
    monkeypatch.syspath_prepend(str(source))
    monkeypatch.delitem(sys.modules, "engineering_recovery_observer", raising=False)
    monkeypatch.delitem(sys.modules, "local_observation_v2", raising=False)
    import engineering_recovery_observer as legacy
    import local_observation_v2 as v2

    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(legacy, "PROFILE_HOME", home.resolve())
    return home, legacy, v2
