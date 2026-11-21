"""Private SQLite staging-copy connection boundary."""

from pathlib import Path

import pytest

from hermes_cli.sqlite_private_copy import connect_private_copy


def test_private_copy_connection_writes_only_inside_declared_root(tmp_path):
    private_root = tmp_path / "staging"
    private_root.mkdir()
    database = private_root / "copy.db"

    with connect_private_copy(database, private_root=private_root) as conn:
        conn.execute("CREATE TABLE sample(value TEXT)")
        conn.execute("INSERT INTO sample VALUES ('ok')")
        conn.commit()

    with connect_private_copy(database, private_root=private_root) as conn:
        assert conn.execute("SELECT value FROM sample").fetchone()[0] == "ok"


@pytest.mark.parametrize("kind", ["outside", "symlink"])
def test_private_copy_connection_refuses_unowned_paths(tmp_path, kind):
    private_root = tmp_path / "staging"
    private_root.mkdir()
    outside = tmp_path / "outside.db"
    candidate = outside
    if kind == "symlink":
        outside.touch()
        candidate = private_root / "linked.db"
        candidate.symlink_to(outside)

    with pytest.raises(ValueError, match="private staging root"):
        with connect_private_copy(candidate, private_root=private_root):
            pass
