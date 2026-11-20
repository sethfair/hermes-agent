"""Read-only SQLite connection boundary."""

import sqlite3

import pytest

from hermes_cli.sqlite_readonly import connect_readonly


def test_readonly_connection_reads_without_mutation(tmp_path):
    database = tmp_path / "board.db"
    with sqlite3.connect(database) as writable:
        writable.execute("CREATE TABLE sample(value TEXT)")
        writable.execute("INSERT INTO sample VALUES ('ok')")

    with connect_readonly(database) as conn:
        assert conn.execute("SELECT value FROM sample").fetchone()[0] == "ok"
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("DELETE FROM sample")


def test_readonly_connection_does_not_create_missing_database(tmp_path):
    database = tmp_path / "missing.db"

    with pytest.raises(sqlite3.OperationalError, match="unable to open"):
        with connect_readonly(database):
            pass

    assert not database.exists()
