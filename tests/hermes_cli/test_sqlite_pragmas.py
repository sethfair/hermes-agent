"""Behavior contracts for closed, integer-valued SQLite PRAGMA writes."""

from __future__ import annotations

import sqlite3

import pytest

from hermes_cli.sqlite_pragmas import IntegerPragma, set_integer_pragma


@pytest.mark.parametrize(
    ("pragma", "value", "readback"),
    [
        (IntegerPragma.BUSY_TIMEOUT, 321, 321),
        (IntegerPragma.CACHE_SIZE, -2048, -2048),
        (IntegerPragma.MMAP_SIZE, 0, 0),
        (IntegerPragma.TEMP_STORE, 2, 2),
        (IntegerPragma.WAL_AUTOCHECKPOINT, 17, 17),
        (IntegerPragma.JOURNAL_SIZE_LIMIT, 4096, 4096),
    ],
)
def test_set_integer_pragma_applies_closed_typed_operation(
    tmp_path, pragma, value, readback
):
    conn = sqlite3.connect(tmp_path / "pragmas.db", isolation_level=None)
    try:
        set_integer_pragma(conn, pragma, value)
        row = conn.execute(f"PRAGMA {pragma.value}").fetchone()
        assert row is not None
        assert row[0] == readback
    finally:
        conn.close()


@pytest.mark.parametrize("value", [True, "1", "1; DROP TABLE tasks"])
def test_set_integer_pragma_rejects_untyped_values(value):
    conn = sqlite3.connect(":memory:", isolation_level=None)
    try:
        with pytest.raises(TypeError, match="integer PRAGMA value"):
            set_integer_pragma(conn, IntegerPragma.BUSY_TIMEOUT, value)
    finally:
        conn.close()