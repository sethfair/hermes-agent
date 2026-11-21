"""Closed typed operations for integer-valued SQLite PRAGMA assignments."""

from __future__ import annotations

import sqlite3
from enum import Enum


class IntegerPragma(Enum):
    """Integer-valued PRAGMA names accepted by :func:`set_integer_pragma`."""

    BUSY_TIMEOUT = "busy_timeout"
    CACHE_SIZE = "cache_size"
    MMAP_SIZE = "mmap_size"
    TEMP_STORE = "temp_store"
    WAL_AUTOCHECKPOINT = "wal_autocheckpoint"
    JOURNAL_SIZE_LIMIT = "journal_size_limit"


def set_integer_pragma(
    connection: sqlite3.Connection,
    pragma: IntegerPragma,
    value: int,
) -> None:
    """Apply a closed integer PRAGMA operation."""
    if type(value) is not int:
        raise TypeError("integer PRAGMA value required")
    if pragma is IntegerPragma.BUSY_TIMEOUT:
        connection.execute(f"PRAGMA busy_timeout={value}")
    elif pragma is IntegerPragma.CACHE_SIZE:
        connection.execute(f"PRAGMA cache_size={value}")
    elif pragma is IntegerPragma.MMAP_SIZE:
        connection.execute(f"PRAGMA mmap_size={value}")
    elif pragma is IntegerPragma.TEMP_STORE:
        connection.execute(f"PRAGMA temp_store={value}")
    elif pragma is IntegerPragma.WAL_AUTOCHECKPOINT:
        connection.execute(f"PRAGMA wal_autocheckpoint={value}")
    elif pragma is IntegerPragma.JOURNAL_SIZE_LIMIT:
        connection.execute(f"PRAGMA journal_size_limit={value}")
    else:
        raise TypeError("IntegerPragma required")
