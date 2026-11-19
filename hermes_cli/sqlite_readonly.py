"""Fail-closed read-only SQLite connection boundary."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sqlite3
from typing import Iterator


@contextmanager
def connect_readonly(
    path: str | Path,
    *,
    immutable: bool = False,
    timeout: float = 5.0,
) -> Iterator[sqlite3.Connection]:
    """Open an existing SQLite database with mutation disabled."""
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    if immutable:
        uri += "&immutable=1"
    conn = sqlite3.connect(uri, uri=True, timeout=timeout)
    try:
        yield conn
    finally:
        conn.close()
