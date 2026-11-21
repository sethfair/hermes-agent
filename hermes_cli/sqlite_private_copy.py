"""Bound writable SQLite connections to caller-owned private staging trees."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
import sqlite3


@contextmanager
def connect_private_copy(
    path: str | Path,
    *,
    private_root: str | Path,
) -> Iterator[sqlite3.Connection]:
    """Open a writable private copy only beneath its declared staging root."""
    root = Path(private_root).resolve(strict=True)
    candidate = Path(path)
    if not root.is_dir() or candidate.is_symlink():
        raise ValueError("SQLite copy must be a regular path under its private staging root")
    resolved = candidate.resolve(strict=False)
    if resolved == root or not resolved.is_relative_to(root):
        raise ValueError("SQLite copy must be beneath its private staging root")
    if resolved.exists() and not resolved.is_file():
        raise ValueError("SQLite copy must be a regular path under its private staging root")
    conn = sqlite3.connect(resolved)
    try:
        yield conn
    finally:
        conn.close()
