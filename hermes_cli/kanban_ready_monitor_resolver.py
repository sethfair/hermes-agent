"""Resolver digest and WIP projection helpers for aged-Ready monitoring."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def binding_digest(conn: sqlite3.Connection, binding: Mapping[str, Any] | None, *,
                   profile: str | None = None,
                   owner_profiles: tuple[str, ...] = ()) -> str | None:
    """Digest routing identity only; mutable WIP must not rotate a resolver generation."""
    if not binding:
        return None
    refs = {
        "owner_ref": str(binding["owner_ref"]),
        "manager_ref": str(binding["manager_ref"]),
        "acknowledgement_authority_refs": tuple(map(str, binding["acknowledgement_authority_refs"])),
    }
    return _digest(json.dumps(refs, sort_keys=True))


def host_wip_census(conn: sqlite3.Connection, config: Mapping[str, Any], *, now: int) -> list[tuple]:
    """Read every locally configured board without creating or repairing one."""
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_ready_monitor as monitor

    expected = config.get("expected_writer_scopes", {})
    pending = set(expected)
    census = []

    def collect(board_conn: sqlite3.Connection) -> None:
        identity = board_conn.execute(
            "SELECT board_scope_id FROM kanban_board_identity WHERE singleton=1"
        ).fetchone()
        board = identity[0] if identity else None
        if board not in pending:
            return
        profiles = {str(profile) for member in expected[board]["members"]
                    for profile in member["profiles"]}
        try:
            scopes = {row["profile"]: row for row in board_conn.execute(
                "SELECT * FROM kanban_monitor_scope_activation WHERE disabled_at IS NULL")}
            epochs = {profile: scopes[profile]["activation_epoch"] for profile in profiles if profile in scopes}
            counts = {profile: board_conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE status='running' AND assignee=?", (profile,)
            ).fetchone()[0] for profile in profiles}
            complete = {profile: bool(profile in scopes and monitor._projection_proven(
                board_conn, scopes[profile], now=now,
                stale_after_seconds=config["controller_stale_seconds"], config=config)) for profile in profiles}
            failures = {profile: (() if complete[profile] else ("incomplete_board",)) for profile in profiles}
        except sqlite3.Error:
            epochs, counts = {}, {profile: 0 for profile in profiles}
            complete = {profile: False for profile in profiles}
            failures = {profile: ("unreadable_board",) for profile in profiles}
        census.append((board, epochs, counts, complete, failures))
        pending.remove(board)

    collect(conn)
    for meta in kb.list_boards(include_archived=False):
        path = Path(meta["db_path"])
        try:
            other = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
            other.row_factory = sqlite3.Row
            try:
                collect(other)
            finally:
                other.close()
        except (OSError, sqlite3.Error):
            continue
    for board in sorted(pending):
        profiles = {str(profile) for member in expected[board]["members"] for profile in member["profiles"]}
        census.append((board, {}, {profile: 0 for profile in profiles},
                       {profile: False for profile in profiles},
                       {profile: ("missing_board",) for profile in profiles}))
    return census


def owner_wip_totals(conn: sqlite3.Connection, bindings: Mapping[str, Mapping[str, Any]], *,
                     profile_counts: Mapping[str, int] | None = None,
                     completeness: Mapping[str, bool] | None = None) -> list[dict[str, Any]]:
    """Aggregate actual Ready/running cards by the owner bound to each assignee profile."""
    owners: dict[str, set[str]] = {}
    for profile, binding in bindings.items():
        if binding.get("owner_ref"):
            owners.setdefault(_digest(str(binding["owner_ref"])), set()).add(str(profile))
    result = []
    for owner, profiles in sorted(owners.items()):
        if profile_counts is None:
            marks = ",".join("?" for _ in profiles)
            total = conn.execute(f"SELECT COUNT(*) FROM tasks WHERE assignee IN ({marks}) AND status='running'",
                                 tuple(sorted(profiles))).fetchone()[0]
        else:
            total = (sum(profile_counts.get(profile, 0) for profile in profiles)
                     if all(completeness and completeness.get(profile) for profile in profiles) else "UNKNOWN")
        result.append({"owner": owner, "total": total})
    return result
