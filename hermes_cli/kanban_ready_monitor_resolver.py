"""Resolver digest and WIP projection helpers for aged-Ready monitoring."""

from __future__ import annotations

import hashlib
import json
import sqlite3
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


def owner_wip_totals(conn: sqlite3.Connection, bindings: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate actual Ready/running cards by the owner bound to each assignee profile."""
    owners: dict[str, set[str]] = {}
    for profile, binding in bindings.items():
        if binding.get("owner_ref"):
            owners.setdefault(_digest(str(binding["owner_ref"])), set()).add(str(profile))
    result = []
    for owner, profiles in sorted(owners.items()):
        marks = ",".join("?" for _ in profiles)
        total = conn.execute(
            f"SELECT COUNT(*) FROM tasks WHERE assignee IN ({marks}) AND status='running'",
            tuple(sorted(profiles)),
        ).fetchone()[0]
        result.append({"owner": owner, "total": total})
    return result
