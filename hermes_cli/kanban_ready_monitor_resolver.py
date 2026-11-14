"""Resolver digest and WIP projection helpers for aged-Ready monitoring."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Mapping


def binding_digest(conn: sqlite3.Connection, binding: Mapping[str, Any] | None, *,
                   profile: str | None = None,
                   owner_profiles: tuple[str, ...] = ()) -> str | None:
    if not binding:
        return None
    refs = {
        "owner_ref": str(binding["owner_ref"]),
        "manager_ref": str(binding["manager_ref"]),
        "acknowledgement_authority_refs": tuple(map(str, binding["acknowledgement_authority_refs"])),
    }
    profiles = tuple(sorted(set(owner_profiles or ((profile,) if profile else ()))))
    wip = {}
    if profiles:
        marks = ",".join("?" for _ in profiles)
        rows = conn.execute(
            f"SELECT assignee, status, COUNT(*) AS total FROM tasks "
            f"WHERE assignee IN ({marks}) AND status IN ('running','ready') GROUP BY assignee,status",
            profiles,
        ).fetchall()
        wip = {name: {"running": 0, "ready": 0} for name in profiles}
        for row in rows:
            wip[row["assignee"]][row["status"]] = row["total"]
    return hashlib.sha256(json.dumps({"refs": refs, "owner_wip": wip}, sort_keys=True).encode()).hexdigest()
