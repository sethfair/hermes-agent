"""Normalize finite SQL and classify Kanban protected effects."""

from __future__ import annotations

import hashlib
import re


_PROTECTED_TABLES = {"tasks", "task_runs", "task_links", "task_events", "task_transition_receipts"}
_PROTECTED_TASK_COLUMNS = {
    "status", "block_kind", "block_recurrences", "block_identity", "block_identity_state",
    "claim_lock", "claim_expires", "worker_pid", "worker_started_at", "current_run_id",
    "action_generation", "park_generation", "recurrence", "recurrence_count",
}
_IDENTIFIER = r'(?P<table>"[^"]+"|`[^`]+`|\[[^]]+\]|[A-Za-z_][A-Za-z0-9_]*)'


def normalize_sql(statement: str) -> str:
    without_comments = re.sub(r"/\*.*?\*/|--[^\n]*", " ", statement, flags=re.DOTALL)
    return " ".join(without_comments.split()).strip()


def statement_hash(statement: str) -> str:
    return hashlib.sha256(normalize_sql(statement).encode("utf-8")).hexdigest()


def _identifier(raw: str) -> str:
    return raw.strip('"`[]').lower()


def _update_columns(fragment: str) -> set[str]:
    match = re.search(
        r"\bSET\b(?P<body>.*?)(?:\bWHERE\b|\bRETURNING\b|;|$)", fragment,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return set()
    return {
        _identifier(item.group(1))
        for item in re.finditer(r'(?:^|,)\s*("[^"]+"|`[^`]+`|\[[^]]+\]|[A-Za-z_][A-Za-z0-9_]*)\s*=', match.group("body"))
    }


def protected_effects(statement: str) -> tuple[str, ...]:
    sql = normalize_sql(statement)
    effects: set[str] = set()
    insert_pattern = rf"\b(?:INSERT(?:\s+OR\s+REPLACE)?|REPLACE)\s+INTO\s+{_IDENTIFIER}"
    for match in re.finditer(insert_pattern, sql, re.IGNORECASE):
        table = _identifier(match.group("table"))
        if table in _PROTECTED_TABLES:
            effects.add(f"{table}.insert")
            tail = sql[match.end():]
            conflict = re.search(
                r"\bON\s+CONFLICT\b.*?\bDO\s+UPDATE\b(?P<body>.*?)(?:;|$)", tail,
                re.IGNORECASE | re.DOTALL,
            )
            if conflict and table == "tasks":
                for column in _update_columns(conflict.group("body")) & _PROTECTED_TASK_COLUMNS:
                    effects.add(f"tasks.update.{column}")
    for match in re.finditer(
        rf"\bUPDATE\s+(?:OR\s+\w+\s+)?{_IDENTIFIER}\s+SET\b", sql, re.IGNORECASE,
    ):
        table = _identifier(match.group("table"))
        if table not in _PROTECTED_TABLES:
            continue
        if table == "tasks":
            columns = _update_columns(sql[match.start():]) & _PROTECTED_TASK_COLUMNS
            effects.update(f"tasks.update.{column}" for column in columns)
        else:
            effects.add(f"{table}.update")
    for match in re.finditer(rf"\bDELETE\s+FROM\s+{_IDENTIFIER}", sql, re.IGNORECASE):
        table = _identifier(match.group("table"))
        if table in _PROTECTED_TABLES:
            effects.add(f"{table}.delete")
    for match in re.finditer(
        rf"\bCREATE\s+(?:TEMP(?:ORARY)?\s+)?TRIGGER\b.*?\bON\s+{_IDENTIFIER}", sql,
        re.IGNORECASE | re.DOTALL,
    ):
        table = _identifier(match.group("table"))
        if table in _PROTECTED_TABLES:
            effects.add(f"schema.trigger.{table}")
    for match in re.finditer(
        rf"\b(?:CREATE|ALTER|DROP)\s+TABLE\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?{_IDENTIFIER}",
        sql, re.IGNORECASE,
    ):
        table = _identifier(match.group("table"))
        if table in _PROTECTED_TABLES:
            effects.add(f"schema.table.{table}")
    return tuple(sorted(effects))
