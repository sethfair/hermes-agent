"""Task graph validation and receipt-backed triage decomposition adapters."""
from __future__ import annotations

import secrets
import sqlite3
from typing import Optional


def _validate_children_graph(children: list) -> None:
    """Validate sibling references and reject dependency cycles before storage."""
    for index, child in enumerate(children):
        if not isinstance(child, dict):
            raise ValueError(f"child[{index}] is not a dict")
        title = child.get("title")
        if not isinstance(title, str) or not title.strip():
            raise ValueError(f"child[{index}].title is required")
        parents = child.get("parents") or []
        if not isinstance(parents, list):
            raise ValueError(f"child[{index}].parents must be a list")
        for parent in parents:
            if not isinstance(parent, int) or parent < 0 or parent >= len(children):
                raise ValueError(
                    f"child[{index}].parents[{parent}] is not a valid index into children"
                )
            if parent == index:
                raise ValueError(f"child[{index}] cannot list itself as a parent")

    incoming = [0] * len(children)
    adjacency: list[list[int]] = [[] for _ in children]
    for index, child in enumerate(children):
        for parent in child.get("parents") or []:
            adjacency[parent].append(index)
            incoming[index] += 1
    queue = [index for index, count in enumerate(incoming) if count == 0]
    seen = 0
    while queue:
        seen += 1
        for child in adjacency[queue.pop()]:
            incoming[child] -= 1
            if incoming[child] == 0:
                queue.append(child)
    if seen != len(children):
        raise ValueError("cyclic dependency detected in decomposed children list")


def decompose_triage_task(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    root_assignee: Optional[str],
    children: list[dict],
    author: Optional[str] = None,
    auto_promote: bool = True,
) -> Optional[list[str]]:
    """Atomically fan a triage root into a receipt-backed dependency graph."""
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_task_state import decompose_task_transition

    if not children:
        return None
    _validate_children_graph(children)
    if root_assignee is not None:
        root_assignee = kb._canonical_assignee(root_assignee)
    child_ids = tuple(kb._new_task_id() for _ in children)
    outcome = decompose_task_transition(
        conn,
        task_id,
        actor_scope="legacy",
        operation_id=f"decompose:{task_id}:{secrets.token_hex(8)}",
        root_assignee=root_assignee,
        children=tuple(children),
        child_ids=child_ids,
        author=author,
    )
    if not outcome.committed:
        return None
    if auto_promote:
        kb.recompute_ready(conn)
    return list(outcome.child_ids)
