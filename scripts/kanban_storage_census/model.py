"""Value objects shared by the Kanban storage census."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, order=True)
class InventoryItem:
    path: str
    function: str
    sink_ordinal: int
    statement_hash: str
    effects: tuple[str, ...]
    rationale: str

    def as_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["effects"] = list(self.effects)
        return value


@dataclass(frozen=True, order=True)
class Problem:
    path: str
    line: int
    kind: str
    detail: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)
