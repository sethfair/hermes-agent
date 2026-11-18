"""Pure models and validation for protected Kanban task transitions."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Optional

IDENTITY_VERSION = 1
IDENTITY_STATES = frozenset({"none", "known", "unknown"})
_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_TASK_ID_RE = re.compile(r"^t_[0-9a-f]{8}$")


@dataclass(frozen=True)
class BlockIdentity:
    state: str
    canonical: Optional[str] = None
    kind: Optional[str] = None
    subject: Optional[str] = None
    unknown_cause: Optional[str] = None


def canonicalize_block_identity(kind: Optional[str], subject: Optional[str]) -> BlockIdentity:
    if kind is None:
        return BlockIdentity("unknown", unknown_cause="missing_kind")
    if subject is None:
        return BlockIdentity("unknown", kind=kind, unknown_cause="missing_subject")
    if not isinstance(subject, str):
        return BlockIdentity("unknown", kind=kind, unknown_cause="invalid_subject")
    if kind == "dependency":
        normalized = subject
        valid = _TASK_ID_RE.fullmatch(normalized) is not None
    elif kind == "needs_input":
        normalized = subject.strip().lower()
        parts = normalized.split("/")
        valid = len(parts) == 2 and all(_SLUG_RE.fullmatch(part) for part in parts)
    elif kind in {"capability", "transient"}:
        normalized = subject.strip().lower()
        valid = _SLUG_RE.fullmatch(normalized) is not None
    else:
        return BlockIdentity("unknown", unknown_cause="invalid_kind")
    if not valid:
        return BlockIdentity("unknown", kind=kind, unknown_cause="invalid_subject")
    canonical = json.dumps(
        {"kind": kind, "subject": normalized, "v": IDENTITY_VERSION},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    )
    return BlockIdentity("known", canonical, kind, normalized)


def effective_block_identity(row: Any) -> BlockIdentity:
    state = _value(row, "block_identity_state")
    raw = _value(row, "block_identity")
    kind = _value(row, "block_kind")
    recurrences = int(_value(row, "block_recurrences") or 0)
    if state == "none":
        if raw is None and kind is None and recurrences == 0:
            return BlockIdentity("none")
        return BlockIdentity("unknown", unknown_cause="inconsistent_state")
    if state == "unknown":
        return BlockIdentity("unknown", unknown_cause="stored_unknown")
    if state != "known" or not isinstance(raw, str):
        return BlockIdentity("unknown", unknown_cause="invalid_state")
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return BlockIdentity("unknown", unknown_cause="malformed_identity")
    if not isinstance(parsed, dict) or set(parsed) != {"kind", "subject", "v"}:
        return BlockIdentity("unknown", unknown_cause="malformed_identity")
    candidate = canonicalize_block_identity(parsed.get("kind"), parsed.get("subject"))
    if parsed.get("v") != IDENTITY_VERSION or candidate.state != "known" \
            or candidate.canonical != raw or candidate.kind != kind \
            or recurrences < (0 if kind == "dependency" else 1):
        return BlockIdentity("unknown", unknown_cause="malformed_identity")
    return candidate


def identity_event_fields(identity: BlockIdentity) -> dict[str, object]:
    if identity.state == "known":
        return {"identity_version": IDENTITY_VERSION, "identity_state": "known", "identity": identity.canonical}
    return {
        "identity_version": IDENTITY_VERSION,
        "identity_state": "unknown",
        "unknown_cause": identity.unknown_cause or "unknown",
    }


def _value(row: Any, key: str) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError, TypeError):
        return None
