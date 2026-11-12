"""Canonical version 2 semantic codec for the WRI-2385 resolver boundary.

The codec is deliberately private. It encodes already-validated semantic values;
closed protocol schemas remain responsible for field admission and ordering.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from struct import pack
from typing import TypeAlias

_MAGIC = b"WRI2385\x02"


@dataclass(frozen=True, slots=True)
class UIntV2:
    """An arbitrary-precision unsigned protocol integer."""

    value: int

    def __post_init__(self) -> None:
        if type(self.value) is not int or self.value < 0:
            raise ValueError("UIntV2 requires a non-negative integer")


@dataclass(frozen=True, slots=True)
class SIntV2:
    """An arbitrary-precision signed protocol integer."""

    value: int

    def __post_init__(self) -> None:
        if type(self.value) is not int:
            raise ValueError("SIntV2 requires an integer")


@dataclass(frozen=True, slots=True)
class RecordV2:
    """A closed record whose values are already in contract field order."""

    values: tuple[CanonicalValueV2, ...]

    def __post_init__(self) -> None:
        if type(self.values) is not tuple:
            raise ValueError("RecordV2 values must be a tuple")


CanonicalValueV2: TypeAlias = (
    None
    | bool
    | str
    | bytes
    | UIntV2
    | SIntV2
    | list["CanonicalValueV2"]
    | dict[str, "CanonicalValueV2"]
    | RecordV2
)


def _sized(payload: bytes) -> bytes:
    if len(payload) > 0xFFFFFFFF:
        raise ValueError("canonical value exceeds the uint32 length limit")
    return pack(">I", len(payload)) + payload


def _counted(tag: bytes, values: list[CanonicalValueV2] | tuple[CanonicalValueV2, ...]) -> bytes:
    if len(values) > 0xFFFFFFFF:
        raise ValueError("canonical collection exceeds the uint32 item limit")
    return tag + pack(">I", len(values)) + b"".join(_encode(value) for value in values)


def _magnitude(value: int) -> bytes:
    return value.to_bytes((value.bit_length() + 7) // 8, "big")


def _encode(value: CanonicalValueV2) -> bytes:
    if value is None:
        return b"\x00"
    if value is False:
        return b"\x01"
    if value is True:
        return b"\x02"
    if isinstance(value, UIntV2):
        return b"\x03" + _sized(_magnitude(value.value))
    if isinstance(value, SIntV2):
        return b"\x04" + bytes((value.value < 0,)) + _sized(_magnitude(abs(value.value)))
    if isinstance(value, str):
        return b"\x05" + _sized(value.encode("utf-8"))
    if isinstance(value, bytes):
        return b"\x06" + _sized(value)
    if isinstance(value, list):
        return _counted(b"\x07", value)
    if isinstance(value, RecordV2):
        return _counted(b"\x08", value.values)
    if isinstance(value, dict):
        for key in value:
            if type(key) is not str:
                raise TypeError("canonical map keys must be strings")
        items = sorted(value.items(), key=lambda item: item[0].encode("utf-8"))
        if len(items) > 0xFFFFFFFF:
            raise ValueError("canonical map exceeds the uint32 item limit")
        return b"\x09" + pack(">I", len(items)) + b"".join(
            _encode(key) + _encode(item) for key, item in items
        )
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_bytes(value: CanonicalValueV2) -> bytes:
    """Encode one semantic value with the selected v2 codec and version marker."""

    return _MAGIC + _encode(value)


def domain_preimage(domain: str, *members: CanonicalValueV2) -> bytes:
    """Encode one closed domain tuple with the domain as its first member."""

    if not isinstance(domain, str) or not domain:
        raise ValueError("domain must be a non-empty string")
    return canonical_bytes(RecordV2((domain, *members)))


def semantic_digest(domain: str, *members: CanonicalValueV2) -> str:
    """Return lowercase SHA-256 for a complete version 2 domain preimage."""

    return hashlib.sha256(domain_preimage(domain, *members)).hexdigest()
