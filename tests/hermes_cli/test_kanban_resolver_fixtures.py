"""WRI-2385 v2 producer fixture and canonical-codec contracts."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import struct

import pytest


def test_v2_canonical_codec_is_available() -> None:
    spec = importlib.util.find_spec("hermes_cli.kanban_resolver_codec")
    assert spec is not None, "missing WRI-2385 v2 canonical codec"


def _length(payload: bytes) -> bytes:
    return struct.pack(">I", len(payload)) + payload


def _reference_encode(value: object) -> bytes:
    """Independent fixture-side adapter for the selected v2 tagged codec."""
    from hermes_cli.kanban_resolver_codec import RecordV2, SIntV2, UIntV2

    if value is None:
        return b"\x00"
    if value is False:
        return b"\x01"
    if value is True:
        return b"\x02"
    if isinstance(value, UIntV2):
        magnitude = value.value.to_bytes((value.value.bit_length() + 7) // 8, "big")
        return b"\x03" + _length(magnitude)
    if isinstance(value, SIntV2):
        magnitude = abs(value.value).to_bytes((abs(value.value).bit_length() + 7) // 8, "big")
        return b"\x04" + bytes((value.value < 0,)) + _length(magnitude)
    if isinstance(value, str):
        return b"\x05" + _length(value.encode("utf-8"))
    if isinstance(value, bytes):
        return b"\x06" + _length(value)
    if isinstance(value, list):
        return b"\x07" + struct.pack(">I", len(value)) + b"".join(
            _reference_encode(item) for item in value
        )
    if isinstance(value, RecordV2):
        return b"\x08" + struct.pack(">I", len(value.values)) + b"".join(
            _reference_encode(item) for item in value.values
        )
    if isinstance(value, dict):
        ordered = sorted(value.items(), key=lambda item: item[0].encode("utf-8"))
        return b"\x09" + struct.pack(">I", len(ordered)) + b"".join(
            _reference_encode(key) + _reference_encode(item) for key, item in ordered
        )
    raise TypeError(type(value).__name__)


def test_canonical_codec_matches_independent_tagged_reference() -> None:
    from hermes_cli.kanban_resolver_codec import (
        RecordV2,
        SIntV2,
        UIntV2,
        canonical_bytes,
    )

    semantic = RecordV2(
        (
            "wri2385/test/v2",
            None,
            False,
            True,
            UIntV2(0),
            UIntV2(256),
            SIntV2(-257),
            "é😀",
            b"\x00\xff",
            ["z", UIntV2(1)],
            {"😀": "astral", "é": "bmp"},
        )
    )

    assert canonical_bytes(semantic) == b"WRI2385\x02" + _reference_encode(semantic)


def test_domain_preimage_and_digest_are_stable_and_domain_separated() -> None:
    from hermes_cli.kanban_resolver_codec import (
        UIntV2,
        domain_preimage,
        semantic_digest,
    )

    preimage = domain_preimage("hermes/machine-id/v1", "linux-machine-id", b"a" * 32)
    assert preimage.startswith(b"WRI2385\x02")
    assert semantic_digest("hermes/machine-id/v1", "linux-machine-id", b"a" * 32) == hashlib.sha256(
        preimage
    ).hexdigest()
    assert semantic_digest("wri2385/verifier-key/v2", b"a" * 32) != semantic_digest(
        "hermes/machine-id/v1", b"a" * 32
    )


def test_codec_rejects_ambiguous_or_noncanonical_values() -> None:
    from hermes_cli.kanban_resolver_codec import RecordV2, SIntV2, UIntV2, canonical_bytes

    for value in (1, -1, 1.5, {"x": "ok", 1: "bad"}, ("not", "a", "record")):
        with pytest.raises((TypeError, ValueError)):
            canonical_bytes(value)
    with pytest.raises(ValueError):
        UIntV2(-1)
    with pytest.raises(ValueError):
        RecordV2("not-a-tuple")  # type: ignore[arg-type]
    assert canonical_bytes(SIntV2(0)) != canonical_bytes(UIntV2(0))


def test_committed_office_fixture_packet_is_complete_and_reproducible(tmp_path: Path) -> None:
    from tests.fixtures.kanban.triage_resolver.tools.generate_fixtures import generate_packet

    fixture_root = (
        Path(__file__).parents[1]
        / "fixtures/kanban/triage_resolver/wri2385.triage-resolver.v2"
    )
    manifest = json.loads((fixture_root / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["contract_version"] == "wri2385.triage-resolver.v2"
    assert manifest["fixture_format_version"] == "wri2385.fixture-packet.v2"
    assert manifest["codec"] == "wri2385-tagged-binary-v2"
    assert manifest["domain_registry_count"] == 23

    required = {
        "codec-vectors.json",
        "office-principal-assertion.json",
        "office-sign-request.json",
        "office-sign-response.json",
        "core-invoke-request.json",
        "core-invoke-response-completed.json",
        "core-invoke-response-unknown.json",
        "resolver-request-frame.bin",
        "resolver-response-frame.bin",
        "resolver-envelopes.json",
    }
    assert required <= set(manifest["files"])
    for relative, expected_sha256 in manifest["files"].items():
        payload = (fixture_root / relative).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == expected_sha256

    generated = tmp_path / "packet"
    generate_packet(generated)
    assert {
        path.relative_to(fixture_root): path.read_bytes()
        for path in sorted(fixture_root.rglob("*"))
        if path.is_file()
    } == {
        path.relative_to(generated): path.read_bytes()
        for path in sorted(generated.rglob("*"))
        if path.is_file()
    }
