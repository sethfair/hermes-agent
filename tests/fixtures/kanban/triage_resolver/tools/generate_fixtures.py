"""Generate the deterministic WRI-2385 Office-facing v2 fixture packet."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import struct
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from hermes_cli.kanban_resolver_codec import RecordV2, UIntV2, domain_preimage, semantic_digest

CONTRACT_VERSION = "wri2385.triage-resolver.v2"
FIXTURE_FORMAT_VERSION = "wri2385.fixture-packet.v2"
SOURCE_REVISION = "bba2061988e6c5a05c9b32626f616f2f7576b8ab"
DOMAINS = (
    "hermes/machine-id/v1",
    "wri2385/core-invoke-request/v2",
    "wri2385/core-invoke-response/v2",
    "wri2385/office-principal-assertion/v2",
    "wri2385/office-sign-claim/v2",
    "wri2385/triage-request/v2",
    "wri2385/authority-evidence/v2",
    "wri2385/verifier-key/v2",
    "wri2385/profile-marker/v2",
    "wri2385/authority-writer-inventory/v2",
    "wri2385/authority-source-manifest/v2",
    "wri2385/shared-authority-projection/v2",
    "wri2385/authority-migration-receipt/v2",
    "wri2385/triage-snapshot/v2",
    "wri2385/triage-cause/v2",
    "wri2385/triage-diagnosis/v2",
    "wri2385/triage-child/v2",
    "wri2385/triage-child-payload/v2",
    "wri2385/reused-card/v2",
    "wri2385/triage-preview/v2",
    "wri2385/reviewed-startup-evidence/v2",
    "wri2385/resolver-stored-result/v2",
    "wri2385/host-authority-writer-request/v2",
)


def _b64(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _wire_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _frame(payload: bytes) -> bytes:
    return struct.pack(">I", len(payload)) + payload


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _build_files() -> dict[str, bytes]:
    source_bytes = b"0123456789abcdef0123456789abcdef"
    machine_id = semantic_digest("hermes/machine-id/v1", "linux-machine-id", source_bytes)
    board_id = "b" * 64
    board = "fixture-board"
    operation = "inspect"
    request_id = "123e4567-e89b-42d3-a456-426614174000"
    root_task_id = "t_fixture_root"
    board_ref = {"machine_id": machine_id, "board_id": board_id, "board": board}
    board_record = RecordV2((machine_id, board_id, board))
    request_digest = semantic_digest(
        "wri2385/triage-request/v2",
        CONTRACT_VERSION,
        operation,
        request_id,
        board_record,
        root_task_id,
    )

    claim_fields: dict[str, Any] = {
        "assertion_version": "wri2385.office-principal-assertion.v2",
        "issuer": "writerflow/hermes-office",
        "algorithm": "Ed25519",
        "kid": "fixture-key-v2",
        "principal": "office:user:seth",
        "session_id": "fixture-session-0000000000000001",
        "session_epoch": 7,
        "principal_epoch": 3,
        "audience": "nousresearch/hermes-agent:wri2385-triage-resolver",
        "machine_id": machine_id,
        "board_id": board_id,
        "operation": operation,
        "request_id": request_id,
        "request_digest": request_digest,
        "jti": "ABEiM0RVZneImaq7zN3u_w",
        "issued_at": 1_800_000_000,
        "expires_at": 1_800_000_015,
    }
    claim_members = (
        claim_fields["assertion_version"],
        claim_fields["issuer"],
        claim_fields["algorithm"],
        claim_fields["kid"],
        claim_fields["principal"],
        claim_fields["session_id"],
        UIntV2(claim_fields["session_epoch"]),
        UIntV2(claim_fields["principal_epoch"]),
        claim_fields["audience"],
        claim_fields["machine_id"],
        claim_fields["board_id"],
        claim_fields["operation"],
        claim_fields["request_id"],
        claim_fields["request_digest"],
        claim_fields["jti"],
        UIntV2(claim_fields["issued_at"]),
        UIntV2(claim_fields["expires_at"]),
    )
    claim_bytes = domain_preimage("wri2385/office-principal-assertion/v2", *claim_members)
    claim_digest = semantic_digest("wri2385/office-sign-claim/v2", *claim_members)
    private_key = Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    signature = private_key.sign(claim_bytes)
    assertion = {**claim_fields, "signature": _b64(signature)}

    resolver_request = {
        "contract_version": CONTRACT_VERSION,
        "operation": operation,
        "request_id": request_id,
        "board_ref": board_ref,
        "root_task_id": root_task_id,
        "principal_assertion": assertion,
    }
    request_frame = _frame(_wire_json(resolver_request))
    request_frame_digest = semantic_digest(
        "wri2385/core-invoke-request/v2", UIntV2(len(request_frame)), request_frame
    )

    denial = {
        "contract_version": CONTRACT_VERSION,
        "operation": operation,
        "request_id": request_id,
        "error_code": "authority_unavailable",
        "message": "resolver authority is unavailable",
        "receipt": None,
    }
    parse_error = {
        "envelope_version": "wri2385.triage-resolver.parse-error.v2",
        "contract_version": None,
        "operation": None,
        "request_id": None,
        "error_code": "malformed_request",
        "message": "request frame is malformed",
        "receipt": None,
    }
    response_frame = _frame(_wire_json(denial))
    response_digest = semantic_digest(
        "wri2385/core-invoke-response/v2", UIntV2(len(response_frame)), response_frame
    )

    office_sign_request = {
        "sign_request_version": "wri2385.office-sign.v2",
        "kid": claim_fields["kid"],
        "office_assertion_row_id": "123e4567-e89b-42d3-a456-426614174001",
        "claim_digest": claim_digest,
        "claim_bytes": _b64(claim_bytes),
    }
    office_sign_response = {
        "sign_request_version": office_sign_request["sign_request_version"],
        "kid": office_sign_request["kid"],
        "office_assertion_row_id": office_sign_request["office_assertion_row_id"],
        "claim_digest": claim_digest,
        "signature": _b64(signature),
        "signer_status": "signed",
    }
    core_invoke_request = {
        "invoke_version": "wri2385.core-invoke.v2",
        "machine_id": machine_id,
        "board_id": board_id,
        "board": board,
        "request_length": len(request_frame),
        "request_sha256": request_frame_digest,
        "request_bytes": _b64(request_frame),
    }
    core_common = {
        "invoke_version": "wri2385.core-invoke.v2",
        "machine_id": machine_id,
        "board_id": board_id,
        "board": board,
        "request_sha256": request_frame_digest,
    }
    core_completed = {
        **core_common,
        "result": "completed",
        "response_length": len(response_frame),
        "response_sha256": response_digest,
        "response_bytes": _b64(response_frame),
    }
    core_unknown = {
        **core_common,
        "result": "unknown",
        "response_length": None,
        "response_sha256": None,
        "response_bytes": None,
    }
    codec_vectors = {
        "codec": "wri2385-tagged-binary-v2",
        "magic_hex": "5752493233383502",
        "domains": list(DOMAINS),
        "machine_id": {
            "source_kind": "linux-machine-id",
            "source_bytes_hex": source_bytes.hex(),
            "preimage_hex": domain_preimage(
                "hermes/machine-id/v1", "linux-machine-id", source_bytes
            ).hex(),
            "digest": machine_id,
        },
        "assertion": {
            "preimage_hex": claim_bytes.hex(),
            "digest": _sha(claim_bytes),
            "claim_digest": claim_digest,
            "private_key_seed_hex": bytes(range(32)).hex(),
            "public_key_hex": public_key.hex(),
            "signature_hex": signature.hex(),
        },
        "request": {
            "semantic_digest": request_digest,
            "frame_length": len(request_frame),
            "frame_digest": request_frame_digest,
        },
        "response": {
            "frame_length": len(response_frame),
            "frame_digest": response_digest,
        },
    }
    return {
        "codec-vectors.json": _json_bytes(codec_vectors),
        "office-principal-assertion.json": _json_bytes(assertion),
        "office-sign-request.json": _json_bytes(office_sign_request),
        "office-sign-response.json": _json_bytes(office_sign_response),
        "core-invoke-request.json": _json_bytes(core_invoke_request),
        "core-invoke-response-completed.json": _json_bytes(core_completed),
        "core-invoke-response-unknown.json": _json_bytes(core_unknown),
        "resolver-request-frame.bin": request_frame,
        "resolver-response-frame.bin": response_frame,
        "resolver-envelopes.json": _json_bytes(
            {"parse_error": parse_error, "operation_denial": denial}
        ),
    }


def generate_packet(output: Path) -> None:
    files = _build_files()
    output.mkdir(parents=True, exist_ok=True)
    for relative, payload in files.items():
        _write(output / relative, payload)
    manifest = {
        "contract_version": CONTRACT_VERSION,
        "fixture_format_version": FIXTURE_FORMAT_VERSION,
        "codec": "wri2385-tagged-binary-v2",
        "source_revision": SOURCE_REVISION,
        "domain_registry_count": len(DOMAINS),
        "generator_commands": [
            "python -m tests.fixtures.kanban.triage_resolver.tools.generate_fixtures",
            "node --test tests/fixtures/kanban/triage_resolver/tools/*.test.mjs",
        ],
        "files": {name: _sha(payload) for name, payload in sorted(files.items())},
    }
    _write(output / "manifest.json", _json_bytes(manifest))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).parents[1] / "wri2385.triage-resolver.v2",
    )
    args = parser.parse_args()
    generate_packet(args.out)


if __name__ == "__main__":
    main()
