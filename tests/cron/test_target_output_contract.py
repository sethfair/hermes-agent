"""WRI-2276 target output grammar fails before any observer or worker promotion."""

import json

import pytest

from cron.scheduler_local_observation import bind_canonical_output


def test_absent_v2_store_cannot_bind_valid_looking_output(tmp_path, monkeypatch):
    from gateway import session_cron

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    monkeypatch.setattr(session_cron, "current_admission", lambda: (object(), "session", "admission", "job", "execution"))
    envelope = {"version": 2, "execution_id": "execution", "nonce_digest": "a" * 64,
                "generation": 1, "epoch_id": "b" * 64, "issuance_digest": "c" * 64,
                "wakeAgent": True}
    with pytest.raises(OSError):
        bind_canonical_output({"id": "job", "execution_id": "execution"}, json.dumps(envelope))
    assert not (tmp_path / "profile" / "cron" / "recovery-job" / "local-observation-v2.sqlite").exists()


@pytest.mark.parametrize("payload", [
    '{}',
    '{"version":2,"version":2}',
    '{"version":2,"execution_id":"execution","nonce_digest":"' + 'a' * 64 +
    '","generation":1,"epoch_id":"' + 'b' * 64 +
    '","issuance_digest":"' + 'c' * 64 + '","wakeAgent":false}',
    '\ufffd',
    ' ' * 16385,
    '{"version":2,"execution_id":"other"}',
])
def test_invalid_output_never_reaches_v2_store(tmp_path, monkeypatch, payload):
    from gateway import session_cron

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    monkeypatch.setattr(session_cron, "current_admission", lambda: (object(), "session", "admission", "job", "execution"))
    with pytest.raises((RuntimeError, ValueError)):
        bind_canonical_output({"id": "job", "execution_id": "execution"}, payload)
    assert not (tmp_path / "profile" / "cron").exists()
