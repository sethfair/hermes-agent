"""WRI-2276 target output grammar fails before any observer or worker promotion."""

import json
import pytest

from cron.scheduler_local_observation import bind_canonical_output

pytest_plugins = ("tests.cron.fixtures.wri2276_store_fixture",)

def test_output_cannot_substitute_a_different_nonce_even_with_matching_provisional(real_v2, monkeypatch):
    from cron import executions, jobs
    from gateway import session_cron

    home, legacy, v2 = real_v2
    job = jobs.create_job(prompt="synthetic", schedule="every 1h")
    claimed = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
    assert isinstance(claimed, dict)
    attempt = executions.create_execution(job["id"], source="test")
    execution_id = attempt["id"]
    assert executions.mark_execution_running(execution_id)
    owner = claimed["fire_claim"]["by"]
    monkeypatch.setattr(session_cron, "current_admission",
                        lambda: (object(), "session", "admission", job["id"], execution_id))
    with jobs.fire_claim_fence(job["id"], expected_owner=owner) as held:
        assert held
        executions.reserve_prerun_binding(execution_id, job["id"], owner)
        executions.prepare_prerun_binding(execution_id, job["id"], "admission", "a" * 64, owner)
    monkeypatch.setattr(legacy, "JOB_ID", job["id"])
    directory = home / "cron" / f"recovery-{job['id']}"
    directory.mkdir(mode=0o700)
    source = {"version": 1, "job_id": job["id"], "board_id": legacy.BOARD,
              "ledger_sha": "d" * 64, "admission_generation": 0,
              "admission_state": "invalidating", "pending": {}, "roots": {},
              "current_issued": {}}
    (directory / "local-observation-v1.json").write_bytes(json.dumps(source).encode())
    store = v2.migrate(directory)
    store.begin(execution_id, "d" * 64)
    pending = {"fingerprint": "f" * 64, "event_ids": [7], "observed_at": 42.5,
               "observation_id": "b" * 64, "attachment_bearing": False}
    epoch = store.commit(execution_id, "d" * 64, {"t_00000001": pending})
    envelope = {"version": 2, "execution_id": execution_id, "nonce_digest": "b" * 64,
                "generation": epoch["generation"], "epoch_id": epoch["epoch_id"],
                "issuance_digest": epoch["issuance_digest"],
                "wakeAgent": True}
    with pytest.raises(RuntimeError, match="nonce"):
        bind_canonical_output({**claimed, "execution_id": execution_id}, json.dumps(envelope))
    with executions._transaction() as db:
        saved = db.execute("SELECT bind_state,epoch_id,unresolved_pin FROM cron_prerun_bindings "
                           "WHERE execution_id=?", (execution_id,)).fetchone()
    assert tuple(saved) == ("preparing", None, 1)


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
    '{"version":2,"execution_id":"execution","nonce_digest":"[REDACTED]",'
    '"generation":1,"epoch_id":"' + 'b' * 64 +
    '","issuance_digest":"' + 'c' * 64 + '","wakeAgent":true}',
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
