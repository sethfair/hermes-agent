"""WRI-2276 target output grammar fails before any observer or worker promotion."""

import json
import sqlite3

import pytest

from cron.scheduler_local_observation import bind_canonical_output


def test_output_cannot_substitute_a_different_nonce_even_with_matching_provisional(tmp_path, monkeypatch):
    from cron import executions, jobs
    from gateway import session_cron

    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
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
    db_path = home / "cron" / f"recovery-{job['id']}" / "local-observation-v2.sqlite"
    db_path.parent.mkdir(mode=0o700)
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE epochs (execution_id TEXT,profile TEXT,job TEXT,status TEXT,"
                   "nonce_digest TEXT,generation INTEGER,epoch_id TEXT,issuance_digest TEXT)")
        db.execute("INSERT INTO epochs VALUES (?,?,?,?,?,?,?,?)",
                   (execution_id, str(home.resolve()), job["id"], "provisional", "b" * 64,
                    1, "c" * 64, "d" * 64))
    envelope = {"version": 2, "execution_id": execution_id, "nonce_digest": "b" * 64,
                "generation": 1, "epoch_id": "c" * 64, "issuance_digest": "d" * 64,
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
