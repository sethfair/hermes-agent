"""WRI-2276 output binding against the archived, uninstalled v2 Store implementation."""

import json
import sqlite3

import pytest

from cron.scheduler_local_observation import bind_canonical_output

pytest_plugins = ("tests.cron.fixtures.wri2276_store_fixture",)


def test_actual_archived_store_binds_only_prepared_nonce_and_issued_identity(real_v2, monkeypatch):
    from cron import executions, jobs
    from gateway import session_cron

    home, legacy, v2 = real_v2
    job = jobs.create_job(prompt="synthetic", schedule="every 1h")
    claimed = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
    assert isinstance(claimed, dict)
    monkeypatch.setattr(legacy, "JOB_ID", job["id"])
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
    envelope = {"version": 2, "execution_id": execution_id, "nonce_digest": "a" * 64,
                "generation": epoch["generation"], "epoch_id": epoch["epoch_id"],
                "issuance_digest": epoch["issuance_digest"], "wakeAgent": True}
    target = {**claimed, "execution_id": execution_id}
    with pytest.raises(RuntimeError, match="nonce"):
        bind_canonical_output(target, json.dumps({**envelope, "nonce_digest": "b" * 64}))
    with executions._transaction() as db:
        row = db.execute("SELECT bind_state,unresolved_pin FROM cron_prerun_bindings WHERE execution_id=?",
                         (execution_id,)).fetchone()
    assert tuple(row) == ("preparing", 1)
    with sqlite3.connect(store.path) as db:
        original = db.execute("SELECT payload FROM metadata WHERE id=1").fetchone()[0]
        mutated = json.loads(original)
        mutated["caller_approved"] = True
        db.execute("UPDATE metadata SET payload=? WHERE id=1", (v2.encode(mutated),))
    with pytest.raises(RuntimeError, match="metadata"):
        bind_canonical_output(target, json.dumps(envelope))
    with sqlite3.connect(store.path) as db:
        db.execute("UPDATE metadata SET payload=? WHERE id=1", (original,))
    with sqlite3.connect(store.path) as db:
        wrong_board = json.loads(original)
        wrong_board["board"] = "foreign-board"
        db.execute("UPDATE metadata SET payload=? WHERE id=1", (v2.encode(wrong_board),))
    with pytest.raises(RuntimeError, match="metadata"):
        bind_canonical_output(target, json.dumps(envelope))
    with sqlite3.connect(store.path) as db:
        db.execute("UPDATE metadata SET payload=? WHERE id=1", (original,))
    with sqlite3.connect(store.path) as db:
        wrong_profile = json.loads(original)
        wrong_profile["profile"] = str(home / "other-profile")
        db.execute("UPDATE metadata SET payload=? WHERE id=1", (v2.encode(wrong_profile),))
    with pytest.raises(RuntimeError, match="metadata"):
        bind_canonical_output(target, json.dumps(envelope))
    with sqlite3.connect(store.path) as db:
        db.execute("UPDATE metadata SET payload=? WHERE id=1", (original,))
    live = jobs.get_job(job["id"])
    assert isinstance(live, dict) and isinstance(live.get("fire_claim"), dict)
    changed_claim = {**live, "fire_claim": {**live["fire_claim"], "by": "foreign-firer"}}
    jobs.save_jobs([changed_claim])
    with pytest.raises(RuntimeError, match="claim"):
        bind_canonical_output(target, json.dumps(envelope))
    jobs.save_jobs([live])
    bound = bind_canonical_output(target, json.dumps(envelope))
    assert bound["bind_state"] == "bound"
    assert bound["nonce_digest"] == "a" * 64
    assert bound["epoch_id"] == epoch["epoch_id"]
    assert bound["unresolved_pin"] == 1
