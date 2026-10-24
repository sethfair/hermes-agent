"""Target producer unresolved evidence survives terminal retention pressure (WRI-2276)."""

from contextlib import asynccontextmanager
import json

import pytest

from cron import executions, jobs, scheduler_authority


def test_unresolved_target_binding_pins_terminal_execution(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    job = jobs.create_job(prompt="synthetic", schedule="every 1h")
    claimed = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
    assert isinstance(claimed, dict)
    execution = executions.create_execution(job["id"], source="test")
    running = executions.mark_execution_running(execution["id"])
    assert running is not None
    with jobs.fire_claim_fence(job["id"], expected_owner=claimed["fire_claim"]["by"]) as held:
        assert held
        reserved = executions.reserve_prerun_binding(execution["id"], job["id"], claimed["fire_claim"]["by"])
    assert reserved["bind_state"] == "awaiting_canonical"
    monkeypatch.setattr(executions, "MAX_TERMINAL_EXECUTIONS", 0)
    finished = executions.finish_execution(execution["id"], success=False, error="synthetic abort")
    assert finished is not None and finished["status"] == "failed"
    assert executions.get_execution(execution["id"])["status"] == "failed"


def test_unresolved_quota_denies_new_binding_without_eviction(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    monkeypatch.setattr(executions, "MAX_TARGET_UNRESOLVED_BINDINGS", 1, raising=False)
    ids = []
    for index in range(2):
        job = jobs.create_job(prompt=f"synthetic-{index}", schedule="every 1h")
        claimed = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
        assert isinstance(claimed, dict)
        attempt = executions.create_execution(job["id"], source="test")
        assert executions.mark_execution_running(attempt["id"])
        ids.append(attempt["id"])
        with jobs.fire_claim_fence(job["id"], expected_owner=claimed["fire_claim"]["by"]) as held:
            assert held
            if index == 0:
                executions.reserve_prerun_binding(attempt["id"], job["id"], claimed["fire_claim"]["by"])
                assert executions.finish_execution(attempt["id"], success=True)["status"] == "completed"
                executions.begin_local_finalization(attempt["id"])
                assert executions.mirror_local_finalization(attempt["id"], "succeeded", "a" * 64)
            else:
                with pytest.raises(RuntimeError, match="quota"):
                    executions.reserve_prerun_binding(attempt["id"], job["id"], claimed["fire_claim"]["by"])
    with executions._transaction() as db:
        rows = db.execute("SELECT execution_id,unresolved_pin FROM cron_prerun_bindings").fetchall()
    assert [tuple(row) for row in rows] == [(ids[0], 1)], "local mirror alone cannot release a v2 receipt pin"


def test_ownerless_canonical_recovery_keeps_target_disposition_unpromoted(tmp_path, monkeypatch):
    from hermes_cli import gateway_client

    home = tmp_path / "profile"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    script = home / "scripts" / "observer.py"
    script.parent.mkdir()
    script.write_text("print('synthetic')\n")
    with jobs.use_cron_store(home / "cron"):
        job = jobs.create_job(prompt="synthetic", schedule="every 1h", script=str(script), deliver="local")
        (home / "config.yaml").write_text(json.dumps({"cron": {"local_observation_target_job_id": job["id"]}}))
        claimed = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
        assert isinstance(claimed, dict)
        execution = executions.create_execution(job["id"], source="test")
        assert executions.mark_execution_running(execution["id"])
        with jobs.fire_claim_fence(job["id"], expected_owner=claimed["fire_claim"]["by"]) as held:
            assert held
            executions.reserve_prerun_binding(execution["id"], job["id"], claimed["fire_claim"]["by"])
        with executions._transaction() as db:
            db.execute("UPDATE executions SET process_id=?,pid=? WHERE id=?",
                       ("dead-owner", 999999, execution["id"]))
        journal = scheduler_authority.journal_path(job["id"], execution["id"])
        journal.parent.mkdir(parents=True, exist_ok=True)
        journal.write_text(json.dumps({"params": {"job_id": job["id"],
                              "request_id": execution["id"], "extra_prompt": None}, "receipt": None}))

        class Peer:
            async def rpc(self, method, **params):
                return {"status": "terminal", "job": claimed,
                        "result": [True, "document", "answer", None]}

        @asynccontextmanager
        async def connected():
            yield Peer()

        monkeypatch.setattr(gateway_client, "connect_gateway", connected)
        scheduler_authority.reconcile_pending()
        with executions._transaction() as db:
            terminal = db.execute("SELECT status FROM executions WHERE id=?", (execution["id"],)).fetchone()
            binding = db.execute("SELECT unresolved_pin FROM cron_prerun_bindings WHERE execution_id=?",
                                 (execution["id"],)).fetchone()
            finalizations = db.execute("SELECT count(*) FROM cron_local_finalizations").fetchone()[0]
        assert terminal[0] == "completed" and binding[0] == 1 and finalizations == 0


def test_canonical_nonce_refuses_changed_original_execution_owner(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    job = jobs.create_job(prompt="synthetic", schedule="every 1h")
    claimed = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
    assert isinstance(claimed, dict)
    execution = executions.create_execution(job["id"], source="test")
    assert executions.mark_execution_running(execution["id"])
    owner = claimed["fire_claim"]["by"]
    with jobs.fire_claim_fence(job["id"], expected_owner=owner) as held:
        assert held
        executions.reserve_prerun_binding(execution["id"], job["id"], owner)
        with executions._transaction() as db:
            db.execute("UPDATE executions SET process_id=?,pid=? WHERE id=?",
                       ("different-process", 999999, execution["id"]))
        with pytest.raises(RuntimeError, match="owner|firer"):
            executions.prepare_prerun_binding(execution["id"], job["id"], "admission", "a" * 64, owner)
    with executions._transaction() as db:
        state = db.execute("SELECT bind_state,nonce_digest FROM cron_prerun_bindings WHERE execution_id=?",
                           (execution["id"],)).fetchone()
    assert tuple(state) == ("awaiting_canonical", None)

def test_canonical_preparation_rejects_wrong_job_profile_and_claim(tmp_path, monkeypatch):
    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    job = jobs.create_job(prompt="synthetic", schedule="every 1h")
    other = jobs.create_job(prompt="other", schedule="every 1h")
    claimed = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
    assert isinstance(claimed, dict)
    execution = executions.create_execution(job["id"], source="test")
    assert executions.mark_execution_running(execution["id"])
    owner = claimed["fire_claim"]["by"]
    with jobs.fire_claim_fence(job["id"], expected_owner=owner) as held:
        assert held
        executions.reserve_prerun_binding(execution["id"], job["id"], owner)
        for wrong_job, wrong_owner in ((other["id"], owner), (job["id"], "forged")):
            with pytest.raises(RuntimeError):
                executions.prepare_prerun_binding(execution["id"], wrong_job, "admission", "a" * 64, wrong_owner)
        monkeypatch.setenv("HERMES_HOME", str(tmp_path / "other-profile"))
        with pytest.raises(RuntimeError):
            executions.prepare_prerun_binding(execution["id"], job["id"], "admission", "a" * 64, owner)
        monkeypatch.setenv("HERMES_HOME", str(home))
        assert executions.prepare_prerun_binding(execution["id"], job["id"], "admission", "a" * 64, owner)["bind_state"] == "preparing"
        with pytest.raises(RuntimeError, match="admission"):
            executions.bind_prerun_output(execution["id"], job["id"], "wrong", owner,
                                          nonce_digest="a" * 64, generation=1, epoch_id="b" * 64,
                                          issuance_digest="c" * 64, output_sha256="d" * 64)
        with pytest.raises(RuntimeError, match="claim"):
            executions.bind_prerun_output(execution["id"], job["id"], "admission", "forged",
                                          nonce_digest="a" * 64, generation=1, epoch_id="b" * 64,
                                          issuance_digest="c" * 64, output_sha256="d" * 64)
    with executions._transaction() as db:
        state = db.execute("SELECT bind_state,epoch_id,unresolved_pin FROM cron_prerun_bindings WHERE execution_id=?",
                           (execution["id"],)).fetchone()
    assert tuple(state) == ("preparing", None, 1)


def test_canonical_preparation_rejects_mismatched_reserved_firer_fingerprint(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    job = jobs.create_job(prompt="synthetic", schedule="every 1h")
    claimed = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
    assert isinstance(claimed, dict)
    execution = executions.create_execution(job["id"], source="test")
    assert executions.mark_execution_running(execution["id"])
    owner = claimed["fire_claim"]["by"]
    with jobs.fire_claim_fence(job["id"], expected_owner=owner) as held:
        assert held
        executions.reserve_prerun_binding(execution["id"], job["id"], owner)
        with executions._transaction() as db:
            db.execute("UPDATE cron_prerun_bindings SET process_id=? WHERE execution_id=?",
                       ("different-reserved-firer", execution["id"]))
        with pytest.raises(RuntimeError, match="original firer"):
            executions.prepare_prerun_binding(execution["id"], job["id"], "admission", "a" * 64, owner)
    with executions._transaction() as db:
        state = db.execute("SELECT bind_state,admission_id FROM cron_prerun_bindings WHERE execution_id=?",
                           (execution["id"],)).fetchone()
    assert tuple(state) == ("awaiting_canonical", None)
