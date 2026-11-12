"""WRI-2276: native job retention cannot erase unresolved producer witnesses."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


def test_completed_oneshot_retention_keeps_unresolved_execution_pin(tmp_path, monkeypatch):
    from cron import executions, jobs

    home = tmp_path / "rowan"
    monkeypatch.setenv("HERMES_HOME", str(home))
    with jobs.use_cron_store(home):
        execution = executions.create_execution("61a6b39fba27", source="direct")
        assert executions.mark_execution_running(execution["id"])
        with executions._transaction() as db:
            row = executions._fetch(db, execution["id"])
            db.execute("""INSERT INTO cron_prerun_bindings
                (execution_id,job_id,profile_home,process_id,pid,process_started_at,
                 fire_claim_by,bind_state,admission_id,source_sha256,script_sha256)
                VALUES (?,?,?,?,?,?,?,'bound',?,?,?)""",
                (execution["id"], "61a6b39fba27", str(home), row["process_id"], row["pid"],
                 row["process_started_at"], "owner", "admission", "a" * 64, "b" * 64))
        now = datetime.now(UTC)
        old = (now - timedelta(days=30)).isoformat()
        record = {"id": "61a6b39fba27", "state": "completed",
                  "schedule": {"kind": "once"}, "last_run_at": old,
                  "canonical_completions": [execution["id"]]}
        rows = [record]
        assert jobs._sweep_completed_oneshots(rows, now) is False
        assert rows == [record], "unresolved pin lost its native completion witness"
