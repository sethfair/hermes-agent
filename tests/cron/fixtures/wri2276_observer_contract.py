"""Test-only, real-SQLite WRI-2276 observer contract adapter; never production observer-v2.

Executed as an independent OS process from the synthetic worker/test driver. The actual
observer integration must replace this fixture before any composed-green claim.
"""

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys

from cron import executions, jobs


def stage(execution_id: str, root: str, expected_revision: int) -> dict:
    home = Path(os.environ["HERMES_HOME"]).resolve()
    job_id = os.environ["WRI2276_SYNTHETIC_JOB_ID"]
    producer_path = home / "cron" / "executions.db"
    observer_path = home / "cron" / f"recovery-{job_id}" / "local-observation-v2.sqlite"
    with sqlite3.connect(f"file:{producer_path}?mode=ro", uri=True) as producer:
        producer.row_factory = sqlite3.Row
        row = producer.execute(
            """SELECT b.*,e.status AS execution_status,e.process_id AS execution_process,
                      e.pid AS execution_pid,e.process_started_at AS execution_birth
               FROM cron_prerun_bindings b JOIN executions e ON e.id=b.execution_id
               WHERE b.execution_id=? AND b.job_id=? AND e.job_id=?""",
            (execution_id, job_id, job_id),
        ).fetchone()
    assert row is not None and row["bind_state"] == "bound" and row["execution_status"] == "running"
    assert row["profile_home"] == str(home)
    assert row["process_id"] == row["execution_process"]
    assert row["pid"] == row["execution_pid"] and row["process_started_at"] == row["execution_birth"]
    assert executions._owner_is_live(row["pid"], row["process_started_at"])
    job = jobs.get_job(job_id)
    assert job is not None and job["fire_claim"]["by"] == row["fire_claim_by"]
    with jobs.fire_claim_fence(job_id, expected_owner=row["fire_claim_by"]) as held:
        assert held and jobs.get_job(job_id) is not None
        with sqlite3.connect(observer_path) as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            db.row_factory = sqlite3.Row
            epoch = db.execute("SELECT * FROM epochs WHERE execution_id=?", (execution_id,)).fetchone()
            assert epoch is not None and epoch["profile"] == str(home) and epoch["job"] == job_id
            assert all(epoch[k] == row[k] for k in ("nonce_digest", "generation", "epoch_id", "issuance_digest"))
            current = db.execute("SELECT revision FROM metadata WHERE id=1").fetchone()
            assert current is not None and current[0] == expected_revision
            with sqlite3.connect(f"file:{home / 'cron' / 'synthetic-board.sqlite'}?mode=ro", uri=True) as board:
                evidence = board.execute("SELECT evidence FROM roots WHERE root=?", (root,)).fetchone()
            assert evidence is not None
            digest = hashlib.sha256(evidence[0].encode()).hexdigest()
            issued = db.execute("SELECT observation_id,evidence_digest FROM epoch_issued WHERE execution_id=? AND root=?",
                                (execution_id, root)).fetchone()
            assert issued is not None and issued[1] == digest
            db.execute("INSERT INTO stages VALUES (?,?,?,?)", (execution_id, root, digest, expected_revision + 1))
            db.execute("UPDATE metadata SET revision=revision+1 WHERE id=1 AND revision=?", (expected_revision,))
            assert db.total_changes == 2
            assert db.execute("SELECT count(*) FROM checked").fetchone()[0] == 0
            db.commit()
    return {"state": "staged", "root": root, "revision": expected_revision + 1}


if __name__ == "__main__":
    print(json.dumps(stage(sys.argv[1], sys.argv[2], int(sys.argv[3]))))
