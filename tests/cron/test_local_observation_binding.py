"""WRI-2276: real child output cannot become a bound issuance by itself."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from tests.cron.local_observation_support import (
    native_tmp_path, copy_package, prerun_bootstrap, bootstrap_text, check_board,
    write_current_manifest)


@pytest.mark.parametrize("nonce_mode", ["correct", "wrong"])
def test_bound_output_requires_producer_nonce_and_current_issuance(tmp_path, monkeypatch, nonce_mode):
    from cron import executions, jobs

    home = tmp_path / "rowan"
    scripts = home / "scripts"
    scripts.mkdir(parents=True)
    modules = scripts / "observer_v2"
    modules.mkdir()
    copy_package(modules)
    monkeypatch.setenv("HERMES_HOME", str(home))
    try:
        from cron.scheduler_local_observation import load_private_package
        admitted = load_private_package(modules)
        legacy, v2 = admitted['engineering_recovery_observer'], admitted['local_observation_v2']

        directory = home / "cron" / f"recovery-{legacy.JOB_ID}"
        directory.mkdir(parents=True, mode=0o700)
        monkeypatch.setattr(legacy, "PROFILE_HOME", home)
        ledger = {"version": 2, "board": legacy.BOARD, "job": legacy.JOB_ID,
                  "legacy_raw": {}, "roots": {}}
        (directory / "ledger.json").write_bytes(legacy._bytes(ledger))
        ledger_sha = legacy.read_ledger(directory)[1]
        legacy.commit_cutover_marker(directory, ledger_sha, 0, "missing")
        old = {"version": 1, "board_id": legacy.BOARD, "job_id": legacy.JOB_ID,
               "ledger_sha": ledger_sha, "admission_generation": 0,
               "admission_state": "invalidating", "pending": {}, "roots": {},
               "current_issued": {}}
        (directory / "local-observation-v1.json").write_bytes(v2.encode(old))
        v2.migrate(directory)

        from hermes_cli import kanban_db
        board = tmp_path / "board.db"
        check_board(board, tmp_path)
        with sqlite3.connect(board) as db:
            db.executescript(kanban_db.SCHEMA_SQL)
        monkeypatch.setenv("HERMES_KANBAN_DB", str(board))
        with sqlite3.connect(home / 'cron/notepad.db') as db:
            db.execute('CREATE TABLE cron_notepad(job_id TEXT,key TEXT,value TEXT,updated_at TEXT)')
        script = modules / 'observer_private_entry.py'
        bootstrap = tmp_path / 'bootstrap.py'
        code = prerun_bootstrap(modules)
        if nonce_mode == 'wrong':
            # Corrupt only the separate child's emitted nonce, after real native issuance.
            code = code.replace('entry.main([])',
                'original_encode=v2.encode\n'
                'def corrupt(value):\n value=dict(value); value["nonce_digest"]="0"*64; return original_encode(value)\n'
                'v2.encode=corrupt\nentry.main([])')
        bootstrap.write_text(code, encoding='utf-8')
        monkeypatch.setenv("HERMES_HOME", str(home))
        with jobs.use_cron_store(home):
            original_uuid = jobs.uuid.uuid4
            class TargetId:
                hex = legacy.JOB_ID
            monkeypatch.setattr(jobs.uuid, "uuid4", lambda: TargetId())
            job = jobs.create_job(prompt=None, schedule="every 1h", name="binding fixture",
                                  script=str(script.relative_to(scripts)), no_agent=True)
            monkeypatch.setattr(jobs.uuid, "uuid4", original_uuid)
            claimed = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
        assert isinstance(claimed, dict)
        trust_patch = write_current_manifest(home, modules, claimed)
        payload = tmp_path / "claimed.json"
        payload.write_text(json.dumps(claimed), encoding="utf-8")
        env = dict(os.environ, HERMES_HOME=str(home))
        parent = subprocess.run(
            [sys.executable, "-c",
             "import json,pathlib,sys\n" + trust_patch
             + "from cron import scheduler,scheduler_script\n"
             f"scheduler_script._posix_cron_script_argv=lambda script: ([sys.executable,{str(bootstrap)!r},str(script)],{{}})\n"
             f"job=json.loads(pathlib.Path({str(payload)!r}).read_text())\n"
             "assert scheduler.run_one_job(job, adapters=None, loop=None)"],
            cwd=str(Path(__file__).resolve().parents[2]), env=env,
            capture_output=True, text=True, timeout=30, check=False,
        )
        assert parent.returncode == 0, parent.stderr
        current = executions.latest_execution(job["id"])
        assert current and current["status"] == ("completed" if nonce_mode == "correct" else "failed")
        with sqlite3.connect(home / "cron" / "executions.db") as db:
            row = db.execute("SELECT bind_state,nonce_digest,generation,epoch_id,issuance_digest,output_sha256 FROM cron_prerun_bindings WHERE execution_id=?", (current["id"],)).fetchone()
        assert row
        if nonce_mode == "wrong":
            assert row[0] == "unknown" and row[-1] is None
            with sqlite3.connect(home / "cron" / "executions.db") as db:
                denied = db.execute("SELECT state FROM cron_local_finalizations WHERE execution_id=?",
                                    (current["id"],)).fetchone()
            assert denied == ("unknown",), "failed issuance has no durable producer denial"
        else:
            assert row[0] == "bound", "real v2 child issuance was not independently bound"
            assert all(row[1:]), "bound issuance lacks a nonce, epoch or output digest"
            assert v2.Store(directory).header()["epoch_id"] == row[3]
            persisted_job = jobs.get_job(job["id"])
            assert current["id"] in persisted_job.get("canonical_completions", []), (
                "native completion omitted the exact execution witness")
            with sqlite3.connect(home / "cron" / "executions.db") as db:
                final = db.execute("SELECT state FROM cron_local_finalizations WHERE execution_id=?",
                                   (current["id"],)).fetchone()
            assert final == ("not_applicable",), "completed no-stage original owner lacks final disposition"
            with sqlite3.connect(directory / "local-observation-v2.sqlite") as db:
                outcome = db.execute("SELECT payload FROM outcomes WHERE execution_id=?",
                                     (current["id"],)).fetchone()
            assert outcome and v2.strict_json(outcome[0])["state"] == "not_applicable", (
                "no-stage disposition lacks original-parent v2 outcome")
            before_duplicate = jobs.get_job(job["id"])
            assert jobs.mark_job_run(job["id"], True,
                                     expected_fire_owner=claimed["fire_claim"]["by"],
                                     execution_id=current["id"]) is False
            assert jobs.get_job(job["id"]) == before_duplicate
    finally:
        # The owned native_tmp_path fixture clears only its admitted package registry.
        pass
