"""WRI-2276: native detached-worker reservation before a target pre-run script."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

from tests.waiting import wait_until
from tests.cron.local_observation_support import (native_tmp_path, copy_package,
    prerun_bootstrap, check_board, write_current_manifest)


def test_target_running_claim_reserves_before_script(tmp_path, monkeypatch):
    from cron import executions, jobs

    home = tmp_path / "rowan"
    scripts = home / "scripts"
    scripts.mkdir(parents=True)
    started = tmp_path / "script-started"
    release = tmp_path / "script-release"
    source = scripts / 'observer_v2'
    source.mkdir()
    copy_package(source)
    monkeypatch.setenv('HERMES_HOME', str(home))
    from cron.scheduler_local_observation import load_private_package
    from hermes_cli import kanban_db
    modules = load_private_package(source)
    legacy, v2 = modules['engineering_recovery_observer'], modules['local_observation_v2']
    directory = home / 'cron' / ('recovery-' + legacy.JOB_ID)
    directory.mkdir(parents=True, mode=0o700)
    ledger = {'version':2,'board':legacy.BOARD,'job':legacy.JOB_ID,'legacy_raw':{},'roots':{}}
    (directory/'ledger.json').write_bytes(legacy._bytes(ledger))
    sha = legacy.read_ledger(directory)[1]
    legacy.commit_cutover_marker(directory, sha, 0, 'missing')
    old = {'version':1,'board_id':legacy.BOARD,'job_id':legacy.JOB_ID,'ledger_sha':sha,
           'admission_generation':0,'admission_state':'invalidating','pending':{},'roots':{},'current_issued':{}}
    (directory/'local-observation-v1.json').write_bytes(v2.encode(old))
    v2.migrate(directory)

    board = tmp_path / 'board.db'
    check_board(board, tmp_path)
    with sqlite3.connect(board) as db:
        db.executescript(kanban_db.SCHEMA_SQL)
    with sqlite3.connect(home/'cron/notepad.db') as db:
        db.execute('CREATE TABLE cron_notepad(job_id TEXT,key TEXT,value TEXT,updated_at TEXT)')
    monkeypatch.setenv('HERMES_KANBAN_DB', str(board))
    script = source / 'observer_private_entry.py'
    bootstrap = tmp_path / 'probe-bootstrap.py'
    bootstrap.write_text(
        "import os, pathlib, time\n"
        f"started = pathlib.Path({str(started)!r})\n"
        f"release = pathlib.Path({str(release)!r})\n"
        "started.write_text(os.environ.get('HERMES_LOCAL_OBSERVATION_NONCE', 'absent'))\n"
        "deadline = time.monotonic() + 20\n"
        "while not release.exists() and time.monotonic() < deadline:\n"
        "    time.sleep(0.05)\n"
        "if not release.exists(): raise SystemExit('release timeout')\n"
        + prerun_bootstrap(source),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(home))
    with jobs.use_cron_store(home):
        original_uuid = jobs.uuid.uuid4
        class TargetId:
            hex = "61a6b39fba27"
        monkeypatch.setattr(jobs.uuid, "uuid4", lambda: TargetId())
        job = jobs.create_job(prompt=None, schedule="every 1h", name="observation fixture",
                              script=str(script.relative_to(scripts)), no_agent=True)
        monkeypatch.setattr(jobs.uuid, "uuid4", original_uuid)
        claimed = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
    assert isinstance(claimed, dict)
    trust_patch = write_current_manifest(home, source, claimed)
    payload = tmp_path / "claimed.json"
    payload.write_text(json.dumps(claimed), encoding="utf-8")
    env = dict(os.environ, HERMES_HOME=str(home))
    parent = subprocess.Popen(
        [sys.executable, "-c",
         "import json, pathlib, sys\n" + trust_patch
         + "from cron import scheduler,scheduler_script\n"
         f"scheduler_script._posix_cron_script_argv=lambda script: ([sys.executable,{str(bootstrap)!r},str(script)],{{}})\n"
         f"job=json.loads(pathlib.Path({str(payload)!r}).read_text())\n"
         "assert scheduler.run_one_job(job, adapters=None, loop=None)"],
        cwd=str(Path(__file__).resolve().parents[2]), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        assert wait_until(started.exists, timeout=15), "native worker never reached script"
        current = executions.latest_execution(job["id"])
        assert current and current["status"] == "running"
        assert current["pid"] != os.getpid()
        with sqlite3.connect(home / "cron" / "executions.db") as db:
            present = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cron_prerun_bindings'").fetchone()
            rows = (db.execute("SELECT execution_id, job_id, bind_state, unresolved_pin, nonce_digest, output_sha256 FROM cron_prerun_bindings WHERE execution_id=?", (current["id"],)).fetchall()
                    if present else [])
        assert rows, "native worker started target script without durable prerun reservation"
        assert rows[0][0:2] == (current["id"], job["id"])
        assert rows[0][2:4] == ("preparing", 1)
        assert rows[0][4] and rows[0][5] is None
    finally:
        release.write_text("release", encoding="utf-8")
        try:
            parent.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            parent.kill()
            parent.communicate(timeout=5)
