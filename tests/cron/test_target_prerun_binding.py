"""WRI-2276: the original firer reserves a durable binding before canonical execution."""

import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time

import pytest

from tests.waiting import wait_until


def _write_test_finalizer(tmp_path, scripts, hold_finalizer, omit_receipt=False):
    """Read-only child proposes stage selectors; only the original parent can commit."""
    finalizer = scripts / "local-finalize.py"
    finalizer.write_text(
        "import json, os, pathlib, sqlite3\n"
        "home = pathlib.Path(os.environ['HERMES_HOME'])\n"
        "jid = os.environ['HERMES_LOCAL_OBSERVATION_JOB_ID']\n"
        "eid = os.environ['HERMES_LOCAL_OBSERVATION_EXECUTION_ID']\n"
        "epoch_id = os.environ['HERMES_LOCAL_OBSERVATION_EPOCH_ID']\n"
        f"if {hold_finalizer is True!r}:\n"
        f" pathlib.Path({str(tmp_path / 'finalizer-started')!r}).touch()\n"
        f" while not pathlib.Path({str(tmp_path / 'release-finalizer')!r}).exists(): __import__('time').sleep(0.05)\n"
        "with sqlite3.connect(home / 'cron' / 'executions.db') as producer:\n"
        " row = producer.execute('SELECT status FROM executions WHERE id=? AND job_id=?', (eid,jid)).fetchone()\n"
        " assert row == ('completed',), row\n"
        "with sqlite3.connect(home / 'cron' / ('recovery-' + jid) / 'local-observation-v2.sqlite') as observer:\n"
        " observer.execute('BEGIN')\n"
        " epoch = observer.execute('SELECT payload FROM epochs WHERE execution_id=?', (eid,)).fetchone()\n"
        " assert epoch and json.loads(epoch[0])['epoch_id'] == epoch_id\n"
        " stages = observer.execute('SELECT root FROM stages WHERE execution_id=? ORDER BY root', (eid,)).fetchall()\n"
        f" roots = [row[0] for row in stages if not ({omit_receipt!r} and row[0] == 't_00000002')]\n"
        "print(json.dumps({'roots':roots}, separators=(',', ':')))\n",
        encoding="utf-8",
    )
    finalizer.chmod(0o700)
    return finalizer


def _seed_isolated_board(home, env, parent_case):
    if parent_case:
        from hermes_cli import kanban_db
        board_path = home / "kanban.db"
        with sqlite3.connect(board_path) as board:
            board.executescript(kanban_db.SCHEMA_SQL)
            board.commit()
        env["HERMES_KANBAN_DB"] = str(board_path)
        env["HERMES_KANBAN_HOME"] = str(home)
    else:
        with sqlite3.connect(home / "cron" / "synthetic-board.sqlite") as board:
            board.execute("CREATE TABLE roots (root TEXT PRIMARY KEY, evidence TEXT NOT NULL)")
            board.executemany("INSERT INTO roots VALUES (?,?)",
                              [("t_00000001", "event-a"), ("t_00000002", "event-b")])

def _seed_native_roots(home):
    with sqlite3.connect(home / "kanban.db") as board:
        board.executemany(
            "INSERT INTO tasks(id,title,body,assignee,status,created_at) VALUES(?,?,?,?,?,?)",
            [(name, "fixture", "", "test", "blocked", 1)
             for name in ("t_00000001", "t_00000002")],
        )
        board.commit()


def _firer_program(job_id, info_path, parent_case, hold_finalizer, tmp_path: Path):
    return (
        "import json, os, pathlib\n"
        "from cron import jobs, executions, scheduler\n"
        f"if {parent_case!r}:\n"
        " import engineering_recovery_observer as legacy\n"
        " legacy.PROFILE_HOME = pathlib.Path(os.environ['HERMES_HOME']).resolve()\n"
        f" legacy.JOB_ID = {job_id!r}\n"
        f"if {hold_finalizer == 'after-v2'!r}:\n"
        " original_mirror = executions.mirror_local_finalization\n"
        " def held_mirror(execution_id, state, digest):\n"
        f"  pathlib.Path({str(tmp_path / 'finalizer-committed')!r}).touch()\n"
        f"  while not pathlib.Path({str(tmp_path / 'release-finalizer')!r}).exists(): __import__('time').sleep(0.05)\n"
        "  return original_mirror(execution_id, state, digest)\n"
        " executions.mirror_local_finalization = held_mirror\n"
        f"if {hold_finalizer == 'before-v2'!r}:\n"
        " import observer_parent_seam as seam\n"
        " def hold_before_commit():\n"
        f"  pathlib.Path({str(tmp_path / 'finalizer-started')!r}).touch()\n"
        f"  while not pathlib.Path({str(tmp_path / 'release-finalizer')!r}).exists(): __import__('time').sleep(0.05)\n"
        " seam._TEST_BEFORE_COMMIT = hold_before_commit\n"
        f"job = jobs.claim_job_for_fire({job_id!r}, manual=True, return_job=True)\n"
        "assert isinstance(job, dict), job\n"
        "execution = executions.create_execution(job['id'], source='test-firer')\n"
        "job['execution_id'] = execution['id']\n"
        f"pathlib.Path({str(info_path)!r}).write_text(json.dumps({{'pid': os.getpid(), 'execution_id': execution['id'], 'claim': job['fire_claim']['by']}}))\n"
        "scheduler._run_one_job_body(job)\n"
    )


def _assert_killed_finalizer_pending(tmp_path, hold_finalizer, firer, home, info, epoch_db):
    marker = "finalizer-started" if hold_finalizer == "before-v2" else "finalizer-committed"
    wait_until((tmp_path / marker).exists, timeout=15, what="held local finalization kill window")
    os.kill(firer.pid, signal.SIGKILL)
    wait_until(lambda: firer.poll() is not None, timeout=10, what="original firer killed")
    (tmp_path / "release-finalizer").touch()
    with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
        terminal = db.execute("SELECT status FROM executions WHERE id=?", (info["execution_id"],)).fetchone()
        disposition = db.execute("SELECT state FROM cron_local_finalizations WHERE execution_id=?", (info["execution_id"],)).fetchone()
    with sqlite3.connect(f"file:{epoch_db}?mode=ro", uri=True) as db:
        outcomes = db.execute("SELECT count(*) FROM outcomes WHERE execution_id=?", (info["execution_id"],)).fetchone()[0]
        checked = db.execute("SELECT count(*) FROM records WHERE kind='checked'").fetchone()[0]
    assert terminal == ("completed",) and disposition == ("pending",)
    assert (outcomes, checked) == ((0, 0) if hold_finalizer == "before-v2" else (1, 2))

    # A new process may fire after the dead owner, but cannot settle the prior pin.
    contender = subprocess.run(
        [sys.executable, "-c",
         "import json,os,pathlib,sqlite3\n"
         "from cron import jobs\n"
         f"new=jobs.claim_job_for_fire({info['job_id']!r},manual=True,return_job=True)\n"
         "assert isinstance(new,dict)\n"
         "with sqlite3.connect(pathlib.Path(os.environ['HERMES_HOME'])/'cron'/'executions.db') as db:\n"
         f" state=db.execute('SELECT state FROM cron_local_finalizations WHERE execution_id=?',({info['execution_id']!r},)).fetchone()\n"
         "print(json.dumps({'owner':new['fire_claim']['by'],'old_state':state[0]}))\n"],
        cwd=Path(__file__).resolve().parents[2],
        env={**{k: os.environ[k] for k in ("PATH", "LANG", "TZ") if k in os.environ},
             "HERMES_HOME": str(home), "PYTHONPATH": str(Path(__file__).resolve().parents[2])},
        capture_output=True, text=True, timeout=15, check=False,
    )
    assert contender.returncode == 0, contender.stderr
    observed = json.loads(contender.stdout)
    assert observed["owner"] != info["claim"] and observed["old_state"] == "pending"


def _assert_absent_v2_fails_closed(firer, home, job, info):
    wait_until(lambda: firer.poll() is not None, timeout=35,
               what="absent v2 fails the real original firer")
    observer_path = home / "cron" / f"recovery-{job['id']}" / "local-observation-v2.sqlite"
    assert not observer_path.exists()
    with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
        state = db.execute(
            "SELECT e.status,b.bind_state,b.unresolved_pin FROM executions e "
            "JOIN cron_prerun_bindings b ON b.execution_id=e.id WHERE e.id=?",
            (info["execution_id"],),
        ).fetchone()
        assert state is not None and state[0] != "completed" and state[1:] == ("preparing", 1)
        assert db.execute("SELECT count(*) FROM cron_local_finalizations").fetchone()[0] == 0


def _assert_original_reservation(home, job, info):
    with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        execution = db.execute("SELECT * FROM executions WHERE id=?", (info["execution_id"],)).fetchone()
        assert execution is not None and execution["status"] == "running"
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "cron_prerun_bindings" in tables, "real canonical admission lacked original firer reservation"
        rows = db.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?",
                          (info["execution_id"],)).fetchall()
        assert len(rows) == 1 and rows[0]["bind_state"] in ("awaiting_canonical", "preparing")
        assert rows[0]["job_id"] == job["id"]


def _assert_queued_delivery_is_separate(home, job, info):
    from cron import jobs
    with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
        assert db.execute("SELECT delivery_outcome FROM executions WHERE id=?",
                          (info["execution_id"],)).fetchone() == ("queued",)
    assert jobs.get_job(job["id"])["last_status"] == "delivery_queued"


def _assert_terminal_and_receipts(home, execution_id, epoch_db, stage_roots,
                                  omit_receipt, firer_log):
    with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
        terminal = db.execute("SELECT status FROM executions WHERE id=?", (execution_id,)).fetchone()
        tables = {name for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert terminal == ("completed",) and "cron_local_finalizations" in tables
        disposition = db.execute("SELECT state FROM cron_local_finalizations WHERE execution_id=?",
                                 (execution_id,)).fetchone()
    if omit_receipt:
        assert disposition == ("pending",), "missing one checked root must not mirror success"
    else:
        assert disposition == (("succeeded" if stage_roots else "not_applicable"),), firer_log.read()
    with sqlite3.connect(f"file:{epoch_db}?mode=ro", uri=True) as db:
        if not stage_roots and not omit_receipt:
            row = db.execute("SELECT payload FROM outcomes WHERE execution_id=?", (execution_id,)).fetchone()
            assert row is not None
            outcome = json.loads(row[0])
            assert outcome["state"] == "not_applicable" and outcome["records"] == []
            assert db.execute("SELECT COUNT(*) FROM records WHERE kind='checked'").fetchone() == (0,)
            assert db.execute("SELECT COUNT(*) FROM events WHERE json_extract(payload,'$.event')='finalize'").fetchone() == (1,)
            return
        row = db.execute("SELECT payload FROM outcomes WHERE execution_id=?", (execution_id,)).fetchone()
        outcome = json.loads(row[0]) if row else None
        checked = db.execute("SELECT seq,root,payload FROM records WHERE kind='checked' ORDER BY root").fetchall()
        stage_events = db.execute("SELECT payload FROM events WHERE json_extract(payload,'$.event')='stage'").fetchall()
        final_events = db.execute("SELECT payload FROM events WHERE json_extract(payload,'$.event')='finalize'").fetchall()
        stages = db.execute("SELECT root FROM stages WHERE execution_id=? ORDER BY root", (execution_id,)).fetchall()
        if not omit_receipt:
            assert outcome is not None and outcome["state"] == "succeeded"
            assert [record["root"] for record in outcome["records"]] == ["t_00000001", "t_00000002"]
            assert [root for _, root, _ in checked] == ["t_00000001", "t_00000002"]
            assert len(stage_events) == 2 and len(final_events) == 1 and not stages
            for seq, root, payload in checked:
                assert next(record for record in outcome["records"] if record["root"] == root)["checked_seq"] == seq
                assert json.loads(payload)["local_observation_id"]
                assert db.execute("SELECT checked_seq,pending_seq FROM latest WHERE root=?", (root,)).fetchone() == (seq, None)
        else:
            assert outcome is None and not checked and not final_events
            assert stages == [("t_00000001",), ("t_00000002",)]


@pytest.mark.platforms("linux")
@pytest.mark.parametrize(
    "stage_roots,tamper,kill_script,kill_after_commit,hold_finalizer,omit_receipt",
    [(False, False, False, False, False, False), (True, False, False, False, False, False),
     (False, True, False, False, False, False), (False, False, True, False, False, False),
     (False, False, False, True, False, False)],
    ids=["no-stage", "two-serial-stages", "tampered-prerun-script",
         "sigkill-script-before-commit", "sigkill-script-after-provisional"],
)
def test_target_distinct_process_binds_before_stage_and_finalizes_only_after_terminal(
    tmp_path, monkeypatch, model_peer, stage_roots, tamper, kill_script, kill_after_commit,
    hold_finalizer, omit_receipt,
):
    """A real gateway admission must not outrun the firer's SQLite reservation."""
    from cron import jobs
    from hermes_cli.config_defaults import DEFAULT_CONFIG
    from tests.gateway.test_normal_runtime_boot import control
    import threading

    model_peer.blocked = threading.Event()
    model_peer.release = threading.Event()

    home = tmp_path / "profile"
    home.mkdir(mode=0o700)
    scripts = home / "scripts"
    scripts.mkdir(mode=0o700)
    started = tmp_path / "script-started"
    release = tmp_path / "release-script"
    script = scripts / "local-observation.py"
    parent_case = tamper != "absent-v2"
    script.write_text(
        "import hashlib, json, os, pathlib, sqlite3, time\n"
        f"pathlib.Path({str(started)!r}).write_text(str(os.getppid()))\n"
        f"pathlib.Path({str(tmp_path / 'script-pid')!r}).write_text(str(os.getpid()))\n"
        f"release = pathlib.Path({str(release)!r})\n"
        "while not release.exists(): time.sleep(0.05)\n"
        "nonce = os.environ.get('HERMES_LOCAL_OBSERVATION_NONCE')\n"
        "execution_id = os.environ.get('HERMES_LOCAL_OBSERVATION_EXECUTION_ID')\n"
        f"pathlib.Path({str(tmp_path / 'script-identity')!r}).write_text(json.dumps({{'nonce_present':bool(nonce),'execution_present':bool(execution_id)}}))\n"
        "job_id = os.environ.get('HERMES_LOCAL_OBSERVATION_JOB_ID')\n"
        "assert nonce and execution_id and job_id, 'scoped producer identity absent'\n"
        "digest = hashlib.sha256(bytes.fromhex(nonce)).hexdigest()\n"
        f"if {tamper == 'absent-v2'!r}:\n"
        " epoch_id = hashlib.sha256((execution_id + digest).encode()).hexdigest()\n"
        " print(json.dumps({'version':2,'execution_id':execution_id,'nonce_digest':digest,'generation':1,'epoch_id':epoch_id,'issuance_digest':hashlib.sha256(b'synthetic-empty-issuance').hexdigest(),'wakeAgent':True}))\n"
        " raise SystemExit(0)\n"
        "import engineering_recovery_observer as legacy, local_observation_v2 as v2\n"
        "legacy.PROFILE_HOME = pathlib.Path(os.environ['HERMES_HOME']).resolve()\n"
        "legacy.JOB_ID = job_id\n"
        "directory = legacy.PROFILE_HOME / 'cron' / ('recovery-' + job_id)\n"
        "directory.mkdir(mode=0o700, exist_ok=True)\n"
        "ledger = {'version':2,'board':legacy.BOARD,'job':job_id,'legacy_raw':{},'roots':{}}\n"
        "(directory / 'ledger.json').write_bytes(legacy._bytes(ledger))\n"
        "ledger_sha = legacy.read_ledger(directory)[1]\n"
        "legacy.commit_cutover_marker(directory, ledger_sha, 0, 'missing')\n"
        "source = {'version':1,'job_id':job_id,'board_id':legacy.BOARD,'ledger_sha':ledger_sha,'admission_generation':0,'admission_state':'invalidating','pending':{},'roots':{},'current_issued':{}}\n"
        "(directory / 'local-observation-v1.json').write_bytes(v2.encode(source))\n"
        "store = v2.migrate(directory)\n"
        "store.begin(execution_id,ledger_sha)\n"
        "observations = {}\n"
        f"if {stage_roots!r}:\n"
        " board = pathlib.Path(os.environ['HERMES_KANBAN_DB'])\n"
        " fingerprints, events, attached = legacy.board_observation(board, ['t_00000001','t_00000002'])\n"
        " assert not attached\n"
        " for root in ('t_00000001','t_00000002'):\n"
        "  observations[root] = {'fingerprint':fingerprints[root],'event_ids':events[root],"
        "'observed_at':42.5,'observation_id':hashlib.sha256(root.encode()).hexdigest(),"
        "'attachment_bearing':False}\n"
        "epoch = store.commit(execution_id,ledger_sha,observations)\n"
        "epoch_id, issued = epoch['epoch_id'], epoch['issuance_digest']\n"
        f"pathlib.Path({str(tmp_path / 'script-committed')!r}).touch()\n"
        f"if {kill_after_commit!r}:\n"
        f" while not pathlib.Path({str(tmp_path / 'release-output')!r}).exists(): time.sleep(0.05)\n"
        "print(json.dumps({'version':2,'execution_id':execution_id,'nonce_digest':digest,'generation':1,'epoch_id':epoch_id,'issuance_digest':issued,'wakeAgent':True}, separators=(',', ':')))\n",
        encoding="utf-8",
    )
    script.chmod(0o700)
    finalizer = _write_test_finalizer(tmp_path, scripts, hold_finalizer, omit_receipt)
    model_url = f"http://127.0.0.1:{model_peer.server_port}/v1"
    (home / "config.yaml").write_text(json.dumps({
        "_config_version": DEFAULT_CONFIG["_config_version"],
        "gateway": {"multiplex_profiles": False},
        "model": {"provider": "custom", "default": "local-wire-stub", "base_url": model_url},
        "auxiliary": {"title_generation": {"enabled": False}},
    }), encoding="utf-8")
    root = Path(__file__).resolve().parents[2]
    from tests.cron.fixtures.wri2276_store_fixture import materialize_archived_v2
    archived = materialize_archived_v2(tmp_path / "archived-v2-source", parent=parent_case)
    env = {k: os.environ[k] for k in ("PATH", "LANG", "TZ") if k in os.environ}
    env.update(HERMES_HOME=str(home), HOME=str(tmp_path), USERPROFILE=str(tmp_path),
               PYTHONPATH=os.pathsep.join((str(archived), str(root))), PYTHONUNBUFFERED="1", OPENAI_API_KEY="synthetic-loopback-only",
               OPENAI_BASE_URL=model_url)
    monkeypatch.setenv("HERMES_HOME", str(home))
    # The synthetic recurring job is never due during the gateway boot/ticker.
    job = jobs.create_job(prompt="BLOCK_FIFO synthetic board" if stage_roots else "Check synthetic board",
                          schedule="every 1h", deliver="bot-chat" if tamper == "queued" else "local",
                          script=str(script))
    config = json.loads((home / "config.yaml").read_text())
    config["cron"] = {"local_observation_target_job_id": job["id"],
                      "local_observation_prerun_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
                      "local_observation_finalizer_script": str(finalizer),
                      "local_observation_finalizer_sha256": hashlib.sha256(finalizer.read_bytes()).hexdigest()}
    (home / "config.yaml").write_text(json.dumps(config))
    if tamper is True:
        script.write_text(script.read_text() + "\n# altered after opt-in\n")
    _seed_isolated_board(home, env, parent_case)
    if stage_roots:
        _seed_native_roots(home)
    firer_info = tmp_path / "firer.json"
    firer_code = _firer_program(job["id"], firer_info, parent_case, hold_finalizer, tmp_path)
    gateway_log = (tmp_path / "gateway.log").open("w+")
    firer_log = (tmp_path / "firer.log").open("w+")
    gateway = subprocess.Popen([sys.executable, "-m", "gateway.run"], cwd=root, env=env,
                               stdin=subprocess.DEVNULL, stdout=gateway_log, stderr=subprocess.STDOUT)
    firer = None
    try:
        def gateway_ready():
            if gateway.poll() is not None:
                return False
            try:
                return control(home, "identify").get("state") == "ready"
            except (OSError, ValueError):
                return False
        try:
            wait_until(gateway_ready, timeout=60, what="isolated gateway ready")
        except AssertionError as exc:
            gateway_log.flush(); gateway_log.seek(0)
            raise AssertionError(f"PRETEST gateway={gateway.poll()} {gateway_log.read()}") from exc
        firer = subprocess.Popen([sys.executable, "-c", firer_code], cwd=root, env=env,
                                 stdin=subprocess.DEVNULL, stdout=firer_log, stderr=subprocess.STDOUT)
        try:
            wait_until(lambda: firer_info.exists() or firer.poll() is not None, timeout=20)
            assert firer_info.exists()
            wait_until(lambda: started.exists() or firer.poll() is not None, timeout=25)
            if not tamper:
                assert started.exists()
        except AssertionError as exc:
            firer_log.flush(); firer_log.seek(0)
            gateway_log.flush(); gateway_log.seek(0)
            raise AssertionError(f"PRETEST firer={firer.poll()} {firer_log.read()} gateway={gateway_log.read()}") from exc
        info = json.loads(firer_info.read_text())
        info["job_id"] = job["id"]
        assert info["pid"] == firer.pid != gateway.pid
        if tamper is True:
            assert not started.exists(), "changed target script ran despite configured exact digest"
            with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
                status = db.execute("SELECT status FROM executions WHERE id=?", (info["execution_id"],)).fetchone()
            assert status is not None and status[0] != "completed"
            return
        assert int(started.read_text()) == gateway.pid
        if kill_script:
            script_pid_path = tmp_path / "script-pid"
            wait_until(script_pid_path.exists, timeout=5)
            os.kill(int(script_pid_path.read_text()), signal.SIGKILL)
            wait_until(lambda: firer.poll() is not None, timeout=35,
                       what="firer to record killed script as nonpositive")
            with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
                state = db.execute(
                    "SELECT e.status,b.bind_state FROM executions e JOIN cron_prerun_bindings b ON b.execution_id=e.id WHERE e.id=?",
                    (info["execution_id"],),
                ).fetchone()
            assert state is not None and state[0] != "completed" and state[1] != "bound"
            assert not (home / "cron" / f"recovery-{job['id']}" / "local-observation-v2.sqlite").exists()
            return
        if kill_after_commit:
            release.touch()
            wait_until((tmp_path / "script-committed").exists, timeout=30,
                       what="provisional SQLite commit before stdout")
            os.kill(int((tmp_path / "script-pid").read_text()), signal.SIGKILL)
            wait_until(lambda: firer.poll() is not None, timeout=35,
                       what="firer to record postcommit script kill")
            observer_path = home / "cron" / f"recovery-{job['id']}" / "local-observation-v2.sqlite"
            with sqlite3.connect(f"file:{observer_path}?mode=ro", uri=True) as db:
                assert db.execute("SELECT count(*) FROM epochs").fetchone()[0] == 1
                assert db.execute("SELECT count(*) FROM records WHERE kind='checked'").fetchone()[0] == 0
                assert db.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 0
            with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
                row = db.execute(
                    "SELECT e.status,b.bind_state FROM executions e JOIN cron_prerun_bindings b ON b.execution_id=e.id WHERE e.id=?",
                    (info["execution_id"],),
                ).fetchone()
            assert row is not None and row[0] != "completed" and row[1] == "preparing"
            return
        # This is an admitted real RPC: the canonical process reached the script barrier.
        _assert_original_reservation(home, job, info)
        with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
            prepared_nonce = db.execute("SELECT nonce_digest FROM cron_prerun_bindings WHERE execution_id=?",
                                        (info["execution_id"],)).fetchone()[0]
        release.touch()
        identity_path = tmp_path / "script-identity"
        wait_until(identity_path.exists, timeout=5)
        identity = json.loads(identity_path.read_text())
        assert identity == {"nonce_present": True, "execution_present": True}, (
            "admitted canonical script lacked scoped producer identity")
        if tamper == "absent-v2":
            _assert_absent_v2_fails_closed(firer, home, job, info)
            return
        epoch_db = home / "cron" / f"recovery-{job['id']}" / "local-observation-v2.sqlite"
        def committed_epoch():
            if not epoch_db.exists():
                return None
            try:
                with sqlite3.connect(f"file:{epoch_db}?mode=ro", uri=True) as db:
                    row = db.execute("SELECT execution_id,payload FROM epochs").fetchone()
                    return (row[0], json.loads(row[1])) if row else None
            except sqlite3.OperationalError:
                return None
        epoch = wait_until(committed_epoch, timeout=15, what="script committed provisional epoch")
        assert epoch is not None and epoch[0] == info["execution_id"]
        assert epoch[1]["epoch_id"]
        def binding_row():
            with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
                return db.execute(
                    "SELECT bind_state, nonce_digest, epoch_id, output_sha256 FROM cron_prerun_bindings WHERE execution_id=?",
                    (info["execution_id"],),
                ).fetchone()
        wait_until(lambda: (binding_row() or (None,))[0] == "bound" or firer.poll() is not None,
                   timeout=40, what="bound producer row or finished original firer")
        binding = binding_row()
        assert binding is not None and binding[0] == "bound"
        assert binding[1] == prepared_nonce and binding[2] == epoch[1]["epoch_id"] and binding[3]
        if stage_roots:
            assert model_peer.blocked.wait(15), ("real canonical model did not reach its stage barrier; "
                                                 f"firer={firer.poll()} log={firer_log.read()} gateway={gateway_log.read()}")
            adapter = root / "tests" / "cron" / "fixtures" / "target_parent_stage.py"
            stage_env = dict(env, WRI2276_SYNTHETIC_JOB_ID=job["id"])
            for index, root_id in enumerate(("t_00000001", "t_00000002")):
                with sqlite3.connect(f"file:{epoch_db}?mode=ro", uri=True) as db:
                    revision = json.loads(db.execute("SELECT payload FROM metadata WHERE id=1").fetchone()[0])["revision"]
                stale = subprocess.run(
                    [sys.executable, str(adapter), info["execution_id"], root_id, str(revision - 1)],
                    cwd=root, env=stage_env, capture_output=True, text=True, timeout=10,
                    check=False,
                )
                assert stale.returncode != 0, "stale observer revision staged a root"
                with sqlite3.connect(f"file:{epoch_db}?mode=ro", uri=True) as db:
                    assert db.execute("SELECT count(*) FROM stages").fetchone()[0] == index
                staged = subprocess.run(
                    [sys.executable, str(adapter), info["execution_id"], root_id, str(revision)],
                    cwd=root, env=stage_env, capture_output=True, text=True, timeout=10,
                    check=False,
                )
                assert staged.returncode == 0, staged.stderr
                assert json.loads(staged.stdout) == {"state": "staged", "root": root_id,
                                                     "revision": revision + 1}
                with sqlite3.connect(f"file:{epoch_db}?mode=ro", uri=True) as db:
                    assert db.execute("SELECT count(*) FROM records WHERE kind='checked'").fetchone()[0] == 0
                    assert db.execute("SELECT count(*) FROM stages").fetchone()[0] == index + 1
            with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
                assert db.execute("SELECT status FROM executions WHERE id=?", (info["execution_id"],)).fetchone() == ("running",)
            model_peer.release.set()
        if hold_finalizer in ("before-v2", "after-v2"):
            _assert_killed_finalizer_pending(tmp_path, hold_finalizer, firer, home, info, epoch_db)
            return
        if hold_finalizer:
            wait_until((tmp_path / "finalizer-started").exists, timeout=15,
                       what="finalizer entering held outer fire fence")
            with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
                assert db.execute("SELECT status FROM executions WHERE id=?", (info["execution_id"],)).fetchone() == ("completed",)
            contender_code = (
                "import json, os, pathlib, sqlite3\n"
                "from cron import jobs\n"
                f"pathlib.Path({str(tmp_path / 'contender-attempt')!r}).touch()\n"
                f"claimed = jobs.claim_job_for_fire({job['id']!r}, manual=True, return_job=True)\n"
                "assert isinstance(claimed, dict)\n"
                "with sqlite3.connect(pathlib.Path(os.environ['HERMES_HOME']) / 'cron' / 'executions.db') as db:\n"
                f" state = db.execute('SELECT state FROM cron_local_finalizations WHERE execution_id=?', ({info['execution_id']!r},)).fetchone()\n"
                f"pathlib.Path({str(tmp_path / 'contender-result')!r}).write_text(json.dumps({{'state':state,'claim':claimed['fire_claim']['by']}}))\n"
            )
            contender = subprocess.Popen([sys.executable, "-c", contender_code], cwd=root, env=env,
                                         stdin=subprocess.DEVNULL, stdout=firer_log,
                                         stderr=subprocess.STDOUT)
            try:
                wait_until((tmp_path / "contender-attempt").exists, timeout=10)
                assert not (tmp_path / "contender-result").exists()
                (tmp_path / "release-finalizer").touch()
                wait_until((tmp_path / "contender-result").exists, timeout=15)
                contender_result = json.loads((tmp_path / "contender-result").read_text())
                assert contender_result["state"] == ["succeeded"]
                assert contender_result["claim"] != info["claim"]
            finally:
                (tmp_path / "release-finalizer").touch()
                if contender.poll() is None:
                    contender.terminate()
                contender.wait(timeout=8)
        wait_until(lambda: firer.poll() is not None, timeout=40, what="original firer terminal")
        assert model_peer.requests, ("loopback agent was not reached through canonical process; "
                                     f"job={jobs.get_job(job['id'])} firer={firer_log.read()} gateway={gateway_log.read()}")
        firer_log.flush(); firer_log.seek(0)
        gateway_log.flush(); gateway_log.seek(0)
        _assert_terminal_and_receipts(home, info["execution_id"], epoch_db, stage_roots,
                                      omit_receipt, firer_log)
        if tamper == "queued":
            _assert_queued_delivery_is_separate(home, job, info)
    finally:
        model_peer.release.set()
        release.touch()
        (tmp_path / "release-output").touch()
        (tmp_path / "release-finalizer").touch()
        if firer is not None:
            try:
                firer.wait(timeout=8)
            except subprocess.TimeoutExpired:
                firer.terminate()
                try:
                    firer.wait(timeout=4)
                except subprocess.TimeoutExpired:
                    firer.kill()
                    firer.wait(timeout=4)
        gateway.send_signal(signal.SIGINT)
        try:
            gateway.wait(timeout=12)
        except subprocess.TimeoutExpired:
            gateway.kill()
            gateway.wait(timeout=4)
        gateway_log.close()
        firer_log.close()
