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
    """Isolated observer-contract executable: no production observer import or fallback."""
    finalizer = scripts / "local-finalize.py"
    finalizer.write_text(
        "import hashlib, json, os, pathlib, sqlite3\n"
        "home = pathlib.Path(os.environ['HERMES_HOME'])\n"
        "jid = os.environ['HERMES_LOCAL_OBSERVATION_JOB_ID']\n"
        "eid = os.environ['HERMES_LOCAL_OBSERVATION_EXECUTION_ID']\n"
        "epoch_id = os.environ['HERMES_LOCAL_OBSERVATION_EPOCH_ID']\n"
        f"if {hold_finalizer is True or hold_finalizer == 'before-v2'!r}:\n"
        f" pathlib.Path({str(tmp_path / 'finalizer-started')!r}).touch()\n"
        f" while not pathlib.Path({str(tmp_path / 'release-finalizer')!r}).exists(): __import__('time').sleep(0.05)\n"
        "with sqlite3.connect(home / 'cron' / 'executions.db') as producer:\n"
        " row = producer.execute('SELECT status FROM executions WHERE id=? AND job_id=?', (eid,jid)).fetchone()\n"
        " assert row == ('completed',), row\n"
        "with sqlite3.connect(home / 'cron' / ('recovery-' + jid) / 'local-observation-v2.sqlite') as observer:\n"
        " observer.execute('PRAGMA journal_mode=DELETE')\n"
        " observer.execute('PRAGMA synchronous=FULL')\n"
        " observer.execute('BEGIN IMMEDIATE')\n"
        " epoch = observer.execute('SELECT epoch_id FROM epochs WHERE execution_id=? AND job=?', (eid,jid)).fetchone()\n"
        " assert epoch == (epoch_id,), epoch\n"
        " stages = observer.execute('SELECT root,evidence_digest FROM stages WHERE execution_id=? ORDER BY root', (eid,)).fetchall()\n"
        " if stages:\n"
        "  assert len(stages) == 2, stages\n"
        "  with sqlite3.connect(home / 'cron' / 'synthetic-board.sqlite') as board:\n"
        "   for root,evidence_digest in stages:\n"
        "    current = board.execute('SELECT evidence FROM roots WHERE root=?', (root,)).fetchone()\n"
        "    assert current and hashlib.sha256(current[0].encode()).hexdigest() == evidence_digest\n"
        f"    if not ({omit_receipt!r} and root == 'root-b'): observer.execute('INSERT INTO checked VALUES (?,?,?)', (eid,root,evidence_digest))\n"
        "  state = 'succeeded'\n"
        " else: state = 'not_applicable'\n"
        " digest = hashlib.sha256((state + ':' + eid + ':' + epoch_id + ':' + ','.join(root for root,_ in stages)).encode()).hexdigest()\n"
        " observer.execute('INSERT INTO outcomes VALUES (?,?,?)', (eid,state,digest))\n"
        " observer.commit()\n"
        f"if {hold_finalizer == 'after-v2'!r}:\n"
        f" pathlib.Path({str(tmp_path / 'finalizer-committed')!r}).touch()\n"
        f" while not pathlib.Path({str(tmp_path / 'release-finalizer')!r}).exists(): __import__('time').sleep(0.05)\n"
        "print(json.dumps({'state':state,'digest':digest,'execution_id':eid,'epoch_id':epoch_id}))\n",
        encoding="utf-8",
    )
    finalizer.chmod(0o700)
    return finalizer


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
        checked = db.execute("SELECT count(*) FROM checked WHERE execution_id=?", (info["execution_id"],)).fetchone()[0]
    assert terminal == ("completed",) and disposition == ("pending",)
    assert (outcomes, checked) == ((0, 0) if hold_finalizer == "before-v2" else (1, 2))


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
        outcome = db.execute("SELECT state,digest FROM outcomes WHERE execution_id=?", (execution_id,)).fetchone()
        checked = db.execute("SELECT root FROM checked WHERE execution_id=? ORDER BY root", (execution_id,)).fetchall()
    assert outcome is not None and outcome[0] == ("succeeded" if stage_roots else "not_applicable")
    assert checked == ([("root-a",)] if omit_receipt else
                       [("root-a",), ("root-b",)] if stage_roots else [])


@pytest.mark.platforms("linux")
@pytest.mark.parametrize(
    "stage_roots,tamper,kill_script,kill_after_commit,hold_finalizer,omit_receipt",
    [(False, False, False, False, False, False), (True, False, False, False, False, False),
     (False, True, False, False, False, False), (False, False, True, False, False, False),
     (False, False, False, True, False, False), (True, False, False, False, True, False),
     (True, False, False, False, False, True),
     (True, False, False, False, "before-v2", False),
     (True, False, False, False, "after-v2", False)],
    ids=["no-stage", "two-serial-stages", "tampered-prerun-script",
         "sigkill-script-before-commit", "sigkill-script-after-provisional",
         "second-fire-waits-through-finalizer", "missing-root-receipt-refuses-mirror",
         "sigkill-firer-terminal-before-v2", "sigkill-firer-v2-before-mirror"],
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
        "db_path = pathlib.Path(os.environ['HERMES_HOME']) / 'cron' / ('recovery-' + job_id) / 'local-observation-v2.sqlite'\n"
        "db_path.parent.mkdir(mode=0o700, exist_ok=True)\n"
        "with sqlite3.connect(db_path) as db:\n"
        " db.execute('PRAGMA journal_mode=DELETE')\n"
        " db.execute('PRAGMA synchronous=FULL')\n"
        " db.execute('BEGIN IMMEDIATE')\n"
        " db.execute('CREATE TABLE IF NOT EXISTS epochs (execution_id TEXT PRIMARY KEY, profile TEXT NOT NULL, job TEXT NOT NULL, nonce_digest TEXT NOT NULL, generation INTEGER NOT NULL, epoch_id TEXT NOT NULL, issuance_digest TEXT NOT NULL, status TEXT NOT NULL)')\n"
        " db.execute('CREATE TABLE IF NOT EXISTS stages (execution_id TEXT NOT NULL, root TEXT NOT NULL, evidence_digest TEXT NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY(execution_id,root))')\n"
        " db.execute('CREATE TABLE IF NOT EXISTS outcomes (execution_id TEXT PRIMARY KEY, state TEXT NOT NULL, digest TEXT NOT NULL)')\n"
        " db.execute('CREATE TABLE IF NOT EXISTS checked (execution_id TEXT NOT NULL, root TEXT NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(execution_id,root))')\n"
        " db.execute('CREATE TABLE IF NOT EXISTS epoch_issued (execution_id TEXT NOT NULL, root TEXT NOT NULL, observation_id TEXT NOT NULL, evidence_digest TEXT NOT NULL, PRIMARY KEY(execution_id,root))')\n"
        " db.execute('CREATE TABLE IF NOT EXISTS metadata (id INTEGER PRIMARY KEY, revision INTEGER NOT NULL)')\n"
        " db.execute('INSERT OR IGNORE INTO metadata VALUES (1,1)')\n"
        " epoch_id = hashlib.sha256((execution_id + digest).encode()).hexdigest()\n"
        " issued = hashlib.sha256(b'synthetic-empty-issuance').hexdigest()\n"
        " db.execute('INSERT INTO epochs VALUES (?,?,?,?,?,?,?,?)', (execution_id,os.environ['HERMES_HOME'],job_id,digest,1,epoch_id,issued,'provisional'))\n"
        " for root,evidence in [('root-a','event-a'),('root-b','event-b')]:\n"
        "  db.execute('INSERT INTO epoch_issued VALUES (?,?,?,?)', (execution_id,root,'obs-'+root,hashlib.sha256(evidence.encode()).hexdigest()))\n"
        " db.commit()\n"
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
    env = {k: os.environ[k] for k in ("PATH", "LANG", "TZ") if k in os.environ}
    env.update(HERMES_HOME=str(home), HOME=str(tmp_path), USERPROFILE=str(tmp_path),
               PYTHONPATH=str(root), PYTHONUNBUFFERED="1", OPENAI_API_KEY="synthetic-loopback-only",
               OPENAI_BASE_URL=model_url)
    monkeypatch.setenv("HERMES_HOME", str(home))
    # The synthetic recurring job is never due during the gateway boot/ticker.
    job = jobs.create_job(prompt="BLOCK_FIFO synthetic board" if stage_roots else "Check synthetic board",
                          schedule="every 1h", deliver="local", script=str(script))
    config = json.loads((home / "config.yaml").read_text())
    config["cron"] = {"local_observation_target_job_id": job["id"],
                      "local_observation_prerun_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
                      "local_observation_finalizer_script": str(finalizer),
                      "local_observation_finalizer_sha256": hashlib.sha256(finalizer.read_bytes()).hexdigest()}
    (home / "config.yaml").write_text(json.dumps(config))
    if tamper:
        script.write_text(script.read_text() + "\n# altered after opt-in\n")
    with sqlite3.connect(home / "cron" / "synthetic-board.sqlite") as board:
        board.execute("CREATE TABLE roots (root TEXT PRIMARY KEY, evidence TEXT NOT NULL)")
        board.executemany("INSERT INTO roots VALUES (?,?)",
                          [("root-a", "event-a"), ("root-b", "event-b")])
    firer_info = tmp_path / "firer.json"
    firer_code = (
        "import json, os, pathlib\n"
        "from cron import jobs, executions, scheduler\n"
        "job = jobs.claim_job_for_fire(" + repr(job["id"]) + ", manual=True, return_job=True)\n"
        "assert isinstance(job, dict), job\n"
        "execution = executions.create_execution(job['id'], source='test-firer')\n"
        "job['execution_id'] = execution['id']\n"
        f"pathlib.Path({str(firer_info)!r}).write_text(json.dumps({{'pid': os.getpid(), 'execution_id': execution['id'], 'claim': job['fire_claim']['by']}}))\n"
        "scheduler._run_one_job_body(job)\n"
    )
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
        assert info["pid"] == firer.pid != gateway.pid
        if tamper:
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
            wait_until((tmp_path / "script-committed").exists, timeout=15,
                       what="provisional SQLite commit before stdout")
            os.kill(int((tmp_path / "script-pid").read_text()), signal.SIGKILL)
            wait_until(lambda: firer.poll() is not None, timeout=35,
                       what="firer to record postcommit script kill")
            observer_path = home / "cron" / f"recovery-{job['id']}" / "local-observation-v2.sqlite"
            with sqlite3.connect(f"file:{observer_path}?mode=ro", uri=True) as db:
                assert db.execute("SELECT count(*) FROM epochs").fetchone()[0] == 1
                assert db.execute("SELECT count(*) FROM checked").fetchone()[0] == 0
                assert db.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 0
            with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
                row = db.execute(
                    "SELECT e.status,b.bind_state FROM executions e JOIN cron_prerun_bindings b ON b.execution_id=e.id WHERE e.id=?",
                    (info["execution_id"],),
                ).fetchone()
            assert row is not None and row[0] != "completed" and row[1] == "preparing"
            return
        # This is an admitted real RPC: the canonical process reached the script barrier.
        with sqlite3.connect(f"file:{home / 'cron' / 'executions.db'}?mode=ro", uri=True) as db:
            db.row_factory = sqlite3.Row
            execution = db.execute("SELECT * FROM executions WHERE id=?", (info["execution_id"],)).fetchone()
            assert execution is not None and execution["status"] == "running"
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            assert "cron_prerun_bindings" in tables, (
                "real distinct-process canonical admission reached script, but original firer "
                "did not reserve its durable awaiting_canonical binding")
            rows = db.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?",
                              (info["execution_id"],)).fetchall()
            assert len(rows) == 1 and rows[0]["bind_state"] in ("awaiting_canonical", "preparing")
            assert rows[0]["job_id"] == job["id"]
        release.touch()
        identity_path = tmp_path / "script-identity"
        wait_until(identity_path.exists, timeout=5)
        identity = json.loads(identity_path.read_text())
        assert identity == {"nonce_present": True, "execution_present": True}, (
            "admitted canonical script lacked scoped producer identity")
        epoch_db = home / "cron" / f"recovery-{job['id']}" / "local-observation-v2.sqlite"
        def committed_epoch():
            if not epoch_db.exists():
                return None
            try:
                with sqlite3.connect(f"file:{epoch_db}?mode=ro", uri=True) as db:
                    return db.execute("SELECT execution_id, nonce_digest, epoch_id FROM epochs").fetchone()
            except sqlite3.OperationalError:
                return None
        epoch = wait_until(committed_epoch, timeout=15, what="script committed provisional epoch")
        assert epoch[0] == info["execution_id"]
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
        assert binding[1:3] == epoch[1:3] and binding[3]
        if stage_roots:
            assert model_peer.blocked.wait(15), "real canonical model did not reach its stage barrier"
            adapter = root / "tests" / "cron" / "fixtures" / "wri2276_observer_contract.py"
            stage_env = dict(env, WRI2276_SYNTHETIC_JOB_ID=job["id"])
            for revision, root_id in enumerate(("root-a", "root-b"), start=1):
                stale = subprocess.run(
                    [sys.executable, str(adapter), info["execution_id"], root_id, "0"],
                    cwd=root, env=stage_env, capture_output=True, text=True, timeout=10,
                    check=False,
                )
                assert stale.returncode != 0, "stale observer revision staged a root"
                with sqlite3.connect(f"file:{epoch_db}?mode=ro", uri=True) as db:
                    assert db.execute("SELECT count(*) FROM stages").fetchone()[0] == revision - 1
                staged = subprocess.run(
                    [sys.executable, str(adapter), info["execution_id"], root_id, str(revision)],
                    cwd=root, env=stage_env, capture_output=True, text=True, timeout=10,
                    check=False,
                )
                assert staged.returncode == 0, staged.stderr
                assert json.loads(staged.stdout) == {"state": "staged", "root": root_id,
                                                     "revision": revision + 1}
                with sqlite3.connect(f"file:{epoch_db}?mode=ro", uri=True) as db:
                    assert db.execute("SELECT count(*) FROM checked").fetchone()[0] == 0
                    assert db.execute("SELECT count(*) FROM stages").fetchone()[0] == revision
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
        assert model_peer.requests, "loopback agent was not reached through canonical process"
        firer_log.flush(); firer_log.seek(0)
        gateway_log.flush(); gateway_log.seek(0)
        _assert_terminal_and_receipts(home, info["execution_id"], epoch_db, stage_roots,
                                      omit_receipt, firer_log)
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
