"""WRI-2276 original worker and separate stage children against native board/v2."""

from __future__ import annotations

import json
import hashlib
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from tests.waiting import wait_until
from tests.cron.local_observation_support import (
    native_tmp_path, copy_package, bootstrap_text, prerun_bootstrap, check_board,
    write_current_manifest)


def _seed_discovery(directory, native, legacy, roots):
    with sqlite3.connect(native) as db:
        db.execute('CREATE TABLE cron_notepad(job_id TEXT,key TEXT,value TEXT,updated_at TEXT)')
        db.executemany('INSERT INTO cron_notepad VALUES(?,?,?,?)',
                       [(legacy.JOB_ID, 'recovery_root_' + root, '{"status":"blocked"}', 'original')
                        for root in (roots if len(roots) <= 470 else [])])
    ledger = {"version": 2, "board": legacy.BOARD, "job": legacy.JOB_ID,
              "legacy_raw": legacy.read_legacy(native)['raw'], "roots": {}}
    (directory / "ledger.json").write_bytes(legacy._bytes(ledger))
    ledger_sha = legacy.read_ledger(directory)[1]
    ack_sha = 'missing'
    if len(roots) > 470:
        # Typed retained-root discovery avoids an oversized legacy raw ledger.
        # Empty fingerprints grant no checked proof; real native_commit observes every root.
        ack = {'version':1,'board':legacy.BOARD,'job':legacy.JOB_ID,
               'local':{root:'' for root in roots},'sources':{}}
        (directory/'ack.json').write_bytes(legacy._bytes(ack))
        ack_sha = legacy.read_ack(directory)[1]
    legacy.commit_cutover_marker(directory, ledger_sha, len(ledger['legacy_raw']), ack_sha)
    return ledger, ledger_sha


def run_parent_fixture(tmp_path, monkeypatch, *, crash_phase, root_count, historical=False,
                       second_fire=False, old_claim_attempt=False):
    from cron import executions, jobs
    from hermes_cli import kanban_db

    home = tmp_path / "rowan"
    source = home / "scripts" / "observer_v2"
    source.mkdir(parents=True)
    copy_package(source)
    monkeypatch.setenv("HERMES_HOME", str(home))
    try:
        from cron.scheduler_local_observation import load_private_package
        admitted = load_private_package(source)
        legacy, v2 = admitted['engineering_recovery_observer'], admitted['local_observation_v2']

        roots = [f"t_{i:08x}" for i in range(1, root_count + 1)]
        board = tmp_path / "board.db"
        check_board(board, tmp_path)
        with sqlite3.connect(board) as db:
            db.executescript(kanban_db.SCHEMA_SQL)
            db.executemany("INSERT INTO tasks(id,title,body,assignee,status,created_at) VALUES(?,?,?,?,?,?)",
                           [(root, "fixture", "", "tester", "blocked", 1) for root in roots])
            if crash_phase == "event_heavy":
                # Typed native event history, not thousands of artificial CLI processes.
                db.executemany("INSERT INTO task_events(task_id,kind,created_at) VALUES(?,?,?)",
                               [(roots[0], "commented", 1)] * 3500)
            db.commit()
        monkeypatch.setenv("HERMES_KANBAN_DB", str(board))
        monkeypatch.setenv("HERMES_HOME", str(home))
        monkeypatch.setattr(legacy, "PROFILE_HOME", home)
        directory = home / "cron" / f"recovery-{legacy.JOB_ID}"
        directory.mkdir(parents=True, mode=0o700)
        ledger, ledger_sha = _seed_discovery(directory, home / 'cron/notepad.db', legacy, roots)
        old_pending, old_checked = _historical_rows(roots, historical)
        old = {"version": 1, "board_id": legacy.BOARD, "job_id": legacy.JOB_ID,
               "ledger_sha": ledger_sha, "admission_generation": 0,
               "admission_state": "invalidating", "pending": old_pending, "roots": old_checked,
               "current_issued": {}}
        (directory / "local-observation-v1.json").write_bytes(v2.encode(old))
        v2.migrate(directory)

        if historical:
            assert v2.Store(directory).history_count() == root_count + 192
        roots_file = tmp_path / "roots.json"
        roots_file.write_text(json.dumps(roots), encoding="utf-8")
        script = source / 'observer_private_entry.py'
        bootstrap = tmp_path / 'bootstrap.py'
        bootstrap.write_text(prerun_bootstrap(source), encoding='utf-8')
        with jobs.use_cron_store(home):
            original_uuid = jobs.uuid.uuid4
            class TargetId:
                hex = legacy.JOB_ID
            monkeypatch.setattr(jobs.uuid, "uuid4", lambda: TargetId())
            job = jobs.create_job(prompt="Inspect current local observation.", schedule="every 1h",
                                  name="two-root fixture", script=str(script.relative_to(home / 'scripts')), no_agent=False)
            monkeypatch.setattr(jobs.uuid, "uuid4", original_uuid)
            claimed = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
            execution = executions.create_execution(job["id"], source="direct")
            assert executions.mark_execution_handoff_pending(execution["id"])
        assert isinstance(claimed, dict)
        trust_patch = write_current_manifest(home, source, claimed)
        claimed["execution_id"] = execution["id"]
        payload = tmp_path / "worker.json"
        ack = tmp_path / "worker.ready"
        payload.write_text(json.dumps({"job": claimed, "profile_home": str(home)}), encoding="utf-8")
        reserved = tmp_path / "reserved"
        reserve_release = tmp_path / "reserve-release"
        bound = tmp_path / "bound"
        release = tmp_path / "release"
        rejected = tmp_path / "stale-claim-rejected"
        harness = (
            "import os,pathlib,time,sys\n" + bootstrap_text(source)
            + trust_patch
            + (
               "seam._TEST_BEFORE_COMMIT=lambda: os._exit(77)\n"
               if crash_phase == "before_commit" else "")
            +
            "from cron import scheduler,scheduler_local_observation as producer\n"
            "from cron import scheduler_script\n"
            f"scheduler_script._posix_cron_script_argv=lambda script: ([sys.executable,{str(bootstrap)!r},str(script)],{{}})\n"
            "from types import SimpleNamespace\n"
            "from unittest.mock import MagicMock\n"
            "scheduler._load_cron_job_config=lambda *_: SimpleNamespace(cfg={},model='fixture')\n"
            "scheduler._resolve_cron_agent_setup=lambda *_: SimpleNamespace(blocked=None,model='fixture',fallback_notice=None)\n"
            "scheduler._construct_cron_agent=lambda *a,**kw: MagicMock()\n"
            "def downstream_agent(agent,prompt,*args,**kwargs):\n"
            f" pathlib.Path({str(tmp_path / 'agent-prompt.txt')!r}).write_text(prompt)\n"
            " return {'completed':True,'final_response':scheduler.SILENT_MARKER}\n"
            "scheduler._run_agent_with_watchdog=downstream_agent\n"
            + ("original_prepare=producer.prepare_terminal\n"
               "def change_after_capture(*args,**kwargs):\n"
               " captured=original_prepare(*args,**kwargs)\n"
               f" path=pathlib.Path({str(script)!r})\n"
               " path.write_text(path.read_text()+'\\n# post-capture changed\\n')\n"
               " return captured\n"
               "producer.prepare_terminal=change_after_capture\n"
               if crash_phase == 'script_changed' else '')
            + ("original_prepare=producer.prepare_terminal\n"
               "def change_epoch_after_capture(*args,**kwargs):\n"
               " captured=original_prepare(*args,**kwargs)\n"
               f" with v2.Store(pathlib.Path({str(directory)!r}))._connection(write=True) as (db,meta):\n"
               "  meta['epoch_id']='0'*64; v2._write_metadata(db,meta)\n"
               " return captured\n"
               "producer.prepare_terminal=change_epoch_after_capture\n"
               if crash_phase == 'stale_epoch' else '')
            + ("producer._TEST_AFTER_COMMIT=lambda: os._exit(78)\n"
               if crash_phase == "after_commit" else "")
            + ("import sqlite3\n"
               "def change_binding():\n"
               f" db=sqlite3.connect({str(home / 'cron' / 'executions.db')!r})\n"
               f" db.execute('UPDATE cron_prerun_bindings SET nonce_digest=? WHERE execution_id=?',('0'*64,{execution['id']!r}))\n"
               " db.commit();db.close()\n"
               "producer._TEST_AFTER_COMMIT=change_binding\n"
               if crash_phase == "binding_changed_postcommit" else "")
            + (
               "def no_space(*_args): raise OSError('ENOSPC injected at v2 capacity check')\n"
               "v2._check_capacity=no_space\n"
               if crash_phase == "disk_full" else "")
            + ("def lost_reply(*_args): raise TimeoutError('proposal child reply lost')\n"
               "producer._read_proposal=lost_reply\n"
               if crash_phase == "lost_proposal_reply" else "")
            +
            f"reserved=pathlib.Path({str(reserved)!r});reserve_release=pathlib.Path({str(reserve_release)!r})\n"
            f"bound=pathlib.Path({str(bound)!r});release=pathlib.Path({str(release)!r})\n"
            "def after_reserve():\n"
            " reserved.write_text('awaiting')\n"
            " deadline=time.monotonic()+20\n"
            " while not reserve_release.exists() and time.monotonic()<deadline: time.sleep(0.02)\n"
            " if not reserve_release.exists(): raise RuntimeError('reserve timeout')\n"
            "def after_bind():\n"
            " bound.write_text('bound')\n"
            + (
               f" try: seam.ParentAuthority.capture({execution['id']!r},{job['id']!r},'stale-owner')\n"
               " except seam.v2.Stale as exc:\n"
               "  assert str(exc) == 'original fire claim missing', str(exc)\n"
               f"  pathlib.Path({str(rejected)!r}).write_text('denied')\n"
               " else: raise AssertionError('stale fire claim captured parent authority')\n"
               if old_claim_attempt else "")
            + " deadline=time.monotonic()+180\n"
            " while not release.exists() and time.monotonic()<deadline: time.sleep(0.02)\n"
            " if not release.exists(): raise RuntimeError('stage timeout')\n"
            "producer._TEST_AFTER_RESERVE=after_reserve\n"
            "producer._TEST_AFTER_BIND=after_bind\n"
            f"assert scheduler._run_external_worker_payload(pathlib.Path({str(payload)!r}),pathlib.Path({str(ack)!r}))\n"
        )
        worker = subprocess.Popen([sys.executable, "-c", harness], cwd=str(Path(__file__).resolve().parents[2]),
                                  env=dict(os.environ, HERMES_HOME=str(home)),
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            assert wait_until(reserved.exists, timeout=15), "original worker never reserved"
            with sqlite3.connect(home / "cron" / "executions.db") as db:
                awaiting = db.execute("SELECT bind_state,unresolved_pin,nonce_digest,output_sha256,pid "
                                      "FROM cron_prerun_bindings WHERE execution_id=?",
                                      (execution["id"],)).fetchone()
            assert awaiting == ("awaiting_canonical", 1, None, None, worker.pid)
            reserve_release.write_text("go", encoding="utf-8")
            assert wait_until(lambda: bound.exists() or worker.poll() is not None,
                              timeout=90), "worker did not settle native v2 issuance"
            assert bound.exists(), f"worker exited before bound issuance: {worker.returncode}"
            if old_claim_attempt:
                assert wait_until(rejected.exists, timeout=10), "stale claim was not challenged"
            observed = executions.latest_execution(job["id"])
            assert observed and observed["id"] == execution["id"] and observed["status"] == "running"
            with sqlite3.connect(home / "cron" / "executions.db") as db:
                pin = db.execute("SELECT bind_state,pid FROM cron_prerun_bindings WHERE execution_id=?",
                                 (execution["id"],)).fetchone()
                bound_epoch = db.execute('SELECT epoch_id FROM cron_prerun_bindings WHERE execution_id=?',
                                         (execution['id'],)).fetchone()[0]
                captured_binding = db.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?",
                                              (execution["id"],)).fetchone()
            assert pin == ("bound", worker.pid)
            for root in roots[:2]:
                stage = (
                    bootstrap_text(source)
                    + "from hermes_cli import kanban_db as kb\n"
                    f"store=v2.Store(pathlib.Path({str(directory)!r}))\n"
                    "header=store.header()\n"
                    f"issued={{i['root']:i for i in store.page_current({execution['id']!r},header['epoch_id'],header['generation'],header['issuance_digest'],max_items=2)['items']}}[{root!r}]\n"
                    "with kb.pin_first_board_resolution():\n"
                    f" entry.main(['local-stage',{execution['id']!r},{root!r},issued['observation_id'],str(header['revision'])])\n"
                )
                child = subprocess.run([sys.executable, "-c", stage],
                                       cwd=str(Path(__file__).resolve().parents[2]),
                                       env=dict(os.environ, HERMES_HOME=str(home)),
                                       capture_output=True, text=True, timeout=20, check=False)
                assert child.returncode == 0, child.stderr
            if root_count > 2 and crash_phase != "extra_stage":
                import observer_parent_seam as seam
                fingerprints, event_ids, attached = legacy.board_observation(board, roots[2:])
                assert not attached
                marker = legacy._cutover_path(directory, ledger)
                cutover_sha = hashlib.sha256(marker.read_bytes()).hexdigest()
                store = v2.Store(directory)
                with store._connection(write=True) as (db, meta):
                    for root in roots[2:]:
                        ident, pending = db.execute(
                            "SELECT observation_id,pending_seq FROM issued WHERE root=?", (root,)).fetchone()
                        preimage = db.execute("SELECT checked_seq FROM latest WHERE root=?", (root,)).fetchone()[0]
                        evidence = {"version": 1, "execution_id": execution["id"],
                                    "epoch_id": meta["epoch_id"], "root": root,
                                    "pending_seq": pending, "observation_id": ident,
                                    "local_fingerprint": fingerprints[root],
                                    "consumed_event_ids": event_ids[root],
                                    "attachment_bearing": False, "ledger_sha": meta["ledger_sha"],
                                    "cutover_sha": cutover_sha}
                        stage = {"execution_id": execution["id"], "root": root,
                                 "pending_seq": pending, "checked_preimage_seq": preimage,
                                 "observation_id": ident, "epoch_id": meta["epoch_id"],
                                 "stage_revision": meta["revision"], "evidence_version": 1,
                                 "board_evidence_digest": seam.evidence_digest(evidence)}
                        db.execute("INSERT INTO stages(execution_id,root,payload) VALUES(?,?,?)",
                                   (execution["id"], root, v2.encode(stage)))
                        db.execute("INSERT INTO events(payload) VALUES(?)",
                                   (v2.encode({"event": "stage", **stage, "board_evidence": evidence}),))
                        meta["revision"] += 1
                    v2._write_metadata(db, meta)
            if crash_phase == "extra_stage":
                with v2.Store(directory)._connection(write=True) as (db, meta):
                    root = roots[2]
                    ident, pending = db.execute("SELECT observation_id,pending_seq FROM issued WHERE root=?",
                                                (root,)).fetchone()
                    db.execute("INSERT INTO stages(execution_id,root,payload) VALUES(?,?,?)",
                               (execution["id"], root, v2.encode({"execution_id": execution["id"],
                                 "root": root, "pending_seq": pending, "observation_id": ident,
                                 "epoch_id": meta["epoch_id"]})))
            assert v2.Store(directory).header()["checked_count"] == (192 if historical else 0)
            if crash_phase == "writer_first":
                with kanban_db.pin_first_board_resolution():
                    from hermes_cli import kanban_db_connect
                    writer = kanban_db_connect.connect()
                    try:
                        kanban_db.add_comment(writer, roots[0], "test-writer", "changed before parent commit")
                    finally:
                        writer.close()
            if crash_phase == "attachment_after_stage":
                with kanban_db.pin_first_board_resolution():
                    from hermes_cli import kanban_db_connect
                    writer = kanban_db_connect.connect()
                    try:
                        attachment = tmp_path / "private-evidence.txt"
                        attachment.write_bytes(b"fixture-only")
                        kanban_db.add_attachment(writer, roots[0], filename=attachment.name,
                                                 stored_path=str(attachment), size=attachment.stat().st_size)
                    finally:
                        writer.close()


        finally:
            reserve_release.write_text("go", encoding="utf-8")
            release.write_text("go", encoding="utf-8")
            _out, err = worker.communicate(timeout=180)
        assert worker.returncode == {None: 0, "before_commit": 77,
                                     "after_commit": 78, "writer_first": 1,
                                     "stale_epoch": 1, "script_changed": 1,
                                     "binding_changed_postcommit": 1, "extra_stage": 1,
                                     "disk_full": 1, "lost_proposal_reply": 1,
                                     "attachment_after_stage": 1, "event_heavy": 1}[crash_phase], err
        assert (tmp_path / 'agent-prompt.txt').is_file(), 'real agent path was not reached'
        prompt = (tmp_path / 'agent-prompt.txt').read_text()
        assert f'{script} local-select {execution["id"]} {bound_epoch}' in prompt, (
            'reachable agent prompt does not use the admitted V2 selector')
        if crash_phase == "event_heavy":
            assert "single outcome proof item exceeds page" in err, err
        with sqlite3.connect(directory / "local-observation-v2.sqlite") as db:
            rows = db.execute("SELECT root,kind FROM records ORDER BY seq").fetchall()
            outcome = db.execute("SELECT payload FROM outcomes WHERE execution_id=?", (execution["id"],)).fetchone()
            stages_left = db.execute("SELECT COUNT(*) FROM stages WHERE execution_id=?", (execution["id"],)).fetchone()[0]
        assert [kind for _, kind in rows].count("checked") == (
            (192 if historical else 0)
            + (0 if crash_phase in {"before_commit", "writer_first", "stale_epoch", "script_changed", "extra_stage", "disk_full", "lost_proposal_reply", "attachment_after_stage", "event_heavy"} else root_count))
        assert (outcome is None) if crash_phase in {"before_commit", "writer_first", "stale_epoch", "script_changed", "extra_stage", "disk_full", "lost_proposal_reply", "attachment_after_stage", "event_heavy"} else (
            outcome is not None and v2.strict_json(outcome[0])["state"] == "succeeded")
        assert stages_left == (root_count if crash_phase in {"before_commit", "writer_first", "stale_epoch", "script_changed", "extra_stage", "disk_full", "lost_proposal_reply", "attachment_after_stage", "event_heavy"} else 0)
        with sqlite3.connect(home / "cron" / "executions.db") as db:
            final = db.execute("SELECT state FROM cron_local_finalizations WHERE execution_id=?", (execution["id"],)).fetchone()
            after_binding = db.execute("SELECT * FROM cron_prerun_bindings WHERE execution_id=?",
                                       (execution["id"],)).fetchone()
        expected_final = ("succeeded",) if crash_phase is None else (
            ("unknown",) if crash_phase in {"writer_first", "stale_epoch", "script_changed",
                                           "binding_changed_postcommit", "extra_stage", "disk_full",
                                           "lost_proposal_reply", "attachment_after_stage", "event_heavy"} else ("pending",))
        assert final == expected_final
        assert (after_binding != captured_binding) if crash_phase == "binding_changed_postcommit" else (
            after_binding == captured_binding)
        assert execution["id"] in jobs.get_job(job["id"])["canonical_completions"]
        if historical:
            assert v2.Store(directory).history(roots[0])[0]["payload"] == old_pending[roots[0]]
            first_old_checked = next(iter(old_checked))
            assert v2.Store(directory).current(first_old_checked)["checked"] == old_checked[first_old_checked]
        if second_fire:
            assert crash_phase is None
            _assert_second_fire_no_replay(home, job, jobs, executions, payload, harness,
                                          directory, execution, root_count)
    finally:
        # Package objects and owned descendants are settled by native_tmp_path.
        pass


def _historical_rows(roots, enabled):
    if not enabled:
        return {}, {}
    pending = {root: {"fingerprint": hashlib.sha256(root.encode()).hexdigest(),
                      "event_ids": [], "observed_at": 42.5,
                      "observation_id": hashlib.sha256(("old-" + root).encode()).hexdigest(),
                      "attachment_bearing": False}
               for root in roots}
    checked = {f"t_{100000 + i:08x}": {
                   "local_fingerprint": f"old-{i}", "consumed_event_ids": [],
                   "local_observed_at": 12.5,
                   "local_observation_id": hashlib.sha256(f"old-id-{i}".encode()).hexdigest(),
                   "ledger_effect_receipt_id_or_null": None,
                   "local_cursor_sha": hashlib.sha256(f"cursor-{i}".encode()).hexdigest()}
               for i in range(192)}
    return pending, checked


def _assert_second_fire_no_replay(home, job, jobs, executions, payload, harness,
                                  directory, first_execution, root_count):
    with jobs.use_cron_store(home):
        again = jobs.claim_job_for_fire(job["id"], manual=True, return_job=True)
        second_execution = executions.create_execution(job["id"], source="direct")
        assert executions.mark_execution_handoff_pending(second_execution["id"])
    assert isinstance(again, dict), "the second native occurrence was not admitted"
    again["execution_id"] = second_execution["id"]
    payload.write_text(json.dumps({"job": again, "profile_home": str(home)}), encoding="utf-8")
    second = subprocess.run([sys.executable, "-c", harness],
                            cwd=str(Path(__file__).resolve().parents[2]),
                            env=dict(os.environ, HERMES_HOME=str(home)),
                            capture_output=True, text=True, timeout=120, check=False)
    assert second.returncode == 0, second.stderr
    terminal = executions.latest_execution(job["id"])
    assert terminal and terminal["id"] != first_execution["id"] and terminal["status"] == "completed"
    with sqlite3.connect(directory / "local-observation-v2.sqlite") as db:
        checked = db.execute("SELECT COUNT(*) FROM records WHERE kind='checked'").fetchone()[0]
    assert checked == root_count, "second fire replayed prior checked roots"
    with sqlite3.connect(home / "cron" / "executions.db") as db:
        disposition = db.execute("SELECT state FROM cron_local_finalizations WHERE execution_id=?",
                                 (terminal["id"],)).fetchone()
    assert disposition == ("not_applicable",)
