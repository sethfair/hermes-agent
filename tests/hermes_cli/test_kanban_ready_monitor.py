"""Behavioral contracts for aged-Ready monitoring."""

import sqlite3
import json
import uuid

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc

from hermes_cli import kanban_ready_monitor as monitor
from hermes_cli import kanban_ready_monitor_resolver as resolver
from hermes_cli.config_defaults_kanban import AGED_READY_DEFAULTS


def test_ready_monitor_generation_exposes_frozen_classifier_api():
    assert monitor.READY_ENTRY_CLASSIFIERS
    assert callable(monitor.ready_generation)
    assert callable(monitor.canonical_guard_fingerprint)
    assert callable(monitor.classify_ready_state)


def _db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(kb.SCHEMA_SQL)
    kbc._migrate_add_optional_columns(conn)
    return conn


def test_schema_migration_is_additive_and_identity_is_stable():
    conn = _db()
    tables = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    assert {
        "kanban_board_identity", "kanban_monitor_scope_activation",
        "kanban_dispatch_attempts", "kanban_claim_admissions",
        "kanban_writer_faults", "kanban_controller_observations",
        "kanban_ready_tick_observations",
    } <= tables
    admission_col = {r["name"] for r in conn.execute("PRAGMA table_info(tasks)")}
    assert "dispatch_admission_id" in admission_col
    first = conn.execute(
        "SELECT board_scope_id FROM kanban_board_identity WHERE singleton=1"
    ).fetchone()[0]
    monitor.ensure_schema(conn)
    assert conn.execute(
        "SELECT board_scope_id FROM kanban_board_identity WHERE singleton=1"
    ).fetchone()[0] == first


def test_unadmitted_ready_claim_is_ignored_and_fault_is_durable():
    conn = _db()
    task_id = "task-ready"
    conn.execute(
        "INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)",
        (task_id, "Ready", "ready", 100, "p1"),
    )
    conn.execute(
        "INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
        (task_id, "created", '{"status":"ready"}', 100),
    )
    monitor.activate_scope(conn, profile="p1", state="pending")
    conn.commit()
    with kb.write_txn(conn):
        run_id = kb._claim_and_open_run(conn, task_id, "ready", "old", 200, 150)
    assert run_id is None
    assert conn.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()[0] == "ready"
    assert conn.execute("SELECT COUNT(*) FROM task_runs").fetchone()[0] == 0
    assert conn.execute(
        "SELECT diagnostic_code FROM kanban_writer_faults"
    ).fetchone()[0] == "unadmitted_ready_claim_refused"


def test_matching_admission_is_single_use_and_replay_is_refused():
    conn = _db()
    task_id = "task-admitted"
    conn.execute(
        "INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)",
        (task_id, "Ready", "ready", 100, "p1"),
    )
    conn.execute(
        "INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
        (task_id, "created", '{"status":"ready"}', 100),
    )
    scope = monitor.activate_scope(conn, profile="p1", state="active", legacy_exclusion_proof_state="proven", roster_generation="r1", roster_digest="rd")
    ctx = monitor.MonitorWriterContext(
        board_scope_id=scope["board_scope_id"], profile="p1", writer_ref="w1",
        controller_generation="g1", activation_epoch=scope["activation_epoch"],
        roster_generation="r1", roster_digest="rd", installed_revision="rev",
    )
    attempt = monitor.begin_dispatch_attempt(conn, controller=ctx, surface="standalone", now=150)
    token = monitor.arm_claim_admission(
        conn, attempt_id=attempt["id"], task_id=task_id, ready_generation=1, run_generation=0,
    )
    conn.commit()
    with kb.write_txn(conn):
        run_id = kb._claim_and_open_run(
            conn, task_id, "ready", "new", 200, 150, admission_id=token
        )
    assert run_id is not None
    assert conn.execute(
        "SELECT state FROM kanban_claim_admissions WHERE token=?", (token,)
    ).fetchone()[0] == "consumed"


def test_invalid_admission_latches_a_durable_unknown_fault():
    conn = _db()
    task_id = "task-invalid-admission"
    conn.execute("INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)",
                 (task_id, "Ready", "ready", 100, "p1"))
    conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
                 (task_id, "created", '{"status":"ready"}', 100))
    scope = monitor.activate_scope(conn, profile="p1", state="active", legacy_exclusion_proof_state="proven")
    conn.commit()
    with kb.write_txn(conn):
        assert kb._claim_and_open_run(conn, task_id, "ready", "bad", 200, 150,
                                      admission_id="not-a-token") is None
    assert conn.execute("SELECT COUNT(*) FROM kanban_writer_faults WHERE diagnostic_code='invalid_claim_admission'").fetchone()[0] == 1
    health = monitor.ready_health_snapshot(conn)
    assert health["profiles"][0]["state"] == "UNKNOWN"
    assert scope["activation_epoch"]


def test_begin_attempt_commits_before_real_claim_transaction():
    conn = _db()
    task_id = "task-durable-attempt"
    conn.execute("INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)",
                 (task_id, "Ready", "ready", 100, "p1"))
    conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
                 (task_id, "created", '{"status":"ready"}', 100))
    scope = monitor.activate_scope(conn, profile="p1", state="active", legacy_exclusion_proof_state="proven")
    ctx = monitor.MonitorWriterContext(
        board_scope_id=scope["board_scope_id"], profile="p1", writer_ref="w1",
        controller_generation="g1", activation_epoch=scope["activation_epoch"],
        roster_generation=scope["roster_generation"], roster_digest=scope["roster_digest"],
        installed_revision="rev",
    )
    monitor.publish_controller_observation(conn, controller=ctx)
    attempt = monitor.begin_dispatch_attempt(conn, controller=ctx, surface="standalone", now=150)
    assert attempt and not conn.in_transaction
    token = monitor.arm_claim_admission(conn, attempt_id=attempt["id"], task_id=task_id,
                                        ready_generation=1, run_generation=0)
    conn.commit()
    with kb.write_txn(conn):
        assert kb._claim_and_open_run(conn, task_id, "ready", "new", 200, 150,
                                      admission_id=token) is not None


def test_tick_ledger_replay_does_not_increment_or_duplicate_episode():
    conn = _db()
    task_id = "task-replay"
    conn.execute(
        "INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)",
        (task_id, "Ready", "ready", 100, "p1"),
    )
    conn.execute(
        "INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
        (task_id, "created", '{"status":"ready"}', 100),
    )
    scope = monitor.activate_scope(conn, profile="p1", state="active", legacy_exclusion_proof_state="proven", roster_generation="r1", roster_digest="rd")
    ctx = monitor.MonitorWriterContext(
        board_scope_id=scope["board_scope_id"], profile="p1", writer_ref="w1",
        controller_generation="g1", activation_epoch=scope["activation_epoch"],
        roster_generation="r1", roster_digest="rd", installed_revision="rev", tick_id="tick-1",
    )
    attempt = monitor.begin_dispatch_attempt(conn, controller=ctx, surface="standalone", now=200)
    decision = {"task_id": task_id, "decision": "eligible_missed", "min_age_seconds": 0, "min_eligible_misses": 1}
    assert monitor.record_dispatch_tick(conn, controller=ctx, attempt_id=attempt["id"], decisions=[decision], now=200)
    # Without a complete roster/heartbeat/attempt proof the recorder must not
    # advance the durable miss counter, even when the dispatcher said eligible.
    assert conn.execute("SELECT eligible_miss_count FROM kanban_ready_state WHERE task_id=?", (task_id,)).fetchone()[0] == 0
    assert monitor.record_dispatch_tick(conn, controller=ctx, attempt_id=attempt["id"], decisions=[decision], now=201) is False
    assert conn.execute("SELECT eligible_miss_count FROM kanban_ready_state WHERE task_id=?", (task_id,)).fetchone()[0] == 0


def test_native_claim_closes_episode_and_review_claim_does_not():
    conn = _db()
    task_id = "task-close"
    conn.execute("INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)", (task_id, "Ready", "ready", 100, "p1"))
    conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)", (task_id, "created", '{"status":"ready"}', 100))
    monitor.observe_ready(conn, task_id, now=300, activation_epoch="epoch", min_age_seconds=0, min_eligible_misses=1)
    monitor.observe_ready(conn, task_id, now=301, activation_epoch="epoch", min_age_seconds=0, min_eligible_misses=1)
    from hermes_cli import kanban_db as db
    claimed = db.claim_task(conn, task_id, claimer="p1")
    assert claimed is not None
    closed = conn.execute("SELECT state,outcome FROM kanban_ready_episodes").fetchone()
    assert (closed["state"], closed["outcome"]) == ("closed", "native_claim")

    review_id = "review-close"
    conn.execute("INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)", (review_id, "Review", "ready", 100, "p1"))
    conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)", (review_id, "created", '{"status":"ready"}', 100))
    monitor.observe_ready(conn, review_id, now=300, activation_epoch="epoch", min_age_seconds=0, min_eligible_misses=1)
    monitor.observe_ready(conn, review_id, now=301, activation_epoch="epoch", min_age_seconds=0, min_eligible_misses=1)
    conn.execute("UPDATE tasks SET status='review' WHERE id=?", (review_id,))
    conn.commit()
    db.claim_review_task(conn, review_id, claimer="p1")
    assert conn.execute("SELECT state FROM kanban_ready_episodes WHERE task_id=?", (review_id,)).fetchone()[0] == "open"


def test_resolver_schedule_is_bounded_and_redacted():
    conn = _db()
    conn.execute("INSERT INTO kanban_ready_episodes(id,task_id,ready_generation,opened_at,first_observed_at,last_observed_at) VALUES(?,?,?,?,?,?)", ("ep-r", "task", 1, 10, 10, 10))
    missing = monitor.project_escalation_intents(conn, "ep-r", bindings={})
    assert missing["diagnostic"] == "resolver_missing"
    owner = str(uuid.uuid4())
    binding = {"owner_ref": owner, "manager_ref": str(uuid.uuid4()),
               "acknowledgement_authority_refs": [str(uuid.uuid4())]}
    complete = monitor.project_escalation_intents(conn, "ep-r", bindings=binding,
                                                  owner_retry_seconds=(10, 20), manager_retry_seconds=(30,), now=10)
    assert [row["state"] for row in complete["intents"]].count("due") == 1
    assert all(row["state"] == "planned" for row in complete["intents"][1:])
    assert "owner_id" not in repr(complete)
    assert {row[0] for row in conn.execute("SELECT kind FROM task_events")} == {"aged_ready_diagnostic", "aged_ready_resolver_changed", "aged_ready_intent_due"}
    resolver_event = json.loads(conn.execute("SELECT payload FROM task_events WHERE kind='aged_ready_resolver_changed'").fetchone()[0])
    due = json.loads(conn.execute("SELECT payload FROM task_events WHERE kind='aged_ready_intent_due'").fetchone()[0])
    assert set(resolver_event) == {"episode_id", "from_stage", "to_stage", "routing_generation",
                                   "retry_ordinal", "from_resolver_digest", "to_resolver_digest"}
    assert resolver_event["from_resolver_digest"] is None
    assert resolver_event["to_resolver_digest"] == monitor._digest(owner)
    assert set(due) == {"episode_id", "intent_id", "routing_generation", "stage", "ordinal"}
    monitor.project_escalation_intents(conn, "ep-r", bindings=binding,
                                       owner_retry_seconds=(10, 20), manager_retry_seconds=(30,), now=1000)
    assert conn.execute("SELECT COUNT(*) FROM kanban_ready_escalation_intents WHERE state='due'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM task_events WHERE kind='aged_ready_intent_due'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM kanban_ready_episodes").fetchone()[0] == 1


def test_resolver_identity_excludes_wip_and_owner_totals_use_real_tasks():
    conn = _db()
    owner = str(uuid.uuid4())
    binding = {"owner_ref": owner, "manager_ref": str(uuid.uuid4()),
               "acknowledgement_authority_refs": [str(uuid.uuid4())]}
    bindings = {"p1": binding, "p2": binding}
    before = resolver.binding_digest(conn, binding, profile="p1", owner_profiles=("p1", "p2"))
    conn.execute("INSERT INTO tasks(id,title,status,created_at,assignee) VALUES('a','A','running',1,'p1')")
    conn.execute("INSERT INTO tasks(id,title,status,created_at,assignee) VALUES('b','B','ready',1,'p2')")
    after = resolver.binding_digest(conn, binding, profile="p1", owner_profiles=("p1", "p2"))
    assert before == after
    assert resolver.owner_wip_totals(conn, bindings) == [{"owner": monitor._digest(owner), "total": 1}]


def test_truthful_hold_closes_once_but_ready_and_intent_do_not():
    conn = _db()
    task_id = "task-hold"
    conn.execute("INSERT INTO tasks(id,title,status,created_at) VALUES(?,?,?,?)", (task_id, "Hold", "ready", 100))
    conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)", (task_id, "created", '{"status":"ready"}', 100))
    monitor.observe_ready(conn, task_id, now=300, activation_epoch="epoch", min_age_seconds=0, min_eligible_misses=1)
    assert not monitor.reconcile_truthful_hold(conn, task_id, status="ready", now=301)
    assert monitor.project_escalation_intents(conn, "missing", bindings={})["state"] == "closed"
    conn.execute("UPDATE tasks SET status='blocked' WHERE id=?", (task_id,))
    assert monitor.reconcile_truthful_hold(conn, task_id, now=302)
    assert not monitor.reconcile_truthful_hold(conn, task_id, now=303)


def test_malformed_new_ready_event_does_not_fall_back_to_old_generation():
    conn = _db()
    conn.execute("INSERT INTO tasks(id,title,status,created_at) VALUES('t','T','ready',10)")
    conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES('t','created','{\"status\":\"ready\"}',10)")
    conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES('t','promoted','not-json',20)")
    assert monitor.ready_generation(conn, "t") is None


def test_writer_context_requires_unique_configured_member_and_exact_partition():
    conn = _db()
    board = conn.execute("SELECT board_scope_id FROM kanban_board_identity").fetchone()[0]
    roster_generation = str(uuid.uuid4())
    controller = str(uuid.uuid4())
    config = {"dispatch_profiles": ["p1"], "aged_ready": {
        "observation_enabled": True,
        "controller_id": controller,
        "expected_writer_scopes": {board: {"roster_generation": roster_generation, "members": [{
            "writer_ref": controller, "profiles": ["p1"],
            "protocol_version": 1, "compatible_revisions": [monitor.COMPATIBILITY_IDENTITY],
        }]}}
    }}
    roster_digest = monitor.parse_aged_ready_config(config)["expected_writer_scopes"][board]["roster_digest"]
    scope = monitor.activate_scope(conn, profile="p1", roster_generation=roster_generation, roster_digest=roster_digest)
    ctx = monitor.MonitorWriterContext(
        board_scope_id=board, profile="p1", writer_ref=controller,
        controller_generation="boot", activation_epoch=scope["activation_epoch"],
        roster_generation=roster_generation, roster_digest=roster_digest, installed_revision="rev",
    )
    assert monitor.validate_writer_context(conn, ctx, config) is None
    bad = monitor.MonitorWriterContext(**{**ctx.__dict__, "roster_digest": "other"})
    assert monitor.validate_writer_context(conn, bad, config) == "incompatible_writer_context"
    bad_config = {**config, "dispatch_profiles": ["p1", "p2"]}
    assert monitor.validate_writer_context(conn, ctx, bad_config) == "writer_scope_incomplete"


def test_aged_ready_parser_is_strict_about_types_bounds_and_roster_shape():
    assert monitor.parse_aged_ready_config({})["observation_enabled"] is False
    assert monitor.parse_aged_ready_config(AGED_READY_DEFAULTS)["observation_enabled"] is False
    board = str(uuid.uuid4()); generation = str(uuid.uuid4()); controller = str(uuid.uuid4())
    valid = {"dispatch_profiles": ["p1"], "aged_ready": {"observation_enabled": True, "controller_id": controller,
                            "min_age_seconds": 1, "min_eligible_misses": 1,
                            "controller_stale_seconds": 1, "owner_retry_seconds": [1],
                            "manager_retry_seconds": [1], "expected_writer_scopes": {board: {
                                "roster_generation": generation, "members": [{"writer_ref": controller,
                                    "profiles": ["p1"], "protocol_version": 1,
                                    "compatible_revisions": [monitor.COMPATIBILITY_IDENTITY]}]}},
                            "resolver_bindings": {"p1": {"owner_ref": str(uuid.uuid4()),
                                "manager_ref": str(uuid.uuid4()),
                                "acknowledgement_authority_refs": [str(uuid.uuid4())]}}}}
    parsed = monitor.parse_aged_ready_config(valid)
    assert parsed["min_age_seconds"] == 1
    assert parsed["expected_writer_scopes"][board]["roster_digest"] == monitor.canonical_roster_digest(
        board, generation, parsed["expected_writer_scopes"][board]["members"])
    for bad in ({"aged_ready": {"surprise": 1}},
                {"aged_ready": {"min_age_seconds": True}},
                {"aged_ready": {"owner_retry_seconds": [2, 1]}},
                {"aged_ready": {"owner_retry_seconds": [0]}},
                {"aged_ready": {"observation_enabled": True, "controller_id": True}},
                {"aged_ready": {"observation_enabled": True, "controller_id": controller}},
                {"dispatch_profiles": ["p1", "p1"], "aged_ready": {}},
                {"aged_ready": {"dispatch_profiles": ["p1"]}},
                {"aged_ready": {"controller_id": "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"}},
                {"aged_ready": {"controller_id": str(uuid.uuid4()), "expected_writer_scopes": {
                    str(uuid.uuid4()): {"roster_generation": str(uuid.uuid4()), "members": [], "extra": 1}}}},
                {"aged_ready": {"resolver_bindings": {"p": {"owner_ref": "o"}}}},
                {"aged_ready": {"expected_writer_scopes": []}}):
        try:
            monitor.parse_aged_ready_config(bad)
        except ValueError:
            pass
        else:
            raise AssertionError("malformed aged_ready config was accepted")


def test_public_health_marks_numeric_scope_facts_unknown_when_roster_is_incomplete():
    conn = _db()
    task_id = "public-unknown"
    conn.execute("INSERT INTO tasks(id,title,status,created_at) VALUES(?,?,?,?)", (task_id, "T", "ready", 1))
    conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)",
                 (task_id, "created", '{"status":"ready"}', 1))
    monitor.observe_ready(conn, task_id, now=2, activation_epoch="disabled")
    monitor.activate_scope(conn, profile="p1", state="active")
    health = monitor.ready_health_snapshot(conn, config={"aged_ready": {}})
    profile = health["profiles"][0]
    assert profile["state"] == "UNKNOWN"
    assert profile["running_wip"] == profile["ready_count"] == profile["eligible_count"] == "UNKNOWN"
    assert profile["cap"] == "UNKNOWN"
    assert health["owners"] and health["owners"][0]["total"] == "UNKNOWN"
    card = health["cards"][0]
    assert all(card[key] == "UNKNOWN" for key in ("ready_age_seconds", "eligible_miss_count", "eligible_count", "freshness"))


def test_complete_roster_provisionally_admits_current_tick_and_advances_episode():
    conn = _db(); task_id = "complete-roster"
    conn.execute("INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)", (task_id, "T", "ready", 1, "p1"))
    conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)", (task_id, "created", '{"status":"ready"}', 1))
    writer = str(uuid.uuid4()); generation = str(uuid.uuid4())
    board = conn.execute("SELECT board_scope_id FROM kanban_board_identity").fetchone()[0]
    owner = str(uuid.uuid4())
    config = {"dispatch_profiles": ["p1"], "aged_ready": {"observation_enabled": True,
        "controller_id": writer, "expected_writer_scopes": {board: {"roster_generation": generation,
        "members": [{"writer_ref": writer, "profiles": ["p1"], "protocol_version": 1,
        "compatible_revisions": [monitor.COMPATIBILITY_IDENTITY]}]}}, "resolver_bindings": {"p1": {
        "owner_ref": owner, "manager_ref": str(uuid.uuid4()),
        "acknowledgement_authority_refs": [str(uuid.uuid4())]}}}}
    digest = monitor.parse_aged_ready_config(config)["expected_writer_scopes"][board]["roster_digest"]
    scope = monitor.activate_scope(conn, profile="p1", state="active", legacy_exclusion_proof_state="proven", roster_generation=generation, roster_digest=digest)
    ctx = monitor.MonitorWriterContext(board, "p1", writer, "boot", scope["activation_epoch"], generation, digest, "rev", 1, monitor.COMPATIBILITY_IDENTITY, "", ("p1",), 1, 1)
    tick = ctx.for_tick(); monitor.publish_controller_observation(conn, controller=tick)
    attempt = monitor.begin_dispatch_attempt(conn, controller=tick, surface="test", config=config, now=10)
    assert monitor.record_dispatch_tick(conn, controller=tick, attempt_id=attempt["id"], decisions=[{"task_id": task_id, "decision": "eligible_missed"}], config=config, now=10)
    state = conn.execute("SELECT eligible_miss_count,episode_id FROM kanban_ready_state WHERE task_id=?", (task_id,)).fetchone()
    assert state["eligible_miss_count"] == 1 and state["episode_id"]
    assert monitor.ready_health_snapshot(conn, now=10, config=config)["owners"] == [{
        "owner": monitor._digest(owner), "total": 0, "eligible_count": "UNKNOWN", "suppressed_count": "UNKNOWN"}]
    monitor.record_writer_faults(conn, "incompatible_writer_context")
    assert monitor.ready_health_snapshot(conn, now=10, config=config)["profiles"][0]["state"] == "UNKNOWN"


def _host_wip_fixture(tmp_path, monkeypatch, *, missing=False):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    boards = []
    for slug in ("one", "two"):
        kb.create_board(slug)
        conn = kbc.connect(board=slug)
        board = conn.execute("SELECT board_scope_id FROM kanban_board_identity").fetchone()[0]
        boards.append((conn, board))
    writer = str(uuid.uuid4()); generation = str(uuid.uuid4()); owner = str(uuid.uuid4())
    scopes = {board: {"roster_generation": generation, "members": [{
        "writer_ref": writer, "profiles": ["p1"], "protocol_version": 1,
        "compatible_revisions": [monitor.COMPATIBILITY_IDENTITY]}]} for _conn, board in boards}
    if missing:
        scopes[str(uuid.uuid4())] = scopes[boards[0][1]]
    config = {"dispatch_profiles": ["p1"], "aged_ready": {"observation_enabled": True,
        "controller_id": writer, "expected_writer_scopes": scopes, "resolver_bindings": {"p1": {
        "owner_ref": owner, "manager_ref": str(uuid.uuid4()),
        "acknowledgement_authority_refs": [str(uuid.uuid4())]}}}}
    parsed = monitor.parse_aged_ready_config(config)
    for index, (conn, board) in enumerate(boards):
        digest = parsed["expected_writer_scopes"][board]["roster_digest"]
        scope = monitor.activate_scope(conn, profile="p1", state="active",
            legacy_exclusion_proof_state="proven", roster_generation=generation, roster_digest=digest)
        ctx = monitor.MonitorWriterContext(board, "p1", writer, "boot", scope["activation_epoch"],
            generation, digest, "rev", dispatch_profiles=("p1",)).for_tick()
        monitor.publish_controller_observation(conn, controller=ctx)
        attempt = monitor.begin_dispatch_attempt(conn, controller=ctx, surface="test", config=config, now=10)
        task = f"running-{index}"
        conn.execute("INSERT INTO tasks(id,title,status,created_at,assignee) VALUES(?,?,?,?,?)", (task, task, "ready", 1, "p1"))
        conn.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,?,?,?)", (task, "created", '{"status":"ready"}', 1))
        token = monitor.arm_claim_admission(conn, attempt_id=attempt["id"], task_id=task, ready_generation=1, run_generation=0)
        conn.commit()
        with kb.write_txn(conn):
            assert kb._claim_and_open_run(conn, task, "ready", "test", 10, 10, admission_id=token)
        assert monitor.record_dispatch_tick(conn, controller=ctx, attempt_id=attempt["id"], decisions=[], config=config, now=10)
    return boards, config, monitor._digest(owner)


def test_health_aggregates_owner_wip_across_configured_boards(tmp_path, monkeypatch):
    boards, config, owner = _host_wip_fixture(tmp_path, monkeypatch)
    health = monitor.ready_health_snapshot(boards[0][0], now=10, config=config)
    assert health["profiles"][0]["running_wip"] == 2
    assert health["owners"] == [{"owner": owner, "total": 2,
        "eligible_count": "UNKNOWN", "suppressed_count": "UNKNOWN"}]


def test_missing_contributing_board_makes_profile_and_owner_wip_unknown(tmp_path, monkeypatch):
    boards, config, owner = _host_wip_fixture(tmp_path, monkeypatch, missing=True)
    health = monitor.ready_health_snapshot(boards[0][0], now=10, config=config)
    assert health["profiles"][0]["running_wip"] == "UNKNOWN"
    assert health["owners"] == [{"owner": owner, "total": "UNKNOWN",
        "eligible_count": "UNKNOWN", "suppressed_count": "UNKNOWN"}]


def test_unreadable_contributing_board_makes_profile_and_owner_wip_unknown(tmp_path, monkeypatch):
    boards, config, owner = _host_wip_fixture(tmp_path, monkeypatch)
    boards[1][0].execute("DROP TABLE kanban_monitor_scope_activation")
    boards[1][0].commit()
    boards[1][0].close()
    health = monitor.ready_health_snapshot(boards[0][0], now=10, config=config)
    census = resolver.host_wip_census(boards[0][0], monitor.parse_aged_ready_config(config), now=10)
    assert any(row[4]["p1"] == ("unreadable_board",) for row in census)
    assert health["profiles"][0]["running_wip"] == "UNKNOWN"
    assert health["owners"][0] == {"owner": owner, "total": "UNKNOWN",
        "eligible_count": "UNKNOWN", "suppressed_count": "UNKNOWN"}
