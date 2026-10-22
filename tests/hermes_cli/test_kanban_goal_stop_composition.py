"""Goal worker follow-up disposition through the actual one-shot callers."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path
from functools import wraps
from itertools import pairwise
import sqlite3

import pytest

import cli
from run_agent import AIAgent
from agent.auxiliary_unavailable import AuxiliaryClientUnavailable
from hermes_cli import goals, kanban_db as kb
from hermes_cli.kanban_db import KANBAN_TERMINAL_PROVIDER_EXIT_CODE, KANBAN_RATE_LIMIT_EXIT_CODE
from hermes_cli.kanban_goal_result import GoalLoopProgress, GoalTurnResult


@pytest.mark.parametrize("quiet", [False, True], ids=["nonquiet", "quiet"])
@pytest.mark.parametrize("fail_on_turn", [2, 3])
@pytest.mark.parametrize("failure,expected_code", [
    ({"completed": False, "failed": True, "failure_reason": "auth"}, 78),
    ({"failed": True}, 1),
    ({"partial": True}, 1),
    ({"completed": False}, 1),
    ({"failed": True, "partial": True, "completed": False, "failure_reason": "unknown"}, 1),
    *[({"failed": True, "failure_reason": reason}, 75) for reason in
      ("rate_limit", "upstream_rate_limit", "billing", "overloaded", "server_error", "timeout")],
    *[({"failed": True, "failure_reason": reason}, 78) for reason in
      ("auth", "auth_permanent", "model_not_found", "ssl_cert_verification", "upstream_blocked")],
    ({"interrupted": True, "failed": True, "failure_reason": "auth"}, 130),
    (None, 1),
    ("non-dict-result", 1),
    ("no_fresh_settlement", 1),
    ({}, 0),
    (ValueError("worker error"), 1),
    (TimeoutError("worker timeout is not infrastructure"), 1),
    (KeyboardInterrupt(), 130),
])
def test_followup_failed_provider_turn_reaches_process_exit(monkeypatch, quiet, fail_on_turn, failure, expected_code, capsys):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_followup")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "1")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setattr("tools.kanban_tools.register_current_worker_from_env", lambda: True)
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    finalizations, summaries = [], []
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: finalizations.append(c.session_id))
    hooks = []
    monkeypatch.setattr(cli, "_emit_interrupted_session_end", lambda *a, **k: hooks.append(k))

    monkeypatch.setattr(cli, "_route_single_query_images", lambda c, q, e, i, u: e)
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lambda c, tid: SimpleNamespace(title="task", body="acceptance", goal_max_turns=5))
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    monkeypatch.setattr(kb, "goal_run_status", lambda c, tid, run: "review" if failure == {} and len(calls) >= fail_on_turn else "running")
    actions = []
    monkeypatch.setattr(kb, "block_task", lambda *a, **k: actions.append("block"))

    def judge(*args, **kwargs):
        actions.append("judge")
        return "continue", "more work", False, None, False

    monkeypatch.setattr(goals, "judge_goal", judge)
    def success(index):
        return {"final_response": f"receipt {index}", "completed": True,
                "messages": [], "input_tokens": index * 11}
    failed = {"final_response": "provider error", "messages": [], "input_tokens": 37, **failure} if isinstance(failure, dict) else failure
    calls = []
    def run_conversation(**kw):
        calls.append(kw)
        result = failed if len(calls) == fail_on_turn else success(len(calls))
        if result == "no_fresh_settlement":
            return None
        if isinstance(result, BaseException):
            raise result
        return result
    agent = SimpleNamespace(session_id="s1", run_conversation=run_conversation)

    def chat(*args, **kwargs):
        calls.append((args, kwargs))
        value = failed if len(calls) == fail_on_turn else success(len(calls))
        if value == "no_fresh_settlement":
            return "apparent successful display despite stale previous turn"
        if isinstance(value, BaseException):
            raise value
        stub._last_turn_result = value
        return stub._last_turn_result.get("final_response", "") if isinstance(stub._last_turn_result, dict) else "unsettled display"

    stub = SimpleNamespace(
        session_id="s1", conversation_history=[], agent=agent, _single_query_mode=False,
        _claim_active_session=lambda *a, **k: True,
        _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
        console=SimpleNamespace(print=lambda *a, **k: None),
        _show_security_advisories=lambda: None, chat=chat,
        _print_exit_summary=lambda **k: summaries.append(k), _last_turn_result=None,
    )
    actual_adapter = cli._run_kanban_goal_loop_q if quiet else cli._run_kanban_goal_loop_chat
    observed = []
    def observe_adapter(*args, **kwargs):
        outcome = actual_adapter(*args, **kwargs)
        observed.append((outcome, kwargs["progress"]))
        return outcome
    monkeypatch.setattr(cli, "_run_kanban_goal_loop_q" if quiet else "_run_kanban_goal_loop_chat", observe_adapter)
    with pytest.raises(SystemExit) as exc:
        cli._run_single_query_mode(stub, "task", None, quiet, True)
    assert len(calls) == fail_on_turn
    assert actions == ["judge"] * (fail_on_turn - 1)
    assert exc.value.code == expected_code
    output = capsys.readouterr()
    assert output.err.count(f"[kanban-worker-exit] rc={expected_code}") == 1
    assert finalizations == ["s1"]
    assert len(summaries) == (0 if quiet else 1)
    assert len(observed) == 1
    outcome, progress = observed[0]
    assert outcome.turns_used == progress.turns_used == fail_on_turn
    if isinstance(failed, dict):
        assert outcome.last_turn.result == progress.last_turn.result == failed
        assert outcome.process_result["input_tokens"] == 37
        assert outcome.process_result["final_response"] == "provider error"
    elif isinstance(failed, BaseException):
        assert outcome.last_turn.result == success(fail_on_turn - 1)
        assert outcome.process_result["final_response"] == f"receipt {fail_on_turn - 1}"
        assert outcome.process_result["input_tokens"] == (fail_on_turn - 1) * 11
    else:
        assert outcome.last_turn.result is None
        assert outcome.process_result.get("final_response") is None
    if not isinstance(failed, dict):
        assert outcome.process_result["failed"] is True
    assert hooks == ([{"reason": "keyboard_interrupt"}] if isinstance(failure, KeyboardInterrupt) else [])
    if quiet:
        assert output.err.count("session_id: s1") == 1
        if isinstance(failed, dict) and failed:
            assert output.out.count("provider error") == 1
    else:
        assert "session_id: s1" not in output.err


@pytest.mark.parametrize("quiet", [False, True])
@pytest.mark.parametrize("fail_on_turn", [2, 3])
@pytest.mark.parametrize("phase,error,code", [
    ("status", TimeoutError("status unavailable"), 75),
    ("status", ValueError("invalid status"), 1),
    ("status", KeyboardInterrupt(), 130),
    ("status", sqlite3.OperationalError("busy"), 75),
    ("judge", ConnectionError("judge unavailable"), 75),
    ("judge", ValueError("bad judge"), 1),
    ("judge", KeyboardInterrupt(), 130),
    ("judge", sqlite3.OperationalError("locked"), 75),
])
def test_followup_infrastructure_after_latest_success_reaches_actual_caller(
        monkeypatch, capsys, quiet, fail_on_turn, phase, error, code):
    """A successful later worker turn is evidence, never authority to lose a later error."""
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "1")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setattr("tools.kanban_tools.register_current_worker_from_env", lambda: True)
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    monkeypatch.setattr(cli, "_route_single_query_images", lambda c, q, e, i, u: e)
    finalized, hooks, judged = [], [], []
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: finalized.append(c.session_id))
    monkeypatch.setattr(cli, "_emit_interrupted_session_end", lambda c, **kw: hooks.append((c.session_id, kw)))
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lambda *a: SimpleNamespace(
        title="task", body="acceptance", goal_max_turns=5))
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    if isinstance(error, sqlite3.OperationalError):
        error.sqlite_errorcode = (sqlite3.SQLITE_BUSY if phase == "status" else
                                  sqlite3.SQLITE_LOCKED | (2 << 8))
    statuses = []
    def status(*args):
        if len(calls) >= fail_on_turn:
            agent.session_id = "rotated-session"
        statuses.append(args)
        if phase == "status" and len(statuses) == fail_on_turn:
            raise error
        return "running"
    monkeypatch.setattr(kb, "goal_run_status", status)
    monkeypatch.setattr(kb, "block_task", lambda *a, **k: pytest.fail("no block after infrastructure error"))
    def judge(*args):
        judged.append(args)
        if phase == "judge" and len(judged) == fail_on_turn:
            raise error
        return "continue", "next fixture slice", False, None, False
    monkeypatch.setattr(goals, "judge_goal", judge)
    calls = []
    def raw():
        calls.append(len(calls) + 1)
        return {"final_response": f"receipt {len(calls)}", "completed": True,
                "input_tokens": len(calls) * 11, "messages": []}
    agent = SimpleNamespace(session_id="initial-session", run_conversation=lambda **kw: raw())
    stub = SimpleNamespace(
        session_id="initial-session", agent=agent, conversation_history=[], _last_turn_result=None,
        _claim_active_session=lambda *a, **k: True, _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
        console=SimpleNamespace(print=lambda *a, **k: None), _show_security_advisories=lambda: None,
        _print_exit_summary=lambda **k: summaries.append(k),
    )
    summaries = []
    def chat(*args, **kwargs):
        stub._last_turn_result = raw()
        return stub._last_turn_result["final_response"]
    stub.chat = chat
    original_adapter = cli._run_kanban_goal_loop_q if quiet else cli._run_kanban_goal_loop_chat
    outcomes = []
    def observe_adapter(*args, **kwargs):
        outcome = original_adapter(*args, **kwargs)
        outcomes.append(outcome)
        return outcome
    monkeypatch.setattr(cli, "_run_kanban_goal_loop_q" if quiet else "_run_kanban_goal_loop_chat", observe_adapter)
    with pytest.raises(SystemExit) as exc:
        cli._run_single_query_mode(stub, "task", None, quiet, True)
    assert exc.value.code == code and len(calls) == fail_on_turn
    assert len(statuses) == fail_on_turn
    assert len(judged) == (fail_on_turn if phase == "judge" else fail_on_turn - 1)
    assert finalized == ["rotated-session"]
    assert len(outcomes) == 1 and outcomes[0].turns_used == fail_on_turn
    assert outcomes[0].last_turn.result["input_tokens"] == fail_on_turn * 11
    assert outcomes[0].process_result["input_tokens"] == fail_on_turn * 11
    assert outcomes[0].process_result["final_response"] == f"receipt {fail_on_turn}"
    assert outcomes[0].process_result["failed"] is True
    assert outcomes[0].process_result["error"]
    assert len(summaries) == (0 if quiet else 1)
    assert hooks == ([("rotated-session", {"reason": "keyboard_interrupt"})]
                     if isinstance(error, KeyboardInterrupt) else [])
    output = capsys.readouterr()
    assert output.err.count(f"[kanban-worker-exit] rc={code}") == 1
    if quiet:
        assert output.err.count("session_id: rotated-session") == 1
        assert f"receipt {fail_on_turn}" in output.out
    else:
        assert "session_id: rotated-session" not in output.err


@pytest.mark.parametrize("quiet", [False, True])
@pytest.mark.parametrize("error", [AuxiliaryClientUnavailable("reauthentication required"),
                                    TypeError("bad auxiliary config"), Exception("auth-like prose"),
                                    TimeoutError("aux timeout")])
def test_real_judge_flag_reaches_both_actual_caller_exits(monkeypatch, quiet, error, capsys):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "1")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setattr("tools.kanban_tools.register_current_worker_from_env", lambda: True)
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    monkeypatch.setattr(cli, "_route_single_query_images", lambda c, q, e, i, u: e)
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: None)
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lambda c, tid: SimpleNamespace(title="task", body="acceptance", goal_max_turns=3))
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    monkeypatch.setattr(kb, "goal_run_status", lambda c, tid, run: "running")
    monkeypatch.setattr(kb, "block_task", lambda *a, **k: pytest.fail("no block after judge failure"))
    raw = {"final_response": "first nonempty receipt", "completed": True, "input_tokens": 19}
    calls = []
    def run_conversation(**kwargs):
        calls.append(kwargs)
        return raw
    agent = SimpleNamespace(session_id="s1", run_conversation=run_conversation)
    stub = SimpleNamespace(
        session_id="s1", agent=agent, conversation_history=[], _last_turn_result=None,
        _claim_active_session=lambda *a, **k: True, _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
        console=SimpleNamespace(print=lambda *a, **k: None), _show_security_advisories=lambda: None,
        _print_exit_summary=lambda **k: None,
    )
    def chat(*args, **kwargs):
        calls.append((args, kwargs))
        stub._last_turn_result = raw
        return raw["final_response"]
    stub.chat = chat
    actual = goals.judge_goal
    seen = []
    def observe(*args, **kwargs):
        verdict = actual(*args, **kwargs)
        seen.append(verdict)
        return verdict
    monkeypatch.setattr(goals, "judge_goal", observe)
    with patch("agent.auxiliary_client.call_llm", side_effect=error):
        with pytest.raises(SystemExit) as exc:
            cli._run_single_query_mode(stub, "task", None, quiet, True)
    assert exc.value.code == 1
    assert len(calls) == 1
    assert len(seen) == 1 and seen[0][0] == "continue" and seen[0][-1] is True
    captured = capsys.readouterr()
    assert captured.err.count("[kanban-worker-exit] rc=1") == 1
    if quiet:
        assert "judge transport failure" in captured.err and captured.err.count("session_id: s1") == 1
        assert "first nonempty receipt" in captured.out


@pytest.mark.parametrize("turn", [2, 3])
@pytest.mark.parametrize("kind,code", [("returned", 78), ("interrupt", 130), ("raised", 1)])
def test_quiet_emitter_followup_failure_is_one_terminal_json_result(monkeypatch, capsys, turn, kind, code):
    import json
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "1")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setattr("tools.kanban_tools.register_current_worker_from_env", lambda: True)
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    monkeypatch.setattr(cli, "_route_single_query_images", lambda c, q, e, i, u: e)
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: None)
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lambda c, tid: SimpleNamespace(title="task", body="acceptance", goal_max_turns=3))
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    monkeypatch.setattr(kb, "goal_run_status", lambda c, tid, run: "running")
    judges = []
    monkeypatch.setattr(goals, "judge_goal", lambda goal, response: judges.append(response) or ("continue", "next", False, None, False))
    first = {"final_response": "first", "completed": True, "input_tokens": 1}
    failed = {"final_response": "", "failed": True, "completed": False,
              "failure_reason": "auth", "error": "credential revoked", "input_tokens": 37}
    calls = []
    def run_conversation(**kwargs):
        calls.append(kwargs)
        if len(calls) == turn:
            if kind == "interrupt":
                raise KeyboardInterrupt()
            if kind == "raised":
                raise RuntimeError("worker raised")
            return failed
        return first
    agent = SimpleNamespace(session_id="stream-session", run_conversation=run_conversation)
    stub = SimpleNamespace(
        session_id="stream-session", model="test-model", agent=agent, conversation_history=[],
        _claim_active_session=lambda *a, **k: True, _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
    )
    with pytest.raises(SystemExit) as exc:
        cli._run_single_query_mode(stub, "task", None, True, True, stream_json=True)
    assert exc.value.code == code
    assert len(calls) == turn and judges == ["first"] * (turn - 1)
    output = capsys.readouterr()
    events = [json.loads(line) for line in output.out.splitlines()]
    assert [e["type"] for e in events] == ["system", "result"]
    assert events[-1]["exit_code"] == code
    assert events[-1]["tokens"]["input"] == (37 if kind == "returned" else 1)
    assert events[-1]["text"] == ("" if kind == "returned" else "first")
    assert events[-1]["error"] == ("credential revoked" if kind == "returned" else
                                   "worker: KeyboardInterrupt" if kind == "interrupt" else
                                   "worker: RuntimeError: worker raised")
    assert output.err.count("session_id: stream-session") == 1
    assert output.err.count(f"[kanban-worker-exit] rc={code}") == 1


@pytest.mark.parametrize("quiet", [False, True])
@pytest.mark.parametrize("turn", [2, 3])
@pytest.mark.parametrize("raised,code", [(ValueError("adapter bug"), 1),
                                           (KeyboardInterrupt(), 130),
                                           (SystemExit(42), 42)])
def test_goal_caller_preserves_published_turn_on_adapter_escape(monkeypatch, capsys, quiet, turn, raised, code):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setattr("tools.kanban_tools.register_current_worker_from_env", lambda: True)
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    monkeypatch.setattr(cli, "_route_single_query_images", lambda c, q, e, i, u: e)
    finalized = []
    hooks = []
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: finalized.append(c.session_id))
    monkeypatch.setattr(cli, "_emit_interrupted_session_end", lambda c, **k: hooks.append(c.session_id))
    first = {"final_response": "first", "completed": True, "input_tokens": 2}
    latest = {"final_response": f"receipt {turn}", "completed": True, "input_tokens": 21 * turn}
    agent = SimpleNamespace(session_id="new-session", run_conversation=lambda **k: first)
    stub = SimpleNamespace(
        session_id="old-session", agent=agent, conversation_history=[], _single_query_mode=False,
        _claim_active_session=lambda *a, **k: True, _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
        console=SimpleNamespace(print=lambda *a, **k: None), _show_security_advisories=lambda: None,
        _print_exit_summary=lambda **k: summaries.append(k), _last_turn_result=first,
        model="test-model",
    )
    summaries = []
    def chat(*args, **kwargs):
        stub._last_turn_result = first
        return "first"
    stub.chat = chat
    def adapter(_cli, _first, *args, progress, **kwargs):
        progress.turns_used = turn
        progress.last_turn = GoalTurnResult(latest["final_response"], latest, 0)
        raise raised
    monkeypatch.setattr(cli, "_run_kanban_goal_loop_q" if quiet else "_run_kanban_goal_loop_chat", adapter)
    with pytest.raises(SystemExit) as exc:
        cli._run_single_query_mode(stub, "task", None, quiet, True, stream_json=quiet)
    assert exc.value.code == code
    assert finalized == (["old-session"] if code == 42 and not quiet else ["new-session"])
    assert hooks == (["new-session"] if isinstance(raised, KeyboardInterrupt) else [])
    captured = capsys.readouterr()
    output = captured.err
    if code != 42:
        assert f"[kanban-worker-exit] rc={code}" in output
        if quiet:
            assert output.count("session_id: new-session") == 1
            import json
            events = [json.loads(line) for line in captured.out.splitlines()]
            assert [event["type"] for event in events] == ["system", "result"]
            assert events[-1]["exit_code"] == code
            assert events[-1]["text"] == latest["final_response"]
            assert events[-1]["tokens"]["input"] == latest["input_tokens"]
            assert events[-1]["error"]
        else:
            assert len(summaries) == 1
    else:
        assert "[kanban-worker-exit]" not in output
        assert not summaries


@pytest.mark.parametrize("quiet,quiet_effect", [
    (False, "sync"), (True, "sync"), (True, "history"), (True, "display"),
])
@pytest.mark.parametrize("turn", [2, 3])
@pytest.mark.parametrize("error", [RuntimeError("post-settle side effect"), KeyboardInterrupt()])
def test_actual_followup_wrapper_preserves_settled_evidence_after_side_effect_raise(
        monkeypatch, capsys, quiet, turn, error, quiet_effect):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "1")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setattr("tools.kanban_tools.register_current_worker_from_env", lambda: True)
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    monkeypatch.setattr(cli, "_route_single_query_images", lambda c, q, e, i, u: e)
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lambda *a: SimpleNamespace(
        title="task", body="acceptance", goal_max_turns=5))
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    observed, finalizations, hooks, summaries = [], [], [], []
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: finalizations.append(c.session_id))
    monkeypatch.setattr(cli, "_emit_interrupted_session_end", lambda c, **k: hooks.append(c.session_id))
    statuses, judges = [], []
    monkeypatch.setattr(kb, "goal_run_status", lambda *a: statuses.append(a) or "running")
    monkeypatch.setattr(kb, "block_task", lambda *a, **k: pytest.fail("side-effect failure cannot block"))
    monkeypatch.setattr(goals, "judge_goal", lambda *a: judges.append(a) or ("continue", "more", False, None, False))
    calls = []
    def raw():
        calls.append(len(calls) + 1)
        if quiet and len(calls) == turn:
            agent.session_id = "s2"
        return {"final_response": f"receipt {len(calls)}", "completed": True,
                "input_tokens": len(calls) * 17, "messages": []}
    agent = SimpleNamespace(session_id="s1", run_conversation=lambda **k: raw())
    class HistoryFixture(SimpleNamespace):
        def __setattr__(self, name, value):
            if quiet and quiet_effect == "history" and name == "conversation_history" and len(calls) == turn:
                raise error
            return super().__setattr__(name, value)
    stub = HistoryFixture(
        session_id="s1", agent=agent, conversation_history=[], _last_turn_result=None,
        _claim_active_session=lambda *a, **k: True, _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
        console=SimpleNamespace(print=lambda *a, **k: None), _show_security_advisories=lambda: None,
        _print_exit_summary=lambda **k: summaries.append(k),
    )
    def chat(*args, **kwargs):
        stub._last_turn_result = raw()
        if len(calls) == turn:
            agent.session_id = "s2"
            raise error  # actual rendered wrapper observes freshly settled _last_turn_result
        return stub._last_turn_result["final_response"]
    stub.chat = chat
    if quiet:
        if quiet_effect == "display":
            import builtins
            actual_print = builtins.print
            def print_with_failure(*args, **kwargs):
                if args == (f"receipt {turn}",):
                    raise error
                return actual_print(*args, **kwargs)
            monkeypatch.setattr(builtins, "print", print_with_failure)
        actual_sync = cli._sync_cli_session_id_from_agent
        raised_sync = []
        def sync(owner):
            if quiet_effect == "sync" and len(calls) == turn and not raised_sync:
                raised_sync.append(True)
                agent.session_id = "s2"
                raise error  # actual quiet wrapper already published returned raw turn
            return actual_sync(owner)
        monkeypatch.setattr(cli, "_sync_cli_session_id_from_agent", sync)
    actual_adapter = cli._run_kanban_goal_loop_q if quiet else cli._run_kanban_goal_loop_chat
    def observe_adapter(*args, **kwargs):
        result = actual_adapter(*args, **kwargs)
        observed.append((result, kwargs["progress"]))
        return result
    monkeypatch.setattr(cli, "_run_kanban_goal_loop_q" if quiet else "_run_kanban_goal_loop_chat", observe_adapter)
    with pytest.raises(SystemExit) as exc:
        cli._run_single_query_mode(stub, "task", None, quiet, True)
    code = 130 if isinstance(error, KeyboardInterrupt) else 1
    assert exc.value.code == code and len(calls) == turn
    assert len(judges) == turn - 1 and len(statuses) == turn - 1
    assert len(observed) == 1
    outcome, progress = observed[0]
    assert progress.turns_used == outcome.turns_used == turn
    assert outcome.last_turn.result["input_tokens"] == turn * 17
    assert outcome.last_turn.result["final_response"] == f"receipt {turn}"
    assert outcome.process_result["input_tokens"] == turn * 17
    assert outcome.process_result["final_response"] == f"receipt {turn}"
    assert outcome.process_result["failed"] is True
    assert finalizations == ["s2"] and hooks == (["s2"] if code == 130 else [])
    assert len(summaries) == (0 if quiet else 1)
    output = capsys.readouterr()
    assert output.err.count(f"[kanban-worker-exit] rc={code}") == 1
    assert output.err.count("session_id: s2") == (1 if quiet else 0)


@pytest.mark.parametrize("quiet", [False, True])
@pytest.mark.parametrize("phase,mutation,readback,code", [
    ("lookup", None, None, 75),
    ("lookup_interrupt", None, None, 130),
    ("lookup_busy", None, None, 75),
    ("lookup_schema", None, None, 1),
    ("false", False, "ready", 1),
    ("false", False, "review", 0),
    ("false", False, "superseded", 0),
    ("raise", TimeoutError("block busy"), "running", 75),
    ("raise", ValueError("block rejected"), "running", 1),
    ("raise", KeyboardInterrupt(), "running", 130),
    ("readback", False, TimeoutError("readback busy"), 75),
    ("readback", False, ValueError("readback invalid"), 1),
    ("readback", False, KeyboardInterrupt(), 130),
    ("readback_busy", False, None, 75),
    ("block_locked", None, "running", 75),
])
def test_goal_block_and_lookup_disposition_reaches_both_callers(
        monkeypatch, capsys, quiet, phase, mutation, readback, code):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "19")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setattr("tools.kanban_tools.register_current_worker_from_env", lambda: True)
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    monkeypatch.setattr(cli, "_route_single_query_images", lambda c, q, e, i, u: e)
    finalized, hooks, reads, blocks, judges, calls, outcomes = [], [], [], [], [], [], []
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: finalized.append(c.session_id))
    monkeypatch.setattr(cli, "_emit_interrupted_session_end", lambda c, **k: hooks.append(k))
    def lookup(conn, tid):
        if phase in ("lookup_busy", "lookup_schema"):
            error = sqlite3.OperationalError("opaque")
            error.sqlite_errorcode = (sqlite3.SQLITE_BUSY if phase == "lookup_busy" else sqlite3.SQLITE_ERROR)
            raise error
        if phase == "lookup":
            raise TimeoutError("lookup busy")
        if phase == "lookup_interrupt":
            raise KeyboardInterrupt()
        return SimpleNamespace(title="task", body="goal", goal_max_turns=3)
    monkeypatch.setattr(kb, "get_task", lookup)
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    def status(conn, tid, run):
        reads.append((tid, run))
        assert run == 19
        if len(reads) == 1:
            return "running"
        if phase == "readback_busy":
            error = sqlite3.OperationalError("opaque")
            error.sqlite_errorcode = sqlite3.SQLITE_BUSY
            raise error
        if isinstance(readback, BaseException):
            raise readback
        return readback
    monkeypatch.setattr(kb, "goal_run_status", status)
    def block(conn, tid, *, reason, expected_run_id):
        blocks.append((tid, expected_run_id))
        assert expected_run_id == 19
        if phase == "block_locked":
            error = sqlite3.OperationalError("opaque")
            error.sqlite_errorcode = sqlite3.SQLITE_LOCKED | (2 << 8)
            raise error
        if isinstance(mutation, BaseException):
            raise mutation
        return mutation
    monkeypatch.setattr(kb, "block_task", block)
    monkeypatch.setattr(goals, "judge_goal", lambda *a: judges.append(a) or
                        ("blocked", "fixture blocker", False, None, False))
    raw = {"final_response": "current receipt", "completed": True, "input_tokens": 19}
    agent = SimpleNamespace(session_id="s1", run_conversation=lambda **kw: calls.append(kw) or raw)
    stub = SimpleNamespace(
        session_id="s1", agent=agent, conversation_history=[], _last_turn_result=None,
        _claim_active_session=lambda *a, **k: True, _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
        console=SimpleNamespace(print=lambda *a, **k: None), _show_security_advisories=lambda: None,
        _print_exit_summary=lambda **k: None,
    )
    def chat(*args, **kwargs):
        calls.append(args)
        stub._last_turn_result = raw
        return raw["final_response"]
    stub.chat = chat
    actual = cli._run_kanban_goal_loop_q if quiet else cli._run_kanban_goal_loop_chat
    def observe(*args, **kwargs):
        value = actual(*args, **kwargs)
        outcomes.append(value)
        return value
    monkeypatch.setattr(cli, "_run_kanban_goal_loop_q" if quiet else "_run_kanban_goal_loop_chat", observe)
    with pytest.raises(SystemExit) as exc:
        cli._run_single_query_mode(stub, "task", None, quiet, True)
    assert exc.value.code == code
    assert len(calls) == 1 and len(outcomes) == 1
    assert len(reads) == (0 if phase.startswith("lookup") else
                          1 if isinstance(mutation, KeyboardInterrupt) else 2)
    assert len(judges) == len(blocks) == (0 if phase.startswith("lookup") else 1)
    outcome = outcomes[0]
    assert outcome.turns_used == 1 and outcome.last_turn.result == raw
    assert outcome.process_result["input_tokens"] == 19
    assert outcome.process_result["final_response"] == "current receipt"
    assert outcome.process_result.get("failed") is (None if code == 0 else True)
    assert finalized == ["s1"]
    assert hooks == ([{"reason": "keyboard_interrupt"}] if code == 130 else [])
    output = capsys.readouterr()
    assert output.err.count(f"[kanban-worker-exit] rc={code}") == 1


def _real_agent(tmp_path, *, max_iterations=10):
    with (patch("model_tools.get_tool_definitions", return_value=[]),
          patch("model_tools.check_toolset_requirements", return_value={}),
          patch("agent.process_bootstrap.OpenAI")):
        instance = AIAgent(
            session_id="goal-composition", api_key="test-key", base_url="https://example.invalid/v1",
            provider="openai-compat", model="test/model", max_iterations=max_iterations,
            quiet_mode=True, skip_context_files=True, skip_memory=True,
        )
    instance._cached_system_prompt = "stable test prompt"
    instance._session_db = None
    instance.save_trajectories = False
    instance.compression_enabled = False
    instance._cleanup_task_resources = lambda *_a, **_kw: None
    instance._save_trajectory = lambda *_a, **_kw: None
    return instance


def _model_answer(text):
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=text, tool_calls=None), finish_reason="stop")],
        model="test/model", usage=None,
    )


def test_real_agent_goal_followup_retains_two_nudges_and_three_successful_turns(monkeypatch, tmp_path):
    from agent import turn_context, conversation_loop
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "1")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setenv("HERMES_VERIFY_ON_STOP", "0")
    monkeypatch.setattr("tools.kanban_tools.register_current_worker_from_env", lambda: True)
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    monkeypatch.setattr(cli, "_route_single_query_images", lambda c, q, e, i, u: e)
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: None)
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lambda c, tid: SimpleNamespace(title="task", body="evidence receipts", goal_max_turns=5))
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    checks = []
    monkeypatch.setattr(kb, "goal_run_status", lambda c, tid, run: "review" if len(checks) == 2 else "running")
    monkeypatch.setattr(kb, "block_task", lambda *a, **k: pytest.fail("no block"))
    monkeypatch.setattr(goals, "judge_goal", lambda goal, response: checks.append((goal, response)) or ("continue", "another slice", False, None, False))
    agent = _real_agent(tmp_path)
    reset = turn_context._reset_per_turn_agent_state
    finalizer = conversation_loop.finalize_turn
    reset_calls = []
    finalized_calls = []
    @wraps(reset)
    def observed_reset(owner):
        reset_calls.append(owner)
        return reset(owner)
    @wraps(finalizer)
    def observed_finalizer(owner, **kwargs):
        result = finalizer(owner, **kwargs)
        finalized_calls.append((owner, result))
        return result
    monkeypatch.setattr(turn_context, "_reset_per_turn_agent_state", observed_reset)
    monkeypatch.setattr(conversation_loop, "finalize_turn", observed_finalizer)
    receipts = []
    for i in range(3):
        artifact = tmp_path / f"slice{i}.txt"
        artifact.write_text(f"observed fixture slice {i}")
        receipts.append(f"observed {artifact.name}")
    answers = iter([_model_answer(receipts[0]), _model_answer(receipts[0]), _model_answer(receipts[0]),
                    _model_answer(receipts[1]), _model_answer(receipts[2])])
    observed_prompts = []
    def model_call(kwargs):
        observed_prompts.append(kwargs["messages"])
        return next(answers)
    agent._interruptible_api_call = model_call
    settled = []
    real_run = agent.run_conversation
    def record_run(**kwargs):
        result = real_run(**kwargs)
        settled.append(result)
        return result
    agent.run_conversation = record_run
    stub = SimpleNamespace(
        session_id=agent.session_id, conversation_history=[], agent=agent, _single_query_mode=False,
        _claim_active_session=lambda *a, **k: True, _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
    )
    with (patch("hermes_cli.plugins.has_hook", return_value=False),
          patch("hermes_cli.plugins.invoke_hook", return_value=[])):
        with pytest.raises(SystemExit) as exc:
            cli._run_single_query_mode(stub, "task", None, True, True)
    assert exc.value.code == 0
    assert len(settled) == 3
    assert reset_calls == [agent] * 3
    assert len(finalized_calls) == 3
    assert [r for _, r in finalized_calls] == settled
    assert all(r["completed"] is True and not r["failed"] and not r["partial"] and not r["interrupted"] for r in settled)
    assert [r["final_response"] for r in settled] == receipts
    assert agent._kanban_stop_nudges == 2
    assert len(checks) == 2
    assert checks[0][0] == "task\n\nevidence receipts"
    assert len(observed_prompts) == 5
    assert all(messages[0]["content"] == "stable test prompt" for messages in observed_prompts)
    assert "Another bounded check may follow" in str(observed_prompts[1])
    assert "repeated bounded check, not a rejection" in str(observed_prompts[2])
    assert stub.conversation_history == settled[-1]["messages"]
    for result in settled:
        messages = result["messages"]
        assert messages[0]["role"] == "user"  # system prompt is API-only, not persisted history
        assert all(row["role"] in {"system", "user", "assistant", "tool"} for row in messages)
        assert all(left["role"] != right["role"] for left, right in pairwise(messages))
        assert messages[-1]["role"] == "assistant" and messages[-1]["content"] == result["final_response"]
        assert sum(row["role"] == "assistant" and row["content"] == result["final_response"]
                   for row in messages[max(i for i, row in enumerate(messages) if row["role"] == "user") + 1:]) == 1
        assert not any("kanban-stop" in str(row) for row in messages)


@pytest.mark.parametrize("fail_followup,display_mode", [
    (False, "normal"), (True, "normal"), (False, "mute"), (False, "caught_render_error"),
])
def test_real_agent_rendered_goal_followup_uses_settled_turn_not_display(
        monkeypatch, tmp_path, fail_followup, display_mode):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "1")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setenv("HERMES_VERIFY_ON_STOP", "0")
    monkeypatch.setattr("tools.kanban_tools.register_current_worker_from_env", lambda: True)
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: None)
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lambda c, tid: SimpleNamespace(title="task", body="test receipt", goal_max_turns=3))
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    checks = []
    monkeypatch.setattr(kb, "goal_run_status", lambda c, tid, run: "review" if checks and display_mode == "normal" else "running")
    blocks = []
    monkeypatch.setattr(kb, "block_task", lambda *a, **k: blocks.append(k) or True)
    def judge(goal, response):
        checks.append(response)
        return ("blocked" if display_mode != "normal" and len(checks) == 2 else "continue",
                "one more slice", False, None, False)
    monkeypatch.setattr(goals, "judge_goal", judge)
    instance = _real_agent(tmp_path)
    answers = iter([_model_answer("fixture first receipt") for _ in range(3)] +
                   [_model_answer("fixture second receipt")])
    def model_call(kwargs):
        if fail_followup and len(checks) == 1:
            raise ValueError("fixture provider failure")
        return next(answers)
    instance._interruptible_api_call = model_call
    rendered = []
    stub = SimpleNamespace(
        session_id=instance.session_id, conversation_history=[], agent=instance, _single_query_mode=False,
        _claim_active_session=lambda *a, **k: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _print_exit_summary=lambda **k: None,
        console=SimpleNamespace(print=lambda *a, **k: None), _show_security_advisories=lambda: None,
        _prompt_start_time=None, _last_turn_result=None, _voice_continuous=False, _voice_tts=False,
        _flush_stream=lambda: None, _chat_resolve_interrupt=lambda *a: (None, False),
        _chat_print_reasoning_box=lambda turn: None,
        _chat_print_response_panel=lambda turn, response: (
            (_ for _ in ()).throw(RuntimeError("fixture display failure"))
            if display_mode == "caught_render_error" and len(checks) == 1
            else rendered.append(response)),
        _emit_focus_recovery_line=lambda: None, _ring_bell=lambda **k: None,
        provider="openai-compat", model="test/model",
    )
    def chat(prompt, **kwargs):
        raw = instance.run_conversation(user_message=prompt, conversation_history=stub.conversation_history)
        if fail_followup and len(checks) == 1 and raw.get("failed"):
            # Exercise the renderer's empty-answer error-copy branch using the real
            # agent's failed settlement, with only its display field blanked.
            raw = {**raw, "final_response": ""}
        turn = SimpleNamespace(result=raw, use_streaming_tts=False, text_queue=None,
                               mute_notification_reply=display_mode == "mute" and len(checks) == 1)
        cli.HermesCLI._chat_settle_turn(stub, turn)
        try:
            return cli.HermesCLI._chat_render_turn(stub, turn, None, None)
        except RuntimeError as exc:
            assert display_mode == "caught_render_error" and str(exc) == "fixture display failure"
            return None  # the real chat orchestrator catches a post-settle render error
    stub.chat = chat
    with (patch("hermes_cli.plugins.has_hook", return_value=False),
          patch("hermes_cli.plugins.invoke_hook", return_value=[])):
        with pytest.raises(SystemExit) as exc:
            cli._run_single_query_mode(stub, "task", None, False, True)
    assert exc.value.code == (1 if fail_followup else 0)
    assert rendered[0] == "fixture first receipt"
    if fail_followup:
        assert len(rendered) == 2 and rendered[1] and rendered[1] != "fixture second receipt"
        assert "fixture provider failure" in rendered[1]
        assert stub._last_turn_result["failed"] is True
        assert not stub._last_turn_result.get("final_response")
    elif display_mode == "normal":
        assert rendered == ["fixture first receipt", "fixture second receipt"]
    else:
        assert rendered == ["fixture first receipt"]
    assert checks == (["fixture first receipt", ""] if display_mode != "normal" else ["fixture first receipt"])
    assert len(blocks) == (1 if display_mode != "normal" else 0)
    if not fail_followup:
        assert stub._last_turn_result["final_response"] == "fixture second receipt"


def test_real_agent_low_budget_finalizer_books_native_timeout_not_goal_block(monkeypatch, tmp_path):
    from hermes_cli import kanban_db_connect as kbc
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setenv("HERMES_VERIFY_ON_STOP", "0")
    kb._INITIALIZED_PATHS.clear()
    kb.init_db()
    with kbc.connect() as conn:
        task_id = kb.create_task(conn, title="low-budget fixture", assignee="builder")
        claim = kb.claim_task(conn, task_id, claimer="builder:low")
        assert claim is not None
    monkeypatch.setenv("HERMES_KANBAN_TASK", task_id)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(claim.current_run_id))
    monkeypatch.setenv("HERMES_KANBAN_DB", str(kb.kanban_db_path()))
    monkeypatch.setenv("HERMES_KANBAN_CLAIM_LOCK", "builder:low")
    instance = _real_agent(tmp_path, max_iterations=1)
    instance.iteration_budget.max_total = 1
    instance._interruptible_api_call = lambda kwargs: _model_answer("observed fixture receipt")
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    monkeypatch.setattr(cli, "_route_single_query_images", lambda c, q, e, i, u: e)
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: None)
    observed = []
    real_run = instance.run_conversation
    def recording_run(**kwargs):
        result = real_run(**kwargs)
        observed.append(result)
        return result
    instance.run_conversation = recording_run
    stub = SimpleNamespace(
        session_id=instance.session_id, agent=instance, conversation_history=[],
        _claim_active_session=lambda *a, **k: True, _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
    )
    with (patch("hermes_cli.plugins.has_hook", return_value=False),
          patch("hermes_cli.plugins.invoke_hook", return_value=[]),
          patch.object(goals, "judge_goal", side_effect=AssertionError("goal judge cannot run")),
          patch.object(kb, "block_task", side_effect=AssertionError("goal block cannot run"))):
        with pytest.raises(SystemExit) as exc:
            cli._run_single_query_mode(stub, "fixture work", None, True, True)
    assert exc.value.code == 1
    assert len(observed) == 1
    result = observed[0]
    assert result["completed"] is False
    assert result["final_response"] == "observed fixture receipt"
    assert cli._single_query_exit_code(result) == 1
    with kbc.connect() as conn:
        run = kb.latest_run(conn, task_id)
        task = kb.get_task(conn, task_id)
    assert run is not None and run.outcome == "timed_out"
    assert task is not None and task.status != "blocked" and task.claim_lock is None


@pytest.mark.parametrize("raw,code", [
    ({}, 0), ({"failed": True}, 1), ({"partial": True}, 1), ({"completed": False}, 1),
    ({"interrupted": True, "failed": True, "failure_reason": "auth"}, 130),
    ({"failed": True, "failure_reason": "rate_limit"}, KANBAN_RATE_LIMIT_EXIT_CODE),
    ({"failed": True, "failure_reason": "auth"}, KANBAN_TERMINAL_PROVIDER_EXIT_CODE),
    (None, 1), ("rendered-but-unsettled", 1),
])
def test_followup_classifies_whole_raw_turn_not_display(monkeypatch, raw, code):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    turn = cli._single_query_exit_code(raw)
    assert turn == code
    from hermes_cli.cli_single_query import _classify_kanban_goal_turn
    classified = _classify_kanban_goal_turn(raw, response="apparent success")
    assert classified.exit_code == code
    assert classified.response == "apparent success"
    assert classified.result == raw if isinstance(raw, dict) else classified.result is None


def _direct_loop(monkeypatch, *, statuses, verdict=("continue", "more", False, None, False),
                 follow=None, block=lambda reason: True, max_turns=3, use_real_judge=False):
    first = GoalTurnResult("first receipt", {"final_response": "first receipt", "completed": True}, 0)
    progress = GoalLoopProgress(first)
    events = []
    states = iter(statuses)
    def status():
        events.append("status")
        next_state = next(states)
        if isinstance(next_state, BaseException):
            raise next_state
        return next_state
    actual_judge = goals.judge_goal
    def judge(*args):
        events.append("judge")
        if isinstance(verdict, BaseException):
            raise verdict
        return actual_judge(*args) if use_real_judge else verdict
    def worker(prompt):
        events.append("worker")
        if isinstance(follow, BaseException):
            raise follow
        return follow
    def native_block(reason):
        events.append("block")
        if isinstance(block, BaseException):
            raise block
        return block(reason)
    monkeypatch.setattr(goals, "judge_goal", judge)
    from hermes_cli.cli_single_query import _goal_infrastructure_exit_code
    outcome = goals.run_kanban_goal_loop(
        task_id="t_testowned", goal_text="acceptance", run_turn=worker,
        task_status_fn=status, block_fn=native_block,
        infrastructure_exit_code=_goal_infrastructure_exit_code,
        first_turn=first, progress=progress, max_turns=max_turns)
    return outcome, events


@pytest.mark.parametrize("raw,code", [
    ({"completed": False, "failure_reason": "auth", "failed": True, "tokens": 42}, 78),
    ({"partial": True, "tokens": 13}, 1),
    ({"failed": True, "failure_reason": "timeout", "tokens": 3}, 75),
    ({"interrupted": True, "failed": True, "failure_reason": "auth"}, 130),
    (None, 1),
])
def test_followup_result_stops_before_status_judge_or_budget_block(monkeypatch, raw, code):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    from hermes_cli.cli_single_query import _classify_kanban_goal_turn
    result, events = _direct_loop(monkeypatch, statuses=["running"],
                                  follow=_classify_kanban_goal_turn(raw), max_turns=2)
    assert result.exit_code == code
    assert result.turns_used == 2
    assert events == ["status", "judge", "worker"]
    assert result.last_turn.result == raw
    if raw is not None:
        assert result.process_result == raw
    else:
        assert result.process_result["failed"] is True


@pytest.mark.parametrize("error,code", [(TimeoutError("status timeout"), 75),
                                           (ConnectionError("offline"), 75),
                                           (ValueError("schema"), 1)])
def test_goal_status_infrastructure_distinguishes_typed_transients(monkeypatch, error, code):
    result, events = _direct_loop(monkeypatch, statuses=[error])
    assert result.exit_code == code
    assert events == ["status"]
    assert result.process_result["completed"] is False


@pytest.mark.parametrize("readback,expected", [
    ("review", 0), ("done", 0), ("blocked", 0), ("changes_requested", 0),
    ("superseded", 0), (None, 0), ("running", 1), ("ready", 1),
    (TimeoutError("busy"), 75), (ValueError("bad readback"), 1),
    (KeyboardInterrupt(), 130),
])
@pytest.mark.parametrize("block_error", [None, TimeoutError("block busy"), ValueError("block rejected")])
def test_false_native_block_readback_once_and_never_retries(monkeypatch, readback, expected, block_error):
    result, events = _direct_loop(monkeypatch, statuses=["running", readback],
                                  verdict=("blocked", "cannot proceed", False, None, False),
                                  block=block_error if block_error else lambda reason: False)
    if readback in ("running", "ready") and isinstance(block_error, TimeoutError):
        expected = 75
    assert result.exit_code == expected
    assert events == ["status", "judge", "block", "status"]
    assert result.turns_used == 1
    assert result.process_result["completed"] is (expected == 0)
    if expected:
        assert result.process_result["failed"] is True
        assert result.process_result["final_response"] == "first receipt"


@pytest.mark.parametrize("code,expected", [
    (sqlite3.SQLITE_BUSY, 75), (sqlite3.SQLITE_LOCKED | (2 << 8), 75),
    (sqlite3.SQLITE_ERROR, 1), (None, 1),
])
@pytest.mark.parametrize("phase", ["status", "judge", "readback", "block"])
def test_numeric_sqlite_error_reaches_goal_phase(monkeypatch, phase, code, expected):
    error = sqlite3.OperationalError("opaque")
    if code is not None:
        error.sqlite_errorcode = code
    statuses = [error] if phase == "status" else ["running", error if phase == "readback" else "running"]
    result, events = _direct_loop(
        monkeypatch, statuses=statuses,
        verdict=error if phase == "judge" else ("blocked", "cannot proceed", False, None, False),
        block=error if phase == "block" else lambda reason: False,
    )
    assert result.exit_code == expected
    assert result.process_result["failed"] is True
    assert result.process_result["final_response"] == "first receipt"
    assert events == ({"status": ["status"], "judge": ["status", "judge"]}.get(
        phase, ["status", "judge", "block", "status"]))


def test_real_block_commit_then_hook_raise_reconciles_terminal_status(monkeypatch, tmp_path):
    from hermes_cli import kanban_db_connect as kbc
    from hermes_cli.cli_single_query import _goal_infrastructure_exit_code
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb._INITIALIZED_PATHS.clear()
    kb.init_db()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="committed block", assignee="builder")
        claim = kb.claim_task(conn, tid, claimer="builder:1")
        assert claim is not None
    first = GoalTurnResult("receipt", {"completed": True}, 0)
    readings = []
    def status():
        with kbc.connect() as conn:
            value = kb.goal_run_status(conn, tid, claim.current_run_id)
        readings.append(value)
        return value
    def block(reason):
        with kbc.connect() as conn:
            return kb.block_task(conn, tid, reason=reason, expected_run_id=claim.current_run_id)
    monkeypatch.setattr(kb, "_fire_task_hook", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("hook after commit")))
    monkeypatch.setattr(goals, "judge_goal", lambda *a: ("blocked", "cannot proceed", False, None, False))
    outcome = goals.run_kanban_goal_loop(
        task_id=tid, goal_text="committed block", first_turn=first, progress=GoalLoopProgress(first),
        run_turn=lambda p: pytest.fail("no model"), task_status_fn=status, block_fn=block,
        infrastructure_exit_code=_goal_infrastructure_exit_code)
    assert outcome.exit_code == 0 and outcome.outcome == "blocked_by_worker"
    assert readings == ["running", "blocked"]
    with kbc.connect() as conn:
        task = kb.get_task(conn, tid)
        run = kb.latest_run(conn, tid)
    assert task is not None and task.status == "blocked"
    assert run is not None and run.outcome == "blocked"


def test_real_board_reclaim_before_block_refusal_preserves_successor(monkeypatch, tmp_path):
    from hermes_cli import kanban_db_connect as kbc, kanban_db_dispatch as kbd
    from hermes_cli.cli_single_query import _goal_infrastructure_exit_code
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    kb._INITIALIZED_PATHS.clear()
    kb.init_db()
    host = kb._claimer_id().split(":", 1)[0]
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="race target", assignee="builder")
        old = kb.claim_task(conn, tid, claimer=f"{host}:old")
        assert old is not None
        monkeypatch.setattr(kbd, "_process_fingerprint", lambda pid: f"fingerprint-{pid}")
        kbd._set_worker_pid(conn, tid, 87001)
    first = GoalTurnResult("first", {"completed": True}, 0)
    progress = GoalLoopProgress(first)
    observed = []
    successor = []
    failure_baseline = []
    def status():
        with kbc.connect() as conn:
            value = kb.goal_run_status(conn, tid, old.current_run_id)
        observed.append(value)
        return value
    def block(reason):
        with kbc.connect() as conn:
            kbd._record_task_failure(conn, tid, error="old worker reclaimed", outcome="rate_limited",
                                     release_claim=True, end_run=True)
            new = kb.claim_task(conn, tid, claimer=f"{host}:new")
            assert new is not None
            kbd._set_worker_pid(conn, tid, 87002)
            successor.append(new.current_run_id)
            failure_baseline.append(new.consecutive_failures)
            return kb.block_task(conn, tid, reason=reason, expected_run_id=old.current_run_id)
    monkeypatch.setattr(goals, "judge_goal", lambda *a: ("blocked", "cannot proceed", False, None, False))
    outcome = goals.run_kanban_goal_loop(
        task_id=tid, goal_text="race target", first_turn=first, progress=progress,
        run_turn=lambda p: pytest.fail("no model after block"), task_status_fn=status, block_fn=block,
        infrastructure_exit_code=_goal_infrastructure_exit_code)
    assert outcome.exit_code == 0 and outcome.outcome == "stopped"
    assert observed == ["running", "superseded"]
    with kbc.connect() as conn:
        new_task = kb.get_task(conn, tid)
        new_run = kb.latest_run(conn, tid)
        successor_row = conn.execute("SELECT worker_pid, worker_started_at, claim_lock, claim_expires "
                                     "FROM tasks WHERE id=?", (tid,)).fetchone()
    assert new_task is not None and new_task.status == "running"
    assert new_task.current_run_id == successor[0]
    assert new_task.claim_lock == f"{host}:new" and new_task.worker_pid == 87002
    assert successor_row["worker_started_at"] == "fingerprint-87002"
    assert new_run is not None and new_run.id == successor[0] and new_run.outcome is None
    old_log = kb.worker_log_path(tid)
    old_log.parent.mkdir(parents=True, exist_ok=True)
    with old_log.open("a", encoding="utf-8") as stream:
        stream.write("old worker log\n[kanban-worker-exit] rc=1\n")
    kbd._recent_worker_exits.clear()
    checked = []
    def alive(pid, fingerprint):
        checked.append((pid, fingerprint))
        return pid == 87002 and fingerprint == "fingerprint-87002"
    monkeypatch.setattr(kbd, "_worker_alive", alive)
    with kbc.connect() as conn:
        kbd.detect_crashed_workers(conn)
        survivor = kb.get_task(conn, tid)
        survivor_run = kb.latest_run(conn, tid)
        survivor_row = conn.execute("SELECT worker_pid, worker_started_at, claim_lock, claim_expires "
                                    "FROM tasks WHERE id=?", (tid,)).fetchone()
    assert survivor is not None and survivor.current_run_id == successor[0]
    assert survivor.status == "running" and survivor.consecutive_failures == failure_baseline[0]
    assert survivor.claim_lock == new_task.claim_lock and survivor.worker_pid == new_task.worker_pid
    assert dict(survivor_row) == dict(successor_row)
    assert checked == [(87002, "fingerprint-87002")]
    assert survivor_run is not None and survivor_run.outcome is None


@pytest.mark.parametrize("error", [
    TimeoutError("temporary outage"), TypeError("bad auxiliary settings"), Exception("auth-like reason"),
    AuxiliaryClientUnavailable("reauthentication required"),
])
def test_real_judge_catch_all_flag_is_bounded_one_not_neutral_retry(monkeypatch, error):
    actual = goals.judge_goal
    seen = []
    def observing_judge(*args, **kwargs):
        verdict = actual(*args, **kwargs)
        seen.append(verdict)
        return verdict
    monkeypatch.setattr(goals, "judge_goal", observing_judge)
    with patch("agent.auxiliary_client.call_llm", side_effect=error):
        result, events = _direct_loop(monkeypatch, statuses=["running"], use_real_judge=True)
    assert seen[0][0] == "continue"
    assert seen[0][-1] is True
    assert result.exit_code == 1
    assert result.process_result["failed"] is True
    assert events == ["status", "judge"]


@pytest.mark.parametrize("raw_code,expected", [
    (sqlite3.SQLITE_BUSY, 75),
    (sqlite3.SQLITE_LOCKED | (2 << 8), 75),
    (sqlite3.SQLITE_ERROR, 1),
])
def test_sqlite_infrastructure_requires_numeric_busy_or_locked_code(raw_code, expected):
    from hermes_cli.cli_single_query import _goal_infrastructure_exit_code
    error = sqlite3.OperationalError("opaque")
    error.sqlite_errorcode = raw_code
    assert _goal_infrastructure_exit_code(error) == expected


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


@pytest.mark.parametrize("rendered", [False, True])
@pytest.mark.parametrize("run_id,task,lookup_error,expected", [
    ("missing_task_id", None, None, 1),
    (None, None, None, 1), ("0", None, None, 1), (" 1", None, None, 1),
    ("1 ", None, None, 1), ("+1", None, None, 1), ("01", None, None, 1),
    ("1", None, None, 1), ("1", SimpleNamespace(title="", body="", goal_max_turns=3), None, 1),
    ("1", None, KeyboardInterrupt(), 130),
    ("1", None, TimeoutError("lookup unavailable"), 75),
    ("1", None, ValueError("bad lookup"), 1),
    ("1", None, "busy", 75), ("1", None, "locked_extended", 75),
    ("1", None, "schema", 1),
])
def test_goal_adapter_validates_run_task_and_lookup(
        monkeypatch, rendered, run_id, task, lookup_error, expected):
    if run_id == "missing_task_id":
        monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    else:
        monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    if run_id is None:
        monkeypatch.delenv("HERMES_KANBAN_RUN_ID", raising=False)
    else:
        monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "1" if run_id == "missing_task_id" else run_id)
    queries = []
    def lookup(conn, tid):
        queries.append(tid)
        if isinstance(lookup_error, str):
            exc = sqlite3.OperationalError("opaque")
            exc.sqlite_errorcode = {
                "busy": sqlite3.SQLITE_BUSY,
                "locked_extended": sqlite3.SQLITE_LOCKED | (2 << 8),
                "schema": sqlite3.SQLITE_ERROR,
            }[lookup_error]
            raise exc
        if lookup_error:
            raise lookup_error
        return task
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lookup)
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    monkeypatch.setattr(kb, "goal_run_status", lambda *a: pytest.fail("invalid adapter entered status"))
    monkeypatch.setattr(goals, "judge_goal", lambda *a: pytest.fail("invalid adapter entered judge"))
    first = GoalTurnResult("receipt", {"final_response": "receipt", "completed": True, "input_tokens": 19}, 0)
    progress = GoalLoopProgress(first)
    stub = SimpleNamespace(agent=SimpleNamespace(session_id="s1"), session_id="s1",
                           conversation_history=[], chat=lambda p: pytest.fail("no model"))
    adapter = cli._run_kanban_goal_loop_chat if rendered else cli._run_kanban_goal_loop_q
    outcome = adapter(stub, first, progress=progress)
    assert outcome.exit_code == expected and outcome.turns_used == 1
    assert outcome.last_turn is first
    assert outcome.process_result["final_response"] == "receipt"
    assert outcome.process_result["input_tokens"] == 19
    assert outcome.process_result["failed"] is True
    assert len(queries) == (1 if run_id == "1" else 0)


@pytest.mark.parametrize("raw", [None, "invalid", {}, 7])
def test_invalid_typed_followup_return_stops_without_reentry(monkeypatch, raw):
    outcome, events = _direct_loop(monkeypatch, statuses=["running"], follow=raw)
    assert outcome.exit_code == 1 and outcome.turns_used == 2
    assert events == ["status", "judge", "worker"]
    assert outcome.last_turn.result["final_response"] == "first receipt"
    assert outcome.process_result["failed"] is True
    assert outcome.process_result["final_response"] == "first receipt"


@pytest.mark.parametrize("kind,code", [
    ("context_refusal", 1), ("rate_limited", 75), ("terminal", 78), ("unknown_start", 1),
])
def test_rendered_followup_context_refusal_and_no_fresh_credential_flags(monkeypatch, kind, code):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "1")
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lambda *a: SimpleNamespace(
        title="task", body="goal", goal_max_turns=3))
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    statuses, judges, calls = [], [], []
    monkeypatch.setattr(kb, "goal_run_status", lambda *a: statuses.append(a) or "running")
    monkeypatch.setattr(kb, "block_task", lambda *a, **k: pytest.fail("no block"))
    monkeypatch.setattr(goals, "judge_goal", lambda *a: judges.append(a) or
                        ("continue", "inspect @file:blocked", False, None, False))
    first = GoalTurnResult("first receipt", {"final_response": "first receipt", "completed": True}, 0)
    progress = GoalLoopProgress(first)
    stub = SimpleNamespace(
        _last_turn_result={"final_response": "stale first"}, _credentials_rate_limited=False,
        _credentials_terminal=False, model="test-model", base_url="", api_key="", provider="openai-compat",
        agent=SimpleNamespace(session_id="s1"), session_id="s1",
    )
    if kind == "context_refusal":
        monkeypatch.setattr("agent.model_metadata.get_model_context_length", lambda *a, **k: 8192)
        monkeypatch.setattr("agent.context_references.preprocess_context_references", lambda *a, **k:
                            SimpleNamespace(expanded=False, blocked=True, references=[], warnings=["fixture refusal"]))
    def chat(prompt):
        calls.append(prompt)
        if kind == "context_refusal":
            assert "@file:blocked" in prompt
            _, refusal = cli.HermesCLI._chat_expand_context_references(stub, prompt)
            assert refusal == "fixture refusal"
            return refusal
        stub._credentials_rate_limited = kind == "rate_limited"
        stub._credentials_terminal = kind == "terminal"
        return "credentials refused"
    stub.chat = chat
    outcome = cli._run_kanban_goal_loop_chat(stub, first, progress=progress)
    assert outcome.exit_code == code and outcome.turns_used == 2
    assert outcome.last_turn.result is None and outcome.process_result["failed"] is True
    assert outcome.process_result.get("final_response") is None
    assert len(statuses) == len(judges) == len(calls) == 1


@pytest.mark.parametrize("rate_limited,terminal,code", [
    (False, False, 1), (True, False, 75), (False, True, 78),
])
def test_rendered_no_fresh_credential_flags_reach_actual_caller(
        monkeypatch, capsys, rate_limited, terminal, code):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "19")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setattr("tools.kanban_tools.register_current_worker_from_env", lambda: True)
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    monkeypatch.setattr(cli, "_route_single_query_images", lambda c, q, e, i, u: e)
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lambda *a: SimpleNamespace(
        title="task", body="acceptance", goal_max_turns=3))
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    judges, statuses, finalized = [], [], []
    monkeypatch.setattr(kb, "goal_run_status", lambda *a: statuses.append(a) or "running")
    monkeypatch.setattr(kb, "block_task", lambda *a, **k: pytest.fail("no block"))
    monkeypatch.setattr(goals, "judge_goal", lambda *a: judges.append(a) or
                        ("continue", "next", False, None, False))
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: finalized.append(c.session_id))
    first = {"final_response": "first", "completed": True, "input_tokens": 19}
    agent = SimpleNamespace(session_id="s1")
    stub = SimpleNamespace(
        session_id="s1", agent=agent, conversation_history=[], _last_turn_result=None,
        _credentials_rate_limited=False, _credentials_terminal=False,
        _claim_active_session=lambda *a, **k: True, _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
        console=SimpleNamespace(print=lambda *a, **k: None), _show_security_advisories=lambda: None,
        _print_exit_summary=lambda **k: None,
    )
    calls = []
    def chat(*args, **kwargs):
        calls.append(args)
        if len(calls) == 1:
            stub._last_turn_result = first
            return "first"
        stub._credentials_rate_limited = rate_limited
        stub._credentials_terminal = terminal
        return "credential startup refused without settlement"
    stub.chat = chat
    original = cli._run_kanban_goal_loop_chat
    outcomes = []
    def observe(*args, **kwargs):
        outcome = original(*args, **kwargs)
        outcomes.append(outcome)
        return outcome
    monkeypatch.setattr(cli, "_run_kanban_goal_loop_chat", observe)
    with pytest.raises(SystemExit) as exc:
        cli._run_single_query_mode(stub, "task", None, False, True)
    assert exc.value.code == code and len(calls) == 2
    assert len(judges) == len(statuses) == len(outcomes) == 1
    assert outcomes[0].turns_used == 2 and outcomes[0].last_turn.result is None
    assert outcomes[0].process_result["failed"] is True
    assert outcomes[0].process_result.get("input_tokens") is None
    assert finalized == ["s1"]
    assert capsys.readouterr().err.count(f"[kanban-worker-exit] rc={code}") == 1


def test_rendered_followup_systemexit_bypasses_classification(monkeypatch):
    """Termination after a settled result must not run the rendered exception handler."""
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_testowned")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "1")
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lambda *a: SimpleNamespace(
        title="task", body="work", goal_max_turns=3))
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    monkeypatch.setattr(kb, "goal_run_status", lambda *a: "running")
    monkeypatch.setattr(goals, "judge_goal", lambda *a: ("continue", "next", False, None, False))
    first = GoalTurnResult("first", {"final_response": "first", "completed": True}, 0)
    progress = GoalLoopProgress(first)
    stub = SimpleNamespace(_last_turn_result=None)

    def chat(prompt):
        stub._last_turn_result = {"final_response": "settled", "completed": True}
        raise SystemExit(42)

    stub.chat = chat
    from hermes_cli import cli_single_query as single
    with patch.object(single, "_classify_kanban_goal_turn", side_effect=AssertionError("classified termination")):
        with pytest.raises(SystemExit) as exc:
            cli._run_kanban_goal_loop_chat(stub, first, progress=progress)
    assert exc.value.code == 42
    assert progress.turns_used == 2
    assert progress.last_turn is first
