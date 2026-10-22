"""Goal worker follow-up disposition through the actual one-shot callers."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path
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
    ({"partial": True}, 1),
    ({"completed": False}, 1),
    ({"failed": True, "failure_reason": "timeout"}, 75),
    ({"interrupted": True, "failed": True, "failure_reason": "auth"}, 130),
    (None, 1),
])
def test_followup_failed_provider_turn_reaches_process_exit(monkeypatch, quiet, fail_on_turn, failure, expected_code, capsys):
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_followup")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "1")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setattr("tools.kanban_tools.register_current_worker_from_env", lambda: True)
    monkeypatch.setattr(cli, "_should_seed_interactive", lambda *a, **k: False)
    monkeypatch.setattr(cli, "_collect_query_images", lambda q, i: (q, []))
    monkeypatch.setattr(cli, "_collect_kanban_task_images", lambda imgs: [])
    monkeypatch.setattr(cli, "_finalize_single_query", lambda c: None)
    monkeypatch.setattr(cli, "_sync_cli_session_id_from_agent", lambda c: None)
    monkeypatch.setattr(cli, "_route_single_query_images", lambda c, q, e, i, u: e)
    monkeypatch.setattr("hermes_cli.kanban_db.get_task", lambda c, tid: SimpleNamespace(title="task", body="acceptance", goal_max_turns=5))
    monkeypatch.setattr("hermes_cli.kanban_db_connect.connect_closing", lambda: _Connection())
    monkeypatch.setattr(kb, "goal_run_status", lambda c, tid, run: "running")
    actions = []
    monkeypatch.setattr(kb, "block_task", lambda *a, **k: actions.append("block"))

    def judge(*args, **kwargs):
        actions.append("judge")
        return "continue", "more work", False, None, False

    monkeypatch.setattr(goals, "judge_goal", judge)
    initial = {"final_response": "first receipt", "completed": True, "messages": []}
    failed = None if failure is None else {"final_response": "provider error", "messages": [], **failure}
    calls = []
    agent = SimpleNamespace(session_id="s1", run_conversation=lambda **kw: calls.append(kw) or (failed if len(calls) == fail_on_turn else initial))

    def chat(*args, **kwargs):
        calls.append((args, kwargs))
        stub._last_turn_result = failed if len(calls) == fail_on_turn else initial
        return stub._last_turn_result["final_response"] if isinstance(stub._last_turn_result, dict) else "unsettled display"

    stub = SimpleNamespace(
        session_id="s1", conversation_history=[], agent=agent, _single_query_mode=False,
        _claim_active_session=lambda *a, **k: True,
        _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
        console=SimpleNamespace(print=lambda *a, **k: None),
        _show_security_advisories=lambda: None, chat=chat,
        _print_exit_summary=lambda **k: None, _last_turn_result=None,
    )
    with pytest.raises(SystemExit) as exc:
        cli._run_single_query_mode(stub, "task", None, quiet, True)
    assert len(calls) == fail_on_turn
    assert actions == ["judge"] * (fail_on_turn - 1)
    assert exc.value.code == expected_code
    assert f"[kanban-worker-exit] rc={expected_code}" in capsys.readouterr().err


@pytest.mark.parametrize("quiet", [False, True])
@pytest.mark.parametrize("error", [AuxiliaryClientUnavailable("reauthentication required"),
                                    TypeError("bad auxiliary config"), TimeoutError("aux timeout")])
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
    assert seen[0][0] == "continue" and seen[0][-1] is True
    output = capsys.readouterr().err
    assert "[kanban-worker-exit] rc=1" in output
    if quiet:
        assert "judge transport failure" in output and output.count("session_id: s1") == 1


def test_quiet_emitter_followup_failure_is_one_terminal_json_result(monkeypatch, capsys):
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
        return first if len(calls) == 1 else failed
    agent = SimpleNamespace(session_id="stream-session", run_conversation=run_conversation)
    stub = SimpleNamespace(
        session_id="stream-session", model="test-model", agent=agent, conversation_history=[],
        _claim_active_session=lambda *a, **k: True, _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
    )
    with pytest.raises(SystemExit) as exc:
        cli._run_single_query_mode(stub, "task", None, True, True, stream_json=True)
    assert exc.value.code == 78
    assert len(calls) == 2 and judges == ["first"]
    output = capsys.readouterr()
    events = [json.loads(line) for line in output.out.splitlines()]
    assert [e["type"] for e in events] == ["system", "result"]
    assert events[-1]["exit_code"] == 78
    assert events[-1]["tokens"]["input"] == 37
    assert events[-1]["error"] == "credential revoked"
    assert output.err.count("session_id: stream-session") == 1
    assert output.err.count("[kanban-worker-exit] rc=78") == 1


@pytest.mark.parametrize("quiet", [False, True])
@pytest.mark.parametrize("raised,code", [(ValueError("adapter bug"), 1),
                                           (KeyboardInterrupt(), 130),
                                           (SystemExit(42), 42)])
def test_goal_caller_preserves_published_turn_on_adapter_escape(monkeypatch, capsys, quiet, raised, code):
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
    latest = {"final_response": "second", "completed": True, "input_tokens": 21}
    agent = SimpleNamespace(session_id="new-session", run_conversation=lambda **k: first)
    stub = SimpleNamespace(
        session_id="old-session", agent=agent, conversation_history=[], _single_query_mode=False,
        _claim_active_session=lambda *a, **k: True, _ensure_runtime_credentials=lambda: True,
        _resolve_turn_agent_config=lambda q: {"signature": "same", "model": "m", "runtime": None},
        _active_agent_route_signature="same", _init_agent=lambda **k: True,
        console=SimpleNamespace(print=lambda *a, **k: None), _show_security_advisories=lambda: None,
        _print_exit_summary=lambda **k: None, _last_turn_result=first,
    )
    def chat(*args, **kwargs):
        stub._last_turn_result = first
        return "first"
    stub.chat = chat
    def adapter(_cli, _first, *args, progress, **kwargs):
        progress.turns_used = 2
        progress.last_turn = GoalTurnResult("second", latest, 0)
        raise raised
    monkeypatch.setattr(cli, "_run_kanban_goal_loop_q" if quiet else "_run_kanban_goal_loop_chat", adapter)
    with pytest.raises(SystemExit) as exc:
        cli._run_single_query_mode(stub, "task", None, quiet, True)
    assert exc.value.code == code
    assert finalized == (["old-session"] if code == 42 and not quiet else ["new-session"])
    assert hooks == (["new-session"] if isinstance(raised, KeyboardInterrupt) else [])
    output = capsys.readouterr().err
    if code != 42:
        assert f"[kanban-worker-exit] rc={code}" in output
        if quiet:
            assert output.count("session_id: new-session") == 1
    else:
        assert "[kanban-worker-exit]" not in output


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


def test_real_agent_rendered_goal_followup_uses_settled_turn_not_display(monkeypatch, tmp_path):
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
    monkeypatch.setattr(kb, "goal_run_status", lambda c, tid, run: "review" if checks else "running")
    monkeypatch.setattr(goals, "judge_goal", lambda goal, response: checks.append(response) or ("continue", "one more slice", False, None, False))
    instance = _real_agent(tmp_path)
    answers = iter([_model_answer("fixture first receipt") for _ in range(3)] +
                   [_model_answer("fixture second receipt")])
    instance._interruptible_api_call = lambda kwargs: next(answers)
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
        _chat_print_response_panel=lambda turn, response: rendered.append(response),
        _emit_focus_recovery_line=lambda: None, _ring_bell=lambda **k: None,
        provider="openai-compat", model="test/model",
    )
    def chat(prompt, **kwargs):
        raw = instance.run_conversation(user_message=prompt, conversation_history=stub.conversation_history)
        turn = SimpleNamespace(result=raw, use_streaming_tts=False, text_queue=None,
                               mute_notification_reply=False)
        cli.HermesCLI._chat_settle_turn(stub, turn)
        return cli.HermesCLI._chat_render_turn(stub, turn, None, None)
    stub.chat = chat
    with (patch("hermes_cli.plugins.has_hook", return_value=False),
          patch("hermes_cli.plugins.invoke_hook", return_value=[])):
        with pytest.raises(SystemExit) as exc:
            cli._run_single_query_mode(stub, "task", None, False, True)
    assert exc.value.code == 0
    assert rendered == ["fixture first receipt", "fixture second receipt"]
    assert checks == ["fixture first receipt"]
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


@pytest.mark.parametrize("readback,expected", [("review", 0), ("superseded", 0),
                                                  ("running", 1), (TimeoutError("busy"), 75)])
def test_false_native_block_readback_once_and_never_retries(monkeypatch, readback, expected):
    result, events = _direct_loop(monkeypatch, statuses=["running", readback],
                                  verdict=("blocked", "cannot proceed", False, None, False),
                                  block=lambda reason: False)
    assert result.exit_code == expected
    assert events == ["status", "judge", "block", "status"]


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
    kb._INITIALIZED_PATHS.clear()
    kb.init_db()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="race target", assignee="builder")
        old = kb.claim_task(conn, tid, claimer="builder:old")
        assert old is not None
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
            new = kb.claim_task(conn, tid, claimer="builder:new")
            assert new is not None
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
    assert new_task is not None and new_task.status == "running"
    assert new_task.current_run_id == successor[0]
    assert new_run is not None and new_run.id == successor[0] and new_run.outcome is None
    with kbc.connect() as conn:
        kbd.detect_crashed_workers(conn)
        survivor = kb.get_task(conn, tid)
        survivor_run = kb.latest_run(conn, tid)
    assert survivor is not None and survivor.current_run_id == successor[0]
    assert survivor.status == "running" and survivor.consecutive_failures == failure_baseline[0]
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
