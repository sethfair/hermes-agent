"""A /goal-set campaign must be complete before replacing an existing goal."""
from dataclasses import asdict

import pytest

from hermes_cli import goals
from hermes_cli.goal_command import dispatch_goal_command
from hermes_cli.commands import resolve_command


def _campaign():
    return {
        "headline": "Publish a reliable CSV export",
        "outcome": "A permitted user can export the agreed fields as a downloadable CSV.",
        "verification": "Run the export tests and inspect a generated CSV against the agreed schema and permissions.",
        "constraints": "Preserve existing access checks and never export fields outside the agreed schema.",
        "boundaries": "Inspect the export service and its tests; do not deploy or change production data.",
        "stop_when": "Pause for the product owner if the field schema or authorisation policy is missing.",
        "quality_bar": "Export handles authorised and denied users, preserves CSV escaping, and has tests showing schema compliance.",
        "stages": [
            {"title": "Confirm interface", "action": "Locate the agreed fields and permission policy in the product specification.", "evidence": "Cite the authoritative schema and policy or identify the missing decision."},
            {"title": "Build the export", "action": "Implement export using the confirmed schema and permission rules.", "evidence": "Review the diff and run export unit tests."},
            {"title": "Verify delivery", "action": "Exercise an authorised and denied export with a fixture.", "evidence": "Inspect the CSV headers and denied-path test output."},
        ],
    }


def _dispatch(manager, prompt="Export CSV"):
    return dispatch_goal_command(manager, prompt, authorize_gate=lambda: None, campaign=True)


def test_goal_set_registry_offers_desktop_and_shared_surfaces():
    cmd = resolve_command("goal-set")
    assert cmd is not None
    assert cmd.argument_mode == "text"
    assert cmd.desktop is None
    assert cmd.busy_policy == "reject"


def test_complete_campaign_sets_contract_and_ordered_criteria_atomically(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    goals._DB_CACHE.clear()
    manager = goals.GoalManager("campaign-success")
    manager.set("existing objective")
    monkeypatch.setattr(goals, "draft_campaign", lambda prompt: _campaign())
    result = _dispatch(manager)
    assert not result.error and result.kickoff and result.prompt
    state = goals.load_goal(manager.session_id)
    assert state.goal == _campaign()["headline"]
    assert state.contract.to_dict()["verification"] == _campaign()["verification"]
    assert len(state.subgoals) == len(_campaign()["stages"]) + 1
    assert state.subgoals[-1].startswith("Well-developed criterion:")
    assert "Confirm interface" in state.subgoals[0]
    assert "Locate the agreed fields" in state.subgoals[0]
    assert "Cite the authoritative schema" in state.subgoals[0]
    assert "Well-developed" in result.output
    assert "Confirm interface" in manager.next_continuation_prompt()
    assert "existing objective" in result.output


@pytest.mark.parametrize("invalid", [None, {}, {"headline": "do it"},
    {**_campaign(), "verification": "TBD"},
    {**_campaign(), "stages": _campaign()["stages"][:1]},
    {**_campaign(), "stages": [{"title": "Build", "action": "Implement", "evidence": ""}] * 3},
])
def test_incomplete_or_unavailable_campaign_preserves_existing_goal(monkeypatch, tmp_path, invalid):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    goals._DB_CACHE.clear()
    manager = goals.GoalManager("campaign-incomplete")
    manager.set("existing objective", contract=goals.GoalContract(verification="existing proof"))
    prior = asdict(manager.state)
    monkeypatch.setattr(goals, "draft_campaign", lambda prompt: invalid)
    result = _dispatch(manager)
    assert result.error and not result.prompt and not result.kickoff
    assert asdict(goals.load_goal(manager.session_id)) == prior


def test_missing_prompt_does_not_call_model_or_replace_goal(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    goals._DB_CACHE.clear()
    manager = goals.GoalManager("campaign-empty")
    manager.set("existing")
    monkeypatch.setattr(goals, "draft_campaign", lambda prompt: pytest.fail("should not draft"))
    result = _dispatch(manager, "   ")
    assert result.error and manager.state.goal == "existing"
