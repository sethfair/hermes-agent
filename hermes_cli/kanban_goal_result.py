"""Process-local evidence and disposition for Kanban goal worker continuations."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GoalTurnResult:
    response: str
    result: dict[str, Any] | None
    exit_code: int


@dataclass
class GoalLoopProgress:
    last_turn: GoalTurnResult
    turns_used: int = 1


@dataclass(frozen=True)
class GoalLoopResult:
    outcome: str
    turns_used: int
    reason: str
    last_turn: GoalTurnResult
    exit_code: int
    process_result: dict[str, Any] | None
    interrupt_session_end_required: bool = False


class GoalLoopInfrastructureError(Exception):
    def __init__(self, phase: str, exit_code: int, message: str):
        self.phase = phase
        self.exit_code = exit_code
        super().__init__(message)


def goal_loop_outcome(progress: GoalLoopProgress, outcome: str, reason: str,
                      exit_code: int = 0, *, interrupted: bool = False, preserve_raw: bool = False) -> GoalLoopResult:
    """Keep factual worker evidence separate from a process-error reporting overlay."""
    raw = progress.last_turn.result
    if exit_code and not preserve_raw:
        process_result = dict(raw or {})
        process_result.update(failed=True, completed=False, interrupted=exit_code == 130, error=reason)
    else:
        process_result = raw
    return GoalLoopResult(outcome, progress.turns_used, reason, progress.last_turn,
                          exit_code, process_result, interrupted)
