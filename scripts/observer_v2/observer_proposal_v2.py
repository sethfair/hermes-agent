"""Read-only staged-root proposal for the original firer's child process.

The parent independently verifies every proposed root and commits the outcome.
This child has no observer checked-write route.
"""
from __future__ import annotations

import os

import engineering_recovery_observer as legacy
import local_observation_v2 as v2


def read_proposal(directory, execution_id, epoch_id):
    if (not isinstance(execution_id, str) or not execution_id
            or not isinstance(epoch_id, str) or not v2.HEX.fullmatch(epoch_id)):
        raise v2.Stale("invalid proposal selectors")
    store = v2.Store(directory)
    with store._connection() as (db, metadata):
        if (metadata["execution_id"] != execution_id
                or metadata["epoch_id"] != epoch_id
                or metadata["state"] != "provisional"):
            raise v2.Stale("proposal epoch not current")
        roots = []
        for root, raw in db.execute("SELECT root,payload FROM stages WHERE execution_id=? ORDER BY root COLLATE BINARY",
                                    (execution_id,)):
            stage = v2.strict_json(raw)
            if (stage.get("root") != root or stage.get("execution_id") != execution_id
                    or stage.get("epoch_id") != epoch_id or not v2.ROOT.fullmatch(root)
                    or roots and root <= roots[-1]):
                raise v2.Stale("proposal stage identity changed")
            roots.append(root)
            if len(roots) > v2.MAX_CURRENT:
                raise v2.Overflow("proposal stage cardinality exceeded")
        return v2.stage_proposal(execution_id, epoch_id, roots)


def main():
    if os.environ.get("HERMES_LOCAL_OBSERVATION_JOB_ID") != legacy.JOB_ID:
        raise v2.Stale("proposal job not scoped")
    directory = legacy._checked_home() / "cron" / f"recovery-{legacy.JOB_ID}"
    result = read_proposal(directory, os.environ.get("HERMES_LOCAL_OBSERVATION_EXECUTION_ID"),
                           os.environ.get("HERMES_LOCAL_OBSERVATION_EPOCH_ID"))
    print(v2.encode(result).decode("ascii"))


if __name__ == "__main__":
    main()
