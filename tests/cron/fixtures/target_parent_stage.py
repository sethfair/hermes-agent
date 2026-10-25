"""Stage one issued root against the isolated archived parent Store and native board."""

import json
import os
from pathlib import Path
import sqlite3
import sys

from hermes_cli import kanban_db
import engineering_recovery_observer as legacy
from local_observation_v2 import Store


def main(execution_id, root, revision):
    home = Path(os.environ["HERMES_HOME"]).resolve()
    job_id = os.environ["WRI2276_SYNTHETIC_JOB_ID"]
    legacy.PROFILE_HOME = home
    legacy.JOB_ID = job_id
    directory = home / "cron" / f"recovery-{job_id}"
    with sqlite3.connect(f"file:{directory / 'local-observation-v2.sqlite'}?mode=ro", uri=True) as db:
        row = db.execute("SELECT payload FROM epoch_issued WHERE execution_id=? AND root=?",
                         (execution_id, root)).fetchone()
    if row is None:
        raise ValueError("no issued root")
    observation_id = json.loads(row[0])["observation_id"]
    result = Store(directory).stage_observation(
        execution_id, root, observation_id, int(revision),
        kanban_db.kanban_db_path(board=legacy.BOARD),
    )
    print(json.dumps({"state": "staged", "root": root, "revision": result["revision"]}))


if __name__ == "__main__":
    main(*sys.argv[1:])
