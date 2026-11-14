# Implementation evidence

Plan: `/home/macpro/.hermes/kanban/attachments/t_59d54268/corrected-aged-ready-alert-escalation-plan-r2.md`
Plan SHA-256: `d574e25882aacd8a48d8a8e62400d1686e188959b296f49122c85b390ef9d3a6`
Pinned/base HEAD: `f92d19d6d2f0c7e3aa7d0f656b2e023d43316ac4`
Owner: Owen (sole integrator); executor branch: `feat/aged-ready-core-monitoring`

## RED/GREEN log

| Increment | Phase | Command | Exit | Commit/tree |
|---|---|---|---:|---|
| A-1A | RED (intentional missing-interface probe) | `scripts/run_tests.sh tests/hermes_cli/test_kanban_ready_monitor.py -k 'generation or writer or fingerprint'` | 1 (ImportError: future module absent; first attempt was PRETEST during environment bootstrap) | base `f92d19d6d2f0c7e3aa7d0f656b2e023d43316ac4`, tree `bc57514be217ffb500696374a02156c2bc4458d5` |
| A-1A | GREEN | `scripts/run_tests.sh tests/hermes_cli/test_kanban_ready_monitor.py -k 'generation or writer or fingerprint'` | 0 (1 passed) | pre-commit tree recorded below |
| A-schema | GREEN/regression | `scripts/run_tests.sh tests/hermes_cli/test_kanban_db_init.py tests/hermes_cli/test_kanban_db_repair.py -k 'migration or schema or init'` | 0 (5 passed) | pre-commit tree recorded below |

The initial runner bootstrap did not reach pytest and is classified PRETEST. The subsequent
run reached pytest and observed the planned absent-interface failure.

## Task 2/3 continuation evidence (exact commands)

| Increment | Phase | Command | Exit | Result |
|---|---|---|---:|---|
| B-2 | RED | `scripts/run_tests.sh tests/hermes_cli/test_kanban_ready_monitor.py -k 'schema or claim or admission or generation'` | 1 | 1 passed, 3 behavioral failures: missing additive tables and activation API |
| B-2 | GREEN | `scripts/run_tests.sh tests/hermes_cli/test_kanban_ready_monitor.py -k 'schema or claim or admission or generation'` | 0 | 4 passed |
| B-3 | GREEN | `scripts/run_tests.sh tests/hermes_cli/test_kanban_ready_monitor.py tests/hermes_cli/test_kanban_ready_monitor_dispatch.py` | 0 | 5 passed, 1 initial test corrected to include its required committed Ready event |
| B-2/3 | GREEN | `scripts/run_tests.sh tests/hermes_cli/test_kanban_transfer.py -k 'export or import or roundtrip'` | 0 | 6 passed |
| B-2 | GREEN | `scripts/run_tests.sh tests/hermes_cli/test_kanban_db_init.py tests/hermes_cli/test_kanban_db_repair.py -k 'schema or migration or init'` | 0 | 3 passed in the selected files |
| B-3 | GREEN | `scripts/run_tests.sh tests/hermes_cli/test_kanban_dispatch_lock.py tests/hermes_cli/test_kanban_per_profile_cap.py` | 0 | 4 passed |
| B-2/3 | GREEN | `python -m compileall -q hermes_cli/kanban_ready_monitor.py hermes_cli/kanban_db.py hermes_cli/kanban_db_connect.py hermes_cli/kanban_db_dispatch.py hermes_cli/kanban_transfer.py tests/hermes_cli/test_kanban_ready_monitor.py tests/hermes_cli/test_kanban_ready_monitor_dispatch.py` | 0 | Compile check passed |

The combined nine-file runner invocation was interrupted by the runner leaving an orphaned
subprocess while the transfer file was executing; it is not claimed as passing evidence. The
individual transfer and dispatcher/database commands above are the attributable regressions.

Final attributable reruns:

- `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_ready_monitor.py tests/hermes_cli/test_kanban_ready_monitor_dispatch.py` → exit `0` (`6 passed`).
- `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_db_init.py tests/hermes_cli/test_kanban_db_repair.py tests/hermes_cli/test_kanban_transfer.py -k 'schema or migration or init or export or import or roundtrip'` → exit `0` (`11 passed`).
- `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_dispatch_claim_allowlist.py tests/hermes_cli/test_kanban_dispatch_lock.py tests/hermes_cli/test_kanban_per_profile_cap.py -k 'dispatch or cap or lock or allowlist'` → partial output before the runner orphaned its final subprocess; not claimed as a passing command. Earlier attributable result: allowlist `17 passed`, dispatch lock `2 passed`, per-profile cap `2 passed`.
- `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_ready_monitor.py tests/hermes_cli/test_kanban_ready_monitor_dispatch.py tests/plugins/test_kanban_dashboard_plugin.py -k 'dispatch or ready_monitor or protected'` → exit `0` (`8 passed`).
