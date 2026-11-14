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

## Task 4–7 continuation evidence from HEAD `4715fea989`

| Increment | Phase | Command | Exit | Result |
|---|---|---|---:|---|
| C-4 | RED | `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_ready_monitor.py -k 'native_claim or resolver or schema or claim'` | 1 | Two test-seam failures: an uncommitted review fixture transaction and a redaction assertion matching the public stage label. No production failure. |
| C-4 | GREEN | `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_ready_monitor.py -k 'native_claim or resolver or schema or claim or replay'` | 0 | 6 passed: native Ready claim closure, review non-closure, bounded resolver scheduling, schema and replay contracts. |
| C-5/6/7 | GREEN | `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_ready_monitor.py -k 'native_claim or resolver or schema or claim or replay'` | 0 | Final focused monitor proof: 6 passed. |
| C-compile | GREEN | `python -m compileall -q hermes_cli/kanban_ready_monitor.py hermes_cli/kanban_db_dispatch.py gateway/kanban_watchers_dispatcher.py hermes_cli/kanban.py plugins/kanban/dashboard/plugin_api.py` | 0 | Touched production modules compile. |
| C-diff | GREEN | `git diff --check` and changed-line count | 0 | No whitespace errors; 304 changed lines, below the 2,400-line ceiling. |

Task 4–7 remains read-only at the consumer boundary: no delivery, acknowledgement, activation,
installation, restart, or Office mutation API was added. Gateway and standalone dispatch contexts carry
one boot writer/controller generation and exact board-scope activation metadata; CLI and dashboard
one-shot dispatch remain refused on protected scopes without that context.

Final attributable rerun:

- `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_ready_monitor.py tests/hermes_cli/test_kanban_ready_monitor_dispatch.py tests/plugins/test_kanban_dashboard_plugin.py tests/hermes_cli/test_kanban_cli.py -k 'ready_health or ready_monitor or protected or dispatch or parser or native_claim or resolver'` → exit `0` (`4 files, 10 tests passed`).
- `python scripts/check` → exit `2` before checks: environment has Ruff `0.15.10`, checkout requires `0.15.22`; no raw environment mutation performed.
- `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_ready_monitor.py -k 'native_claim or truthful_hold or resolver or replay'` → exit `0` (`5 passed`).

## Exact-head integrator verification

Exact head before publication: `ed825791e89e6db4e7380d45e94ea55d001bf726` (tree `0ee45424dc5e7cbbd858e70ae05c0f81aa6f4bf9`).

- `python scripts/check` → exit `0`; all 17 checks passed, including code-health ratchets.
- `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_ready_monitor.py tests/hermes_cli/test_kanban_ready_monitor_dispatch.py tests/hermes_cli/test_kanban_per_profile_cap.py tests/hermes_cli/test_kanban_host_cap.py tests/hermes_cli/test_kanban_dispatch_claim_allowlist.py tests/hermes_cli/test_kanban_dispatch_lock.py tests/hermes_cli/test_kanban_dispatch_tick_hook.py` → exit `0` (`46 passed`).
- `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_workflow.py tests/hermes_cli/test_kanban_review_lifecycle.py tests/hermes_cli/test_kanban_block_kinds.py tests/hermes_cli/test_kanban_complete_live_claim_guard.py tests/hermes_cli/test_kanban_transfer.py tests/plugins/test_kanban_dashboard_plugin.py tests/hermes_cli/test_config.py tests/hermes_cli/test_kanban_cli.py` → exit `0` (`281 passed`, `4 skipped` Windows-only).
- `scripts/run_tests.sh -j 1 tests/gateway/test_kanban_notifier_watcher_dispatch_gate.py tests/gateway/test_kanban_reconcile_orphans.py` → exit `0` (`11 passed`).
- The broader `tests/gateway -k 'kanban and dispatcher'` invocation exceeded the 420-second tool ceiling and was terminated without a summary; no green claim is made for that aggregate command.
- `tests/hermes_cli/test_kanban_notify.py` is blocked in this Kanban scratch location before notifier behavior: its import of `gateway.run` asks PM to probe the checkout-parent payload manifest at `/home/macpro/.hermes/kanban/workspaces/t_d7d45bf4/manifest.json`, and the suite's real-home I/O guard deliberately refuses every path under the live `.hermes` root. The other four files in that command passed (`58 tests`); the 14 notifier failures are environment-guard failures, not product assertions. No guard was bypassed and no test was marked or weakened.
- Final non-generated diff count: `1,409` additions plus deletions, below the reviewed `2,400` ceiling.

Activation remains OFF/pending/UNKNOWN. No install, restart, deployment, delivery, acknowledgement, alert, escalation, WIP display, Office change, credential change, or real-board mutation was performed.

## Final bounded correction verification

The correction uses the matching open attempt as a transaction-local provisional completion,
dispatches each protected profile partition with its own context/attempt, validates resolver and
writer contracts strictly, keeps incomplete public facts `UNKNOWN`, and records pseudonymous,
stage-explicit resolver intent events.

Exact head verification:

- `git rev-parse HEAD` → `a3bffc15f424841c5459d0f680180c72aa2b6989`
- `git diff main...HEAD --numstat | awk '{a+=$1; d+=$2} END {print a+d}'` → `2246` changed lines
- `git diff --check` → exit 0
- `python scripts/check` → exit 0 (17 checks)
- `scripts/run_tests.sh -j 1 tests/hermes_cli/test_kanban_ready_monitor.py tests/hermes_cli/test_kanban_ready_monitor_dispatch.py tests/hermes_cli/test_kanban_promote.py -k 'ready_monitor or protected or resolver or parser or promote_stuck'` → exit 0 (19 passed)

The broader aggregate was stopped after isolated workers stalled without assertion output; no
passing result is claimed for that aggregate or the dashboard file. The exact count remains below
the 2,400-line cap before publication.
