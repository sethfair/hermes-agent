"""Synthetic behavior contracts for the fail-closed Kanban storage writer census."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parents[2]
_CHECK = REPO / "scripts" / "check_kanban_storage_writers.py"
_PYPROJECT = """
[project]
name = "fixture"
version = "0"
[project.scripts]
fixture = "rootmod:main"
[tool.setuptools.packages.find]
include = ["app", "app.*", "plugins", "plugins.*"]
"""


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    source = {
        "pyproject.toml": _PYPROJECT,
        "setup.py": "def _root_py_modules(): ...\n",
        "app/__init__.py": "",
        "plugins/__init__.py": "",
        "rootmod.py": "def main(): pass\n",
        **files,
    }
    for relative, text in source.items():
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True, timeout=30)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True, timeout=30)
    return tmp_path


def _run(repo: Path, allowlist: Path | None = None) -> tuple[subprocess.CompletedProcess[str], dict]:
    command = [sys.executable, str(_CHECK), "--root", str(repo), "--json"]
    if allowlist is not None:
        command += ["--allowlist", str(allowlist)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
    output = result.stdout + result.stderr
    assert "not implemented" not in output
    assert result.stdout.startswith("{")
    return result, json.loads(result.stdout)


def _write_allowlist(path: Path, entries: list[dict]) -> Path:
    path.write_text(json.dumps({"version": 1, "entries": entries}), encoding="utf-8")
    return path


def test_check_registers_kanban_storage_writer_guard():
    result = subprocess.run(
        [sys.executable, "scripts/check", "--only", "kanban-storage-writers"],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    output = result.stdout + result.stderr
    assert "unknown or no check name" not in output
    assert "kanban-storage-writers" in output
    assert result.returncode != 2


def test_inventory_resolves_alias_wrappers_and_indirect_execute(tmp_path):
    repo = _repo(tmp_path, {
        "app/writers.py": """
SQL = "UPDATE tasks SET status = ? WHERE id = ?"
def wrapper(run, statement):
    run(statement, ("blocked", "t1"))
def write(conn):
    execute = conn.execute
    wrapper(execute, SQL)
""",
    })
    result, report = _run(repo)
    assert result.returncode == 1
    assert len(report["inventory"]) == 1
    item = report["inventory"][0]
    assert item["path"] == "app/writers.py"
    assert item["function"] == "write"
    assert item["sink_ordinal"] == 1
    assert item["effects"] == ["tasks.update.status"]
    assert len(item["statement_hash"]) == 64


@pytest.mark.parametrize("builder", [
    'PREFIX + " status = ? WHERE id = ?"',
    '"UPDATE %s SET status = ? WHERE id = ?" % "tasks"',
    '"UPDATE {} SET status = ? WHERE id = ?".format("tasks")',
    'f"UPDATE {TABLE} SET status = ? WHERE id = ?"',
    '" ".join(["UPDATE", "tasks", "SET status = ? WHERE id = ?"])',
    'statement("tasks")',
])
def test_finite_sql_construction_is_resolved(tmp_path, builder):
    repo = _repo(tmp_path, {
        "app/writers.py": f'''TABLE = "tasks"
PREFIX = "UPDATE tasks SET"
def statement(table):
    return "UPDATE " + table + " SET status = ? WHERE id = ?"
def write(conn):
    sql = {builder}
    conn.execute(sql)
''',
    })
    result, report = _run(repo)
    assert result.returncode == 1
    assert report["inventory"][0]["effects"] == ["tasks.update.status"]
    assert not [problem for problem in report["problems"] if problem["kind"] == "unresolved-sql"]


def test_finite_sql_resolves_keyword_format_arguments(tmp_path):
    repo = _repo(tmp_path, {
        "app/writers.py": '''
INDEX_SQL = "CREATE {if_not_exists}INDEX task_status ON tasks(status)"
def write(conn):
    conn.execute(INDEX_SQL.format(if_not_exists="IF NOT EXISTS "))
''',
    })

    result, report = _run(repo)

    assert result.returncode == 0
    assert report["inventory"] == []
    assert report["problems"] == []


@pytest.mark.parametrize("placeholder_builder", [
    '",".join("?" * len(task_ids))',
    '",".join(["?"] * len(task_ids))',
    '", ".join("?" for _ in task_ids)',
])
def test_runtime_placeholder_count_does_not_hide_fixed_protected_effect(tmp_path, placeholder_builder):
    repo = _repo(tmp_path, {
        "app/writers.py": f'''
from hermes_cli import kanban_db
def write(conn, task_ids):
    placeholders = {placeholder_builder}
    conn.execute(f"UPDATE tasks SET status='triage' WHERE id IN ({{placeholders}})", task_ids)
''',
    })

    result, report = _run(repo)

    assert result.returncode == 1
    assert report["inventory"][0]["effects"] == ["tasks.update.status"]
    assert not [problem for problem in report["problems"] if problem["kind"] == "unresolved-sql"]


def test_finite_list_appends_resolve_all_protected_sql_variants(tmp_path):
    repo = _repo(tmp_path, {
        "app/writers.py": '''
from hermes_cli import kanban_db
def write(conn, title=None, body=None):
    assignments = ["status = 'todo'"]
    if title is not None:
        assignments.append("title = ?")
    if body is not None:
        assignments.append("body = ?")
    conn.execute(f"UPDATE tasks SET {', '.join(assignments)} WHERE id = ?")
''',
    })

    result, report = _run(repo)

    assert result.returncode == 1
    assert len(report["inventory"]) == 4
    assert {tuple(item["effects"]) for item in report["inventory"]} == {
        ("tasks.update.status",),
    }
    assert not [problem for problem in report["problems"] if problem["kind"] == "unresolved-sql"]


def test_finite_branches_and_cte_dml_are_all_inventoried(tmp_path):
    repo = _repo(tmp_path, {
        "app/writers.py": """
def write(conn, replace):
    sql = ("WITH chosen AS (SELECT 1) UPDATE tasks SET status='blocked'" 
           if replace else 'InSeRt Or RePlAcE INTO "task_events" (task_id) VALUES (?)')
    conn.execute(sql)
""",
    })
    result, report = _run(repo)
    assert result.returncode == 1
    assert {tuple(item["effects"]) for item in report["inventory"]} == {
        ("task_events.insert",), ("tasks.update.status",),
    }


def test_insert_upsert_delete_and_trigger_effects_are_classified(tmp_path):
    repo = _repo(tmp_path, {
        "plugins/demo/writers.py": """
def write(conn):
    conn.executescript('''
      INSERT INTO tasks (id, status) VALUES ('t', 'ready')
        ON CONFLICT(id) DO UPDATE SET status='blocked';
      DELETE FROM task_links WHERE child_id='t';
      CREATE TRIGGER task_guard AFTER UPDATE ON tasks
        BEGIN INSERT INTO task_events(task_id) VALUES (NEW.id); END;
    ''')
    conn.executemany("INSERT INTO task_events(task_id) VALUES (?)", [("t",)])
""",
    })
    result, report = _run(repo)
    assert result.returncode == 1
    effects = {effect for item in report["inventory"] for effect in item["effects"]}
    assert {"tasks.insert", "tasks.update.status", "task_links.delete",
            "schema.trigger.tasks", "task_events.insert"} <= effects
    assert {item["path"] for item in report["inventory"]} == {"plugins/demo/writers.py"}


def test_root_modules_and_packaged_plugin_roots_are_scanned_but_untracked_files_are_not(tmp_path):
    repo = _repo(tmp_path, {
        "rootmod.py": "def main(conn): conn.execute(\"UPDATE tasks SET status='blocked'\")\n",
        "plugins/demo/writer.py": "def write(conn): conn.execute(\"DELETE FROM tasks\")\n",
        "outside/writer.py": "def write(conn): conn.execute(\"DELETE FROM tasks\")\n",
    })
    (repo / "app/untracked.py").write_text(
        "def write(conn): conn.execute(\"DELETE FROM tasks\")\n", encoding="utf-8")
    result, report = _run(repo)
    assert result.returncode == 1
    assert {item["path"] for item in report["inventory"]} == {
        "plugins/demo/writer.py", "rootmod.py",
    }


@pytest.mark.parametrize(("source", "kind"), [
    ("def broken(:\n", "parse-error"),
    ("from hermes_cli import kanban_db\ndef write(conn, sql): conn.execute(sql)\n", "unresolved-sql"),
    ("from hermes_cli import kanban_db\ndef write(conn, table): conn.execute(f'UPDATE {table} SET status=1')\n", "unresolved-sql"),
    ("from hermes_cli import kanban_db\ndef write(conn):\n    sink = mystery.execute\n    sink(unknown)\n", "unresolved-alias"),
])
def test_parse_dynamic_identifier_and_alias_fail_closed(tmp_path, source, kind):
    repo = _repo(tmp_path, {"app/writers.py": source})
    result, report = _run(repo)
    assert result.returncode == 1
    assert kind in {problem["kind"] for problem in report["problems"]}


def test_non_sql_execute_receivers_are_not_treated_as_database_sinks(tmp_path):
    repo = _repo(tmp_path, {
        "agent/__init__.py": "",
        "agent/relay_tools.py": "def execute(command): return command\n",
        "app/writers.py": '''
from hermes_cli import kanban_db
from agent import relay_tools
def dispatch(command):
    return relay_tools.execute(command)
''',
    })

    result, report = _run(repo)

    assert result.returncode == 0
    assert report["inventory"] == []
    assert report["problems"] == []


def test_variant_overflow_fails_closed(tmp_path):
    choices = ", ".join(repr(f"UPDATE tasks SET status='{index}'") for index in range(70))
    repo = _repo(tmp_path, {
        "app/writers.py": f"def write(conn, i):\n    sql = [{choices}][i]\n    conn.execute(sql)\n",
    })
    result, report = _run(repo)
    assert result.returncode == 1
    assert "variant-overflow" in {problem["kind"] for problem in report["problems"]}


def test_raw_writable_connect_and_protected_factory_import_are_refused(tmp_path):
    repo = _repo(tmp_path, {
        "app/writers.py": """
import sqlite3 as sql
from hermes_cli.kanban_db_connect import _protected_connection as privileged
def write(path):
    return sql.connect(path / "kanban.db"), privileged
""",
    })
    result, report = _run(repo)
    assert result.returncode == 1
    assert {problem["kind"] for problem in report["problems"]} >= {
        "raw-writable-connect", "protected-factory-import",
    }


def test_exact_allowlist_accepts_only_matching_inventory_and_rejects_stale_or_duplicate(tmp_path):
    repo = _repo(tmp_path, {
        "app/writers.py": "def write(conn): conn.execute(\"UPDATE tasks SET status='blocked'\")\n",
    })
    _, first = _run(repo)
    entry = first["inventory"][0]

    allowed = _write_allowlist(tmp_path / "allowed.json", [entry])
    result, report = _run(repo, allowed)
    assert result.returncode == 0
    assert report["problems"] == []

    stale_entry = {**entry, "statement_hash": "0" * 64}
    stale = _write_allowlist(tmp_path / "stale.json", [stale_entry])
    result, report = _run(repo, stale)
    assert result.returncode == 1
    assert {problem["kind"] for problem in report["problems"]} >= {
        "unallowlisted-writer", "stale-allowlist",
    }

    duplicate = _write_allowlist(tmp_path / "duplicate.json", [entry, entry])
    result, report = _run(repo, duplicate)
    assert result.returncode == 1
    assert "duplicate-allowlist" in {problem["kind"] for problem in report["problems"]}


def test_utf8_bom_in_pyproject_does_not_hide_production_writers(tmp_path):
    repo = _repo(tmp_path, {
        "app/writers.py": "def write(conn): conn.execute(\"UPDATE tasks SET status='blocked'\")\n",
    })
    (repo / "pyproject.toml").write_text(_PYPROJECT, encoding="utf-8-sig")

    result, report = _run(repo)

    assert result.returncode == 1
    assert report["inventory"][0]["path"] == "app/writers.py"
    assert not [problem for problem in report["problems"] if problem["kind"] == "discovery-error"]


def test_utf8_bom_in_exact_allowlist_is_accepted(tmp_path):
    repo = _repo(tmp_path, {
        "app/writers.py": "def write(conn): conn.execute(\"UPDATE tasks SET status='blocked'\")\n",
    })
    _, first = _run(repo)
    allowlist = tmp_path / "allowed.json"
    allowlist.write_text(
        json.dumps({"version": 1, "entries": first["inventory"]}),
        encoding="utf-8-sig",
    )

    result, report = _run(repo, allowlist)

    assert result.returncode == 0
    assert report["problems"] == []


def test_non_target_write_requires_a_closed_table_and_column_enum(tmp_path):
    repo = _repo(tmp_path, {
        "app/writers.py": """
from hermes_cli import kanban_db
TABLES = {"settings": {"value"}}
def set_value(conn, table, column):
    if table not in TABLES or column not in TABLES[table]:
        raise ValueError
    conn.execute(f"UPDATE {table} SET {column} = ?", ("x",))
""",
    })
    result, report = _run(repo)
    assert result.returncode == 0
    assert report["inventory"] == []
    assert report["problems"] == []


def test_inventory_is_deterministic(tmp_path):
    repo = _repo(tmp_path, {
        "app/z.py": "def z(conn): conn.execute(\"DELETE FROM tasks\")\n",
        "app/a.py": "def a(conn): conn.execute(\"UPDATE tasks SET status='ready'\")\n",
    })
    first_result, first = _run(repo)
    second_result, second = _run(repo)
    assert first_result.returncode == second_result.returncode == 1
    assert first == second
    assert [(row["path"], row["sink_ordinal"]) for row in first["inventory"]] == [
        ("app/a.py", 1), ("app/z.py", 1),
    ]
