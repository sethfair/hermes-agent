"""AST census engine for Kanban storage writers."""

from __future__ import annotations

import ast
import itertools
import json
import re
from collections import defaultdict
from pathlib import Path

from .discovery import production_python_files
from .evaluator import MAX_VARIANTS, FiniteEvaluator, Values
from .model import InventoryItem, Problem
from .sql import protected_effects, statement_hash


_SINK_NAMES = {"execute", "executemany", "executescript"}
_PROTECTED_FACTORY = "_protected_connection"


def _function_name(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> str:
    names: list[str] = []
    current: ast.AST | None = node
    while current is not None:
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.append(current.name)
        current = parents.get(current)
    return ".".join(reversed(names)) or "<module>"


def _assignments(scope: ast.Module | ast.FunctionDef, parents: dict[ast.AST, ast.AST]) -> dict[str, list[ast.AST]]:
    result: dict[str, list[ast.AST]] = defaultdict(list)
    for node in ast.walk(scope):
        owner = parents.get(node)
        while owner is not None and not isinstance(owner, (ast.FunctionDef, ast.AsyncFunctionDef)):
            owner = parents.get(owner)
        if owner is not (scope if isinstance(scope, ast.FunctionDef) else None):
            continue
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            value = node.value
            if value is None:
                continue
            for target in targets:
                if isinstance(target, ast.Name):
                    result[target.id].append(value)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                and node.func.attr == "append" and len(node.args) == 1 \
                and isinstance(node.func.value, ast.Name):
            # Mark list growth distinctly so the evaluator can enumerate the
            # finite include/exclude variants without treating it as scalar
            # reassignment. Conditionality is intentionally conservative: an
            # append may be absent, so both paths are retained.
            result[node.func.value.id].append(ast.Starred(value=node.args[0], ctx=ast.Load()))
    return dict(result)


def _functions(tree: ast.Module) -> dict[str, ast.FunctionDef]:
    return {node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}


def _imported_module_aliases(tree: ast.Module, root: Path) -> set[str]:
    """Names proven to refer to modules rather than connection-like objects."""
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            aliases.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            base = root.joinpath(*node.module.split("."))
            for alias in node.names:
                if (base / f"{alias.name}.py").is_file() or (base / alias.name / "__init__.py").is_file():
                    aliases.add(alias.asname or alias.name)
    return aliases


def _imported_constant_assignments(tree: ast.Module, root: Path,
                                   parents: dict[ast.AST, ast.AST]) -> dict[str, dict[str, list[ast.AST]]]:
    """Materialize finite imported constants without importing or executing modules."""
    result: dict[str, dict[str, list[ast.AST]]] = defaultdict(dict)
    sql_names = {
        name.id
        for call in ast.walk(tree)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
        and call.func.attr in _SINK_NAMES and call.args
        for name in ast.walk(call.args[0]) if isinstance(name, ast.Name)
    }
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        selected = [
            alias for alias in node.names
            if alias.name.isupper() and (alias.asname or alias.name) in sql_names
        ]
        if not selected:
            continue
        candidate = root.joinpath(*node.module.split("."))
        path = candidate.with_suffix(".py")
        if not path.is_file():
            path = candidate / "__init__.py"
        if not path.is_file():
            continue
        try:
            imported_tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=path.as_posix())
        except (OSError, SyntaxError):
            continue
        imported_parents = {
            child: parent for parent in ast.walk(imported_tree) for child in ast.iter_child_nodes(parent)
        }
        assignments = _assignments(imported_tree, imported_parents)
        evaluator = FiniteEvaluator(assignments, _functions(imported_tree))
        for alias in selected:
            choices = assignments.get(alias.name, [])
            if not choices or any(isinstance(item, ast.Call) for choice in choices for item in ast.walk(choice)):
                continue
            strings: set[str] = set()
            for choice in choices:
                try:
                    value = evaluator.evaluate(choice)
                except RecursionError:
                    break
                if not value.resolved:
                    break
                strings.update(value.strings)
                if len(strings) > MAX_VARIANTS:
                    break
            else:
                if len(strings) <= MAX_VARIANTS:
                    result[_function_name(node, parents)][alias.asname or alias.name] = [
                        ast.Constant(value=string) for string in strings
                    ]
    return dict(result)


def _wrapper_summary(function: ast.FunctionDef) -> tuple[int, ast.AST] | None:
    parameters = [arg.arg for arg in function.args.args]
    for call in (node for node in ast.walk(function) if isinstance(node, ast.Call)):
        if isinstance(call.func, ast.Name) and call.func.id in parameters and call.args:
            return parameters.index(call.func.id), call.args[0]
        if isinstance(call.func, ast.Attribute) and call.func.attr in _SINK_NAMES and call.args:
            return -1, call.args[0]
    return None


def _closed_private_wrapper(function: ast.FunctionDef, tree: ast.Module,
                            parents: dict[ast.AST, ast.AST], evaluator: FiniteEvaluator) -> ast.AST | None:
    """Return the forwarded SQL node when every use is a finite local call."""
    if not function.name.startswith("_"):
        return None
    summary = _wrapper_summary(function)
    if summary is None:
        return None
    _, wrapped_sql = summary
    if not isinstance(wrapped_sql, ast.Name):
        return None
    parameters = [arg.arg for arg in function.args.args]
    try:
        sql_index = parameters.index(wrapped_sql.id)
    except ValueError:
        return None
    uses = [node for node in ast.walk(tree) if isinstance(node, ast.Name) and node.id == function.name]
    if not uses:
        return None
    for use in uses:
        call = parents.get(use)
        if not isinstance(call, ast.Call) or call.func is not use:
            return None
        argument = call.args[sql_index] if sql_index < len(call.args) else next(
            (keyword.value for keyword in call.keywords if keyword.arg == wrapped_sql.id), None,
        )
        if argument is None:
            return None
        try:
            resolved = evaluator.evaluate(argument).resolved
        except RecursionError:
            return None
        if not resolved:
            return None
    return wrapped_sql


def _private_call_environment(function: ast.FunctionDef, tree: ast.Module,
                              parents: dict[ast.AST, ast.AST],
                              evaluator: FiniteEvaluator) -> dict[str, Values]:
    """Infer private parameters only from complete, finite local call sites."""
    parameters = [arg.arg for arg in function.args.args]
    summary = _wrapper_summary(function)
    if not function.name.startswith("_") or not parameters or summary is None:
        return {}
    relevant = {
        node.id for node in ast.walk(summary[1])
        if isinstance(node, ast.Name) and node.id in parameters
    }
    if not relevant:
        return {}
    uses = [node for node in ast.walk(tree) if isinstance(node, ast.Name) and node.id == function.name]
    if not uses:
        return {}
    collected: dict[str, set[str]] = {parameter: set() for parameter in relevant}
    reasons: dict[str, str] = {}
    defaults = dict(zip(parameters[-len(function.args.defaults):], function.args.defaults, strict=False)) \
        if function.args.defaults else {}
    for use in uses:
        call = parents.get(use)
        if not isinstance(call, ast.Call) or call.func is not use:
            return {parameter: Values(reason=f"escaped private helper {function.name}") for parameter in relevant}
        keywords = {keyword.arg: keyword.value for keyword in call.keywords if keyword.arg is not None}
        for parameter in relevant:
            index = parameters.index(parameter)
            argument = call.args[index] if index < len(call.args) else keywords.get(parameter, defaults.get(parameter))
            if argument is None:
                reasons[parameter] = f"unbound private parameter {parameter}"
                continue
            try:
                value = evaluator.evaluate(argument)
            except RecursionError:
                value = Values(reason="recursive finite evaluation")
            if not value.resolved:
                reasons[parameter] = value.reason or "unresolved private parameter"
                continue
            collected[parameter].update(value.strings)
            if len(collected[parameter]) > MAX_VARIANTS:
                reasons[parameter] = "variant-overflow"
    return {
        parameter: Values(reason=reasons[parameter]) if parameter in reasons else Values(frozenset(values))
        for parameter, values in collected.items()
    }


def _closed_enum_environment(function: ast.FunctionDef, tree: ast.Module) -> dict[str, Values]:
    """Resolve identifier parameters only when code proves a closed table/column enum."""
    source = ast.unparse(function)
    if " not in TABLES" not in source:
        return {}
    tables: dict[str, set[str]] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not any(isinstance(t, ast.Name) and t.id == "TABLES" for t in node.targets):
            continue
        if not isinstance(node.value, ast.Dict):
            continue
        for key, value in zip(node.value.keys, node.value.values, strict=True):
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                return {}
            if not isinstance(value, (ast.Set, ast.List, ast.Tuple)):
                return {}
            columns = {item.value for item in value.elts if isinstance(item, ast.Constant) and isinstance(item.value, str)}
            if len(columns) != len(value.elts):
                return {}
            tables[key.value] = columns
    if not tables:
        return {}
    environment: dict[str, Values] = {}
    parameters = {arg.arg for arg in function.args.args}
    if "table" in parameters:
        environment["table"] = Values(frozenset(tables))
    if "column" in parameters:
        environment["column"] = Values(frozenset(column for columns in tables.values() for column in columns))
    return environment


def _loop_environments(node: ast.AST, parents: dict[ast.AST, ast.AST],
                       evaluator: FiniteEvaluator, base: dict[str, Values]) -> list[dict[str, Values]]:
    """Bind enclosing loop targets while preserving tuple-row correlation."""
    loops: list[ast.For] = []
    current = parents.get(node)
    while current is not None and not isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
        if isinstance(current, ast.For):
            loops.append(current)
        current = parents.get(current)
    environments = [dict(base)]
    for loop in reversed(loops):
        if isinstance(loop.target, ast.Name):
            for environment in environments:
                environment[loop.target.id] = evaluator.evaluate(loop.iter, environment)
            continue
        if not isinstance(loop.target, (ast.Tuple, ast.List)) or not all(
            isinstance(target, ast.Name) for target in loop.target.elts
        ):
            continue
        dict_items = isinstance(loop.iter, ast.Call) and isinstance(loop.iter.func, ast.Attribute) \
            and loop.iter.func.attr == "items" and not loop.iter.args \
            and isinstance(loop.iter.func.value, (ast.Name, ast.Dict))
        if dict_items:
            assert isinstance(loop.iter, ast.Call) and isinstance(loop.iter.func, ast.Attribute) \
                and isinstance(loop.iter.func.value, (ast.Name, ast.Dict))
            sources = evaluator.assignments.get(loop.iter.func.value.id, []) \
                if isinstance(loop.iter.func.value, ast.Name) else [loop.iter.func.value]
            rows = [
                ast.Tuple(elts=[key, value], ctx=ast.Load())
                for source in sources if isinstance(source, ast.Dict)
                for key, value in zip(source.keys, source.values, strict=True)
                if key is not None
            ]
            valid_sources = bool(sources) and all(
                isinstance(source, ast.Dict) and all(key is not None for key in source.keys)
                for source in sources
            )
        else:
            sources = evaluator.assignments.get(loop.iter.id, []) if isinstance(loop.iter, ast.Name) else [loop.iter]
            rows = [row for source in sources if isinstance(source, (ast.Tuple, ast.List)) for row in source.elts]
            valid_sources = bool(sources) and all(isinstance(source, (ast.Tuple, ast.List)) for source in sources)
        valid = valid_sources and all(
            isinstance(row, (ast.Tuple, ast.List)) and len(row.elts) == len(loop.target.elts) for row in rows
        )
        if not valid:
            for environment in environments:
                for target in loop.target.elts:
                    assert isinstance(target, ast.Name)
                    environment[target.id] = Values(reason="unresolved tuple loop")
            continue
        expanded: list[dict[str, Values]] = []
        for environment in environments:
            for row in rows:
                assert isinstance(row, (ast.Tuple, ast.List))
                values = [evaluator.evaluate(item, environment) for item in row.elts]
                if any(not value.resolved for value in values):
                    unresolved = next(value for value in values if not value.resolved)
                    variant = dict(environment)
                    for target in loop.target.elts:
                        assert isinstance(target, ast.Name)
                        variant[target.id] = unresolved
                    expanded.append(variant)
                    continue
                for combination in itertools.product(*(value.strings for value in values)):
                    variant = dict(environment)
                    for target, value in zip(loop.target.elts, combination, strict=True):
                        assert isinstance(target, ast.Name)
                        variant[target.id] = Values(frozenset({value}))
                    expanded.append(variant)
                    if len(expanded) > MAX_VARIANTS:
                        overflow = dict(base)
                        for target in loop.target.elts:
                            assert isinstance(target, ast.Name)
                            overflow[target.id] = Values(reason="variant-overflow")
                        return [overflow]
        environments = expanded
    return environments


def _evaluate_in_loops(node: ast.AST, call: ast.Call, parents: dict[ast.AST, ast.AST],
                       evaluator: FiniteEvaluator, base: dict[str, Values]) -> Values:
    output: set[str] = set()
    for environment in _loop_environments(call, parents, evaluator, base):
        value = evaluator.evaluate(node, environment)
        if not value.resolved:
            return value
        output.update(value.strings)
        if len(output) > MAX_VARIANTS:
            return Values(reason="variant-overflow")
    return Values(frozenset(output))


def _import_problems(tree: ast.Module, relative: str) -> list[Problem]:
    problems: list[Problem] = []
    if relative != "hermes_cli/kanban_db_task_state.py":
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "hermes_cli.kanban_db_connect":
                if any(alias.name == _PROTECTED_FACTORY for alias in node.names):
                    problems.append(Problem(relative, node.lineno, "protected-factory-import",
                                            "private protected connection factory imported outside task-state owner"))
    return problems


def _raw_connect_problems(tree: ast.Module, source: str, relative: str) -> list[Problem]:
    module_aliases = {"sqlite3"}
    direct_aliases: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "sqlite3":
                    module_aliases.add(alias.asname or alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module == "sqlite3":
            direct_aliases.update(alias.asname or alias.name for alias in node.names if alias.name == "connect")
    problems: list[Problem] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        raw = (isinstance(node.func, ast.Name) and node.func.id in direct_aliases) or (
            isinstance(node.func, ast.Attribute) and node.func.attr == "connect"
            and isinstance(node.func.value, ast.Name) and node.func.value.id in module_aliases
        )
        if not raw:
            continue
        segment = ast.get_source_segment(source, node) or ""
        read_only = "mode=ro" in segment and any(
            keyword.arg == "uri" and isinstance(keyword.value, ast.Constant) and keyword.value.value is True
            for keyword in node.keywords
        )
        if not read_only and ("kanban" in relative.lower() or "kanban" in segment.lower()):
            problems.append(Problem(relative, node.lineno, "raw-writable-connect",
                                    "raw writable sqlite connection can bypass the Kanban boundary"))
    return problems


def _sink_argument(call: ast.Call, aliases: dict[str, tuple[bool, bool]],
                   functions: dict[str, ast.FunctionDef], parameters: set[str]) -> tuple[ast.AST, bool] | None:
    if isinstance(call.func, ast.Attribute) and call.func.attr in _SINK_NAMES and call.args:
        return call.args[0], False
    if isinstance(call.func, ast.Name) and call.func.id in aliases and call.args:
        return call.args[0], aliases[call.func.id][1]
    if not isinstance(call.func, ast.Name) or call.func.id not in functions:
        return None
    summary = _wrapper_summary(functions[call.func.id])
    if summary is None:
        return None
    executor_index, wrapped_sql = summary
    callee_parameters = [arg.arg for arg in functions[call.func.id].args.args]
    if executor_index >= 0 and executor_index < len(call.args):
        executor = call.args[executor_index]
        is_executor = (isinstance(executor, ast.Attribute) and executor.attr in _SINK_NAMES) or (
            isinstance(executor, ast.Name) and executor.id in aliases)
        if not is_executor:
            return None
    if isinstance(wrapped_sql, ast.Name) and wrapped_sql.id in callee_parameters:
        index = callee_parameters.index(wrapped_sql.id)
        if index < len(call.args):
            return call.args[index], False
    return None


def _scan_scope(scope: ast.Module | ast.FunctionDef, tree: ast.Module,
                parents: dict[ast.AST, ast.AST], functions: dict[str, ast.FunctionDef],
                module_assignments: dict[str, list[ast.AST]], relative: str,
                kanban_relevant: bool, module_aliases: set[str],
                imported_assignments: dict[str, dict[str, list[ast.AST]]]) -> tuple[list[InventoryItem], list[Problem]]:
    function = scope if isinstance(scope, ast.FunctionDef) else None
    owner = _function_name(scope, parents) if function else "<module>"
    assignments = {name: list(values) for name, values in module_assignments.items()}
    if function:
        for name, values in _assignments(function, parents).items():
            assignments.setdefault(name, []).extend(values)
        for name, values in imported_assignments.get(owner, {}).items():
            assignments.setdefault(name, values)
    evaluator = FiniteEvaluator(assignments, functions)
    parameters = {arg.arg for arg in function.args.args} if function else set()
    environment = _closed_enum_environment(function, tree) if function else {}
    if function:
        environment.update(_private_call_environment(
            function, tree, parents, FiniteEvaluator(module_assignments, functions),
        ))
    closed_wrapper_sql = _closed_private_wrapper(function, tree, parents, evaluator) if function else None
    aliases: dict[str, tuple[bool, bool]] = {}
    for node in ast.walk(scope):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Attribute) \
                and node.value.attr in _SINK_NAMES:
            uncertain = isinstance(node.value.value, ast.Name) and node.value.value.id not in parameters
            aliases.update((target.id, (True, uncertain)) for target in node.targets if isinstance(target, ast.Name))
    calls = [node for node in ast.walk(scope) if isinstance(node, ast.Call)
             and _function_name(node, parents) == owner]
    inventory: list[InventoryItem] = []
    problems: list[Problem] = []
    ordinal = 0
    for call in sorted(calls, key=lambda item: (item.lineno, item.col_offset)):
        if isinstance(call.func, ast.Name) and call.func.id in parameters:
            continue
        if isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name) \
                and call.func.value.id in module_aliases:
            continue
        sink = _sink_argument(call, aliases, functions, parameters)
        if sink is None:
            continue
        sql_node, uncertain_alias = sink
        if sql_node is closed_wrapper_sql:
            continue
        ordinal += 1
        if uncertain_alias:
            problems.append(Problem(relative, call.lineno, "unresolved-alias",
                                    "execute alias receiver is not a function parameter"))
            continue
        values = _evaluate_in_loops(sql_node, call, parents, evaluator, environment)
        if not values.resolved:
            if kanban_relevant:
                kind = "variant-overflow" if values.reason == "variant-overflow" else "unresolved-sql"
                problems.append(Problem(relative, call.lineno, kind, values.reason or "unresolved SQL"))
            continue
        for statement in sorted(values.strings):
            effects = protected_effects(statement)
            if effects:
                inventory.append(InventoryItem(
                    relative, _function_name(call, parents), ordinal,
                    statement_hash(statement), effects, "finite SQL reaches protected Kanban effects",
                ))
    return inventory, problems


def _scan_file(root: Path, path: Path) -> tuple[list[InventoryItem], list[Problem]]:
    relative = path.relative_to(root).as_posix()
    try:
        source = path.read_text(encoding="utf-8-sig")
        tree = ast.parse(source, filename=relative)
    except (OSError, SyntaxError) as exc:
        line = exc.lineno if isinstance(exc, SyntaxError) and exc.lineno else 0
        return [], [Problem(relative, line, "parse-error", str(exc))]
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    functions = _functions(tree)
    problems = _import_problems(tree, relative) + _raw_connect_problems(tree, source, relative)
    inventory: list[InventoryItem] = []
    kanban_relevant = "kanban" in relative.lower() or bool(re.search(
        r"hermes_cli(?:[ ]+import[ ]+|[.])kanban|(?:^|[^A-Za-z0-9_])(?:tasks|task_runs|task_links|task_events|task_transition_receipts)(?:$|[^A-Za-z0-9_])",
        source,
    ))

    module_assignments = _assignments(tree, parents)
    imported_assignments = _imported_constant_assignments(tree, root, parents)
    for name, values in imported_assignments.get("<module>", {}).items():
        module_assignments.setdefault(name, values)
    module_aliases = _imported_module_aliases(tree, root)
    for scope in [tree, *functions.values()]:
        found, refused = _scan_scope(
            scope, tree, parents, functions, module_assignments, relative, kanban_relevant,
            module_aliases, imported_assignments,
        )
        inventory.extend(found)
        problems.extend(refused)
    return inventory, problems


def _load_allowlist(path: Path | None) -> tuple[list[dict[str, object]], list[Problem]]:
    if path is None:
        return [], []
    try:
        document = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        return [], [Problem(path.as_posix(), 0, "invalid-allowlist", str(exc))]
    if not isinstance(document, dict) or document.get("version") != 1 or not isinstance(document.get("entries"), list):
        return [], [Problem(path.as_posix(), 0, "invalid-allowlist", "expected version 1 and entries list")]
    entries = document["entries"]
    if not all(isinstance(entry, dict) for entry in entries):
        return [], [Problem(path.as_posix(), 0, "invalid-allowlist", "every entry must be an object")]
    return entries, []


def _entry_key(entry: dict[str, object]) -> str:
    selected = {key: entry.get(key) for key in (
        "path", "function", "sink_ordinal", "statement_hash", "effects", "rationale",
    )}
    return json.dumps(selected, sort_keys=True, separators=(",", ":"))


def analyze_repository(root: Path, allowlist_path: Path | None = None) -> dict[str, list[dict[str, object]]]:
    files, discovery_errors = production_python_files(root)
    inventory: list[InventoryItem] = []
    problems = [Problem("pyproject.toml", 0, "discovery-error", error) for error in discovery_errors]
    for path in files:
        found, refused = _scan_file(root, path)
        inventory.extend(found)
        problems.extend(refused)
    inventory.sort()

    allowlist, allowlist_problems = _load_allowlist(allowlist_path)
    problems.extend(allowlist_problems)
    inventory_dicts = [item.as_dict() for item in inventory]
    actual = {_entry_key(item): item for item in inventory_dicts}
    allowed: dict[str, dict[str, object]] = {}
    for entry in allowlist:
        key = _entry_key(entry)
        if key in allowed:
            problems.append(Problem(str(allowlist_path), 0, "duplicate-allowlist", key))
        allowed[key] = entry
    for key, item in actual.items():
        if key not in allowed:
            problems.append(Problem(str(item["path"]), 0, "unallowlisted-writer", key))
    for key, entry in allowed.items():
        if key not in actual:
            problems.append(Problem(str(entry.get("path", allowlist_path)), 0, "stale-allowlist", key))
    problems.sort()
    return {"inventory": inventory_dicts, "problems": [problem.as_dict() for problem in problems]}
