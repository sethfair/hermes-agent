"""Finite AST evaluator for SQL assembled from closed Python values."""

from __future__ import annotations

import ast
import itertools
from dataclasses import dataclass


MAX_VARIANTS = 64


@dataclass(frozen=True)
class Values:
    strings: frozenset[str] = frozenset()
    reason: str | None = None

    @property
    def resolved(self) -> bool:
        return self.reason is None


class FiniteEvaluator:
    def __init__(self, assignments: dict[str, list[ast.AST]], functions: dict[str, ast.FunctionDef]):
        self.assignments = assignments
        self.functions = functions

    def evaluate(self, node: ast.AST, env: dict[str, Values] | None = None,
                 stack: tuple[str, ...] = ()) -> Values:
        env = env or {}
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return Values(frozenset({node.value}))
        if isinstance(node, ast.Name):
            return self._name(node, env, stack)
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            return self._union(self.evaluate(item, env, stack) for item in node.elts)
        if isinstance(node, ast.Subscript) and isinstance(node.value, (ast.List, ast.Tuple)):
            return self._subscript(node, env, stack)
        if isinstance(node, ast.IfExp):
            return self._union((self.evaluate(node.body, env, stack),
                                self.evaluate(node.orelse, env, stack)))
        if isinstance(node, ast.BinOp):
            return self._binop(node, env, stack)
        if isinstance(node, ast.JoinedStr):
            return self._joined(node, env, stack)
        if isinstance(node, ast.Call):
            return self._call(node, env, stack)
        return Values(reason=f"unsupported {type(node).__name__}")

    def _name(self, node: ast.Name, env: dict[str, Values], stack: tuple[str, ...]) -> Values:
        if node.id in env:
            return env[node.id]
        choices = self.assignments.get(node.id, [])
        if not choices:
            return Values(reason=f"input-derived name {node.id}")
        return self._union(self.evaluate(choice, env, stack) for choice in choices)

    def _subscript(self, node: ast.Subscript, env: dict[str, Values],
                   stack: tuple[str, ...]) -> Values:
        assert isinstance(node.value, (ast.List, ast.Tuple))
        if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, int):
            try:
                return self.evaluate(node.value.elts[node.slice.value], env, stack)
            except IndexError:
                return Values(reason="subscript outside finite sequence")
        if len(node.value.elts) > MAX_VARIANTS:
            return Values(reason="variant-overflow")
        return self._union(self.evaluate(item, env, stack) for item in node.value.elts)

    def _binop(self, node: ast.BinOp, env: dict[str, Values], stack: tuple[str, ...]) -> Values:
        left, right = self.evaluate(node.left, env, stack), self.evaluate(node.right, env, stack)
        if not left.resolved or not right.resolved:
            return Values(reason=left.reason or right.reason)
        try:
            if isinstance(node.op, ast.Add):
                return self._product(left, right, lambda a, b: a + b)
            if isinstance(node.op, ast.Mod):
                return self._product(left, right, lambda a, b: a % b)
        except (TypeError, ValueError) as exc:
            return Values(reason=f"format operation failed: {exc}")
        return Values(reason=f"unsupported binary operator {type(node.op).__name__}")

    def _joined(self, node: ast.JoinedStr, env: dict[str, Values],
                stack: tuple[str, ...]) -> Values:
        result = Values(frozenset({""}))
        for part in node.values:
            if isinstance(part, ast.FormattedValue):
                value = self.evaluate(part.value, env, stack)
            elif isinstance(part, ast.Constant) and isinstance(part.value, str):
                value = Values(frozenset({part.value}))
            else:
                return Values(reason=f"unsupported f-string part {type(part).__name__}")
            if not value.resolved:
                return value
            result = self._product(result, value, lambda a, b: a + b)
        return result

    def _call(self, node: ast.Call, env: dict[str, Values], stack: tuple[str, ...]) -> Values:
        if isinstance(node.func, ast.Attribute) and node.func.attr == "format":
            return self._format_call(node, env, stack)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "join" and node.args:
            return self._join_call(node, env, stack)
        if isinstance(node.func, ast.Name) and node.func.id in self.functions:
            return self._helper_call(node, env, stack)
        return Values(reason=f"unsupported call {ast.unparse(node.func)}")

    def _format_call(self, node: ast.Call, env: dict[str, Values],
                     stack: tuple[str, ...]) -> Values:
        assert isinstance(node.func, ast.Attribute)
        template = self.evaluate(node.func.value, env, stack)
        arguments = [self.evaluate(arg, env, stack) for arg in node.args]
        keywords = {
            keyword.arg: self.evaluate(keyword.value, env, stack)
            for keyword in node.keywords
            if keyword.arg is not None
        }
        unresolved = [value for value in [template, *arguments, *keywords.values()] if not value.resolved]
        if unresolved:
            return Values(reason=unresolved[0].reason)
        output: set[str] = set()
        for base in template.strings:
            for values in itertools.product(*(arg.strings for arg in arguments)):
                keyword_names = tuple(keywords)
                keyword_products = itertools.product(*(keywords[name].strings for name in keyword_names))
                for keyword_values in keyword_products:
                    try:
                        output.add(base.format(
                            *values,
                            **dict(zip(keyword_names, keyword_values, strict=True)),
                        ))
                    except (IndexError, KeyError, ValueError) as exc:
                        return Values(reason=f"format operation failed: {exc}")
        return self._bounded(output)

    def _join_call(self, node: ast.Call, env: dict[str, Values],
                   stack: tuple[str, ...]) -> Values:
        assert isinstance(node.func, ast.Attribute)
        separators = self.evaluate(node.func.value, env, stack)
        sequence = node.args[0]
        if self._runtime_placeholders(sequence):
            if not separators.resolved:
                return separators
            return Values(frozenset({"?"}))
        if isinstance(sequence, ast.Name):
            return self._join_named_sequence(sequence.id, separators, env, stack)
        if not separators.resolved or not isinstance(sequence, (ast.List, ast.Tuple)):
            return Values(reason=separators.reason or "join input is not a finite literal sequence")
        pieces = [self.evaluate(item, env, stack) for item in sequence.elts]
        if any(not piece.resolved for piece in pieces):
            return Values(reason=next(piece.reason for piece in pieces if not piece.resolved))
        return self._bounded({
            separator.join(values)
            for separator in separators.strings
            for values in itertools.product(*(piece.strings for piece in pieces))
        })

    def _join_named_sequence(self, name: str, separators: Values,
                             env: dict[str, Values], stack: tuple[str, ...]) -> Values:
        if not separators.resolved:
            return separators
        choices = self.assignments.get(name, [])
        bases = [choice for choice in choices if isinstance(choice, (ast.List, ast.Tuple))]
        appends = [choice.value for choice in choices if isinstance(choice, ast.Starred)]
        if not bases or len(bases) + len(appends) != len(choices):
            return Values(reason=f"join input {name} is not a finite list")

        sequences: set[tuple[str, ...]] = set()
        for base in bases:
            pieces = [self.evaluate(item, env, stack) for item in base.elts]
            if any(not piece.resolved for piece in pieces):
                return Values(reason=next(piece.reason for piece in pieces if not piece.resolved))
            sequences.update(itertools.product(*(piece.strings for piece in pieces)))
        for append in appends:
            value = self.evaluate(append, env, stack)
            if not value.resolved:
                return value
            sequences = {
                variant
                for current in sequences
                for variant in (current, *((*current, item) for item in value.strings))
            }
            if len(sequences) > MAX_VARIANTS:
                return Values(reason="variant-overflow")
        return self._bounded({
            separator.join(parts)
            for separator in separators.strings
            for parts in sequences
        })

    @staticmethod
    def _runtime_placeholders(node: ast.AST) -> bool:
        """Recognize a runtime-sized sequence containing only SQL bind markers."""
        if isinstance(node, ast.GeneratorExp):
            return (
                isinstance(node.elt, ast.Constant)
                and node.elt.value == "?"
                and len(node.generators) == 1
                and not node.generators[0].ifs
                and not node.generators[0].is_async
            )
        if not isinstance(node, ast.BinOp) or not isinstance(node.op, ast.Mult):
            return False
        for part in (node.left, node.right):
            if isinstance(part, ast.Constant) and part.value == "?":
                return True
            if isinstance(part, (ast.List, ast.Tuple)) and len(part.elts) == 1:
                item = part.elts[0]
                if isinstance(item, ast.Constant) and item.value == "?":
                    return True
        return False

    def _helper_call(self, node: ast.Call, env: dict[str, Values],
                     stack: tuple[str, ...]) -> Values:
        assert isinstance(node.func, ast.Name)
        name = node.func.id
        if name in stack:
            return Values(reason=f"recursive helper {name}")
        function = self.functions[name]
        parameters = [arg.arg for arg in function.args.args]
        if len(node.args) < len(parameters):
            return Values(reason=f"helper {name} has unresolved parameters")
        call_env = {
            parameter: self.evaluate(argument, env, stack)
            for parameter, argument in zip(parameters, node.args, strict=False)
        }
        returns = [item.value for item in ast.walk(function) if isinstance(item, ast.Return) and item.value]
        if not returns:
            return Values(reason=f"helper {name} has no finite return")
        return self._union(self.evaluate(item, call_env, (*stack, name)) for item in returns)

    def _union(self, values) -> Values:
        output: set[str] = set()
        for value in values:
            if not value.resolved:
                return value
            output.update(value.strings)
            if len(output) > MAX_VARIANTS:
                return Values(reason="variant-overflow")
        return Values(frozenset(output))

    def _product(self, left: Values, right: Values, operation) -> Values:
        return self._bounded({operation(a, b) for a in left.strings for b in right.strings})

    @staticmethod
    def _bounded(output: set[str]) -> Values:
        if len(output) > MAX_VARIANTS:
            return Values(reason="variant-overflow")
        return Values(frozenset(output))
