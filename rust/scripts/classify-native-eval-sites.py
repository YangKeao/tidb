#!/usr/bin/env python3
"""Classify the native `Expression::eval` call sites outside the adapter.

`rust/docs/tikv-expression-removal-native-sites.md` is the inventory this
measures: `grep -rn "\\.eval(" --include=*.rs crates | grep -v
'crates/tidb-expr/src/'` returns the hits inside `tidb-expr/src` too, so this
script re-runs the same grep and classifies each hit. The rule is mechanical so
the numbers in that document can be reproduced and compared:

* a hit whose call has no arguments (`.eval()`) is a folding helper, not the
  evaluator (`Constant::eval`, a planner `Column::eval`);
* `metadata.eval(k)` is the planner's test-only metadata helper;
* the statement predicate's own four-argument `eval(row, catalog, db, ctx)` is a
  different entry point;
* a hit mentioning `Row::empty()` evaluates a constant with no input row;
* a hit mentioning `get_row(0)` evaluates one chunk row;
* anything else is a loop variable or comparator.

Two totals are reported, because they answer different questions:

* the **sites** are the raw hits minus the ones that are not `Expression::eval`;
* the **production sites** are those minus known test-file conventions or
  lexically bounded items/scopes excluded from non-test builds by `#[test]` or
  `cfg`. Inner attributes apply only to their enclosing scope; helper/field
  attributes do not taint siblings. Unknown cfg predicates remain unknown.

This is a conservative textual inventory, not Rust name resolution, macro
expansion or a reachability proof. Unproven scopes remain production-labelled;
unsupported item syntax can shorten an exclusion, never extend it to EOF.
Comments and ordinary/raw/byte/C strings and char literals are masked before
matching delimiters. A line containing a production call is retained even when
it also contains a test call.

The driver's six-argument `UpdateExpression::eval` is counted as a site on
purpose: it is a textual dispatch whose branches call `Expression::eval`.

Usage: `rust/scripts/classify-native-eval-sites.py [--list]`, where `--list`
prints every classified hit.
"""
from functools import lru_cache
import os
import re
import subprocess
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "crates")


def masked_rust(source):
    """Hide comments/literals, preserving offsets and newlines for scope bounds.

    This is a small lexical scanner, not a Rust parser or macro expander.
    Lifetimes remain tokens; only syntactically delimited chars are hidden.
    """
    masked = list(source)
    size = len(source)
    index = 0
    raw_string = re.compile(r'(?:br|cr|r)(#*)"')
    char_literal = re.compile(r"'(?:[^'\\\n]|\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|[^\n]))'")
    while index < size:
        start = index
        raw = raw_string.match(source, index) if (
            index == 0 or not (source[index - 1].isalnum() or source[index - 1] == "_")
        ) else None
        if source.startswith("//", index):
            end = source.find("\n", index)
            index = size if end < 0 else end
        elif source.startswith("/*", index):
            index += 2
            depth = 1
            while index < size and depth:
                if source.startswith("/*", index):
                    depth += 1
                    index += 2
                elif source.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
        elif raw:
            delimiter = '"' + raw.group(1)
            end = source.find(delimiter, raw.end())
            index = size if end < 0 else end + len(delimiter)
        elif source[index] == '"':
            index += 1
            while index < size:
                if source[index] == "\\":
                    index += 2
                elif source[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
        elif source[index] == "'" and (char := char_literal.match(source, index)):
            index = char.end()
        else:
            index += 1
            continue
        for offset in range(start, min(index, size)):
            if source[offset] != "\n":
                masked[offset] = " "
    return "".join(masked)


def cfg_values_without_test(tokens):
    """Possible cfg values with test=false; unknown predicates stay unknown."""
    if tokens == ["test"]:
        return {False}
    if len(tokens) < 3 or tokens[1] != "(" or tokens[-1] != ")":
        return {False, True}
    operator = tokens[0]
    if operator not in ("all", "any", "not"):
        return {False, True}
    parts, part, depth = [], [], 0
    for token in tokens[2:-1]:
        if token == "," and depth == 0:
            parts.append(part)
            part = []
            continue
        depth += (token == "(") - (token == ")")
        if depth < 0:
            return {False, True}
        part.append(token)
    if depth:
        return {False, True}
    if part:
        parts.append(part)
    values = [cfg_values_without_test(part) for part in parts]
    if operator == "not":
        return {not value for value in values[0]} if len(values) == 1 else {False, True}
    result = {operator == "all"}
    for choices in values:
        result = {a and b if operator == "all" else a or b
                  for a in result for b in choices}
    return result


def item_end(tokens, pairs, start):
    """Bound the next item/field; never extend an unproven item to EOF.

    A comma/closing delimiter bounds fields; a balanced body bounds functions,
    modules, impls and block initializers. Unsupported syntax can shorten a
    range (conservatively retaining calls), not taint subsequent siblings.
    """
    index = start
    while index < len(tokens):
        token = tokens[index].group()
        if token in (";", ",", "}", "]", ")"):
            return tokens[index].start()
        if token == "{":
            close = pairs.get(index)
            return tokens[close].end() if close is not None else None
        if token in ("(", "["):
            if index not in pairs:
                return None
            index = pairs[index]
        index += 1
    return None


@lru_cache(maxsize=64)
def test_only_ranges(source):
    """Lexically bounded spans known to be excluded from non-test builds."""
    tokens = list(re.finditer(r"[A-Za-z_][A-Za-z_0-9]*|[^\s]", masked_rust(source)))
    pairs, stack = {}, []
    for index, token in enumerate(tokens):
        value = token.group()
        if value in ("(", "[", "{"):
            stack.append(index)
        elif value in (")", "]", "}"):
            if not stack or tokens[stack[-1]].group() != {")": "(", "]": "[", "}": "{"}[value]:
                # Malformed token nesting cannot justify suppressing calls.
                return ()
            pairs[stack.pop()] = index
    ranges, braces = [], []
    index = 0
    while index < len(tokens):
        value = tokens[index].group()
        if value == "{":
            braces.append(index)
        elif value == "}" and braces:
            braces.pop()
        elif value == "#":
            opening = index + 1
            inner = opening < len(tokens) and tokens[opening].group() == "!"
            opening += inner
            if opening < len(tokens) and tokens[opening].group() == "[" and opening in pairs:
                close = pairs[opening]
                body = [token.group() for token in tokens[opening + 1:close]]
                excludes = not inner and body == ["test"]
                if len(body) >= 3 and body[:2] == ["cfg", "("] and body[-1] == ")":
                    excludes = True not in cfg_values_without_test(body[2:-1])
                if excludes:
                    if inner and not braces:
                        ranges.append((0, len(source)))
                    elif inner:
                        scope = braces[-1]
                        if scope in pairs:
                            ranges.append((tokens[scope].start(), tokens[pairs[scope]].end()))
                    else:
                        end = item_end(tokens, pairs, close + 1)
                        if end is not None:
                            ranges.append((tokens[index].start(), end))
                index = close
        index += 1
    return tuple(ranges)


def is_test_only(path, lineno):
    """Whether every textual eval hit on this line is in a known test scope."""
    basename = os.path.basename(path)
    if "tests" in path.split("/") or basename == "tests.rs" or basename.endswith("_tests.rs"):
        return True
    with open(os.path.join(ROOT, path), encoding="utf-8") as source:
        text = source.read()
    lines = text.splitlines(keepends=True)
    line = lines[lineno - 1]
    offset = sum(map(len, lines[:lineno - 1]))
    calls = [offset + match.start() for match in re.finditer(r"\.eval\(", line)]
    ranges = test_only_ranges(text)
    # A line with both a test call and a production call must not be suppressed.
    return bool(calls) and all(any(start <= call < end for start, end in ranges) for call in calls)


def arguments(window, start):
    """The top-level comma-separated arguments of the `eval(` call at `start`."""
    index = window.index("(", start) + 1
    depth = 1
    args = []
    current = []
    while index < len(window) and depth > 0:
        char = window[index]
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth == 0:
                break
        if char == "," and depth == 1:
            args.append("".join(current))
            current = []
        else:
            current.append(char)
        index += 1
    args.append("".join(current))
    return [arg.strip() for arg in args]


def main():
    raw = subprocess.run(
        ["grep", "-rn", r"\.eval(", "--include=*.rs", "."],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout.splitlines()
    raw = [line for line in raw if "/tidb-expr/src/" not in line]

    buckets = {
        "not the evaluator": [],
        "no input row": [],
        "one chunk row": [],
        "row loop/comparator": [],
    }
    for hit in raw:
        path, lineno, _ = hit.split(":", 2)
        with open(os.path.join(ROOT, path), encoding="utf-8") as source:
            lines = source.readlines()
        start = int(lineno) - 1
        # Eight lines, because a call whose arguments span lines has to be
        # classified by its argument list rather than by its first line.
        window = "".join(lines[start:start + 8])
        args = arguments(window, window.index(".eval("))
        if len(args) == 1 and not args[0]:
            buckets["not the evaluator"].append(hit)
        elif "metadata.eval(" in hit or (len(args) == 4 and any("catalog" in a for a in args)):
            buckets["not the evaluator"].append(hit)
        elif "Row::empty()" in hit:
            buckets["no input row"].append(hit)
        elif "get_row(0)" in hit:
            buckets["one chunk row"].append(hit)
        else:
            buckets["row loop/comparator"].append(hit)

    if "--list" in sys.argv:
        for name, hits in buckets.items():
            print(f"## {name} ({len(hits)})")
            for hit in hits:
                path, lineno, _ = hit.split(":", 2)
                marker = " (test-only)" if is_test_only(path, int(lineno)) else ""
                print(f"   {path}:{lineno}{marker}")
    sites = [hit for hit in raw if hit not in buckets["not the evaluator"]]
    production = [
        hit for hit in sites
        if not is_test_only(hit.split(":", 2)[0], int(hit.split(":", 2)[1]))
    ]
    print(f"raw hits {len(raw)}")
    for name, hits in buckets.items():
        print(f"{name:>22}: {len(hits)}")
    print(f"{'Expression::eval sites':>22}: {len(sites)}")
    print(f"{'  of them production':>22}: {len(production)}")
    print(f"{'  of them test-only':>22}: {len(sites) - len(production)}")


if __name__ == "__main__":
    main()
