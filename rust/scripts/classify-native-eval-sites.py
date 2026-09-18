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
* the **production sites** are those minus the hits that only run under
  `cargo test` (a file under a `tests/` directory, or any line below its file's
  first `#[cfg(test)]`). Test-only hits are re-pointed with the corpora rather
  than converted, so they are not part of the pre-deletion routing work.

The driver's six-argument `UpdateExpression::eval` is counted as a site on
purpose: it is a textual dispatch whose branches call `Expression::eval`.

Usage: `rust/scripts/classify-native-eval-sites.py [--list]`, where `--list`
prints every classified hit.
"""
import os
import subprocess
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "crates")


def is_test_only(path, lineno):
    """Whether the hit only runs under `cargo test`."""
    basename = os.path.basename(path)
    if "/tests/" in path or basename == "tests.rs" or basename.endswith("_tests.rs"):
        return True
    with open(os.path.join(ROOT, path), encoding="utf-8") as source:
        for index, line in enumerate(source, start=1):
            if index >= lineno:
                return False
            if "#[cfg(test)]" in line:
                return True
    return False


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
