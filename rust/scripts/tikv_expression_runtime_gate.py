#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
"""Run the fixed differential suite and check actual engine execution receipts.

From the repository root, with the pinned Rust toolchain/dependencies available:
  python3 rust/scripts/tikv_expression_runtime_gate.py --self-test
  python3 rust/scripts/tikv_expression_runtime_gate.py

Use --update only to explicitly review a new successful baseline. Run the command
under the embedding project's memory guard; Cargo and test workers are fixed at
one. This is a CI-callable gate, not a hosted CI configuration or all-SQL coverage.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

RUST = Path(__file__).resolve().parents[1]
COMMAND = ["cargo", "test", "-q", "-p", "tidb-expr", "--features", "tikv-expr",
           "--test", "all", "tikv_coverage::", "--locked", "--offline", "-j1",
           "--", "--nocapture", "--test-threads=1"]
PREFIX = "TIKV_RUNTIME_RECEIPT_V1\t"


def collect(output: str, exit_code: int) -> dict:
    if exit_code != 0:
        raise ValueError(f"Cargo failed ({exit_code}); receipts cannot bless a failed run")
    summaries = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", output)
    if len(summaries) != 1 or int(summaries[0][0]) == 0 or summaries[0][1:] != ("0", "0"):
        raise ValueError("require one nonempty passing test suite with no ignored tests")
    records = []
    for line in output.splitlines():
        if PREFIX not in line:
            continue
        fields = line.split(PREFIX, 1)[1].split("\t")
        if len(fields) != 6 or not fields[0]:
            raise ValueError("malformed runtime receipt")
        label, signatures, rows, engine, borrowed, fallbacks = fields
        if signatures and not re.fullmatch(r"[1-9][0-9]*(?:,[1-9][0-9]*)*", signatures):
            raise ValueError("malformed wire signature list")
        signatures = [int(value) for value in signatures.split(",")] if signatures else []
        rows, engine, borrowed, fallbacks = map(int, (rows, engine, borrowed, fallbacks))
        if signatures != sorted(set(signatures)) or any(value <= 0 for value in signatures):
            raise ValueError("invalid wire signature IDs")
        if rows < 0 or engine != 2 * rows or not 0 <= borrowed <= rows or fallbacks != 0:
            raise ValueError(f"invalid execution accounting: {label}")
        records.append({"label": label, "wire_signatures": signatures, "input_rows": rows,
                        "engine_rows": engine, "borrowed_rows": borrowed, "native_fallbacks": fallbacks})
    if not records or sum(row["engine_rows"] for row in records) == 0:
        raise ValueError("no positive engine execution receipts")
    # Keep duplicate identities: multiplicity itself is part of the gate.
    records.sort(key=lambda row: (row["label"], row["wire_signatures"], row["input_rows"],
                                 row["engine_rows"], row["borrowed_rows"]))
    return {"schema_version": 1,
            "scope": "check()-based native differential fixtures; two requested engine modes, observed borrowed rows; not all SQL shapes",
            "command": COMMAND, "tests_passed": int(summaries[0][0]),
            "fixture_receipts": len(records), "engine_rows": sum(row["engine_rows"] for row in records),
            "borrowed_rows": sum(row["borrowed_rows"] for row in records), "records": records}


def check(expected: dict, actual: dict) -> None:
    if expected != actual:
        keys = ("tests_passed", "fixture_receipts", "engine_rows", "borrowed_rows")
        raise ValueError("runtime receipt drift; review before --update: " +
                         ", ".join(f"{key}={expected.get(key)}->{actual.get(key)}" for key in keys))


def self_test() -> None:
    fixture = PREFIX + "cast\t1,2\t3\t6\t3\t0\n"
    summary = "test result: ok. 1 passed; 0 failed; 0 ignored;\n"
    actual = collect(fixture + summary, 0)
    assert actual["engine_rows"] == 6 and actual["borrowed_rows"] == 3
    assert collect("test prefix ... " + fixture + summary, 0) == actual
    check(actual, actual)
    for output, code in [(fixture + summary, 101), (summary, 0),
                         (fixture + summary.replace("1 passed", "0 passed"), 0),
                         (fixture + summary.replace("0 ignored", "1 ignored"), 0),
                         (fixture.replace("\t6\t", "\t5\t") + summary, 0),
                         (fixture.replace("\t0\n", "\t1\n") + summary, 0),
                         (fixture.replace("1,2", "2,1") + summary, 0),
                         (fixture.replace("1,2", "1,,2") + summary, 0),
                         (fixture.replace("\t3\t0", "\t4\t0") + summary, 0)]:
        try:
            collect(output, code)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid execution evidence accepted")
    changed_records = collect(fixture.replace("cast\t1,2", "different\t1,3") + summary, 0)
    duplicate = collect(fixture + fixture + summary, 0)
    for changed in [dict(actual, engine_rows=5), changed_records, duplicate]:
        try:
            check(actual, changed)
        except ValueError:
            pass
        else:
            raise AssertionError("count/identity drift accepted")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, default=RUST / "docs/tikv-expression-runtime-baseline.json")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    self_test()
    if args.self_test:
        print("runtime gate self-tests passed (including failure/empty/fallback/count/identity rejection)")
        return 0
    env = dict(os.environ, CARGO_BUILD_JOBS="1", CARGO_TERM_COLOR="never")
    result = subprocess.run(COMMAND, cwd=RUST, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    print(result.stdout, end="")
    receipt = collect(result.stdout, result.returncode)
    if args.update:
        args.baseline.write_text(json.dumps(receipt, indent=2, ensure_ascii=False) + "\n")
    else:
        if not args.baseline.exists():
            raise ValueError("missing runtime baseline; a reviewed successful --update is required")
        check(json.loads(args.baseline.read_text()), receipt)
    print(f"runtime gate {'updated' if args.update else 'passed'}: "
          f"{receipt['fixture_receipts']} fixtures, {receipt['engine_rows']} engine rows, "
          f"{receipt['borrowed_rows']} observed borrowed rows")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
