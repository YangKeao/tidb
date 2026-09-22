#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
"""Generate/check the engine-only excluded-shape inventory."""

from __future__ import annotations

import argparse
from collections import Counter
import html
import json
from pathlib import Path

RUST = Path(__file__).resolve().parents[1]
SOURCE = RUST / "docs/tikv-expression-coverage.json"
OUTPUT = RUST / "docs/tikv-expression-unsupported.md"


def code(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, list):
        return ", ".join(map(str, value)) or "—"
    return f"<code>{html.escape(str(value))}</code>"


def text(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, list):
        return ", ".join(map(str, value)) or "—"
    return str(value).replace("|", "\\|").replace("\n", " ")


def render(data: dict) -> str:
    table = data["admission_table"]
    excluded = [row for row in table if row["decision"] == "excluded"]
    names = {row["name"] for row in excluded}
    reasons = Counter(row["reason"] for row in excluded)
    lines = [
        "# TiKV engine-only unsupported inventory",
        "",
        "> Generated from `docs/tikv-expression-coverage.json`. This is an admission/shape inventory, not a claim of full SQL semantic coverage.",
        "",
        "## Contract",
        "",
        "- Admitted expressions execute only in the shared TiKV expression engine.",
        "- Excluded or non-lowerable shapes return a structured engine decline/unsupported error.",
        "- There is no TiDB Rust native expression replay, error replay, or synthetic short-circuit fallback.",
        "- A name can have both admitted and excluded rows because admission is signature-, type-, and shape-specific.",
        "",
        "## Summary",
        "",
        f"- Admission rows: **{len(table)}**",
        f"- Admitted rows: **{sum(row['decision'] == 'admitted' for row in table)}**",
        f"- Excluded rows: **{len(excluded)}**",
        f"- Distinct names with at least one excluded shape: **{len(names)}**",
        "",
        "## Structural refusals",
        "",
        "| Condition | Result |",
        "|---|---|",
        "| Session/executor has no TiKV expression context | Structured required-context error; feature-disabled builds return `Unsupported(\"the engine-only expression demo requires the tikv-expr feature\")` |",
        "| Row-major evaluator program | `ExternalEngine` code 1105: `TiKV expression engine does not admit row-major programs` |",
        "| TiKV lowerer declines a compiled expression | `ExternalEngine` code 1105 with the lowerer decline reason |",
        "| `SHOW ... WHERE` virtual-row resolver | Explicit demo contraction until it carries statement TiKV state |",
        "",
        "## Excluded function shapes",
        "",
        "| SQL name | Signature | Required eval types | Shape | Reason |",
        "|---|---:|---|---|---|",
    ]
    for row in sorted(excluded, key=lambda row: (row["name"], str(row["signature"]), str(row["shape"]), row["reason"])):
        lines.append(
            f"| {code(row['name'])} | {code(row['signature'])} | {text(row['required_eval_types'])} | {code(row['shape'])} | {text(row['reason'])} |"
        )
    lines += ["", "## Reason counts", "", "| Reason | Rows |", "|---|---:|"]
    for reason, count in sorted(reasons.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"| {text(reason)} | {count} |")
    lines += [
        "",
        "## Regeneration",
        "",
        "```bash",
        "cd rust",
        "python3 scripts/tikv_expression_coverage.py --self-check --check",
        "python3 scripts/tikv_expression_unsupported.py --check",
        "```",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = render(json.loads(SOURCE.read_text()))
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != rendered:
            raise SystemExit("unsupported inventory drift; regenerate without --check")
        print("unsupported inventory check passed")
    else:
        OUTPUT.write_text(rendered)
        print(f"wrote {OUTPUT.relative_to(RUST)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
