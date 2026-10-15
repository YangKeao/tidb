#!/usr/bin/env python3
"""Source inventory, not proof of lowering, kernel correctness or performance."""
import argparse
import csv
import re
from pathlib import Path


def rows(source, declaration):
    source = re.sub(r"//[^\n]*|/\*.*?\*/", "", source, flags=re.S)
    start = source.index("= &[", source.index(declaration)) + 4
    depth, begin, quoted, escaped = 0, start, False, False
    for i in range(start, len(source)):
        char = source[i]
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
            continue
        if char == '"':
            quoted = True
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            if depth == 0:
                return
            depth -= 1
        elif char == "," and depth == 0:
            yield source[begin:i].strip()
            begin = i + 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tidb", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--tikv", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--signatures-output", type=Path)
    args = parser.parse_args()
    tikv = args.tikv or args.tidb.parent / "tikv"
    expr = args.tidb / "rust/crates/tidb-expr/src"
    registry = (tikv / "components/tidb_query_expr/src/local/profile.rs").read_text()
    registry = registry.split("pub fn batch_signature", 1)[1].split("/// The consumer", 1)[0]
    admitted = set(re.findall(r"ScalarFuncSig::(\w+)", registry))
    catalog = {}
    for row in rows((expr / "pushdown_catalog.rs").read_text(), "pub const CATALOG"):
        name = re.search(r'"([^"]+)"', row)
        if name:
            catalog.setdefault(name[1], set()).update(re.findall(r"ScalarFuncSig::(\w+)", row))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as out:
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(["function", "arity", "catalog_signatures", "registry_intersection", "semantic_review", "differential_coverage", "benchmark_coverage"])
        for row in rows((expr / "builtin_registry.rs").read_text(), "static FUNCTION_CLASSES"):
            name = re.search(r'"([^"]+)"', row)
            if not name:
                continue
            signatures = catalog.get(name[1], set())
            writer.writerow([name[1], row[row.index(",")+1:-1].strip(), ";".join(sorted(signatures)), ";".join(sorted(signatures & admitted)), "pending", "not_audited", "not_audited"])
    signatures_output = args.signatures_output or args.output.with_name("signature-inventory.csv")
    names = {}
    for name, signatures in catalog.items():
        for signature in signatures:
            names.setdefault(signature, set()).add(name)
    with signatures_output.open("w", newline="") as out:
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(["signature", "catalog_names", "registry_admitted", "semantic_review", "differential_coverage", "benchmark_coverage"])
        for signature in sorted(set(names) | admitted):
            writer.writerow([signature, ";".join(sorted(names.get(signature, []))), signature in admitted, "pending", "not_audited", "not_audited"])
    print(f"Wrote {args.output} and {signatures_output}; metadata is not runtime coverage.")



if __name__ == "__main__":
    main()
