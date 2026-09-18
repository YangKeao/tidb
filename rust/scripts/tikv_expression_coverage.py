#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
"""Static TiDB/TiKV signature inventory; NOT SQL semantic coverage.

Run from any directory, without Rust/Go builds or third-party packages:
  ulimit -v 524288
  python3 rust/scripts/tikv_expression_coverage.py --self-check
  python3 rust/scripts/tikv_expression_coverage.py --check

Defaults use sibling ../tikv and ../cargo-home's pinned tipb checkout. Override
--tikv/--tipb-proto when those sources live elsewhere. Outputs are deterministic
for the same source contents. Baseline source is read from git (never checked
out); current source may include uncommitted work. No distributed policy changes.
"""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import sys

BASE_TIDB = "7a5c468"
BASE_TIKV = "521ac733"
ADAPTER = "rust/crates/tidb-expr/src/tikv.rs"
CATALOG = "rust/crates/tidb-expr/src/pushdown_catalog.rs"
PROTO = "rust/crates/tidb-proto/proto/select.proto"
ENGINE = "components/tidb_query_expr/src/lib.rs"
FACADE = "components/tidb_query_expr/src/standalone.rs"
LAZY = {"LogicalAnd", "LogicalOr", "AddTimeDateTimeNull", "AddTimeDurationNull", "AddTimeStringNull", "NullTimeDiff"} | {
    prefix + suffix for prefix in ("If", "IfNull", "CaseWhen", "Coalesce")
    for suffix in ("Int", "Real", "Decimal", "String", "Time", "Duration", "Json")
}
VOLATILE = {"Rand", "RandomBytes", "Uuid", "SysDateWithFsp", "SysDateWithoutFsp"}
PACKET = {"Concat", "ConcatWs", "Repeat", "Space", "Lpad", "LpadUtf8", "Rpad",
          "RpadUtf8", "Insert", "InsertUtf8", "ToBase64", "FromBase64"}


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True)


def uncomment(text: str) -> str:
    # Preserve quoted strings and newlines, so comments cannot invent support.
    token = r'"(?:\\.|[^"\\])*"|`[^`]*`|/\*[\s\S]*?\*/|//[^\n]*'
    return re.sub(token, lambda m: re.sub(r"[^\n]", " ", m[0])
                  if m[0].startswith(("//", "/*")) else m[0], text)


def body(text: str, marker: str) -> str:
    start = text.index("{", text.index(marker))
    depth = 0
    for match in re.finditer(r'"(?:\\.|[^"\\])*"|`[^`]*`|[{}]', text[start:]):
        token = match[0]
        depth += (token == "{") - (token == "}")
        if depth == 0:
            return text[start + 1:start + match.start()]
    raise ValueError(f"unclosed body: {marker}")


def enum(text: str) -> dict[str, int]:
    return {name: int(number) for name, number in re.findall(
        r"^\s*(\w+)\s*=\s*(\d+)\s*;", body(uncomment(text), "enum ScalarFuncSig"), re.M)}


def dispatch(text: str) -> dict[str, dict]:
    raw = body(text, "fn map_expr_node_to_rpn_func")
    family = ""
    result = {}
    pending = []
    for line in raw.splitlines():
        group = re.search(r"// (impl_\w+)", line)
        if group:
            family = group[1]
        line = uncomment(line)
        pending += re.findall(r"ScalarFuncSig::(\w+)", line.split("=>")[0])
        if "=>" in line and pending:
            target = line.split("=>", 1)[1].strip().rstrip(",")
            for name in pending:
                if name in result:
                    raise ValueError(f"duplicate dispatch: {name}")
                result[name] = {"family": family, "target": target}
            pending = []
    if pending:
        raise ValueError(f"unparsed dispatch: {pending}")
    return result


def refs(text: str, names: dict[str, str]) -> set[str]:
    """Lexical signature evidence only; not an admission claim."""
    # Includes Rust enum identifiers, quoted lookup names, and raw Some(id) below.
    return {names[token.lower()] for token in re.findall(r"\b[A-Z]\w*\b", uncomment(text))
            if token.lower() in names}


def adapter_refs(text: str, upstream: dict[str, int]) -> set[str]:
    names = {name.lower(): name for name in upstream}
    result = refs(text, names)
    by_id = {number: name for name, number in upstream.items()}
    for number in re.findall(r"\bsig\s*:\s*Some\(\s*(\d+)\s*\)", uncomment(text)):
        if int(number) in by_id:
            result.add(by_id[int(number)])
    return result


def generated_adapter_candidates(text: str, upstream: dict[str, int]) -> dict[str, set[str]]:
    """Expand signature-name templates, without pretending to evaluate guards.

    Named prefix/suffix fragments come from the same Rust function; positional
    fragments may be any family spelling. Engine/build/arity/type gates remain
    separate. This deliberately includes enum-only candidates, never support.
    """
    text = uncomment(text.split("#[cfg(test)]", 1)[0])
    families = {"Int", "Real", "Decimal", "String", "Time", "Duration", "Json", "VectorFloat32", "Dec", "Uint"}
    result = collections.defaultdict(set)
    for match in re.finditer(r"\bfn\s+(\w+)\s*\(", text):
        chunk = body(text, match[0])
        literals = set(re.findall(r'"([A-Z][A-Za-z0-9]*)"', chunk))
        for template in re.findall(r'format!\(\s*"([^"\n]*)"', chunk):
            if "{" not in template:
                continue
            pieces = re.split(r"(\{\w*\})", template)
            pattern = ""
            for piece in pieces:
                if piece.startswith("{") and piece.endswith("}"):
                    values = families if piece == "{}" else literals
                    pattern += "(?:" + "|".join(re.escape(value) for value in sorted(values)) + ")"
                else:
                    pattern += re.escape(piece)
            for name in upstream:
                if re.fullmatch(pattern, name, re.I):
                    result[name].add(match[1] + ":" + template)
    return result


def baseline_adapter(text: str, names: dict[str, str]) -> set[str]:
    return refs(body(uncomment(text), "fn lower("), names)


def facade_whitelist(text: str, names: dict[str, str]) -> set[str] | None:
    if "fn validate_signature(" not in text:
        return None
    return refs(body(uncomment(text), "fn validate_signature("), names)


def go_constructor_evidence(root: Path, proto_names: dict[str, str]) -> tuple[dict, list, dict]:
    """Conservative source-reference graph, not a Go type/overload interpreter.

    Follow calls among named production builtin functions and same-receiver
    methods. Dynamic dispatch is not inferred. Results intentionally say
    'candidate': shared class branches may choose different signatures.
    """
    ast = uncomment((root / "pkg/parser/ast/functions.go").read_text())
    constants = dict(re.findall(r'^\s*(\w+)\s*=\s*"([^"\n]*)"', ast, re.M))
    registry = body(uncomment((root / "pkg/expression/builtin.go").read_text()),
                    "var funcs = map[string]functionClass")
    entries = []
    for match in re.finditer(r"ast\.(\w+):\s*&?(\w+)\{([^\n]*)", registry):
        ast_name, cls, tail = match.groups()
        entries.append({"sql_name": constants.get(ast_name, "ast." + ast_name),
                        "go_class": cls, "registry_helpers": re.findall(r"\b(setAdd|setSub)\b", tail)})
    nodes = {}
    for path in sorted((root / "pkg/expression").glob("builtin*.go")):
        if path.name.endswith("_test.go"):
            continue
        text = uncomment(path.read_text())
        for match in re.finditer(r"^func\s+(?:\((\w+)\s+\*(\w+)\)\s+)?(\w+)\s*\(", text, re.M):
            receiver, cls, name = match.groups()
            key = f"{cls}.{name}" if cls else name
            chunk = body(text, match[0])
            signatures = {proto_names[s.lower()] for s in re.findall(r"tipb\.ScalarFuncSig_(\w+)", chunk)
                          if s.lower() in proto_names}
            calls = set(re.findall(r"(?<![.\w])(\w+)\s*\(", chunk))
            if receiver:
                calls.update(f"{cls}.{method}" for method in re.findall(
                    rf"\b{re.escape(receiver)}\.(\w+)\s*\(", chunk))
            nodes[key] = (signatures, calls, str(path.relative_to(root)))
    by_signature = collections.defaultdict(set)
    for entry in entries:
        visited = set()
        pending = [entry["go_class"] + ".getFunction", *entry.pop("registry_helpers")]
        found = set()
        while pending:
            key = pending.pop()
            if key in visited or key not in nodes:
                continue
            visited.add(key)
            signatures, calls, _ = nodes[key]
            found.update(signatures)
            pending.extend(calls - visited)
        entry["candidate_signatures"] = sorted(found)
        entry["evidence"] = "constructor_and_static_helper_references" if found else "no_static_signature_reference"
        for sig in found:
            by_signature[sig].add(entry["sql_name"])
    internal_casts = collections.defaultdict(set)
    for key, (signatures, _, _) in nodes.items():
        if re.fullmatch(r"castAs\w+FunctionClass.getFunction", key):
            for signature in signatures:
                internal_casts[signature].add(key)
    return by_signature, sorted(entries, key=lambda e: e["sql_name"]), internal_casts


def catalog_evidence(text: str, names: dict[str, str]) -> tuple[set[str], dict]:
    clean = uncomment(text)
    direct = set()
    by_name = collections.defaultdict(set)
    for marker in ("pub const CATALOG:", "const DATE_ADD_SIGNATURES:", "const DATE_SUB_SIGNATURES:"):
        start = clean.index(marker)
        section = clean[start:clean.index("\n];", start)]
        direct.update(refs(section, names))
        # Function entries can be multiline; selector patterns are not interpreted.
        for match in re.finditer(r'(?:\w*signature)\(\s*"([^"\n]+)"([\s\S]*?)(?=\b\w*signature\(|\Z)', section):
            by_name[match[1]].update(refs(match[2], names))
    return direct, {name: sorted(sigs) for name, sigs in sorted(by_name.items())}


def borrowed_metadata(root: Path) -> set[str]:
    result = set()
    for path in sorted((root / "components/tidb_query_expr/src").glob("impl_*.rs")):
        text = uncomment(path.read_text())
        for match in re.finditer(r"#\[rpn_fn\(([^\n]*)\)\]((?:\s*#\[[^\n]*\])*)\s*(?:pub\s+)?fn\s+(\w+)", text):
            if re.search(r"\bborrowed\b", match[1]):
                result.add(match[3] + "_fn_meta")
    return result


def risks(name: str) -> list[str]:
    result = []
    if name in LAZY:
        result.append("lazy_children_eager_rpn")
    if name in VOLATILE:
        result.append("volatile_clock_rng_or_uuid")
    if name == "RandWithSeedFirstGen":
        result.append("first_generation_not_session_seeded_sequence")
    if name in PACKET:
        result.append("max_allowed_packet_not_in_facade_context")
    if name == "WeekWithoutMode":
        result.append("default_week_format_hardcoded_zero")
    if name == "CastDurationAsTime" or re.match(r"(?:Add|Sub)DateDuration.*Datetime$", name):
        result.append("current_date_clock_context")
    if name in {"JsonSetSig", "JsonInsertSig", "JsonReplaceSig"}:
        result.append("sql_null_document_differs_from_json_null")
    if name == "JsonMergeSig":
        result.append("later_sql_null_documents_need_nonnull_gate")
        result.append("deprecated_json_merge_warning_absent_use_preserve_alias_only")
    if name == "JsonQuoteSig":
        result.append("control_byte_escaping_requires_safe_literal_gate")
    if name.startswith("Cast"):
        result.append("cast_field_metadata_selects_kernel")
    return result


def make_inventory(args: argparse.Namespace) -> dict:
    root, tikv = args.tidb.resolve(), args.tikv.resolve()
    source_hashes = {}
    def source(repo, path, label):
        text = (repo / path).read_text()
        source_hashes[label + "/" + path] = hashlib.sha256(text.encode()).hexdigest()
        return text
    upstream = enum(args.tipb_proto.read_text())
    source_hashes["tipb/expression.proto"] = hashlib.sha256(args.tipb_proto.read_bytes()).hexdigest()
    normalized = {name.lower(): name for name in upstream}
    if len(normalized) != len(upstream) or len(set(upstream.values())) != len(upstream):
        raise ValueError("ambiguous upstream signature names or numeric IDs")
    current_dispatch = dispatch(source(tikv, ENGINE, "tikv"))
    base_dispatch = dispatch(git(tikv, "show", BASE_TIKV + ":" + ENGINE))
    for name in current_dispatch:
        if name.lower() not in normalized:
            raise ValueError("dispatch missing upstream numeric ID: " + name)
    local = enum(source(root, PROTO, "tidb"))
    base_local = enum(git(root, "show", BASE_TIDB + ":" + PROTO))
    for name, number in local.items():
        if name in upstream and upstream[name] != number:
            raise ValueError("local/upstream numeric ID mismatch: " + name)
    adapter_text = source(root, ADAPTER, "tidb")
    adapter_evidence = adapter_refs(adapter_text, upstream)
    generated_candidates = generated_adapter_candidates(adapter_text, upstream)
    # Additional local adapter modules may be supplied explicitly. They are
    # evidence sources, not automatically trusted as an executable whitelist.
    extras = {path for path in (
        "rust/crates/tidb-expr/src/tikv/lowering.rs",
        "rust/crates/tidb-expr/src/tikv/lowering/families.rs",
    ) if (root / path).exists()} | set(args.adapter_source)
    for extra in sorted(extras):
        text = source(root, extra, "tidb")
        adapter_evidence.update(adapter_refs(text, upstream))
        for name, templates in generated_adapter_candidates(text, upstream).items():
            generated_candidates[name].update(templates)
    base_adapter = baseline_adapter(git(root, "show", BASE_TIDB + ":" + ADAPTER), normalized)
    facade_text = source(tikv, FACADE, "tikv")
    current_facade = facade_whitelist(facade_text, normalized)
    facade_policy = ("explicit_whitelist" if current_facade is not None else
                     "engine_builder_with_safety_preflight" if "fn validate_builder_safety(" in facade_text else
                     "unrecognized_requires_review")
    base_facade = facade_whitelist(git(tikv, "show", BASE_TIKV + ":" + FACADE), normalized)
    catalog, catalog_names = catalog_evidence(source(root, CATALOG, "tidb"), normalized)
    go_sigs, go_entries, internal_casts = go_constructor_evidence(root, normalized)
    borrowed = borrowed_metadata(tikv)
    # Hash all source inputs used for helper/annotation inventories too.
    for repo, label, patterns in ((root, "tidb", ("pkg/expression/builtin*.go", "pkg/parser/ast/functions.go")),
                                  (tikv, "tikv", ("components/tidb_query_expr/src/impl_*.rs",))):
        for pattern in patterns:
            for path in sorted(repo.glob(pattern)):
                if not path.name.endswith("_test.go"):
                    source_hashes[label + "/" + str(path.relative_to(repo))] = hashlib.sha256(path.read_bytes()).hexdigest()
    lookup = {normalized[name.lower()]: (name, entry) for name, entry in current_dispatch.items()}
    baseline_lookup = {normalized[name.lower()] for name in base_dispatch}
    rows = []
    for proto_name, number in sorted(upstream.items(), key=lambda item: item[1]):
        rust_name, entry = lookup.get(proto_name, ("", {}))
        target = entry.get("target", "")
        flagged = risks(rust_name)
        borrowed_state = "no_dispatch"
        if entry:
            metas = set(re.findall(r"\b(\w+_fn_meta)\b", target))
            if metas & borrowed:
                borrowed_state = "annotated_kernel_type_and_tree_dependent"
            elif "map_int_sig" in target and any(x in target for x in ("plus_mapper", "minus_mapper", "multiply_mapper", "mod_mapper", "divide_mapper", "compare_mapper")):
                borrowed_state = "mapper_to_annotated_kernel_type_and_tree_dependent"
            elif "map_" in target:
                borrowed_state = "mapper_requires_runtime_probe"
            else:
                borrowed_state = "no_annotated_direct_loader"
        if not entry:
            classification = "not_dispatched_by_engine"
        elif flagged and any(not risk.startswith("cast_") for risk in flagged):
            classification = "engine_dispatch_semantic_exclusion_or_review"
        elif proto_name in base_adapter:
            classification = "baseline_adapter_signature_with_shape_restrictions"
        else:
            classification = "engine_dispatch_adapter_gap_or_new_mapping_evidence"
        rows.append({
            "signature_id": number, "proto_name": proto_name, "rust_name": rust_name,
            "engine_dispatch": bool(entry), "engine_family": entry.get("family", ""),
            "engine_target": target, "baseline_engine_dispatch": proto_name in baseline_lookup,
            "local_proto_available": proto_name in local,
            "baseline_local_proto_available": proto_name in base_local,
            "baseline_adapter_mapping": proto_name in base_adapter,
            "current_adapter_source_evidence": proto_name in adapter_evidence,
            "current_adapter_generated_name_candidates": sorted(generated_candidates.get(proto_name, ())),
            "baseline_facade_whitelist": proto_name in base_facade,
            "current_facade_whitelist_evidence": None if current_facade is None else proto_name in current_facade,
            "pushdown_catalog_direct_selection": proto_name in catalog,
            "go_constructor_candidate_names": sorted(go_sigs.get(proto_name, ())),
            "go_internal_cast_constructor_evidence": sorted(internal_casts.get(proto_name, ())),
            "borrowed_classification": borrowed_state, "risk_flags": flagged,
            "classification": classification,
        })
    counts = {
        "upstream_enum_variants_including_unspecified": len(rows),
        "engine_dispatched": sum(row["engine_dispatch"] for row in rows),
        "not_engine_dispatched": sum(not row["engine_dispatch"] for row in rows),
        "engine_dispatched_missing_local_proto": sum(row["engine_dispatch"] and not row["local_proto_available"] for row in rows),
        "baseline_adapter_signatures": len(base_adapter), "baseline_facade_signatures": len(base_facade),
        "current_adapter_signature_source_evidence": len(adapter_evidence),
        "current_adapter_generated_signature_candidates": len(generated_candidates),
        "engine_dispatch_with_adapter_source_or_generated_candidate": sum(row["engine_dispatch"] and (row["current_adapter_source_evidence"] or bool(row["current_adapter_generated_name_candidates"])) for row in rows),
        "current_facade_whitelist_evidence": None if current_facade is None else len(current_facade),
        "pushdown_catalog_direct_signatures": len(catalog),
        "pushdown_catalog_names_excluding_date_arithmetic": len(catalog_names),
        "go_registry_names": len(go_entries),
        "go_registry_classes": len({entry["go_class"] for entry in go_entries}),
        "engine_with_go_constructor_candidate": sum(row["engine_dispatch"] and bool(row["go_constructor_candidate_names"]) for row in rows),
        "engine_with_go_internal_cast_evidence": sum(row["engine_dispatch"] and bool(row["go_internal_cast_constructor_evidence"]) for row in rows),
        "engine_lazy_risk": sum(row["engine_dispatch"] and "lazy_children_eager_rpn" in row["risk_flags"] for row in rows),
    }
    return {
        "schema_version": 1,
        "scope": "Static signature/dispatch inventory, not SQL overload or execution coverage",
        "baseline_revisions": {"tidb": git(root, "rev-parse", BASE_TIDB).strip(), "tikv": git(tikv, "rev-parse", BASE_TIKV).strip()},
        "current_revisions": {"tidb": git(root, "rev-parse", "HEAD").strip(), "tikv": git(tikv, "rev-parse", "HEAD").strip()},
        "source_sha256": dict(sorted(source_hashes.items())), "counts": counts,
        "current_facade_policy": facade_policy,
        "current_adapter_evidence_sources": [ADAPTER, *sorted(extras)],
        "engine_family_counts": dict(sorted(collections.Counter(row["engine_family"] for row in rows if row["engine_dispatch"]).items())),
        "limitations": [
            "Only actual map_expr_node_to_rpn_func match arms count as engine dispatch; enum membership alone is not support.",
            "Dispatch still requires valid arity, FieldType, collation/charset, metadata and the existing builder validator.",
            "Local proto omission is informational: PbExpr.sig accepts raw i32 IDs; do not widen distributed pushdown policy.",
            "Adapter source evidence is lexical, NOT proof that a SQL name/type shape reaches or passes compile; generated runtime probes are a separate validation task.",
            "Generated-name candidates expand Rust format! templates using same-function literal prefixes/suffixes and known family spellings; they overapproximate guards and include enum-only candidates, never tested coverage.",
            "Go constructor/helper signature references are candidate relationships, NOT an overload resolver; branches, implicit casts, signedness and dynamic callbacks need review.",
            "Borrowed annotations indicate potential loaders, NOT full-tree admission. Actual PreparedExpression::supports_borrowed remains authoritative; copying fallback is separate.",
            "Baseline TiDB supports signed integer, Double and byte-string storage only, strict constants/columns, 23 signatures/13 names; no parameters/correlation/arrays/hybrid/Decimal/time/JSON/vector bridge.",
            "Baseline TiKV facade supports Int/Real/Bytes/Decimal and 52 signatures; Decimal text interchange is not faithful for hidden fractional digits/resultFrac.",
            "Current-date duration conversions, week default mode, packet limits, statement RNG/clock and warning order require context review beyond signature availability.",
            "Pushdown catalog resolves 118 direct signatures at baseline, but expression_to_pb can still refuse leaves/coercions and only restores the root inferred type; nested return metadata must be preserved by a local adapter.",
        ],
        "baseline_bridge": {
            "tidb_eval_domains": ["signed Int", "Double", "String/Bytes"],
            "tikv_facade_eval_domains": ["Int including unsigned bits", "Real", "Bytes", "Decimal text"],
            "excluded_tidb_types": ["unsigned", "Float32", "Year", "Decimal", "Datetime", "Timestamp", "Duration", "JSON", "VectorFloat32", "BIT", "ENUM", "SET", "arrays"],
            "excluded_expression_shapes": ["parameter", "deferred constant", "correlated column", "nonfinite real", "mixed numeric domains", "implicit casts"],
            "context_fields": ["flags", "sql_mode", "time_zone_name", "time_zone_offset", "div_precision_increment", "max_warning_count"],
            "context_gaps": ["max_allowed_packet", "default_week_format", "statement current time", "session RNG state"],
        },
        "dispatch_special_cases": [
            {"kind": "cast", "detail": "51 named cast arms call map_cast_func; actual child/result FieldTypes, enum/hybrid/year/float flags and constant-vs-column determine the kernel."},
            {"kind": "unsigned", "detail": "Generic integer arithmetic/comparisons choose signedness kernels from both children; some specialized minus/intdiv enum variants are not dispatched."},
            {"kind": "prevalidator", "detail": "LIKE reads children[0] and children[1], ToBinary reads children[0], before the builder invokes validator_ptr; arity must be preflighted."},
            {"kind": "builder_leaf_discrepancy", "detail": "check_expr_tree_supported omits MysqlEnum/MysqlBit, while handle_node_constant supports these typed leaves; build path is authoritative."},
            {"kind": "eager", "detail": "Builder appends every child before parent; IF/IFNULL/CASE/COALESCE/AND/OR branch side effects and warnings cannot be assumed lazy."},
        ],
        "pushdown_fallback_review": [
            {"kind": "nested_metadata", "detail": "expression_to_pb restores the root static FieldType only; recursively preserve nested Decimal precision/FSP/collation for local embedding."},
            {"kind": "time_part_type", "detail": "Catalog hour/minute/second/microsecond request Datetime but dispatched TiKV kernels consume Duration; catalog membership is not executable coverage."},
            {"kind": "composite_date_unit", "detail": "resolve_date_arithmetic uses rsplit('_').next(); local lowering must preserve full composite unit after the date_add_/date_sub_ prefix."},
            {"kind": "distributed_policy", "detail": "Reuse helpers or explicit local-only signatures without adding distributed catalog rows. RoundWithFrac is engine-dispatched even though distributed policy excludes it."},
        ],
        "pushdown_catalog_name_candidates": catalog_names,
        "go_function_constructor_candidates": go_entries,
        "signatures": rows,
    }


def self_check(inventory: dict) -> None:
    rows = inventory["signatures"]
    assert len({row["signature_id"] for row in rows}) == len(rows)
    assert inventory["counts"]["baseline_adapter_signatures"] == 23
    assert inventory["counts"]["baseline_facade_signatures"] == 52
    assert sum(row["baseline_engine_dispatch"] for row in rows) == 510
    by_name = {row["proto_name"].lower(): row for row in rows}
    assert by_name["roundwithfracreal"]["engine_dispatch"]
    assert not by_name["unix timestamp current".replace(" ", "")]["engine_dispatch"]
    assert not by_name["minusintunsignedunsigned"]["engine_dispatch"]
    assert "lazy_children_eager_rpn" in by_name["ifint"]["risk_flags"]
    assert by_name["castintasreal"]["engine_target"] == "map_cast_func(expr)?"
    assert "ceil" in by_name["ceilreal"]["go_constructor_candidate_names"]
    assert "ceiling" in by_name["ceilreal"]["go_constructor_candidate_names"]
    assert len(enum('enum ScalarFuncSig {\n A = 1; // B = 2;\n}')) == 1
    assert adapter_refs('PbExpr { sig: Some(2101) }', {"AbsInt": 2101}) == {"AbsInt"}
    assert set(generated_adapter_candidates('fn cast() { format!("Cast{}As{}", from, to) }', {"CastIntAsReal": 2, "Pi": 2100})) == {"CastIntAsReal"}


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tidb", type=Path, default=root)
    parser.add_argument("--tikv", type=Path, default=root.parent / "tikv")
    parser.add_argument("--tipb-proto", type=Path, default=root.parent / "cargo-home/git/checkouts/tipb-2fa50a6f727755a2/5f9928e/proto/expression.proto")
    parser.add_argument("--adapter-source", action="append", default=[], help="additional source path relative to TiDB, repeatable")
    parser.add_argument("--output-prefix", type=Path, default=root / "rust/docs/tikv-expression-coverage")
    parser.add_argument("--check", action="store_true", help="verify existing outputs without writing")
    parser.add_argument("--self-check", action="store_true", help="run static inventory invariants (not SQL tests)")
    args = parser.parse_args()
    inventory = make_inventory(args)
    if args.self_check:
        self_check(inventory)
    json_text = json.dumps(inventory, indent=2, ensure_ascii=False) + "\n"
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(inventory["signatures"][0]), lineterminator="\n")
    writer.writeheader()
    for row in inventory["signatures"]:
        writer.writerow({key: ";".join(value) if isinstance(value, list) else value for key, value in row.items()})
    for suffix, content in ((".json", json_text), (".csv", output.getvalue())):
        path = args.output_prefix.with_suffix(suffix)
        if args.check:
            if not path.exists() or path.read_text() != content:
                print(f"out of date: {path}", file=sys.stderr)
                return 1
        else:
            path.write_text(content)
    print(json.dumps(inventory["counts"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
