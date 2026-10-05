# Shared JSON source and target policies

**json-source-117 / R120**, following [value-boundary coercion](json-coercion-checkpoint.md). Functional238/245, strict0 and remaining7 stay unchanged. No whole JSON/CAST/M2 or Go-package completion is claimed.

## Ownership

- SDK `codec/native_eval_type.rs` owns the nine source EvalType variants, discriminants, constants, methods, display/error traits and checked-byte conversion. Native `eval_type.rs` is aliases only; `FieldType::eval_type` delegates effective code/raw flags to the shared classifier. Unknown codes remain String even when their byte resembles a known numeric/JSON/Vector code. Array effective code is selected before classification. This is not wire EvalType.
- `native_json_coercion.rs` owns source admission and post-typed-evaluation preparation. It reuses the canonical classifier rather than copying an ETString predicate. Native wrappers reuse the existing metadata projection, error mapping and result carrier.
- `native_mysql_json.rs` owns datatype JSON-target conversion. The native method is a borrowed-input/result/error adapter. Its outer SQL NULL/target NULL guards and diagnostic dispatch are unchanged; the existing parser error mapper merely becomes crate-visible.

## Ordering and domain boundaries

Row source admission precedes child evaluation, including a would-be NULL Vector row. Missing source remains distinct from Vector refusal. Batch support observes SDK admission without allocating a native error; target/arity shape routing and generic recursion stay in the engine. The entire typed source batch still completes before per-value JSON conversion.

The prepared helper checks missing source even for NULL. Actual Int plus UNSIGNED or effective KnownYear becomes UInt; actual Enum/Set names become Bytes only for the shared source ETString category. Unknown13 is not Year, Unknown247 is not known Enum, and array Year has effective JSON code. Target PARSE_TO_JSON chooses document mode; absent target defaults to value mode. Prepared conversion does **not** repeat Vector admission: the PB caller has its own unchanged arity/NULL behavior.

Datatype target conversion is separate: String/Bytes/Enum/Set validate UTF-8 and use the lenient datatype parser; BinaryLiteral reports the original Comparison error. Bit and Raw retain ordinary JSON-string fallback, with distinct UTF-8 error categories. Existing JSON raw-clones; Float32 keeps raw f64, and Time/Duration do not adopt expression FSP6 restamping. No source flags are smuggled into this datatype policy.

## Validation

[Fourteen retained launches](../logs/json-source-summary.txt):12 matched GREEN,1 matched RED and1 zero-match. Final required filters are green: SDK classifier1/target2/source5, full native datatype470, native source1/JSON45/PB2, and four SQL1 gates. Eight new tests (SDK4/native4);4 old SDK and286 old native test bodies remain byte-identical.

The initial classifier gate passed before a supplemental Debug assertion exposed a migration-induced name change: `NativeInvalidEvalType(9)` instead of original `InvalidEvalType(9)`. Its real RED is retained. Keeping the original struct name with an SDK alias fixed it; the same assertion then passed. The first PB filter incorrectly named `tests` instead of `json_path_worker_tests`, selecting zero tests; that receipt is not passing coverage. The corrected filter passed2 existing tests. There were no compile failures or changed old expectations.

New SQL has8 SELECTs:6 successful across two vector modes, plus2 strict document errors in mode1. Its24 successful cells contain12 JSON and12 typed-IN integers. Stored YEAR/default-unsigned and UIntMAX retain uint tags; ENUM/SET names `1`/`true` become document values, while names `bad`/`1,true` work in value-mode IN and fail explicit document CAST with3140/22032. Tests use fixed literals/bytes and source-derived contracts, not output recorded from a provider or target parser. Original test bodies remain unchanged.

The new native source test distinguishes direct metadata/prepared inputs from real Column/Chunk execution: Vector and missing-source admission, invalid-column error precedence, and ordinary row/native-batch conversion. These do not claim PB selection, resource-slot accounting, or projection-seal coverage. Datatype tests use real conversion/reporting entry points with no event/unmapped diagnostic; they do not claim a warning-sink integration test.

## Remaining

Other CAST domains, generic typed child evaluation, broader metadata/M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. Full suites, lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are unverified; historical R100 expression4/unistore1 failures remain. Overall goal stays active.
