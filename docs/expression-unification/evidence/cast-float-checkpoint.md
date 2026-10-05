# Floating CAST and native byte-float parsing

**cast-float-110 / R113**, following [integer CAST](cast-integer-checkpoint.md). Functional **238/245**, strict **0**, remaining **7** stay unchanged. This is partial CAST/M2 ownership, not whole-family or Go-package completion.

## Ownership

SDK `native_cast_float.rs` owns ordinary DOUBLE/FLOAT source selection, parsing demand, diagnostics, narrowing and typed overflow. Its separate value-only entry owns the original `to_f64_for_cast` selector. Native `cast.rs` keeps three thin calls through `tikv/cast_float.rs`; the old expression policies and local Decimal-prefix wrapper are removed. Existing external consumers retain their public helper.

SDK `codec/native_float_parse.rs` owns the native byte prefix, conversion, reported diagnostic ordering and warning-subject helper. Native `convert.rs` keeps its public carriers and actual context effects. The existing TiKV wire character scanner is unchanged: the native NUL and bare-exponent rules are deliberately not aliased to it.

Decimal-to-f64 uses the original visible formatter plus Rust parse through the borrowed SDK Ref. Native delegates; hidden coefficient arithmetic does not replace visible-scale semantics or original raw-input panic behavior. Large visible values still may become infinity rather than the string parser's finite clamp.

## Distinct contracts

- Ordinary String/Bytes uses lossy UTF8 and the byte parser. All JSON requests original Display; quoted JSON strings are not unquoted, and JSON uses FLOAT rather than DOUBLE in diagnostics.
- Ordinary CAST observes only the final truncation event and invokes `handle_truncate` once. Datatype reported parsing separately preserves prefix-before-parse-before-range diagnostics with the original full trimmed subject. The CAST subject helper instead applies trim, first NUL and UTF8-safe byte cap.
- FLOAT narrows only actual Real/Float32, converting infinity after narrowing to positive zero and retaining the original NaN/negative-zero behavior. Other sources first finish conversion and truncation handling, then check range; an in-range result remains the original f64, not a narrowed value.
- Typed overflow text is computed by SDK. Native only maps `Child(E)` unchanged or copies the computed `ConstantFloatCastOverflow` payload.
- Value-only text retains strict UTF8 plus the shared Decimal-prefix policy. Its JSON/Float32/Other requests the original default datatype conversion; SDK discards actual events and folds actual datatype errors. It is not an alias of ordinary CAST.
- Value-only Null/MinNotNull/MaxValue retain the original guarded panic. Caller NULL/range/vector guards remain unchanged; no caller timezone/type/date flags, new C4 profiles, carriers or admission gates are introduced.

## Validation

[Exact commands/counts/hashes](../logs/cast-float-summary.txt): ten matched test launches, nine green and one retained failed new-SQL attempt. SDK parser1/Decimal1/controller2, native bridge1/original cast21/original cast-function16/full datatype463, original integer SQL1 and corrected new SQL1 pass. There were no compilation failures, zero-match runs or interruptions. Six new tests;3 SDK and244 native old test bodies in changed files remain byte-identical. The preexisting vectorized DECIMAL UNION ignore remains unmodeled and was not selected.

The initial new SQL test failed on its nested FLOAT-string expectation; the failure log is retained. Static source investigation showed that [ScalarFunction](../../../rust/crates/tidb-expr/src/scalar_function.rs) applies each inner function's typed finish before the outer DOUBLE runs: `same_eval_family` rejects Real under Float, and [datatype production](../../../rust/crates/tidb-datatype/src/datum_convert.rs) narrows to Float32. The proposed outer-DOUBLE escape was therefore invalid. Only this newly added test is corrected from that unchanged source contract, with an additional direct DOUBLE comparison; production code and all old tests remain unchanged. No actual-output recording or baseline repair is involved.

The corrected SQL test has ten SELECTs across scalar/vector modes: eight successful projections with36 cells and two expected typed-error probes. Nested FLOAT checks the existing typed boundary, while direct DOUBLE retains0.1; controller-only non-narrowing is demonstrated by the bridge/SDK tests, not claimed from SQL. Stored prefix, bare-e, NUL, invalid UTF8, JSON and overflow inputs pin source-derived values and exact warning order. The expected error retains the prior1292 warning without inventing a second1690 warning row.

Native bridge coverage separately checks context-error precedence, actual Float32 conversion, lossy/strict differences, JSON conversion channels, NaN/negative zero and old pure zero-slot behavior. The latter earns no new admission credit. Existing tests and fixture/oracle bodies are not rewritten or recorded from provider output.

## Still open

Other CAST targets, typed/vector/UNION variants, native default datatype conversion actuators and broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Overall goal stays active.
