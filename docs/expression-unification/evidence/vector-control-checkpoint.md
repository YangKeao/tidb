# Shared VECTOR conversion and source type names

**vector-control-113 / R116**, following [SQL stringification](native-sql-string-checkpoint.md). Functional **238/245**, strict **0**, remaining **7** stay unchanged. This is partial CAST/M2 ownership, not whole-family or Go-package completion.

## Single owner

SDK `native_cast_vector.rs` owns expression-level source-name/error policy and calls datatype `codec/native_vector_convert.rs` directly. Native `cast.rs` and `tikv/cast_vector.rs` retain thin input/result projections; there is no host `convert_to` callback or target-FieldType construction. Native datatype `convert_to_vector` delegates to the same shared selector.

SDK `codec/native_type_name.rs` owns TypeStr/TypeToStr naming. Native code projects Known versus Unknown explicitly, preserving Unknown13/253 rather than decoding them as named types. The expression adapter reads the original effective `source.code()`—including array→Json—not array-element code. Naming uses empty charset, so Blob means `text` even if actual source metadata says binary.

## Preserved conversion and errors

Existing vectors clone without new finite-element or global-dimension validation. String/Bytes first undergo strict UTF8 checking, then the existing SDK vector parser. Unsupported source kinds remain distinct. Only afterward is column dimension checked: flen−1 is unspecified; all other values keep the original usize conversion with MAX fallback.

Expression source naming occurs before conversion. Only Unsupported receives `cannot cast from {source_name} to vector`; UTF8 and vector errors retain their original payload Display. No synthetic Display is invented for the shared Unsupported tag. Native datatype errors keep original kind/category mapping, and its result remains exact with no event.

Outer AST/datatype NULL guards and range/diagnostic handling remain. The SDK expression input/result use Option for the actual NULL source/result, preserving the old internal VECTOR arm's direct NULL behavior after source naming. No new C4 profile, carrier, admission gate, dimension cap or normalization is added.

## Validation

[Thirteen matched launches](../logs/vector-control-summary.txt):12 GREEN/1 retained RED; nine distinct gates are green after the correction. This includes SDK name1/type1/controller1, native bridge1/cast21/full datatype464, new SQL1 and original VECTOR1/string1 SQL. One native retry and three supplementary follow-ups are labeled separately; no compile failure, ignored/zero-match/interrupted launch. Five new tests (SDK3/native2), with255 old native test bodies byte-identical; changed SDK files contained no old tests. Initial SDK name/datatype/controller gates passed. The first native bridge run failed its new direct-NULL regression assertion (0 passed/1 failed/1722 filtered; `vector-control-native-entry.log:2830`): the new expression input incorrectly classified NULL as Other. `cast.rs:45–47` documents caller-side NULL handling, but `eval_cast` itself only guards range sentinels; the old VECTOR arm additionally inherited NULL handling from datatype `convert_to_reported:121–122`. The production SDK API now preserves actual NULL explicitly after source naming, and native projections transport it. The native NULL regression assertion remains unchanged; the failing log is retained. Before publication, separate oracle cleanup replaced lower-kernel/parser-derived expectations in the new SDK/native tests with frozen error strings from unchanged `vector_native.rs:125/180`, the existing UTF8 contract, and fixed f32 elements. No production code, old test or fixture changed in that cleanup; extra test-only follow-ups are recorded separately. A separate earlier static preflight corrected attempted Display on the shared error wrapper to formatting actual UTF8/vector payloads; that was not a Cargo failure. New SQL has eight SELECTs: four successful projections across scalar/vector modes, plus four distinct expected error probes alternating modes. Fourteen successful cells include ten vectors and four NULLs. It checks stored text/binary/vector sources, fixed/unspecified dimensions, NULL-before-dimension handling, typed dimension/UTF8/parse/unsupported errors and exact CAST metadata.

Existing raw NaN/infinity/signed-zero vectors, independent clone storage, over-global-limit existing vectors and Unknown/NewDate/array names are unit evidence, not invented SQL routes. Binary-column SQL may materialize a String carrier, so it is not claimed as a forced Datum::Bytes branch. Old tests/fixtures remain unchanged; no provider-output oracle.

## JSON investigation and remaining work

The initial JSON/VECTOR inventory found shared ordinary/typed/value helpers plus still-native typed JSON construction. Those paths cannot be replaced with a generic host expression-policy callback or an ordinary datatype conversion alias. JSON code remains unchanged and pending; this investigation is not migration credit or an approved exception.

Other CAST/JSON/typed-vector/UNION/default conversions, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Overall goal stays active.
