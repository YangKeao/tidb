# Shared typed JSON construction and Datum conversion

**native-json-construct-114 / R117**, following [VECTOR ownership](vector-control-checkpoint.md). Functional **238/245**, strict **0**, remaining **7** are unchanged. This is datatype/partial-CAST foundation, not whole JSON/M2 or Go-package completion.

## Owners and deleted native code

SDK `codec/native_json_construct.rs` owns the13-kind typed constructor and nine scalar primitives. A noncapturing one-level view borrows actual scalar fields, child slices and BTreeMap entries. SDK builds the full logical tree before invoking the existing binary-node encoder; the native adapter does not recursively prebuild another DTO tree or run policy callbacks.

Native `binary_json.rs` removes `typed_value_to_node` and its orphan varint helper. Typed construction, opaque/time/duration constructors, literal/number helpers and the serde string scalar arm delegate to the same SDK primitives. No second scalar encoder is retained.

SDK `codec/native_mysql_json.rs` owns both Datum conversion selectors. Native `datum/convert.rs` projects actual fields and errors. Its19-kind source view is reused from `datum/stringify.rs`, whose only change is private-to-parent visibility—not a new public transport API. Fallback string rendering stays in the already-shared SQL-string owner.

## Distinct policies retained

- Datum::Json clones original type/payload without validation. Typed Binary input instead decodes and re-encodes; malformed/nonfinite embedded binary can fail.
- Typed construction completes recursively before final encoding/depth/key checks. Invalid Number or embedded Binary can therefore win before final TooDeep/KeyTooLong. The native serde container path has different ordering and remains separate.
- Number text tries i64, u64, then f64; Float64 rejects nonfinite values through the original serde Number boundary. Datum Float32 keeps its stored f64, without narrowing. Small UInt values preserve unsigned tags.
- Time stores original core bits and kind, not FSP. Duration stores raw nanoseconds and the original unchecked FSP-to-u32 cast. No temporal constructor normalizes these inputs.
- Direct String/Bytes/BinaryLiteral/Bit UTF8 failures remain InvalidUtf8. Enum/Set/Raw/sentinel fallback failures remain Unsupported(kind,"json"). Vector fallback produces a JSON string, not an array.
- Source-aware Bytes are always opaque; String uses the original exact binary-collation predicate. Only named String with positive flen resizes, including truncation. Unknown254 does not resize, while its raw opaque type byte remains254. Effective array code remains Json.

The generic text parser, lone-surrogate rewrite, serde array/object traversal, inverse type naming and all expression JSON ordinary/typed/value controllers remain unchanged. Root scalar construction uses the shared primitives; no physical allocation, zero-copy or performance equivalence is claimed.

## Validation

[Nine focused gates](../logs/native-json-construct-summary.txt) pass on first attempt: SDK constructor2/Datum1, full native datatype466, existing JSON44/cast-function16, new SQL1 and original typed-CAST1/aggregate1/string1 SQL. No failed, ignored, zero-match, interrupted or retry runs. Six new tests (SDK3/native3);216 old native test bodies in changed files remain byte-identical, and changed SDK files contained no old tests. New SQL has nine SELECTs: eight successful projections across scalar/vector modes and one unchanged invalid-JSON control error. Thirty successful cells comprise20 JSON values,2 SQL NULLs and8 typed-IN integers. Expectations use fixed tags, payload bytes and strings, not constructor/parser-produced expected values.

Stored BINARY/VARBINARY values cover opaque type/padding, unsigned and Decimal values cover numeric tags, datetime/duration values retain existing expression FSP restamping, and stored JSON/string inputs retain document-versus-value behavior. Two-item IN lists with repeated nonconstant columns avoid the single-item IN-to-EQ rewrite. Expression selectors are consumers only, not claimed as migrated.

Raw Float32 payloads, nonfinite values, malformed binary, raw temporal metadata, Number trees, depth/key ordering and Unknown source types are unit domains, not invented SQL coverage. Original tests and fixtures remain immutable.

## Remaining

JSON text/parser/container and expression control, other CAST/typed/vector/UNION/default conversions, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Overall goal stays active.
