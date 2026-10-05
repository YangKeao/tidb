# Shared datatype SQL stringification

**native-sql-string-112 / R115**, following [CHAR/BINARY](cast-string-checkpoint.md). Functional **238/245**, strict **0**, remaining **7** stay unchanged. This closes a datatype actuator implementation, not a whole CAST family, M2 or Go package.

## Single owner

SDK `codec/native_sql_string.rs` owns `sql_bytes/sql_string` source choice over19 actual variants and the fixed/scientific float primitives. Native `datum/stringify.rs` keeps a private borrowed projection, original public methods and mechanical error mapping. The old selector, UTF8 helper and float formatter cluster are removed.

No host formatter callback or generic stringifier fallback remains. Decimal borrows the existing parse Ref; Time supplies raw/kind/fsp directly, Duration raw nanoseconds/fsp, JSON original type/value slices and Vector its existing SDK value. All use already-shared formatting rather than normalized constructors or a second renderer. The R114 CAST callback now reaches this shared implementation through the unchanged datatype API.

## Preserved boundaries

- `sql_bytes` preserves arbitrary String/Bytes/Enum/Set/BinaryLiteral/Bit octets. Raw alone first performs the original strict validation, including its temporary String, before byte cloning.
- `sql_string` performs the second strict UTF8 projection. Original `Utf8Error` offsets/lengths and both range-sentinel identities are preserved. Null produces empty bytes here, not the SQL literal `NULL`.
- Float32's f64 payload narrows inside the SDK primitive. Fixed finite formatting, NaN/+Inf/-Inf and signed zero remain distinct from scientific exponent normalization. Only the two scientific primitive call sites change in the native value-expression selector; general restore/label/row/diagnostic policies remain untouched.
- Decimal retains visible versus storage scale. Date ignores irrelevant clock/fsp; other raw temporal FSP and JSON Display-error panic domains remain unchanged. Malformed JSON containers retain original display behavior. There is no timezone lookup or source normalization.

## Validation

[Nine focused gates](../logs/native-sql-string-summary.txt) pass on first attempt: SDK datatype2/existing controller2, full native datatype464, existing bridge1/cast21/cast-function16, new SQL1 and prior CHAR/BINARY1/floating SQL1. No failed, ignored, zero-match, interrupted or retry runs. Four new tests: two SDK, one native projection test and one SQL test. All193 old native test bodies in changed files remain byte-identical; the two changed SDK Rust files contain no old tests. SQL uses eight SELECTs/28 cells across scalar/vector modes with stored fixed-format Double/Float, visible Decimal scale, Date/Datetime/Duration FSP, JSON quotes/separators/null and exact text/binary payloads.

SQL NULL is an outer CAST guard, not claimed as coverage of the datatype empty-byte Null branch. Raw binary SQL is a byte-bypass regression, not proof of strict conversion accepting invalid UTF8. Sentinel, noncanonical metadata, nonfinite JSON panic and all19 input kinds are checked at unit level. Old test bodies and fixtures remain unchanged; no provider-output recording.

## Still open

Other CAST targets and typed/vector/UNION variants, other datatype conversions, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. No new C4 profile, carrier or admission gate. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are unverified; historical R100 expression4/unistore1 failures remain unrepaired. Overall goal stays active.
