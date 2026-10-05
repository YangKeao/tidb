# CHAR/BINARY control and string type classification

**cast-string-111 / R114**, following [floating CAST](cast-float-checkpoint.md). Functional **238/245**, strict **0**, remaining **7** stay unchanged. This is partial CAST/M2 ownership, not whole-family or Go-package completion.

## Ownership

SDK `native_cast_string.rs` owns CHAR/BINARY source choice, YEAR-zero rendering, charset demand, encoding selection, diagnostics, truncation and padding. Native `cast.rs` delegates through `tikv/cast_string.rs`; six old local policy helpers are removed. Encoding lookup/DECODE reuses the existing SDK implementation rather than adding a second encoder.

SDK `codec/native_string_type.rs` owns string/binary classification. Native FieldType methods delegate through a named/Other projection: Unknown0/13/253 must not be mistaken for Unspecified/YEAR/VarString. The original effective `source.code()` array view is retained. Binary classification requires the original string-type set and exact collation name `binary`, not charset, flags or a canonicalized wire type.

`Datum.sql_string` remains the genuine original datatype conversion service. Its internal source selector and fixed float formatter are still M2 work, not secretly copied into the new controller or claimed as migrated.

## Ordering and boundaries

- Exact explicit CHAR charset `BINARY` takes the early byte route without YEAR-zero specialization, connection charset lookup, decoding or padding. Lower/mixed-case binary and connection-default binary retain the separate ordinary CHAR behavior.
- Ordinary CHAR obtains the connection charset only when needed, before stringification. Source binary metadata and target comparison determine decoding. Partial decoding emits3854 with uppercase hex of all original bytes, then character-length truncation may emit1406. Unknown encoding retains the original lookup fallback.
- Non-decoding ordinary CHAR uses YEAR0→`0000` for actual Int/UInt under YEAR metadata, otherwise the actual strict SQL string result. SDK maps conversion failure to the original unsupported-UTF8 category; native maps the typed result only.
- BINARY applies YEAR specialization before raw-byte or SQL-string conversion. Width processing checks length diagnostics first and only requests the packet limit if padding is needed. Over-limit padding calls the original handler—including its own repeated limit and policy reads—then returns computed NULL or propagates its error. No cached limit or fabricated warning substitutes for that call.
- No-width, equal-width and truncating BINARY avoid packet reads. CHAR never adds that packet gate. Raw String/Bytes/BinaryLiteral/Bit bytes remain distinct from strict SQL stringification of other kinds.

## Validation

[Nine focused gates](../logs/cast-string-summary.txt) pass on first attempt: SDK classification1/controller2, native bridge1/original cast21/original cast-functions16/original binary-literal wrapper1/full datatype463, new SQL1 and original floating SQL1. No failed, ignored, zero-match, interrupted or retry runs. Five new tests; all232 old native test bodies in changed files remain byte-identical (no old SDK tests in the four changed SDK Rust files). The new SQL test has ten SELECTs across scalar/vector modes and32 cells: YEAR versus integer zero, exact SQL binary CHAR behavior, Unicode versus byte truncation, invalid UTF8 byte preservation, padding, GBK partial-decode warning order and packet-limited padding versus permitted truncation/equal/no-width cases.

Native bridge tests separately cover raw charset case variants, Unknown/Unspecified/array metadata, strict ENUM failure, getter ordering and the actual default packet handler reading limits2 then7. SQL canonicalization is not presented as coverage of otherwise unavailable raw metadata branches. Existing tests and fixtures are not changed; no provider output is used as an oracle.

## Still open

Other CAST targets, typed/vector/UNION variants, default datatype conversion actuators including SQL stringification, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. No new C4 profile, carrier, admission gate or performance claim is added. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Overall goal stays active.
