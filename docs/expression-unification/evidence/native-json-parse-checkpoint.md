# Shared datatype JSON parser and container layout

**native-json-parse-115 / R118**, following [typed construction](native-json-construct-checkpoint.md). Functional **238/245**, strict **0**, remaining **7** stay unchanged. This is datatype/partial-CAST foundation, not whole JSON/M2 or Go-package completion.

## Ownership

SDK `codec/native_json_parse.rs` owns `native_json_parse` and `native_json_from_value`, including the existing global surrogate rewrite and serde traversal. Native `binary_json.rs` keeps two thin entry adapters and six-way error projection. The old sanitizer, value/number/array/object encoders, header wrapper and four layout/depth constants are removed. The old test-only literal wrapper remains because an immutable original test calls it.

Crate-only `NativeJsonArrayWriter` and `NativeJsonObjectWriter` in `mysql/json/native_codec.rs` share a private layout/value-entry writer. There is no second native/container encoding algorithm or host policy callback. Node recursion and the public header writer remain unchanged. Existing node container helpers delegate to the same writers as serde traversal.

## Deliberately distinct policies

- Serde arrays allocate their directory before encoding each child, then immediately check/write that child's entry. Node encoding still encodes all children before creating the writer.
- Serde objects sort and preflight all keys before any child. Each key's offset and bytes are written before that child's encoding, followed by its value entry. Node encoding still completes children before clone/sort/key preflight.
- Serde array offsets use checked addition; node array offsets retain ordinary addition. Serde literal access retains indexing; node access retains checked-first-byte errors. Object offset arithmetic and final header checks stay in their original positions.
- Depth is checked on value entry, not on container entry. A depth100 empty container remains allowed; a scalar beyond the limit fails. Serde's own text-recursion failure remains distinct from binary encoding TooDeep.
- `trim` only tests emptiness; the original text goes to serde. Initial trailing-character errors map to TrailingValues. Other initial errors trigger the original rewrite/retry; every retry parse failure maps to InvalidText.
- The sanitizer scans globally, not by JSON string/escape state. Its existing escaped-backslash side effect is retained, not silently repaired. Valid pairs become UTF8; lone surrogates become replacement escapes. This is not an alias to expression strict parsing.

R117 typed construction, raw scalar primitives and Datum conversion remain separate and unchanged. Datum JSON raw-clone is still distinct from typed Binary validation/re-encoding. Expression JSON ordinary/typed/value controllers are unchanged.

## SQL reachability and validation

[Nine focused gates](../logs/native-json-parse-summary.txt) passed on first attempt: SDK parser2/layout3/typed2, full native datatype467, existing expression JSON44, new SQL1 and original R117 typed-construction1/typed-CAST1/aggregate1 SQL. No failed, ignored, zero-match, interrupted or retry runs. Five new tests (SDK3/native2);6 SDK and207 native old test bodies in changed files remain byte-identical. The moved sanitizer is token-identical, ignoring comments and formatter whitespace. New SQL has10 SELECTs:8 successes across two vector modes and2 strict-parser errors;40 successful cells comprise20 text/HEX and20 JSON values. Direct DML assignment of a string to a JSON column reaches the lenient datatype parser. The same stored VARCHAR evaluated as `CAST AS JSON` reaches the existing strict expression parser and rejects lone surrogates. Casting an already stored JSON value succeeds through its existing clone path. SQL source-byte HEX checks prevent testing an accidentally unescaped ordinary string instead of a surrogate escape.

New expectations use fixed source-derived bytes/strings/errors. Depth, key length, rewrite edge cases and internal writer policy remain unit domains, not invented SQL coverage. No old tests/fixtures/oracles are changed; no performance, zero-copy, physical memory or allocator claim follows.

## Remaining

Expression JSON controllers, other CAST/typed/vector/UNION conversions, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Overall goal stays active.
