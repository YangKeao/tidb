# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **native-json-parse-115**, after **native-json-construct-114**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This datatype/partial-CAST foundation earns no family credit. Overall goal remains active.

## Shared JSON parser and container layout

SDK `codec/native_json_parse.rs` owns datatype text parsing, the original global surrogate rewrite and serde traversal. Crate-only array/object writers in `mysql/json/native_codec.rs` share layout writes while preserving serde/node child, key, offset and literal-access ordering.

Native `binary_json.rs` retains thin parse/from_value adapters and removes its old parser/encoding algorithms and layout constants. R117 typed construction/scalar/Datum services stay unchanged.

**Two SQL policies remain distinct:** direct JSON-column writes repair lone surrogates, while strict expression CAST of the same VARCHAR rejects them. Already stored JSON casts successfully. No expression controller was moved or silently aliased.

## Evidence

[Checkpoint](evidence/native-json-parse-checkpoint.md), [exact commands/counts/hashes](logs/native-json-parse-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

**Nine matched gates green on first attempt**, including full native datatype467, JSON44 and four SQL gates. Five new tests;6 SDK and207 native old test bodies unchanged. New SQL covers10 SELECTs and40 successful cells, with exact input escapes and nested binary layouts. The moved sanitizer is token-identical, ignoring comments/formatting.

No new C4 profile, carrier, admission, physical-allocation or performance claim. Historical failures remain retained in prior evidence.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): ordinary/typed/value JSON expression control, other CAST/typed/vector/UNION conversions, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness are unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
