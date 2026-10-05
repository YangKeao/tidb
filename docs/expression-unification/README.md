# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **vector-control-113**, after **native-sql-string-112**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This partial CAST/M2 step earns no family credit. Overall goal remains active.

## VECTOR single owner

SDK `native_cast_vector.rs` owns expression control over shared datatype `codec/native_vector_convert.rs`. Native CAST and datatype adapters delete their duplicate selectors. `codec/native_type_name.rs` owns TypeStr/TypeToStr; Known/Unknown identity and effective array code remain distinct.

Existing vectors only clone before column-dimension checking; text keeps strict UTF8 and the existing parser. No new finite/global-dimension validation. Source names use empty charset before conversion, with original Unsupported versus UTF8/vector errors. Direct NULL behavior is preserved in the SDK, without a host policy callback.

## Evidence and corrected regression

[Checkpoint](evidence/vector-control-checkpoint.md), [exact commands/counts/hashes](logs/vector-control-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

**Nine distinct gates are green**: SDK name/type/controller, native bridge/CAST/full datatype464, new SQL and original VECTOR/string SQL. Thirteen launches comprise12 GREEN and1 retained RED. The new NULL regression test caught a real omitted branch; production was corrected without weakening that assertion. One retry and three supplementary runs are separately labeled. Final new-test expectations use source-derived constants, not lower-parser/kernel outputs.

Five new tests;255 old native test bodies unchanged. New SQL has eight SELECTs: four successful projections across scalar/vector modes and four typed errors, with14 successful cells.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): JSON ordinary/typed/value helpers and typed construction were inventoried but not migrated; other CAST, typed/vector/UNION, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain.

No new C4 profile, carrier, admission or performance claim. Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness are unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
