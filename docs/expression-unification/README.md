# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **native-json-construct-114**, after **vector-control-113**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This datatype/partial-CAST foundation earns no family credit. Overall goal remains active.

## Shared JSON datatype construction

SDK `codec/native_json_construct.rs` owns typed JSON construction and nine scalar primitives. `codec/native_mysql_json.rs` owns ordinary/source-aware Datum conversion. Native code keeps borrowed field projections and error adapters, deleting the old recursive constructor, varint body and duplicated scalar/selection logic.

Datum JSON raw-clone remains distinct from typed Binary validation/re-encoding. Stored Float32 f64, raw temporal fields, source metadata/resize behavior and error categories/order are preserved. Generic text parsing, surrogate rewriting, serde container traversal and expression JSON controllers remain separate.

## Evidence

[Checkpoint](evidence/native-json-construct-checkpoint.md), [exact commands/counts/hashes](logs/native-json-construct-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

**Nine matched gates green on first attempt**, including full native datatype466, JSON44, original cast16 and four SQL gates. Six new tests;216 old native test bodies unchanged. New SQL covers nine SELECTs and30 successful cells with fixed JSON tags, payloads and typed-IN results. No parser/constructor-produced expected values.

No new C4 profile, carrier, admission, physical-allocation or performance claim. Historical failures remain retained in prior evidence.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): JSON text/parser/container and ordinary/typed/value expression control, other CAST/typed/vector/UNION conversions, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness are unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
