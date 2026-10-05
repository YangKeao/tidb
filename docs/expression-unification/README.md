# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **json-coercion-116**, after **native-json-parse-115**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This closes a coercion module, not whole JSON/CAST/M2 or new-family acceptance. Overall goal remains active.

## Shared JSON expression coercion

SDK `native_json_coercion.rs` owns all eleven `builtin_ext/json/value.rs` policies, including ordinary/typed/value CAST, document/value conversion and string/document helpers. Native retains only metadata, storage and error adapters. `Datum::as_shared_json_input` reuses the existing borrowed19-kind projection.

NULL, boolean/opaque precedence, raw Float32, Decimal precision, temporal FSP restamping, strict versus lenient parsing and error classes remain distinct. Typed row/batch preparation and static Vector refusal remain outside this slice.

## Evidence

[Checkpoint](evidence/json-coercion-checkpoint.md), [exact commands/counts/hashes](logs/json-coercion-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

**Nine matched gates green on first attempt:** SDK policy3, native datatype468, JSON45, original CAST16/actual-column1 and four SQL gates. Six new tests;242 old native test bodies unchanged. Direct native tests cover11/11 wrappers. New SQL covers10 SELECTs and38 cells: boolean flags, BIT preparation, numeric/temporal/opaque identities, document/value modes, NULL and typed IN.

Expectations are source-derived, not target-output recordings. Preflight clarified floating `.0` and rounded Decimal document expectations. No new C4 profile, carrier, physical-allocation or performance claim; historical failures remain retained.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): other CAST and typed caller preparation, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness are unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
