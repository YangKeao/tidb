# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-json-tag-167**, following **datum-year-route-166**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole JSON shape/type-dedup, complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_field_value.rs` now owns Go-compatible classification for all nine FieldType JSON tags and unknown keys. Native JSON keeps serde decoding, duplicate/null/map-order behavior, Go slice storage and concrete FieldType projection; its local EqualFold matcher and repeated branch chain are deleted.

## Verification

[Evidence](evidence/field-json-tag-checkpoint.md), [commands/counts/hashes](logs/field-json-tag-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype512, native-new1, existing slice-JSON1 and enum-metadata session SQL1. Two new tests;1 SDK/8 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): full FieldType JSON shape policy, remaining typed/write SQL lowering, expression text rendering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
