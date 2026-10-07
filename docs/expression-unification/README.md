# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-code-policy-148**, following **field-equality-policy-147**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owners `native_type_name.rs` and `native_string_type.rs` now own both default FieldType length/decimal tables and all remaining FieldTypeCode classifiers. Native projects complete named/unknown identity only.

## Verification

[Evidence](evidence/field-code-policy-checkpoint.md), [commands/counts/hashes](logs/field-code-policy-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK2, full native datatype491, new/source FieldType2 and numeric/temporal metadata session SQL1. Three new tests;5 SDK/26 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
