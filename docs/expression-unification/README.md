# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-name-storage-154**, following **field-value-policy-153**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owner `native_type_name.rs` now owns type-name parsing aliases and fixed/DECIMAL storage-width policy. Native names/memory code projects complete identity and metadata only; local matches/table are deleted.

## Verification

[Evidence](evidence/field-name-storage-checkpoint.md), [commands/counts/hashes](logs/field-name-storage-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final Cargo gates GREEN: SDK1, full native datatype498, native shared-policy11, scoped source parser9 and numeric/temporal session SQL1. Three new tests;4 SDK/8 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. A broad `parser_` launch retained RED with unrelated charset registry 26/4; exact `field_type_source::parser_` is 9/9 GREEN.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
