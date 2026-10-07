# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-decimal-meta-149**, following **field-code-policy-148**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owners `native_type_name.rs`, `native_string_type.rs` and `native_eval_type.rs` now own MySQL-integer classification, CHAR/VARCHAR conversion, DECIMAL metadata validity and flen/scale delta controllers. Native projects complete identity/metadata and assigns returned values only.

## Verification

[Evidence](evidence/field-decimal-meta-checkpoint.md), [commands/counts/hashes](logs/field-decimal-meta-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK3, full native datatype492, new/source FieldType2 and numeric/temporal metadata session SQL1. Four new tests;10 SDK/27 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. Split scale/flen APIs preserve overflow partial-mutation order.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
