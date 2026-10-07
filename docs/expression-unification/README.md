# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-string-meta-145**, following **decimal-unsigned-ref-144**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owner `native_string_type.rs` now has named Enum/Set identity and owns ENUM/SET display length plus FieldType HasCharset/BINARY classification. Native projects actual code and flags; raw unknown bytes remain Other.

## Verification

[Evidence](evidence/field-string-meta-checkpoint.md), [commands/counts/hashes](logs/field-string-meta-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final Cargo gates GREEN: SDK1, full native datatype488, new/existing FieldType2 and session SQL consumer1. Two new tests;1 SDK/23 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. The summary retains one test-import-only compile RED.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
