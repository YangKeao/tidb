# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-source-render-157**, following **field-compact-render-156**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit; prior evaluator CAST ledger unchanged.

Existing SDK owner `native_type_name.rs` now owns information-schema, type-description and source-string flags/charset/collation suffix policy. Native code projects compact text, complete identity and metadata only; local parts/suffix bodies are deleted.

## Verification

[Evidence](evidence/field-source-render-checkpoint.md), [commands/counts/hashes](logs/field-source-render-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final Cargo gates GREEN: SDK1, full native datatype502, native-new1, source runtime1 and SHOW COLUMNS session SQL1. Two new tests;7 SDK/30 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. One retained RED was a new-test oracle omitting the width forced by ZEROFILL; only that new expectation changed, then full native passed 502/502.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): full byte restore and expression rendering; typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
