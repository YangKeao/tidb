# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **decimal-unsigned-ref-144**, following **decimal-signed-target-143**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owner `native_integer_convert.rs` now consumes the shared Decimal ref for DECIMAL-to-UNSIGNED, renders canonical visible metadata once and reuses its exact text algorithm. Native retains its public facade and typed-error mapping only.

## Verification

[Evidence](evidence/decimal-unsigned-ref-checkpoint.md), [commands/counts/hashes](logs/decimal-unsigned-ref-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype487, new/existing conversion2 and session SQL consumer1. Two new tests;6 SDK/25 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
