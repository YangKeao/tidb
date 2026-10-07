# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **decimal-signed-target-143**, following **year-text-142**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owner `native_integer_convert.rs` now consumes the shared Decimal ref and owns half-up rounding, lazy saturation, signed target bounds, exact visible source subject and source-overflow precedence. Native only projects the shared converted carrier.

## Verification

[Evidence](evidence/decimal-signed-checkpoint.md), [commands/counts/hashes](logs/decimal-signed-target-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final Cargo gates GREEN: SDK1, full native datatype486, new/existing conversion2 and session SQL consumer1. Two new tests;5 SDK/29 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. The summary retains two attempts that failed only because the same new test assumed ties-even rather than canonical half-up rounding.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
