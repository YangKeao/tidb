# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **numeric-shape-131**, following **numeric-argument-130**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owners now share Decimal target shape, FieldType width/scale limits and JSON Display-to-integer composition. Native projects actual metadata/JSON and context effects. Canonical limits and JSON/integer controllers are reused; array/unknown identity, negative metadata, UTC and warning veto order remain intact.

## Verification

[Evidence](evidence/numeric-shape-checkpoint.md), [commands/counts/hashes](logs/numeric-shape-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Seven matched gates GREEN without failure/retry: SDK2/5, full native datatype477, numeric consumers2/old Real1 and existing SQL2. Four new tests;102 SDK/56 native old touched-file test bodies unchanged. No new Rust files or SQL fixture/probe credit. Existing fraction constant only gains crate-private visibility for reuse.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): final fitting, Real-to-Decimal, other typed/write selectors and SQL lowering paths; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
