# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **real-decimal-132**, following **numeric-shape-131**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK Real-to-Decimal preparation owns exact-integer selection, parsing/error policy, lazy subject demand and warning-before-projection finish. Native provides optional expression text only when demanded; its renderer remains explicitly native and unchanged. SDK owns fallback formatting. Unscaled Int/UInt construction is shared too.

## Verification

[Evidence](evidence/real-decimal-checkpoint.md), [commands/counts/hashes](logs/real-decimal-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six matched gates GREEN without failure/retry: SDK7, native consumers3/old Real1/division1 and existing SQL2. Three new tests;5 SDK/34 native old touched-file test bodies unchanged. Tests cover lazy ParamMarker subject reads, strict errors, fallback/veto and final metadata/context order. No new Rust files or SQL fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): final fitting, expression rendering, other typed/write selectors and SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
