# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **float-composition-128**, following **scalar-datum-127**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK `native_cast_float.rs` composes actual numeric input, JSON Display and shared datatype conversion internally. Native ordinary/value-only bridges no longer supply business callbacks; only actual data, truncate effects and error projection remain. Generic SDK APIs stay compatible. Ordinary/value-only JSON and UTF-8 distinctions, narrowing and error order remain intact.

## Verification

[Evidence](evidence/float-composition-checkpoint.md), [commands/counts/hashes](logs/float-composition-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five matched gates GREEN without failure/retry: SDK4, native new1/old float1/scalar1 and existing SQL1. Three new tests;2 SDK/1 native old touched-file test bodies unchanged. Existing comprehensive float SQL regression passed unchanged; no new SQL fixture/probe credit.

No performance, physical-memory, SQL lowering or whole CAST completion claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other typed/write numeric selectors and SQL lowering paths, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
