# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **numeric-route-133**, following **real-decimal-132**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK numeric head owns early preservation, unsigned/Float32 normalization, numeric target validation, String/hybrid admission and conversion routing. Native applies selected storage and executes existing conversions from actual values/metadata. Static-type demand, error precedence and context effects retain their original order. No new C4 gate/profile.

## Verification

[Evidence](evidence/numeric-route-checkpoint.md), [commands/counts/hashes](logs/numeric-route-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six matched gates GREEN without failure/retry: SDK9, native consumers4/old Real1/arithmetic1 and existing SQL2. Three new tests;7 SDK/35 native old touched-file test bodies unchanged. Existing renderer/final conversion functions are byte-identical. No new Rust files or SQL fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): final fitting, expression rendering, other typed/write selectors and SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
