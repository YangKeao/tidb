# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **numeric-text-136**, following **float-target-135**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Remaining `numeric_helper.rs` algorithms now delegate to existing SDK integer/float/Decimal owners: best-effort parsing, const precision/display length and truncated fixed float text. Fixed layout is factored from the existing formatter, not copied. Distinct parser policies, digit generators, cutovers, error names and numeric edge behavior are retained.

## Verification

[Evidence](evidence/numeric-text-checkpoint.md), [commands/counts/hashes](logs/numeric-text-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six Cargo gates GREEN: SDK3/existing formatter1, full native datatype479, numeric consumers5 and existing SQL2. Initial formatting preflight found a missing parser brace, fixed before Cargo. Four new tests;104 SDK/7 native old touched-file test bodies unchanged. No new Rust files or SQL fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): source conversion/event merging, other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
