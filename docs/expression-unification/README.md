# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **decimal-coerce-129**, following **float-composition-128**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK `native_cast_decimal.rs` composes actual numeric input and shared default conversion internally; native supplies data/warning effects. New `native_coerce_numeric.rs` owns integer classification, mixed-signed comparison, bits/Decimal/f64 projection and nullable truth. Native aliases the integer carrier and maps results/errors, preserving actual ordinals, literal values and existing boolean semantics.

## Verification

[Evidence](evidence/decimal-coerce-checkpoint.md), [commands/counts/hashes](logs/decimal-coerce-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Seven matched gates GREEN without failure/retry: SDK3/2, native new1/old Decimal1/coerce1 and existing SQL Decimal1/comparison1. Four new tests;2 SDK/1 native old touched-file test bodies unchanged. Existing coerce/SQL tests unchanged; no new SQL fixture or probe credit.

No performance, physical-memory, SQL lowering or whole CAST completion claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other typed/write numeric selectors and SQL lowering paths, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
