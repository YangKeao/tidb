# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **numeric-argument-130**, following **decimal-coerce-129**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK `native_numeric_argument.rs` owns String-to-Decimal preparation and byte/JSON-to-Real argument conversion. Canonical parsers/JSON/warning formatting are reused; native supplies actual data, generic context effects and fixed error projection. Scalar/vector truncation, lazy level reads, JSON diagnostic labels, empty/NUL behavior and veto order remain distinct.

## Verification

[Evidence](evidence/numeric-argument-checkpoint.md), [commands/counts/hashes](logs/numeric-argument-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six final gates GREEN: SDK3, new native1/old division1/arithmetic1 and existing SQL2. Eight launches retain a new SDK NUL-expectation failure and new native import compile failure. Source-derived test-only corrections; production/old tests unchanged. Four new tests;32 old native touched-file test bodies unchanged. Actual private vector mode tested through test-only wiring; no new SQL fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): target metadata/final fitting, JSON-to-Int, Real-to-Decimal, other typed/write selectors and SQL lowering paths; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
