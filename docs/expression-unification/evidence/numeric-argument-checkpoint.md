# Shared numeric argument preparation

**numeric-argument-130 / R135**, after [Decimal/coercion composition](decimal-coerce-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

New `native_numeric_argument.rs` owns three existing policies: String-to-Decimal numeric argument preparation, bytes-to-Real and JSON-to-Real. Native `scalar_function.rs`/`ops/real_coerce.rs` supply actual bytes/JSON storage, generic context reads/effects and fixed typed error projection. Parser, JSON conversion/Display/string access and warning formatting reuse canonical datatype owners.

Preserved distinctions: lossy UTF-8 before Unicode trim; scalar named truncation versus vector raw1265; lazy truncate-level reads; failure veto before later effects; byte-real empty-input behavior; JSON strings use DOUBLE/unquoted bounded subjects while other JSON uses FLOAT/document Display. Target metadata/final fitting, JSON-to-Int, Real-to-Decimal and remaining typed/write controllers stay separate.

## Validation

Six final gates GREEN: SDK3, new native1/old division1/arithmetic1, existing SQL2. Eight launches retain one SDK new-test NUL-assumption failure and one native new-test import compile failure; source-derived test-only corrections, no production/old-test changes. [Exact commands/counts/hashes and incident proof](../logs/numeric-argument-summary.txt). Four new tests;32 old native touched-file test bodies unchanged. New consumer exercises the actual private vector-mode via test-only module wiring, without widening production API. Existing SQL file unchanged; no new SQL fixture/probe credit. Full suites/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Broader M2/root/liveDAG/final acceptance remain. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
