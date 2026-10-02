# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **clock-four-60**. Previous: **json-unquote-59**.

## Progress

Frozen denominator245; target221. Functional delegation plus native algorithm deletion: **196/245 (80%)**. Strict final-audited acceptance: **0**; goal remains active. CURTIME/CURRENT_TIME, UTC_TIME, UTC_DATE and UTC_TIMESTAMP add four whole families.49 eligible remain;25 more functional migrations needed. NOW/CURDATE/SYSDATE formatter sharing earns no family credit.

## What changed

- TiKV `native_clock.rs` owns the six relocated epoch-formatting bodies and six fixed family entrypoints. It reuses the wide civil-calendar helper, not chrono or bitpacked Time normalization. Native keeps two aliases for unmigrated consumers.
- Seven unit workers consume actual clock bytes plus a separate precision value where needed. Guarded preparation retains arity/FSP errors and captures the original clock once; the worker performs offset arithmetic, microsecond truncation, half-up carry and formatting.
- Zero-argument time truncates; explicit time precision truncates submicroseconds before rounding; UTC_TIMESTAMP rounds original nanoseconds. Genuine UTC_TIME NULL value-entry uses its own worker without reading the clock. No host formatted answer, new kind/carrier/driver/binding or PB/legacy admission.

## Validation, including the initial failure

[Review evidence](evidence/clock-four-checkpoint.md), [exact commands/hashes](logs/clock-four-summary.txt), [manifest](checkpoint.json), [sole cumulative ledger](migration-progress.json). Six exclusive writers;13 Rust files,1 new source,11 added tests all passing in the final state. All13 formatter checks pass; no dependency/manifest/lock changes.

Ten actual Cargo runs: **7 green,1 initial new SQL RED,2 old full-suite RED**. Green: CPP clocks4/local307+1ignored; native clocks14/original rounding1; corrected SQL2/original typed-clock1/original SYSDATE consumer1. Full expression **1526/4old/94ignored** and unistore **206/1old/13ignored** retain complete prior failure sections after only thread-ID normalization.

The initial SQL tests incorrectly assumed `UTC_TIME(NULL)` was parser-admitted. It is not: existing precision grammar accepts integer literals only. Only the two new tests were corrected, preserving all other fixed outputs and old tests; parser rejection1064 is now explicit. Final SQL has11 direct zero-slot probes covering six SQL value profiles; the seventh NULL profile is verified through native value-entry/SDK/C4. No parser expansion or production repair. Initial0/2 failure receipts remain published. The prior extra JSON_KEYS aggregate type-assertion failure remains unresolved and was not rerun.

StrictM6, broader default-NoColumns request-root integration, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive context/domain/wire/differential/TiFlash/FIPS and prior parser/GB/vector/extreme Decimal exceptions remain deferred. No whole Go-package transcreation, PR readiness or overall completion claim.

`checkpoint.json` pins the paired TiKV commit and common Plan SHA256. Both tracked Plans equal the root Plan. Publication is TiKV first, then TiDB with exact paired SHA/Plan; no force push or automatic PR. The pre-existing untracked client differential `BUILD.bazel` stays excluded. Next read-only candidates: MERGE/PATCH, ANY_VALUE/NAME_CONST and JSON_SUM_CRC32, with their full domain/SDK/admission constraints; no advance credit.
