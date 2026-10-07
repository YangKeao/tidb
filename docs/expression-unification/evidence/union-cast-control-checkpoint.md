# UNION CAST controller migration

**union-cast-control-172 / R178**, after [RAND functional migration](rand-kernel-checkpoint.md). Functional239/245, strict0, remaining6 unchanged; this is a partial CAST deletion step.

`tidb_query_expr::native_cast` owns source-specific UNION CAST negative clamps, zero-vs-convert choices and textual-negative route; the existing SDK integer controller handles `UnsignedInUnion`. Native expression code keeps function-name/Datum projection, concrete Decimal construction and merged DECIMAL target fitting. Seven duplicate native policy branches and the typed-row unsigned special case are removed.

## Validation

Five final Cargo gates GREEN: SDK controller1, all-seven native names1, existing real-to-decimal2, typed-row unsigned1 and existing set-operation SQL1. [Exact commands/counts/hashes and two retained new-test REDs](../logs/union-cast-control-summary.txt). Two new tests;1 TiKV/47 TiDB old touched-file test bodies unchanged. No new Rust/SQL file or fixture/probe credit. Whole CAST remains uncredited because direct legacy `SimpleSig::Cast*` bodies remain native. Baseline unsupported arrays and eight unimplemented Duration/Datetime signatures remain unchanged; no admission is invented. R100 historical failures remain explicit. Full unistore/workspace/lint/release/exhaustive/performance/physical-memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified.
