# RAND TiKV kernel migration

**rand-kernel-171 / R177**, after [numeric diagnostic argument policy](expr-diagnostic-argument-checkpoint.md). Functional **239/245 (97.55%)**, strict0 and remaining6. RAND earns one new functional family credit after six focused GREEN gates; prior238 family objects remain byte-identical.

`tidb_query_crypto::mysql_rng` now owns exact MySQL RAND seed-state derivation as well as the existing recurrence; `tidb_query_expr::impl_math` owns Datum seed source routing. Native `MysqlRng` keeps Mutex-backed state and host time entropy; expression code keeps session RNG, per-occurrence constant-seed identity/lifetime and concrete Datum conversions. The duplicated native seed derivation and source selector are removed.

## Validation

Six Cargo gates GREEN: crypto seed1, query-expr route1, native RNG1, native route/conversion1, typed-row identity1 and session SQL sequence/order1. [Exact commands/counts/hashes](../logs/rand-kernel-summary.txt). Four new tests;57 TiKV/7 TiDB old touched-file test bodies unchanged. No new Rust/SQL file or fixture/probe credit. No new SQL fixture. No PB signature exists in the frozen RAND domain; no admission is invented. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. R100 historical failures remain explicit.
