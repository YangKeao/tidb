# Legacy DECIMAL CAST migration

**legacy-cast-decimal-173 / R179**, after [UNION CAST control](union-cast-control-checkpoint.md). Functional239/245, strict0, remaining6 unchanged; this is a partial CAST deletion step.

`tidb_query_expr::native_cast_decimal` owns the legacy i128 signed/unsigned selection and existing numeric-input DECIMAL conversion while deliberately folding legacy conversion events. TiDB's bridge projects real Datums. Unistore keeps child evaluation, NULL folding and concrete result projection; six local `SimpleSig::*AsDecimal` conversion bodies are deleted.

## Validation

Four final Cargo gates GREEN: SDK1, native bridge1, all-six direct Unistore1 and existing comparison composition1. [Exact commands/counts/hashes and retained new-test compile RED](../logs/legacy-cast-decimal-summary.txt). Two new tests;3 TiKV/116 TiDB old touched-file test bodies unchanged. No new Rust/SQL file or fixture/probe credit. Whole CAST remains uncredited because other direct legacy `SimpleSig::Cast*` target bodies remain native. Baseline unsupported signatures/admissions remain unchanged. R100 historical expression/unistore failures remain explicit; broad suites are not claimed green.
