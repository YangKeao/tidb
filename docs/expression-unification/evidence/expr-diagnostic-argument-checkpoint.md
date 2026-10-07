# Shared numeric diagnostic argument policy

**expr-diagnostic-argument-170 / R176**, after [diagnostic text composition](expr-diagnostic-render-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no complete expression renderer/evaluator, family, whole CAST/M2/package credit.

Existing SDK owner `native_eval_type.rs` now owns column-reference naming, binary-literal display-domain selection and the INTDIV DECIMAL diagnostic argument-wrap predicate. Native `scalar_function.rs` keeps AST/Datum/FieldType projection, actual Go-shortest float formatting, recursion and typed errors.

## Validation

Five targeted Cargo gates GREEN: SDK1, native-new1, existing arithmetic-overflow1, existing numeric-domain1 and session SQL1. [Exact commands/counts/hashes](../logs/expr-diagnostic-argument-summary.txt). Two new tests;15 SDK/33 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Full `tidb-expr` is not used as a green gate because historical R100 has four known unrepaired failures; this remains explicit. No new SQL fixture. Remaining expression rendering/evaluator work, typed/write SQL lowering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified.
